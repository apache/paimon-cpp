/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/defs.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::test {
namespace {

const char kDatabaseName[] = "append_types_compatibility";

std::vector<std::string> WriterPrefixes() {
    return {"python", "rust", "java"};
}

Result<std::shared_ptr<arrow::ChunkedArray>> ReadTable(
    const std::string& table_name, const std::vector<std::string>& field_names,
    const std::map<std::string, std::string>& options = {}) {
    std::string table_path = GetDataDir() + "/parquet/" + kDatabaseName + ".db/" + table_name;

    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.SetOptions(options);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                           scan_context_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                           TableScan::Create(std::move(scan_context)));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, table_scan->CreatePlan());

    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetOptions(options).SetReadFieldNames(field_names);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                           read_context_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           TableRead::Create(std::move(read_context)));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> batch_reader,
                           table_read->CreateReader(plan->Splits()));
    return ReadResultCollector::CollectResult(std::move(batch_reader));
}

std::shared_ptr<arrow::StructArray> GetOnlyStructChunk(
    const std::shared_ptr<arrow::ChunkedArray>& result) {
    if (!result || result->num_chunks() != 1) {
        return nullptr;
    }
    return std::dynamic_pointer_cast<arrow::StructArray>(result->chunk(0));
}

void AssertArrayEqualsJson(const std::shared_ptr<arrow::Array>& actual, const std::string& context,
                           const std::string& expected_json) {
    ASSERT_TRUE(actual) << context;
    arrow::Result<std::shared_ptr<arrow::Array>> expected =
        arrow::ipc::internal::json::ArrayFromJSON(actual->type(), expected_json);
    ASSERT_TRUE(expected.ok()) << context << ": " << expected.status().ToString();
    ASSERT_TRUE(actual->Equals(*expected)) << context << "\nexpected: " << (*expected)->ToString()
                                           << "\nactual: " << actual->ToString();
}

void AssertFieldEqualsJson(const std::shared_ptr<arrow::StructArray>& rows,
                           const std::string& field_name,
                           const std::shared_ptr<arrow::DataType>& expected_type,
                           const std::string& expected_json) {
    std::shared_ptr<arrow::Array> actual = rows->GetFieldByName(field_name);
    ASSERT_TRUE(actual) << field_name;
    ASSERT_TRUE(actual->type()->Equals(expected_type))
        << field_name << " expected type: " << expected_type->ToString()
        << " actual type: " << actual->type()->ToString();
    AssertArrayEqualsJson(actual, field_name, expected_json);
}

Result<std::string> VariantJsonAt(const std::shared_ptr<arrow::StructArray>& variants,
                                  int64_t index) {
    if (variants->IsNull(index)) {
        return std::string("null");
    }
    std::shared_ptr<arrow::BinaryArray> values =
        std::dynamic_pointer_cast<arrow::BinaryArray>(variants->GetFieldByName("value"));
    std::shared_ptr<arrow::BinaryArray> metadata =
        std::dynamic_pointer_cast<arrow::BinaryArray>(variants->GetFieldByName("metadata"));
    if (!values || !metadata) {
        return Status::Invalid("Expected unshredded VARIANT struct");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<GenericVariant> variant,
        GenericVariant::Create(values->GetView(index), metadata->GetView(index), GetDefaultPool()));
    return variant->ToJson();
}

void AssertVariantJsonAt(const std::shared_ptr<arrow::StructArray>& variants, int64_t index,
                         const std::string& expected_json) {
    ASSERT_OK_AND_ASSIGN(std::string actual_json, VariantJsonAt(variants, index));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GenericVariant> expected,
                         GenericVariant::FromJson(expected_json, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::string normalized_expected_json, expected->ToJson());
    ASSERT_EQ(actual_json, normalized_expected_json);
}

}  // namespace

class PaimonReadCompatInteTest : public ::testing::TestWithParam<std::string> {};

TEST_P(PaimonReadCompatInteTest, ReadsCompatibleTypeValues) {
    const std::string& writer_prefix = GetParam();
    TimezoneGuard timezone_guard("Asia/Shanghai");

    // Project every non-BLOB field from the main compatibility table in schema order.
    std::vector<std::string> fields = {
        // Basic numeric types.
        "id",
        "f_boolean",
        "f_tinyint",
        "f_smallint",
        "f_int",
        "f_bigint",
        "f_float",
        "f_double",
        // Character and binary string types.
        "f_char",
        "f_varchar",
        "f_string",
        "f_binary",
        "f_varbinary",
        "f_bytes",
        // DECIMAL values across physical precision boundaries.
        "f_decimal_1_0",
        "f_decimal_9_2",
        "f_decimal_18_2",
        "f_decimal_19_2",
        "f_decimal_38_18",
        "f_decimal_38_38",
        // Date and timestamp types at every supported precision.
        "f_date",
        "f_timestamp_0",
        "f_timestamp_3",
        "f_timestamp_6",
        "f_timestamp_9",
        "f_timestamp_ltz_0",
        "f_timestamp_ltz_3",
        "f_timestamp_ltz_6",
        "f_timestamp_ltz_9",
        // VARIANT and nested container types.
        "f_variant",
        "f_array_int",
        "f_map_string_bigint",
        "f_row",
        "f_array_array_int",
        "f_array_map",
        "f_map_array",
        "f_array_row",
        "f_map_row",
        "f_deep_row",
        "f_array_variant",
        "f_map_variant",
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadTable(writer_prefix + "_types", fields));
    std::shared_ptr<arrow::StructArray> rows = GetOnlyStructChunk(result);
    ASSERT_TRUE(rows);
    ASSERT_EQ(rows->length(), 3);
    ASSERT_EQ(rows->num_fields(), static_cast<int>(fields.size() + 1));

    // Paimon row kind and basic numeric values.
    AssertFieldEqualsJson(rows, "_VALUE_KIND", arrow::int8(), "[0, 0, 0]");
    AssertFieldEqualsJson(rows, "id", arrow::int32(), "[1, 2, 3]");
    AssertFieldEqualsJson(rows, "f_boolean", arrow::boolean(), "[true, null, null]");
    AssertFieldEqualsJson(rows, "f_tinyint", arrow::int8(), "[-8, null, null]");
    AssertFieldEqualsJson(rows, "f_smallint", arrow::int16(), "[1234, null, null]");
    AssertFieldEqualsJson(rows, "f_int", arrow::int32(), "[-123456, null, null]");
    AssertFieldEqualsJson(rows, "f_bigint", arrow::int64(), "[9223372036854770000, null, null]");
    AssertFieldEqualsJson(rows, "f_float", arrow::float32(), "[1.25, null, null]");
    AssertFieldEqualsJson(rows, "f_double", arrow::float64(), "[-12345.6789, null, null]");

    // Character and binary strings, including empty and embedded-null values.
    AssertFieldEqualsJson(rows, "f_char", arrow::utf8(), R"(["char", null, ""])");
    AssertFieldEqualsJson(rows, "f_varchar", arrow::utf8(), R"(["varchar-中文", null, ""])");
    AssertFieldEqualsJson(rows, "f_string", arrow::utf8(), R"(["pypaimon 2.0.0", null, ""])");
    AssertFieldEqualsJson(rows, "f_binary", arrow::binary(), R"(["12345678", null, "ABCDEFGH"])");
    AssertFieldEqualsJson(rows, "f_varbinary", arrow::binary(),
                          R"(["varbinary\u0000value", null, ""])");
    AssertFieldEqualsJson(rows, "f_bytes", arrow::binary(), R"(["bytes\u0000value", null, ""])");

    // DECIMAL values around the int32, int64, and fixed-byte-array precision boundaries.
    AssertFieldEqualsJson(rows, "f_decimal_1_0", arrow::decimal128(1, 0), R"(["9", null, null])");
    AssertFieldEqualsJson(rows, "f_decimal_9_2", arrow::decimal128(9, 2),
                          R"(["1234567.89", null, null])");
    AssertFieldEqualsJson(rows, "f_decimal_18_2", arrow::decimal128(18, 2),
                          R"(["1234567890123456.78", null, null])");
    AssertFieldEqualsJson(rows, "f_decimal_19_2", arrow::decimal128(19, 2),
                          R"(["12345678901234567.89", null, null])");
    AssertFieldEqualsJson(rows, "f_decimal_38_18", arrow::decimal128(38, 18),
                          R"(["12345678901234567890.123456789012345678", null, null])");
    AssertFieldEqualsJson(rows, "f_decimal_38_38", arrow::decimal128(38, 38),
                          R"(["0.12345678901234567890123456789012345678", null, null])");

    // DATE, TIMESTAMP, and TIMESTAMP_LTZ values at second through nanosecond precision.
    AssertFieldEqualsJson(rows, "f_date", arrow::date32(), "[19782, null, null]");
    AssertFieldEqualsJson(rows, "f_timestamp_0", arrow::timestamp(arrow::TimeUnit::SECOND),
                          R"(["2024-02-29 12:34:56", null, null])");
    AssertFieldEqualsJson(rows, "f_timestamp_3", arrow::timestamp(arrow::TimeUnit::MILLI),
                          R"(["2024-02-29 12:34:56.123", null, null])");
    AssertFieldEqualsJson(rows, "f_timestamp_6", arrow::timestamp(arrow::TimeUnit::MICRO),
                          R"(["2024-02-29 12:34:56.123456", null, null])");
    AssertFieldEqualsJson(rows, "f_timestamp_9", arrow::timestamp(arrow::TimeUnit::NANO),
                          R"(["2024-02-29 12:34:56.123456000", null, null])");
    AssertFieldEqualsJson(rows, "f_timestamp_ltz_0",
                          arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Shanghai"),
                          R"(["2024-02-29 12:34:56", null, null])");
    AssertFieldEqualsJson(rows, "f_timestamp_ltz_3",
                          arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Shanghai"),
                          R"(["2024-02-29 12:34:56.123", null, null])");
    AssertFieldEqualsJson(rows, "f_timestamp_ltz_6",
                          arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai"),
                          R"(["2024-02-29 12:34:56.123456", null, null])");
    AssertFieldEqualsJson(rows, "f_timestamp_ltz_9",
                          arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai"),
                          R"(["2024-02-29 12:34:56.123456000", null, null])");

    // First-level ARRAY, MAP, and ROW values with null and empty containers.
    AssertFieldEqualsJson(rows, "f_array_int", arrow::list(arrow::int32()),
                          "[[1, null, 3], null, []]");
    AssertFieldEqualsJson(rows, "f_map_string_bigint", arrow::map(arrow::utf8(), arrow::int64()),
                          R"([[ ["one", 1], ["null", null] ], null, []])");
    std::shared_ptr<arrow::DataType> row_type = arrow::struct_({
        arrow::field("nested_int", arrow::int32()),
        arrow::field("nested_string", arrow::utf8()),
        arrow::field("nested_decimal", arrow::decimal128(19, 4)),
        arrow::field("nested_timestamp", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("nested_timestamp_ltz",
                     arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai")),
    });
    AssertFieldEqualsJson(
        rows, "f_row", row_type,
        R"([[7, "nested", "123456789012345.6789", "2024-02-29 12:34:56.123456000", "2024-02-29 12:34:56.123456000"], null, null])");

    // Nested ARRAY/MAP combinations and containers whose values are ROWs.
    AssertFieldEqualsJson(rows, "f_array_array_int", arrow::list(arrow::list(arrow::int32())),
                          "[[[1, 2], null, []], null, []]");
    AssertFieldEqualsJson(rows, "f_array_map",
                          arrow::list(arrow::map(arrow::utf8(), arrow::int32())),
                          R"([[[["a", 1], ["b", null]], null, []], null, []])");
    AssertFieldEqualsJson(rows, "f_map_array",
                          arrow::map(arrow::utf8(), arrow::list(arrow::int32())),
                          R"([[ ["numbers", [1, null, 3]], ["empty", []] ], null, []])");
    std::shared_ptr<arrow::DataType> array_row_type = arrow::list(arrow::struct_({
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::decimal128(9, 2)),
    }));
    AssertFieldEqualsJson(rows, "f_array_row", array_row_type,
                          R"([[ ["alice", "99.50"], null, ["bob", null] ], null, []])");
    std::shared_ptr<arrow::DataType> map_row_type = arrow::map(
        arrow::utf8(),
        arrow::struct_({arrow::field("enabled", arrow::boolean()),
                        arrow::field("event_time", arrow::timestamp(arrow::TimeUnit::MICRO))}));
    AssertFieldEqualsJson(
        rows, "f_map_row", map_row_type,
        R"([[ ["first", [true, "2024-02-29 12:34:56.123456"]], ["second", null] ], null, []])");

    // Top-level VARIANT values are compared as normalized JSON.
    const std::string object_variant_json =
        R"({"name":"variant-object","count":42,"active":true,"items":[null,1,"x"],"nested":{"decimal":12.34}})";
    const std::string array_variant_json = R"([1,"two",false,{"k":"v"}])";
    std::shared_ptr<arrow::StructArray> variants =
        std::dynamic_pointer_cast<arrow::StructArray>(rows->GetFieldByName("f_variant"));
    ASSERT_TRUE(variants);
    AssertVariantJsonAt(variants, 0, object_variant_json);
    ASSERT_TRUE(variants->IsNull(1));
    ASSERT_TRUE(variants->IsNull(2));

    // Deeply nested ROW -> ARRAY<ROW<VARIANT>> and ROW -> MAP values.
    std::shared_ptr<arrow::StructArray> deep_rows =
        std::dynamic_pointer_cast<arrow::StructArray>(rows->GetFieldByName("f_deep_row"));
    ASSERT_TRUE(deep_rows);
    ASSERT_FALSE(deep_rows->IsNull(0));
    ASSERT_TRUE(deep_rows->IsNull(1));
    ASSERT_TRUE(deep_rows->IsNull(2));
    std::shared_ptr<arrow::ListArray> children =
        std::dynamic_pointer_cast<arrow::ListArray>(deep_rows->GetFieldByName("children"));
    ASSERT_TRUE(children);
    ASSERT_EQ(children->value_length(0), 2);
    std::shared_ptr<arrow::StructArray> child_rows =
        std::dynamic_pointer_cast<arrow::StructArray>(children->values());
    ASSERT_TRUE(child_rows);
    AssertArrayEqualsJson(child_rows->GetFieldByName("leaf_id"), "f_deep_row.children.leaf_id",
                          "[10, 11]");
    std::shared_ptr<arrow::StructArray> leaf_variants =
        std::dynamic_pointer_cast<arrow::StructArray>(child_rows->GetFieldByName("leaf_variant"));
    ASSERT_TRUE(leaf_variants);
    AssertVariantJsonAt(leaf_variants, 0, array_variant_json);
    ASSERT_TRUE(leaf_variants->IsNull(1));
    std::shared_ptr<arrow::MapArray> labels =
        std::dynamic_pointer_cast<arrow::MapArray>(deep_rows->GetFieldByName("labels"));
    ASSERT_TRUE(labels);
    AssertArrayEqualsJson(labels->Slice(0, 1), "f_deep_row.labels",
                          R"([[ ["language", "python"], ["format", "parquet"] ]])");

    // ARRAY<VARIANT> values, including null elements and empty arrays.
    std::shared_ptr<arrow::ListArray> array_variants =
        std::dynamic_pointer_cast<arrow::ListArray>(rows->GetFieldByName("f_array_variant"));
    ASSERT_TRUE(array_variants);
    ASSERT_EQ(array_variants->value_length(0), 3);
    ASSERT_TRUE(array_variants->IsNull(1));
    ASSERT_EQ(array_variants->value_length(2), 0);
    std::shared_ptr<arrow::StructArray> array_variant_values =
        std::dynamic_pointer_cast<arrow::StructArray>(array_variants->values());
    ASSERT_TRUE(array_variant_values);
    AssertVariantJsonAt(array_variant_values, 0, object_variant_json);
    ASSERT_TRUE(array_variant_values->IsNull(1));
    AssertVariantJsonAt(array_variant_values, 2, array_variant_json);

    // MAP<STRING, VARIANT> values and their key/value alignment.
    std::shared_ptr<arrow::MapArray> map_variants =
        std::dynamic_pointer_cast<arrow::MapArray>(rows->GetFieldByName("f_map_variant"));
    ASSERT_TRUE(map_variants);
    ASSERT_EQ(map_variants->value_length(0), 2);
    ASSERT_TRUE(map_variants->IsNull(1));
    ASSERT_EQ(map_variants->value_length(2), 0);
    AssertArrayEqualsJson(map_variants->keys(), "f_map_variant.keys", R"(["object", "array"])");
    std::shared_ptr<arrow::StructArray> map_variant_values =
        std::dynamic_pointer_cast<arrow::StructArray>(map_variants->items());
    ASSERT_TRUE(map_variant_values);
    AssertVariantJsonAt(map_variant_values, 0, object_variant_json);
    AssertVariantJsonAt(map_variant_values, 1, array_variant_json);
}

TEST_P(PaimonReadCompatInteTest, ReadsBlobValues) {
    const std::string& writer_prefix = GetParam();

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadTable(writer_prefix + "_types", {"f_blob", "f_blob_descriptor"},
                                   {{Options::FILE_SYSTEM, "local"},
                                    {Options::BLOB_AS_DESCRIPTOR, "true"},
                                    {Options::BLOB_VIEW_RESOLVE_ENABLED, "false"}}));
    std::shared_ptr<arrow::StructArray> rows = GetOnlyStructChunk(result);
    ASSERT_TRUE(rows);
    ASSERT_EQ(rows->length(), 3);
    ASSERT_EQ(rows->num_fields(), 3);

    // External BLOB descriptors, including a non-empty and an empty blob range.
    std::shared_ptr<arrow::LargeBinaryArray> blobs =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(rows->GetFieldByName("f_blob"));
    ASSERT_TRUE(blobs);
    ASSERT_EQ(blobs->length(), 3);
    ASSERT_TRUE(blobs->IsNull(1));
    std::string_view first = blobs->GetView(0);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> first_descriptor,
                         BlobDescriptor::Deserialize(first.data(), first.size()));
    ASSERT_EQ(first_descriptor->Version(), 2);
    ASSERT_EQ(first_descriptor->Offset(), 4);
    ASSERT_EQ(first_descriptor->Length(), 41);
    ASSERT_EQ(first_descriptor->Uri().substr(first_descriptor->Uri().size() - 5), ".blob");
    std::string_view empty = blobs->GetView(2);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> empty_descriptor,
                         BlobDescriptor::Deserialize(empty.data(), empty.size()));
    ASSERT_EQ(empty_descriptor->Offset(), 61);
    ASSERT_EQ(empty_descriptor->Length(), 0);

    // Resolve the external BLOB and validate its payload instead of its descriptor.
    std::map<std::string, std::string> blob_value_options = {
        {Options::FILE_SYSTEM, "local"},
        {Options::BLOB_AS_DESCRIPTOR, "false"},
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> blob_value_result,
                         ReadTable(writer_prefix + "_types", {"f_blob"}, blob_value_options));
    std::shared_ptr<arrow::StructArray> blob_value_rows = GetOnlyStructChunk(blob_value_result);
    ASSERT_TRUE(blob_value_rows);
    AssertFieldEqualsJson(blob_value_rows, "f_blob", arrow::large_binary(),
                          R"(["ordinary blob payload from pypaimon 2.0.0", null, ""])");

    // Inline BLOB descriptors exercise compatible binary types restored from ARROW:schema.
    std::shared_ptr<arrow::LargeBinaryArray> inline_blobs =
        std::dynamic_pointer_cast<arrow::LargeBinaryArray>(
            rows->GetFieldByName("f_blob_descriptor"));
    ASSERT_TRUE(inline_blobs);
    ASSERT_EQ(inline_blobs->length(), 3);
    ASSERT_FALSE(inline_blobs->IsNull(0));
    ASSERT_TRUE(inline_blobs->IsNull(1));
    ASSERT_TRUE(inline_blobs->IsNull(2));
    std::string_view inline_blob = inline_blobs->GetView(0);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobDescriptor> inline_descriptor,
                         BlobDescriptor::Deserialize(inline_blob.data(), inline_blob.size()));
    ASSERT_EQ(inline_descriptor->Version(), 2);
    ASSERT_EQ(inline_descriptor->Uri(), "file:///nonexistent/pypaimon-all-types-external-blob.bin");
    ASSERT_EQ(inline_descriptor->Offset(), 7);
    ASSERT_EQ(inline_descriptor->Length(), 11);

    // Python and Java store MAP<STRING, BLOB> values in standard separate BLOB files. Validate
    // resolved payloads, null values, and an empty map. Rust stores raw values inline in Parquet,
    // which is not a compatible Paimon BLOB representation and is asserted separately below.
    if (writer_prefix == "rust") {
        return;
    }
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::ChunkedArray> map_blob_result,
        ReadTable(writer_prefix + "_map_blob_types", {"id", "f_map_blob"}, blob_value_options));
    std::shared_ptr<arrow::StructArray> map_blob_rows = GetOnlyStructChunk(map_blob_result);
    ASSERT_TRUE(map_blob_rows);
    ASSERT_EQ(map_blob_rows->length(), 3);
    AssertFieldEqualsJson(map_blob_rows, "id", arrow::int32(), "[1, 2, 3]");
    AssertFieldEqualsJson(map_blob_rows, "f_map_blob",
                          arrow::map(arrow::utf8(), arrow::large_binary()),
                          R"([[ ["left", "blob-map-left"], ["right", null] ], null, []])");
}

TEST(PaimonReadCompatInteStandaloneTest, RejectsNonStandardRustMapBlob) {
    ASSERT_NOK_WITH_MSG(
        ReadTable("rust_map_blob_types", {"f_map_blob"}),
        "Parquet does not support partial projection inside list/map: src map<string, binary");
}

TEST_P(PaimonReadCompatInteTest, ReadsVectorValues) {
    const std::string& writer_prefix = GetParam();

    // Validate every supported VECTOR element type.
    std::vector<std::string> vector_fields = {
        "id",           "f_vector_boolean", "f_vector_tinyint", "f_vector_smallint",
        "f_vector_int", "f_vector_bigint",  "f_vector_float",   "f_vector_double",
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> vector_result,
                         ReadTable(writer_prefix + "_vector_types", vector_fields));
    std::shared_ptr<arrow::StructArray> vector_rows = GetOnlyStructChunk(vector_result);
    ASSERT_TRUE(vector_rows);
    ASSERT_EQ(vector_rows->length(), 2);
    AssertFieldEqualsJson(vector_rows, "id", arrow::int32(), "[1, 2]");
    AssertFieldEqualsJson(vector_rows, "f_vector_boolean",
                          arrow::fixed_size_list(arrow::boolean(), 3),
                          "[[true, false, true], [false, true, false]]");
    AssertFieldEqualsJson(vector_rows, "f_vector_tinyint", arrow::fixed_size_list(arrow::int8(), 3),
                          "[[-1, 0, 1], [2, 3, 4]]");
    AssertFieldEqualsJson(vector_rows, "f_vector_smallint",
                          arrow::fixed_size_list(arrow::int16(), 3),
                          "[[-1000, 0, 1000], [2000, 3000, 4000]]");
    AssertFieldEqualsJson(vector_rows, "f_vector_int", arrow::fixed_size_list(arrow::int32(), 3),
                          "[[-100000, 0, 100000], [200000, 300000, 400000]]");
    AssertFieldEqualsJson(
        vector_rows, "f_vector_bigint", arrow::fixed_size_list(arrow::int64(), 3),
        "[[-10000000000, 0, 10000000000], [20000000000, 30000000000, 40000000000]]");
    AssertFieldEqualsJson(vector_rows, "f_vector_float",
                          arrow::fixed_size_list(arrow::float32(), 3),
                          "[[1.25, -2.5, 3.75], [4.25, 5.5, 6.75]]");
    AssertFieldEqualsJson(vector_rows, "f_vector_double",
                          arrow::fixed_size_list(arrow::float64(), 3),
                          "[[1.125, -2.25, 3.5], [4.125, 5.25, 6.5]]");
}

INSTANTIATE_TEST_SUITE_P(Writers, PaimonReadCompatInteTest, ::testing::ValuesIn(WriterPrefixes()),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                             return info.param;
                         });

struct UnsupportedReadCase {
    std::string name;
    std::string table_suffix;
    std::string field_name;
    std::string expected_error;
};

struct UnsupportedReadParam {
    std::string writer_prefix;
    UnsupportedReadCase read_case;
};

class PaimonUnsupportedTypeInteTest : public ::testing::TestWithParam<UnsupportedReadParam> {};

TEST_P(PaimonUnsupportedTypeInteTest, ReportsExpectedError) {
    const UnsupportedReadParam& test_case = GetParam();
    ASSERT_NOK_WITH_MSG(ReadTable(test_case.writer_prefix + "_" + test_case.read_case.table_suffix,
                                  {test_case.read_case.field_name}),
                        test_case.read_case.expected_error);
}

std::vector<UnsupportedReadParam> UnsupportedReadParams() {
    const std::vector<UnsupportedReadCase> read_cases = {
        {"ArrayBlob", "array_blob_types", "f_array_blob",
         "BLOB field must be a top-level field or the direct value of a top-level MAP field"},
        {"TimePrecision0", "time_types", "f_time_0", "Unsupported type: TIME"},
        {"TimePrecision3", "time_types", "f_time_3", "Unsupported type: TIME"},
        {"TimePrecision6", "time_types", "f_time_6", "Unsupported type: TIME"},
        {"TimePrecision9", "time_types", "f_time_9", "Unsupported type: TIME"},
    };
    std::vector<UnsupportedReadParam> result;
    for (const std::string& writer_prefix : WriterPrefixes()) {
        for (const UnsupportedReadCase& read_case : read_cases) {
            result.push_back({writer_prefix, read_case});
        }
    }
    return result;
}

INSTANTIATE_TEST_SUITE_P(UnsupportedTypes, PaimonUnsupportedTypeInteTest,
                         ::testing::ValuesIn(UnsupportedReadParams()),
                         [](const ::testing::TestParamInfo<UnsupportedReadParam>& info) {
                             return info.param.writer_prefix + info.param.read_case.name;
                         });

}  // namespace paimon::test
