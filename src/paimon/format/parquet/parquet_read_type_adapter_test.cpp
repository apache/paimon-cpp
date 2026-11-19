/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/format/parquet/parquet_read_type_adapter.h"

#include <memory>

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/types/geospatial_type.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::parquet::test {

TEST(ParquetReadTypeAdapterTest, TestNeedsArrayConversion) {
    {
        // single field need cast
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO)),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f0", arrow::timestamp(arrow::TimeUnit::NANO, "UTC")),
        };
        ASSERT_OK_AND_ASSIGN(bool needs_conversion,
                             ParquetReadTypeAdapter::NeedsArrayConversion(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(needs_conversion);
    }
    {
        // field in list need cast
        arrow::FieldVector fields = {
            arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI))),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::SECOND)))};
        ASSERT_OK_AND_ASSIGN(bool needs_conversion,
                             ParquetReadTypeAdapter::NeedsArrayConversion(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(needs_conversion);
    }
    {
        // field in map need cast
        arrow::FieldVector fields = {
            arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                          arrow::timestamp(arrow::TimeUnit::NANO))),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::SECOND),
                                          arrow::timestamp(arrow::TimeUnit::NANO, "UTC")))};
        ASSERT_OK_AND_ASSIGN(bool needs_conversion,
                             ParquetReadTypeAdapter::NeedsArrayConversion(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(needs_conversion);
    }
    {
        // field in struct need cast
        arrow::FieldVector fields = {
            arrow::field("f3", arrow::struct_(
                                   {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                                    arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO))})),
        };
        arrow::FieldVector target_fields = {
            arrow::field("f3",
                         arrow::struct_(
                             {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                              arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO, "UTC"))})),
        };
        ASSERT_OK_AND_ASSIGN(bool needs_conversion,
                             ParquetReadTypeAdapter::NeedsArrayConversion(
                                 arrow::struct_(fields), arrow::struct_(target_fields)));
        ASSERT_TRUE(needs_conversion);
    }
}

TEST(ParquetReadTypeAdapterTest, TestAlreadyLogicalBlobCompatibility) {
    std::shared_ptr<arrow::Field> restored_blob = arrow::field("blob", arrow::large_binary());
    std::shared_ptr<arrow::Field> expected_physical_blob =
        BlobUtils::ToArrowField("blob")->WithType(arrow::binary());

    ASSERT_OK_AND_ASSIGN(bool needs_conversion, ParquetReadTypeAdapter::NeedsArrayConversion(
                                                    arrow::struct_({restored_blob}),
                                                    arrow::struct_({expected_physical_blob})));
    ASSERT_FALSE(needs_conversion);

    // Only a binary field carrying Paimon BLOB metadata is compatible with a large_binary field
    // already restored from ARROW:schema. A plain binary field must still be rejected.
    ASSERT_NOK_WITH_MSG(ParquetReadTypeAdapter::NeedsArrayConversion(
                            arrow::struct_({restored_blob}),
                            arrow::struct_({arrow::field("blob", arrow::binary())})),
                        "source type large_binary and target type binary mismatch");
}

TEST(ParquetReadTypeAdapterTest, TestDictionaryBinaryCompatibility) {
    std::shared_ptr<arrow::DataType> dictionary_type =
        arrow::dictionary(arrow::int32(), arrow::binary());

    ASSERT_OK_AND_ASSIGN(bool needs_conversion, ParquetReadTypeAdapter::NeedsArrayConversion(
                                                    dictionary_type, arrow::binary()));
    ASSERT_FALSE(needs_conversion);

    ASSERT_NOK_WITH_MSG(
        ParquetReadTypeAdapter::NeedsArrayConversion(dictionary_type, arrow::utf8()),
        "source type dictionary<values=binary, indices=int32, ordered=0> and target type string "
        "mismatch");
    ASSERT_NOK_WITH_MSG(ParquetReadTypeAdapter::NeedsArrayConversion(
                            arrow::dictionary(arrow::int32(), arrow::int32()), arrow::int32()),
                        "source type dictionary<values=int32, indices=int32, ordered=0> and "
                        "target type int32 mismatch");
}

TEST(ParquetReadTypeAdapterTest, TestAdaptArray) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI))),
        arrow::field("f3", arrow::struct_(
                               {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
                                arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
    };

    arrow::FieldVector target_fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::SECOND),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, timezone))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::SECOND))),
        arrow::field("f3",
                     arrow::struct_(
                         {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
                          arrow::field("f1", arrow::timestamp(arrow::TimeUnit::NANO, timezone))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
[[["1970-01-01 00:00:01", "1970-01-01 00:00:00.000001"]], ["1970-01-01 00:00:02"], ["1970-01-01 00:00:02", "1970-01-01 00:00:00.000000002"], "1970-01-01 00:00:00.000000002"],
[[["1970-01-01 00:00:03", "1970-01-01 00:00:00.000003"]], ["1970-01-01 00:00:04"], ["1970-01-01 00:00:04", "1970-01-01 00:00:00.000000004"], "1970-01-01 00:00:00.000000004"],
[null, null, null, "1970-01-01 00:00:00.000000004"]
    ])")
            .ValueOrDie());

    std::shared_ptr<arrow::MemoryPool> pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> result_array,
        ParquetReadTypeAdapter::AdaptArray(array, arrow::struct_(target_fields), pool));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(target_fields), R"([
[[["1970-01-01 00:00:01", "1970-01-01 00:00:00.000001"]], ["1970-01-01 00:00:02"], ["1970-01-01 00:00:02", "1970-01-01 00:00:00.000000002"], "1970-01-01 00:00:00.000000002"],
[[["1970-01-01 00:00:03", "1970-01-01 00:00:00.000003"]], ["1970-01-01 00:00:04"], ["1970-01-01 00:00:04", "1970-01-01 00:00:00.000000004"], "1970-01-01 00:00:00.000000004"],
[null, null, null, "1970-01-01 00:00:00.000000004"]
    ])")
            .ValueOrDie());
    ASSERT_TRUE(result_array->Equals(expected_array)) << result_array->ToString();
    ASSERT_OK_AND_ASSIGN(auto slice, ParquetReadTypeAdapter::AdaptArray(
                                         array->Slice(1), arrow::struct_(target_fields), pool));
    ASSERT_TRUE(slice->Equals(expected_array->Slice(1)));
}

TEST(ParquetReadTypeAdapterTest, TestAdaptArrayValidatesResultType) {
    std::shared_ptr<arrow::DataType> src_type = arrow::timestamp(arrow::TimeUnit::MICRO);
    std::shared_ptr<arrow::DataType> target_type = arrow::timestamp(arrow::TimeUnit::MILLI);
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(src_type, R"(["1970-01-01 00:00:01"])")
            .ValueOrDie();
    std::shared_ptr<arrow::MemoryPool> pool = GetArrowPool(GetDefaultPool());

    ASSERT_NOK_WITH_MSG(ParquetReadTypeAdapter::AdaptArray(array, target_type, pool),
                        "after AdaptArray, output type timestamp[us] does not match target type "
                        "timestamp[ms]");
}

TEST(ParquetReadTypeAdapterTest, TimestampConversionCannotRelabelGeospatialSibling) {
    ASSERT_OK_AND_ASSIGN(auto geometry, GeospatialType::GeometryField("g"));
    ASSERT_OK_AND_ASSIGN(auto geography, GeospatialType::GeographyField("g"));
    auto millis = arrow::timestamp(arrow::TimeUnit::MILLI);
    auto seconds = arrow::timestamp(arrow::TimeUnit::SECOND);
    auto reject = [](const std::shared_ptr<arrow::DataType>& source,
                     const std::shared_ptr<arrow::DataType>& target) {
        auto array = arrow::MakeArrayOfNull(source, 1);
        ASSERT_TRUE(array.ok()) << array.status().ToString();
        ASSERT_NOK_WITH_MSG(
            ParquetReadTypeAdapter::AdaptArray(*array, target, GetArrowPool(GetDefaultPool())),
            "Incompatible geospatial type change");
    };
    reject(arrow::struct_({arrow::field("ts", millis), geometry}),
           arrow::struct_({arrow::field("ts", seconds), geography}));
    reject(arrow::map(millis, geometry), arrow::map(seconds, geography));
}

TEST(ParquetReadTypeAdapterTest, GeospatialMetadataSurvivesSlicedNestedArrays) {
    ASSERT_OK_AND_ASSIGN(auto source_field, GeospatialType::GeometryField("g", "epsg:3857"));
    ASSERT_OK_AND_ASSIGN(auto target_field, GeospatialType::GeometryField("g", "EPSG:3857"));
    auto nested_type = [](const std::shared_ptr<arrow::Field>& field) {
        return arrow::struct_({field, arrow::field("row", arrow::struct_({field})),
                               arrow::field("array", arrow::list(field)),
                               arrow::field("map", arrow::map(arrow::int32(), field))});
    };
    auto source_type = nested_type(source_field);
    auto target_type = nested_type(target_field);
    const char* json = R"([
        ["a", ["b"], ["c"], [[1, "d"]]],
        null,
        [null, null, null, null],
        ["e", ["f"], ["g", null], [[2, "h"]]]
    ])";
    auto make_array =
        [&](const std::shared_ptr<arrow::DataType>& type) -> Result<std::shared_ptr<arrow::Array>> {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ipc::internal::json::ArrayFromJSON(type, json));
        auto row = checked_pointer_cast<arrow::StructArray>(array);
        auto map = checked_pointer_cast<arrow::MapArray>(row->field(3));
        auto children = row->fields();
        children[3] = std::make_shared<arrow::MapArray>(
            type->field(3)->type(), map->length(), map->value_offsets(), map->keys(), map->items(),
            map->null_bitmap(), map->null_count());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> result,
            arrow::StructArray::Make(children, type->fields(), row->null_bitmap(),
                                     row->null_count()));
        return result;
    };
    ASSERT_OK_AND_ASSIGN(auto source, make_array(source_type));
    ASSERT_OK_AND_ASSIGN(auto expected, make_array(target_type));
    ASSERT_TRUE(source->type()->Equals(*source_type, true));
    ASSERT_TRUE(expected->type()->Equals(*target_type, true));
    for (int64_t offset : {0, 1}) {
        ASSERT_OK_AND_ASSIGN(auto result,
                             ParquetReadTypeAdapter::AdaptArray(source->Slice(offset), target_type,
                                                                GetArrowPool(GetDefaultPool())));
        ASSERT_TRUE(result->ValidateFull().ok());
        ASSERT_TRUE(result->type()->Equals(*target_type, true));
        ASSERT_TRUE(result->Equals(expected->Slice(offset))) << result->ToString();
    }
}

TEST(ParquetReadTypeAdapterTest, TestNormalizeFileType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"))),
        arrow::field(
            "f3",
            arrow::struct_({arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                            arrow::field("f1", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
    };

    arrow::FieldVector target_fields = {
        arrow::field("f1", arrow::map(arrow::timestamp(arrow::TimeUnit::MILLI),
                                      arrow::timestamp(arrow::TimeUnit::MICRO, timezone))),
        arrow::field("f2", arrow::list(arrow::timestamp(arrow::TimeUnit::MILLI, timezone))),
        arrow::field("f3",
                     arrow::struct_(
                         {arrow::field("f0", arrow::timestamp(arrow::TimeUnit::MILLI)),
                          arrow::field("f1", arrow::timestamp(arrow::TimeUnit::MICRO, timezone))})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO)),
    };

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::DataType> result_type,
                         ParquetReadTypeAdapter::NormalizeFileType(arrow::struct_(fields)));
    ASSERT_TRUE(result_type->Equals(arrow::struct_(target_fields)));
}
}  // namespace paimon::parquet::test
