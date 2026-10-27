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

#include "paimon/format/vortex/vortex_predicate_converter.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::vortex::test {
namespace {

std::shared_ptr<arrow::Schema> TestSchema() {
    return arrow::schema({
        arrow::field("i", arrow::int32()),
        arrow::field("s", arrow::utf8()),
        arrow::field("ts_ms", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_ltz", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
        arrow::field("l", arrow::int64()),
        arrow::field("b", arrow::boolean()),
        arrow::field("dt", arrow::date32()),
        arrow::field("dec_compact", arrow::decimal128(10, 2)),
        arrow::field("dec_wide", arrow::decimal128(19, 2)),
    });
}

bool Pushed(const std::shared_ptr<Predicate>& predicate,
            const std::shared_ptr<arrow::Schema>& schema,
            const VortexPredicateOptions& options = {}) {
    return VortexPredicateConverter::Convert(predicate, schema, options) != nullptr;
}

struct NativeInCase {
    const char* name;
    std::shared_ptr<arrow::DataType> type;
    FieldType field_type;
    const char* values_json;
    std::vector<Literal> literals;
};

std::string NativeErrorMessage(const vx_error* error) {
    if (error == nullptr) {
        return "";
    }
    const vx_string* message = vx_error_get_message(error);
    return message == nullptr ? "unknown Vortex error"
                              : std::string(vx_string_ptr(message), vx_string_len(message));
}

// Check native rows without a Paimon residual filter so conversion alone cannot pass the test.
void AssertNativeRows(const NativeInCase& test_case, const std::shared_ptr<Predicate>& predicate,
                      const std::vector<int64_t>& expected_rows, bool project_rowid_only = false,
                      const VortexPredicateOptions& options = {}, bool expect_native_filter = true,
                      const vx_expression* native_filter_override = nullptr) {
    SCOPED_TRACE(test_case.name);
    auto values_result =
        arrow::ipc::internal::json::ArrayFromJSON(test_case.type, test_case.values_json);
    ASSERT_TRUE(values_result.ok()) << values_result.status().ToString();
    auto ids_result = arrow::ipc::internal::json::ArrayFromJSON(arrow::int64(), "[0,1,2,3]");
    ASSERT_TRUE(ids_result.ok()) << ids_result.status().ToString();
    const arrow::FieldVector fields = {arrow::field("rowid", arrow::int64(), false),
                                       arrow::field("value", test_case.type)};
    const std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    auto array_result =
        arrow::StructArray::Make({ids_result.ValueOrDie(), values_result.ValueOrDie()}, fields);
    ASSERT_TRUE(array_result.ok()) << array_result.status().ToString();

    std::unique_ptr<paimon::test::UniqueTestDirectory> directory =
        paimon::test::UniqueTestDirectory::Create();
    ASSERT_NE(directory, nullptr);
    const std::string path = PathUtil::JoinPath(directory->Str(), "values.vortex");
    vx_error* error = nullptr;
    ScopeGuard release_error([&error]() {
        if (error != nullptr) {
            vx_error_free(error);
        }
    });
    VxSessionPtr session(vx_session_new(), vx_session_free);
    ASSERT_NE(session, nullptr);
    ::ArrowArray exported_array{};
    ::ArrowSchema exported_schema{};
    ScopeGuard release_export([&exported_array, &exported_schema]() {
        if (exported_array.release != nullptr) {
            exported_array.release(&exported_array);
        }
        if (exported_schema.release != nullptr) {
            exported_schema.release(&exported_schema);
        }
    });
    ASSERT_TRUE(
        arrow::ExportArray(*array_result.ValueOrDie(), &exported_array, &exported_schema).ok());
    // Import consumes both Arrow handles; the guard also covers an incomplete failed import.
    VxArrayPtr array(vx_array_from_arrow(&exported_array, &exported_schema, false, &error),
                     vx_array_free);
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(array, nullptr);
    vx_file_write_array(session.get(), path.c_str(), array.get(), &error);
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    LocalFileSystem file_system;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system.Open(path));
    ASSERT_OK_AND_ASSIGN(int64_t length, input->Length());
    auto input_context = std::make_shared<VortexInputContext>(input);
    VxDataSourcePtr source(
        vx_data_source_new_callback(session.get(), VortexInputContext::MakeCallbacks(input_context),
                                    static_cast<uint64_t>(length), &error),
        vx_data_source_free);
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(source, nullptr);
    VxExpressionPtr filter = VortexPredicateConverter::Convert(predicate, schema, options);
    ASSERT_EQ(filter != nullptr, expect_native_filter);
    vx_scan_options scan_options{};
    scan_options.filter = native_filter_override == nullptr ? filter.get() : native_filter_override;
    VxExpressionPtr root(nullptr, vx_expression_free);
    VxExpressionPtr projection(nullptr, vx_expression_free);
    if (project_rowid_only) {
        root = VxExpressionPtr(vx_expression_root(), vx_expression_free);
        ASSERT_NE(root, nullptr);
        const char* names[] = {"rowid"};
        projection =
            VxExpressionPtr(vx_expression_select(names, 1, root.get()), vx_expression_free);
        ASSERT_NE(projection, nullptr);
        scan_options.projection = projection.get();
    }
    VxScanPtr scan(vx_data_source_scan(source.get(), &scan_options, nullptr, &error), vx_scan_free);
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(scan, nullptr);
    std::vector<int64_t> row_ids;
    while (true) {
        VxPartitionPtr partition(vx_scan_next_partition(scan.get(), &error), vx_partition_free);
        ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
        if (partition == nullptr) {
            break;
        }
        ::ArrowArrayStream stream{};
        ScopeGuard release_stream([&stream]() {
            if (stream.release != nullptr) {
                stream.release(&stream);
            }
        });
        // The scan consumes the partition even when it fails.
        const int32_t result =
            vx_partition_scan_arrow(session.get(), partition.release(), &stream, &error);
        ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
        ASSERT_EQ(result, 0);
        auto reader_result = arrow::ImportRecordBatchReader(&stream);
        ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
        const std::shared_ptr<arrow::RecordBatchReader> reader = reader_result.ValueOrDie();
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            const arrow::Status status = reader->ReadNext(&batch);
            ASSERT_TRUE(status.ok()) << status.ToString();
            if (batch == nullptr) {
                break;
            }
            ASSERT_EQ(batch->num_columns(), project_rowid_only ? 1 : 2);
            const std::shared_ptr<arrow::Array> ids = batch->GetColumnByName("rowid");
            ASSERT_NE(ids, nullptr);
            ASSERT_EQ(ids->type_id(), arrow::Type::INT64);
            const auto& typed_ids = checked_cast<const arrow::Int64Array&>(*ids);
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                ASSERT_FALSE(typed_ids.IsNull(row));
                row_ids.push_back(typed_ids.Value(row));
            }
        }
    }
    std::sort(row_ids.begin(), row_ids.end());
    ASSERT_EQ(row_ids, expected_rows);
}

// Compare an unsafe direct native expression with the converter's conservative fallback.
void AssertNativeComparisonRows(const NativeInCase& test_case,
                                const std::shared_ptr<Predicate>& predicate,
                                const vx_scalar* scalar, vx_binary_operator op,
                                const std::vector<int64_t>& expected_rows) {
    VxExpressionPtr root(vx_expression_root(), vx_expression_free);
    ASSERT_NE(root, nullptr);
    VxExpressionPtr column(vx_expression_get_item("value", root.get()), vx_expression_free);
    ASSERT_NE(column, nullptr);
    vx_error* error = nullptr;
    ScopeGuard release_error([&error]() {
        if (error != nullptr) {
            vx_error_free(error);
        }
    });
    VxExpressionPtr literal(vx_expression_literal(scalar, &error), vx_expression_free);
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(literal, nullptr);
    VxExpressionPtr filter(vx_expression_binary(op, column.get(), literal.get()),
                           vx_expression_free);
    ASSERT_NE(filter, nullptr);
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeRows(test_case, predicate, expected_rows, false, {}, false, filter.get()));
}

template <typename T>
NativeInCase IntegerRangeBoundaryCase(const char* name,
                                      const std::shared_ptr<arrow::DataType>& type,
                                      FieldType field_type, const char* values_json) {
    std::vector<Literal> literals;
    // Two disjoint four-value ranges at the extrema. Keep them unsorted and repeat endpoints.
    for (int32_t offset = 0; offset < 4; ++offset) {
        literals.emplace_back(static_cast<T>(std::numeric_limits<T>::max() - offset));
        literals.emplace_back(static_cast<T>(std::numeric_limits<T>::min() + offset));
    }
    literals.emplace_back(std::numeric_limits<T>::max());
    literals.emplace_back(std::numeric_limits<T>::min());
    return {name, type, field_type, values_json, std::move(literals)};
}

TEST(VortexPredicateConverterTest, ComparisonsOnSupportedTypesArePushed) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_TRUE(
        Pushed(PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(static_cast<int32_t>(42))),
               schema));
    ASSERT_TRUE(
        Pushed(PredicateBuilder::NotEqual(0, "i", FieldType::INT, Literal(static_cast<int32_t>(1))),
               schema));
    ASSERT_TRUE(Pushed(
        PredicateBuilder::GreaterThan(0, "l", FieldType::BIGINT, Literal(static_cast<int64_t>(7))),
        schema));
    ASSERT_TRUE(Pushed(
        PredicateBuilder::GreaterOrEqual(0, "i", FieldType::INT, Literal(static_cast<int32_t>(1))),
        schema));
    ASSERT_TRUE(
        Pushed(PredicateBuilder::LessThan(0, "i", FieldType::INT, Literal(static_cast<int32_t>(1))),
               schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::Equal(0, "b", FieldType::BOOLEAN, Literal(true)), schema));
    ASSERT_TRUE(Pushed(
        PredicateBuilder::Equal(0, "s", FieldType::STRING, Literal(FieldType::STRING, "abc", 3)),
        schema));
    ASSERT_TRUE(
        Pushed(PredicateBuilder::Equal(0, "dt", FieldType::DATE, Literal(FieldType::DATE, 19000)),
               schema));
    // Compact decimal (precision <= 18) and wide decimal (precision > 18) both push.
    ASSERT_TRUE(Pushed(PredicateBuilder::Equal(0, "dec_compact", FieldType::DECIMAL,
                                               Literal(Decimal::FromUnscaledLong(12345, 10, 2))),
                       schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::Equal(0, "dec_wide", FieldType::DECIMAL,
                                               Literal(Decimal::FromUnscaledLong(12345, 19, 2))),
                       schema));
}

TEST(VortexPredicateConverterTest, IsNullAndIsNotNullArePushed) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_TRUE(Pushed(PredicateBuilder::IsNull(0, "i", FieldType::INT), schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::IsNotNull(0, "i", FieldType::INT), schema));
}

TEST(VortexPredicateConverterTest, UnknownOrAmbiguousFileColumnsFallBack) {
    const std::shared_ptr<Predicate> equal =
        PredicateBuilder::Equal(0, "value", FieldType::INT, Literal(int32_t{1}));
    const std::shared_ptr<Predicate> is_null = PredicateBuilder::IsNull(0, "value", FieldType::INT);
    const std::shared_ptr<Predicate> is_not_null =
        PredicateBuilder::IsNotNull(0, "value", FieldType::INT);
    for (const std::shared_ptr<arrow::Schema>& schema :
         {std::shared_ptr<arrow::Schema>(), arrow::schema({arrow::field("other", arrow::int32())}),
          arrow::schema(
              {arrow::field("value", arrow::int32()), arrow::field("value", arrow::int32())})}) {
        ASSERT_FALSE(Pushed(equal, schema));
        ASSERT_FALSE(Pushed(is_null, schema));
        ASSERT_FALSE(Pushed(is_not_null, schema));
    }
    const std::shared_ptr<Predicate> present =
        PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(int32_t{1}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> conjunction,
                         PredicateBuilder::And({present, is_null}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> disjunction,
                         PredicateBuilder::Or({present, is_null}));
    ASSERT_TRUE(Pushed(conjunction, TestSchema()));
    ASSERT_FALSE(Pushed(disjunction, TestSchema()));
}

TEST(VortexPredicateConverterTest, ComparisonsRequireMatchingFileAndLiteralTypes) {
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    for (ComparisonBuilder build :
         {PredicateBuilder::Equal, PredicateBuilder::NotEqual, PredicateBuilder::LessThan,
          PredicateBuilder::LessOrEqual, PredicateBuilder::GreaterThan,
          PredicateBuilder::GreaterOrEqual}) {
        ASSERT_FALSE(Pushed(build(0, "i", FieldType::INT, Literal(int64_t{1})), TestSchema()));
        ASSERT_FALSE(Pushed(build(0, "i", FieldType::BIGINT, Literal(int64_t{1})), TestSchema()));
        ASSERT_FALSE(Pushed(build(0, "dt", FieldType::INT, Literal(int32_t{1})), TestSchema()));
        ASSERT_FALSE(Pushed(build(0, "dec_compact", FieldType::DECIMAL,
                                  Literal(Decimal::FromUnscaledLong(12345, 19, 2))),
                            TestSchema()));
        ASSERT_FALSE(Pushed(build(0, "dec_compact", FieldType::DECIMAL,
                                  Literal(Decimal::FromUnscaledLong(12345, 10, 3))),
                            TestSchema()));
    }
}

TEST(VortexPredicateConverterTest, UnboundPredicatesPreserveNativeCandidateRows) {
    const NativeInCase test_case = {"unbound", arrow::int32(), FieldType::INT, "[1,null,2,3]", {}};
    const std::vector<std::shared_ptr<Predicate>> predicates = {
        PredicateBuilder::Equal(2, "missing", FieldType::INT, Literal(int32_t{1})),
        PredicateBuilder::IsNull(2, "missing", FieldType::INT),
        PredicateBuilder::IsNotNull(2, "missing", FieldType::INT),
        PredicateBuilder::Equal(1, "value", FieldType::BIGINT, Literal(int64_t{1})),
    };
    for (const std::shared_ptr<Predicate>& predicate : predicates) {
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeRows(test_case, predicate, {0, 1, 2, 3}, false, {}, false));
    }
}

TEST(VortexPredicateConverterTest, RandomizedIntegerMembershipMatchesIndependentEvaluation) {
    struct IntegerType {
        const char* name;
        std::shared_ptr<arrow::DataType> arrow_type;
        FieldType field_type;
        int64_t minimum;
        int64_t maximum;
    };
    const std::vector<IntegerType> types = {
        {"i8", arrow::int8(), FieldType::TINYINT, std::numeric_limits<int8_t>::min(),
         std::numeric_limits<int8_t>::max()},
        {"i16", arrow::int16(), FieldType::SMALLINT, std::numeric_limits<int16_t>::min(),
         std::numeric_limits<int16_t>::max()},
        {"i32", arrow::int32(), FieldType::INT, std::numeric_limits<int32_t>::min(),
         std::numeric_limits<int32_t>::max()},
        {"i64", arrow::int64(), FieldType::BIGINT, std::numeric_limits<int64_t>::min(),
         std::numeric_limits<int64_t>::max()},
    };
    std::mt19937_64 generator(0x5EED);
    for (const IntegerType& type : types) {
        const std::string json = fmt::format("[{},null,0,{}]", type.minimum, type.maximum);
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("value", type.arrow_type)});
        for (int32_t iteration = 0; iteration < 32; ++iteration) {
            SCOPED_TRACE(fmt::format("{} iteration {}", type.name, iteration));
            std::vector<int64_t> values;
            std::vector<Literal> literals;
            for (uint64_t remaining = 1 + generator() % 80; remaining > 0; --remaining) {
                const uint64_t choice = generator() % 5;
                const int64_t value = choice == 0   ? type.minimum
                                      : choice == 1 ? type.maximum
                                                    : static_cast<int64_t>(generator() % 65) - 32;
                values.push_back(value);
                switch (type.field_type) {
                    case FieldType::TINYINT:
                        literals.emplace_back(static_cast<int8_t>(value));
                        break;
                    case FieldType::SMALLINT:
                        literals.emplace_back(static_cast<int16_t>(value));
                        break;
                    case FieldType::INT:
                        literals.emplace_back(static_cast<int32_t>(value));
                        break;
                    default:
                        literals.emplace_back(value);
                        break;
                }
            }
            const NativeInCase test_case = {type.name, type.arrow_type, type.field_type,
                                            json.c_str(), literals};
            for (bool negate : {false, true}) {
                const std::shared_ptr<Predicate> predicate =
                    negate ? PredicateBuilder::NotIn(1, "value", type.field_type, literals)
                           : PredicateBuilder::In(1, "value", type.field_type, literals);
                std::vector<int64_t> expected;
                const std::vector<int64_t> stored = {type.minimum, 0, type.maximum};
                const std::vector<int64_t> row_ids = {0, 2, 3};
                for (size_t row = 0; row < stored.size(); ++row) {
                    const bool found =
                        std::find(values.begin(), values.end(), stored[row]) != values.end();
                    if (found != negate) {
                        expected.push_back(row_ids[row]);
                    }
                }
                for (MembershipPushdownMode mode :
                     {MembershipPushdownMode::AUTO, MembershipPushdownMode::FORCE}) {
                    const VortexPredicateOptions options{mode, 64};
                    const bool pushed = Pushed(predicate, schema, options);
                    const std::vector<int64_t> candidates =
                        pushed ? expected : std::vector<int64_t>{0, 1, 2, 3};
                    ASSERT_NO_FATAL_FAILURE(
                        AssertNativeRows(test_case, predicate, candidates, false, options, pushed));
                }
            }
        }
    }
}

TEST(VortexPredicateConverterTest, TimestampUsesStoredColumnUnit) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_TRUE(Pushed(PredicateBuilder::Equal(0, "ts_ms", FieldType::TIMESTAMP,
                                               Literal(Timestamp::FromEpochMillis(1234))),
                       schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::Equal(0, "ts_ltz", FieldType::TIMESTAMP,
                                               Literal(Timestamp::FromEpochMillis(1234))),
                       schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::Equal(0, "absent", FieldType::TIMESTAMP,
                                                Literal(Timestamp::FromEpochMillis(1234))),
                        schema));
}

TEST(VortexPredicateConverterTest, TimestampComparisonsRequireLosslessRepresentableValues) {
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("seconds", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("millis", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("micros", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
        arrow::field("nanos", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("local", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai")),
    });
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    for (ComparisonBuilder build :
         {PredicateBuilder::Equal, PredicateBuilder::NotEqual, PredicateBuilder::LessThan,
          PredicateBuilder::LessOrEqual, PredicateBuilder::GreaterThan,
          PredicateBuilder::GreaterOrEqual}) {
        for (const char* name : {"seconds", "millis", "micros", "nanos"}) {
            ASSERT_TRUE(Pushed(
                build(0, name, FieldType::TIMESTAMP, Literal(Timestamp::FromEpochMillis(-1000))),
                schema));
        }
        for (const char* name : {"seconds", "millis", "micros"}) {
            ASSERT_FALSE(Pushed(
                build(0, name, FieldType::TIMESTAMP, Literal(Timestamp::FromEpochMillis(0, 1))),
                schema));
        }
        for (const char* name : {"micros", "nanos"}) {
            for (int64_t millis :
                 {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()}) {
                ASSERT_FALSE(Pushed(build(0, name, FieldType::TIMESTAMP,
                                          Literal(Timestamp::FromEpochMillis(millis))),
                                    schema));
            }
        }
        ASSERT_FALSE(Pushed(
            build(0, "local", FieldType::TIMESTAMP, Literal(Timestamp::FromEpochMillis(1000))),
            schema));
    }
}

TEST(VortexPredicateConverterTest, LosslessTimestampComparisonsFilterNativeFiles) {
    const std::vector<NativeInCase> cases = {
        {"seconds",
         arrow::timestamp(arrow::TimeUnit::SECOND),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(2000))}},
        {"millis",
         arrow::timestamp(arrow::TimeUnit::MILLI),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(2))}},
        {"micros",
         arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(0, 2000))}},
        {"nanos",
         arrow::timestamp(arrow::TimeUnit::NANO),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(0, 2))}},
    };
    for (const NativeInCase& test_case : cases) {
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case,
            PredicateBuilder::Equal(1, "value", FieldType::TIMESTAMP, test_case.literals[0]), {3}));
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeRows(test_case,
                             PredicateBuilder::GreaterOrEqual(1, "value", FieldType::TIMESTAMP,
                                                              test_case.literals[0]),
                             {2, 3}));
    }
}

TEST(VortexPredicateConverterTest, TruncatedTimestampComparisonFallsBackWithoutLosingRows) {
    const NativeInCase test_case = {"micros_fraction",
                                    arrow::timestamp(arrow::TimeUnit::MICRO),
                                    FieldType::TIMESTAMP,
                                    "[0,null,1,-1]",
                                    {}};
    const Literal one_ns(Timestamp::FromEpochMillis(0, 1));
    const std::shared_ptr<Predicate> less_than =
        PredicateBuilder::LessThan(1, "value", FieldType::TIMESTAMP, one_ns);
    ASSERT_OK_AND_ASSIGN(int32_t comparison,
                         Literal(Timestamp::FromEpochMillis(0)).CompareTo(one_ns));
    ASSERT_LT(comparison, 0);
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, less_than, {0, 1, 2, 3}, false, {}, false));

    vx_error* error = nullptr;
    ScopeGuard release_error([&error]() {
        if (error != nullptr) {
            vx_error_free(error);
        }
    });
    VxScalarPtr truncated(
        vx_scalar_new_timestamp(0, VX_TIME_UNIT_MICROSECOND, nullptr, false, &error),
        vx_scalar_free);
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(truncated, nullptr);
    // Truncating 1ns to 0us loses row 0, which the exact comparison above must retain.
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeComparisonRows(test_case, less_than, truncated.get(), VX_OPERATOR_LT, {3}));
}

TEST(VortexPredicateConverterTest, FloatingPointComparisonsFallBackForAllLiterals) {
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    for (bool single_precision : {false, true}) {
        const FieldType field_type = single_precision ? FieldType::FLOAT : FieldType::DOUBLE;
        const std::shared_ptr<arrow::Schema> schema = arrow::schema(
            {arrow::field("value", single_precision ? arrow::float32() : arrow::float64())});
        for (double value : {-0.0, 0.0, 1.5, std::numeric_limits<double>::infinity(),
                             std::numeric_limits<double>::quiet_NaN()}) {
            const Literal literal =
                single_precision ? Literal(static_cast<float>(value)) : Literal(value);
            for (ComparisonBuilder build :
                 {PredicateBuilder::Equal, PredicateBuilder::NotEqual, PredicateBuilder::LessThan,
                  PredicateBuilder::LessOrEqual, PredicateBuilder::GreaterThan,
                  PredicateBuilder::GreaterOrEqual}) {
                ASSERT_FALSE(Pushed(build(0, "value", field_type, literal), schema));
            }
        }
        ASSERT_TRUE(Pushed(PredicateBuilder::IsNull(0, "value", field_type), schema));
        ASSERT_TRUE(Pushed(PredicateBuilder::IsNotNull(0, "value", field_type), schema));
    }
}

TEST(VortexPredicateConverterTest, NativeFloatingPointOrderRequiresConservativeFallback) {
    const NativeInCase test_case = {
        "floating_order", arrow::float64(), FieldType::DOUBLE, "[NaN,null,-0.0,0.0]", {}};
    const double nan = std::numeric_limits<double>::quiet_NaN();
    ASSERT_OK_AND_ASSIGN(int32_t nan_comparison, Literal(nan).CompareTo(Literal(-nan)));
    ASSERT_EQ(nan_comparison, 0);
    ASSERT_OK_AND_ASSIGN(int32_t zero_comparison, Literal(-0.0).CompareTo(Literal(0.0)));
    ASSERT_LT(zero_comparison, 0);
    const std::shared_ptr<Predicate> equal_nan =
        PredicateBuilder::Equal(1, "value", FieldType::DOUBLE, Literal(-nan));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, equal_nan, {0, 1, 2, 3}, false, {}, false));
    VxScalarPtr negative_nan(vx_scalar_new_f64(-nan, false), vx_scalar_free);
    ASSERT_NE(negative_nan, nullptr);
    // Native totalOrder distinguishes NaN signs, while Paimon must retain row 0.
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeComparisonRows(test_case, equal_nan, negative_nan.get(), VX_OPERATOR_EQ, {}));

    const std::shared_ptr<Predicate> equal_negative_zero =
        PredicateBuilder::Equal(1, "value", FieldType::DOUBLE, Literal(-0.0));
    VxScalarPtr negative_zero(vx_scalar_new_f64(-0.0, false), vx_scalar_free);
    ASSERT_NE(negative_zero, nullptr);
    // Signed-zero ordering agrees in this case; it does not establish compatible NaN semantics.
    ASSERT_NO_FATAL_FAILURE(AssertNativeComparisonRows(test_case, equal_negative_zero,
                                                       negative_zero.get(), VX_OPERATOR_EQ, {2}));
}

TEST(VortexPredicateConverterTest, UnsafeComparisonsKeepSafeAndBranchesAndRejectNativeOr) {
    const std::vector<NativeInCase> cases = {
        {"timestamp_fraction",
         arrow::timestamp(arrow::TimeUnit::MICRO),
         FieldType::TIMESTAMP,
         "[0,null,1,-1]",
         {Literal(Timestamp::FromEpochMillis(0, 1))}},
        {"floating_nan",
         arrow::float64(),
         FieldType::DOUBLE,
         "[NaN,null,-0.0,0.0]",
         {Literal(-std::numeric_limits<double>::quiet_NaN())}},
    };
    for (const NativeInCase& test_case : cases) {
        const std::shared_ptr<Predicate> unsafe =
            PredicateBuilder::LessThan(1, "value", test_case.field_type, test_case.literals[0]);
        const std::shared_ptr<Predicate> safe =
            PredicateBuilder::LessThan(0, "rowid", FieldType::BIGINT, Literal(int64_t{3}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                             PredicateBuilder::And({unsafe, safe}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                             PredicateBuilder::Or({unsafe, safe}));
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, and_predicate, {0, 1, 2}, true));
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeRows(test_case, or_predicate, {0, 1, 2, 3}, true, {}, false));
    }
}

TEST(VortexPredicateConverterTest, UnsupportedFunctionsAndLiteralsAreNotPushed) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> starts_with,
                         PredicateBuilder::StartsWith(0, "s", FieldType::STRING,
                                                      Literal(FieldType::STRING, "a", 1)));
    ASSERT_FALSE(Pushed(starts_with, schema));
    ASSERT_FALSE(Pushed(
        PredicateBuilder::Equal(0, "bin", FieldType::BINARY, Literal(FieldType::BINARY, "x", 1)),
        schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(FieldType::INT)), schema));
}

TEST(VortexPredicateConverterTest, InSmallAndLargeListsArePushed) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    for (const int32_t size : {1, 2, 20, 21, 1024}) {
        std::vector<Literal> literals;
        for (int32_t value = 0; value < size; ++value) {
            literals.emplace_back(value);
        }
        ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, literals), schema));
    }
    ASSERT_TRUE(Pushed(
        PredicateBuilder::In(0, "i", FieldType::INT, {Literal(int32_t{1}), Literal(int32_t{1})}),
        schema));
}

TEST(VortexPredicateConverterTest, InSupportedTypesArePushed) {
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("b", arrow::boolean()),
        arrow::field("tiny", arrow::int8()),
        arrow::field("small", arrow::int16()),
        arrow::field("l", arrow::int64()),
        arrow::field("s", arrow::utf8()),
        arrow::field("dt", arrow::date32()),
        arrow::field("dec", arrow::decimal128(10, 2)),
        arrow::field("wide_dec", arrow::decimal128(19, 2)),
    });
    ASSERT_TRUE(Pushed(
        PredicateBuilder::In(0, "b", FieldType::BOOLEAN, {Literal(false), Literal(true)}), schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "tiny", FieldType::TINYINT,
                                            {Literal(int8_t{-1}), Literal(int8_t{2})}),
                       schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "small", FieldType::SMALLINT,
                                            {Literal(int16_t{-1}), Literal(int16_t{2})}),
                       schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "l", FieldType::BIGINT,
                                            {Literal(int64_t{-1}), Literal(int64_t{2})}),
                       schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "s", FieldType::STRING,
                                            {Literal(FieldType::STRING, "", 0),
                                             Literal(FieldType::STRING, "abc", 3)}),
                       schema));
    ASSERT_TRUE(Pushed(
        PredicateBuilder::In(0, "dt", FieldType::DATE,
                             {Literal(FieldType::DATE, -1), Literal(FieldType::DATE, 19000)}),
        schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "dec", FieldType::DECIMAL,
                                            {Literal(Decimal::FromUnscaledLong(123, 10, 2)),
                                             Literal(Decimal::FromUnscaledLong(-456, 10, 2))}),
                       schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "wide_dec", FieldType::DECIMAL,
                                            {Literal(Decimal::FromUnscaledLong(123, 19, 2))}),
                       schema));
}

TEST(VortexPredicateConverterTest, InAndNotInSupportedTypesFilterNativeFilesAndExcludeNullRows) {
    const Decimal::int128_t wide_value = static_cast<Decimal::int128_t>(999999999999999999LL) * 10;
    const std::vector<NativeInCase> cases = {
        {"boolean",
         arrow::boolean(),
         FieldType::BOOLEAN,
         "[true,null,false,true]",
         {Literal(true)}},
        {"tinyint",
         arrow::int8(),
         FieldType::TINYINT,
         "[-1,null,7,2]",
         {Literal(int8_t{-1}), Literal(int8_t{2})}},
        {"smallint",
         arrow::int16(),
         FieldType::SMALLINT,
         "[-1,null,7,2]",
         {Literal(int16_t{-1}), Literal(int16_t{2})}},
        {"int",
         arrow::int32(),
         FieldType::INT,
         "[-1,null,7,2]",
         {Literal(int32_t{-1}), Literal(int32_t{2})}},
        {"bigint",
         arrow::int64(),
         FieldType::BIGINT,
         "[-1,null,7,2]",
         {Literal(int64_t{-1}), Literal(int64_t{2})}},
        {"string",
         arrow::utf8(),
         FieldType::STRING,
         R"(["first",null,"miss","last"])",
         {Literal(FieldType::STRING, "first", 5), Literal(FieldType::STRING, "last", 4)}},
        {"date",
         arrow::date32(),
         FieldType::DATE,
         "[-1,null,7,19000]",
         {Literal(FieldType::DATE, -1), Literal(FieldType::DATE, 19000)}},
        {"decimal",
         arrow::decimal128(10, 2),
         FieldType::DECIMAL,
         R"(["-1.23",null,"7.00","4.56"])",
         {Literal(Decimal::FromUnscaledLong(-123, 10, 2)),
          Literal(Decimal::FromUnscaledLong(456, 10, 2))}},
        {"wide_decimal",
         arrow::decimal128(19, 2),
         FieldType::DECIMAL,
         R"(["-99999999999999999.90",null,"7.00","99999999999999999.90"])",
         {Literal(Decimal(19, 2, -wide_value)), Literal(Decimal(19, 2, wide_value))}},
        {"timestamp_seconds",
         arrow::timestamp(arrow::TimeUnit::SECOND),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(-1000)), Literal(Timestamp::FromEpochMillis(2000))}},
        {"timestamp_millis",
         arrow::timestamp(arrow::TimeUnit::MILLI),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(-1)), Literal(Timestamp::FromEpochMillis(2))}},
        {"timestamp_micros",
         arrow::timestamp(arrow::TimeUnit::MICRO),
         FieldType::TIMESTAMP,
         "[-1,null,7000,2001]",
         {Literal(Timestamp::FromEpochMillis(-1, 999000)),
          Literal(Timestamp::FromEpochMillis(2, 1000))}},
        {"timestamp_nanos",
         arrow::timestamp(arrow::TimeUnit::NANO),
         FieldType::TIMESTAMP,
         "[-1,null,7000000,2000001]",
         {Literal(Timestamp::FromEpochMillis(-1, 999999)),
          Literal(Timestamp::FromEpochMillis(2, 1))}},
        {"timestamp_utc",
         arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
         FieldType::TIMESTAMP,
         "[-1,null,7000,2001]",
         {Literal(Timestamp::FromEpochMillis(-1, 999000)),
          Literal(Timestamp::FromEpochMillis(2, 1000))}},
    };
    for (const NativeInCase& test_case : cases) {
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case, PredicateBuilder::In(1, "value", test_case.field_type, test_case.literals),
            {0, 3}));
        // The null row (rowid=1) must not reappear when the membership test is negated.
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case,
            PredicateBuilder::NotIn(1, "value", test_case.field_type, test_case.literals), {2}));
    }
}

TEST(VortexPredicateConverterTest, NotInSmallAndLargeListsFilterNativeFiles) {
    const NativeInCase test_case = {
        "not_in_list_sizes", arrow::int32(), FieldType::INT, "[0,null,2048,1]", {}};
    for (const int32_t size : {1, 2, 20, 21, 100, 1024}) {
        SCOPED_TRACE(size);
        std::vector<Literal> literals;
        for (int32_t value = 0; value < size; ++value) {
            literals.emplace_back(value);
        }
        const std::vector<int64_t> expected_rows =
            size == 1 ? std::vector<int64_t>{2, 3} : std::vector<int64_t>{2};
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case, PredicateBuilder::NotIn(1, "value", FieldType::INT, literals),
            expected_rows));
    }
}

TEST(VortexPredicateConverterTest, IntegerRangesAtTypeLimitsFilterNativeFiles) {
    const std::vector<NativeInCase> cases = {
        IntegerRangeBoundaryCase<int8_t>("tinyint_range_limits", arrow::int8(), FieldType::TINYINT,
                                         "[-128,null,0,127]"),
        IntegerRangeBoundaryCase<int16_t>("smallint_range_limits", arrow::int16(),
                                          FieldType::SMALLINT, "[-32768,null,0,32767]"),
        IntegerRangeBoundaryCase<int32_t>("int_range_limits", arrow::int32(), FieldType::INT,
                                          "[-2147483648,null,0,2147483647]"),
        IntegerRangeBoundaryCase<int64_t>("bigint_range_limits", arrow::int64(), FieldType::BIGINT,
                                          "[-9223372036854775808,null,0,9223372036854775807]"),
    };
    for (const NativeInCase& test_case : cases) {
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case, PredicateBuilder::In(1, "value", test_case.field_type, test_case.literals),
            {0, 3}));
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case,
            PredicateBuilder::NotIn(1, "value", test_case.field_type, test_case.literals), {2}));
    }
}

TEST(VortexPredicateConverterTest, IntegerRangesPreserveGapsAndSingletonsInNativeFiles) {
    NativeInCase test_case = {
        "integer_ranges_with_gap", arrow::int32(), FieldType::INT, "[-7,null,0,6]", {}};
    for (int32_t value = -8; value <= -1; ++value) {
        test_case.literals.emplace_back(value);
    }
    for (int32_t value = 3; value <= 8; ++value) {
        test_case.literals.emplace_back(value);
    }
    test_case.literals.emplace_back(int32_t{20});
    test_case.literals.emplace_back(int32_t{-8});
    std::reverse(test_case.literals.begin(), test_case.literals.end());
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::In(1, "value", FieldType::INT, test_case.literals), {0, 3}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::NotIn(1, "value", FieldType::INT, test_case.literals), {2}));
    test_case.name = "integer_ranges_singleton";
    test_case.values_json = "[-7,null,20,0]";
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::In(1, "value", FieldType::INT, test_case.literals), {0, 2}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::NotIn(1, "value", FieldType::INT, test_case.literals), {3}));
}

TEST(VortexPredicateConverterTest, FullIntegerDomainAndDuplicateSingletonFilterNativeFiles) {
    NativeInCase test_case = {
        "tinyint_full_domain", arrow::int8(), FieldType::TINYINT, "[-128,null,0,127]", {}};
    // The loop uses int32_t so incrementing the maximum TINYINT cannot wrap.
    for (int32_t value = std::numeric_limits<int8_t>::min();
         value <= std::numeric_limits<int8_t>::max(); ++value) {
        test_case.literals.emplace_back(static_cast<int8_t>(value));
    }
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::In(1, "value", FieldType::TINYINT, test_case.literals),
        {0, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::NotIn(1, "value", FieldType::TINYINT, test_case.literals),
        {}));
    test_case.name = "duplicate_singleton";
    test_case.literals = std::vector<Literal>(8, Literal(int8_t{-128}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::In(1, "value", FieldType::TINYINT, test_case.literals), {0}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::NotIn(1, "value", FieldType::TINYINT, test_case.literals),
        {2, 3}));
}

TEST(VortexPredicateConverterTest, IntegerRangesComposeWithNativePredicatesAndProjection) {
    NativeInCase test_case = {
        "integer_range_compound", arrow::int32(), FieldType::INT, "[-4,null,8,4]", {}};
    for (int32_t value = -4; value <= 4; ++value) {
        test_case.literals.emplace_back(value);
    }
    const std::shared_ptr<Predicate> in =
        PredicateBuilder::In(1, "value", FieldType::INT, test_case.literals);
    const std::shared_ptr<Predicate> not_in =
        PredicateBuilder::NotIn(1, "value", FieldType::INT, test_case.literals);
    const std::shared_ptr<Predicate> first_row =
        PredicateBuilder::Equal(0, "rowid", FieldType::BIGINT, Literal(int64_t{0}));
    const std::shared_ptr<Predicate> first_three =
        PredicateBuilder::LessThan(0, "rowid", FieldType::BIGINT, Literal(int64_t{3}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> in_and,
                         PredicateBuilder::And({in, first_three}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, in_and, {0}, true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> not_in_and,
                         PredicateBuilder::And({not_in, first_three}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in_and, {2}, true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> in_or, PredicateBuilder::Or({in, first_row}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, in_or, {0, 3}, true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> not_in_or,
                         PredicateBuilder::Or({not_in, first_row}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in_or, {0, 2}, true));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> not_in_or_null,
        PredicateBuilder::Or({not_in, PredicateBuilder::IsNull(1, "value", FieldType::INT)}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in_or_null, {1, 2}, true));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, in, {0, 3}, true));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in, {2}, true));
}

TEST(VortexPredicateConverterTest, CompressibleIntegerListsStillValidateEveryLiteral) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> safe;
    for (int32_t value = 0; value < 16; ++value) {
        safe.emplace_back(value);
    }
    for (const Literal& unsafe : {Literal(FieldType::INT), Literal(int64_t{16})}) {
        for (size_t position : {size_t{0}, size_t{8}, safe.size()}) {
            std::vector<Literal> literals = safe;
            literals.insert(literals.begin() + position, unsafe);
            ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, literals), schema));
            ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, literals), schema));
        }
    }
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, safe),
                        arrow::schema({arrow::field("i", arrow::int64())})));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, safe),
                        arrow::schema({arrow::field("i", arrow::int64())})));
}

TEST(VortexPredicateConverterTest, NotInDuplicatesMissesAndAllExcludedFilterNativeFiles) {
    const NativeInCase test_case = {
        "not_in_edges", arrow::int32(), FieldType::INT, "[-1,null,7,2]", {}};
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case,
        PredicateBuilder::NotIn(1, "value", FieldType::INT,
                                {Literal(int32_t{-1}), Literal(int32_t{-1}), Literal(int32_t{2})}),
        {2}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::NotIn(1, "value", FieldType::INT, {Literal(int32_t{99})}),
        {0, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case,
        PredicateBuilder::NotIn(1, "value", FieldType::INT,
                                {Literal(int32_t{-1}), Literal(int32_t{7}), Literal(int32_t{2})}),
        {}));
    const NativeInCase all_null = {
        "not_in_all_null", arrow::int32(), FieldType::INT, "[null,null,null,null]", {}};
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        all_null, PredicateBuilder::NotIn(1, "value", FieldType::INT, {Literal(int32_t{1})}), {}));
}

TEST(VortexPredicateConverterTest, NotInCompoundPredicatesAndProjectionFilterNativeFiles) {
    const NativeInCase test_case = {
        "not_in_compound", arrow::int32(), FieldType::INT, "[-1,null,7,2]", {}};
    const std::shared_ptr<Predicate> not_in = PredicateBuilder::NotIn(
        1, "value", FieldType::INT, {Literal(int32_t{-1}), Literal(int32_t{2})});
    const std::shared_ptr<Predicate> first_row =
        PredicateBuilder::Equal(0, "rowid", FieldType::BIGINT, Literal(int64_t{0}));
    const std::shared_ptr<Predicate> first_three =
        PredicateBuilder::LessThan(0, "rowid", FieldType::BIGINT, Literal(int64_t{3}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({not_in, first_three}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, and_predicate, {2}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({not_in, first_row}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, or_predicate, {0, 2}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> or_null,
        PredicateBuilder::Or({not_in, PredicateBuilder::IsNull(1, "value", FieldType::INT)}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, or_null, {1, 2}));
    // The native filter still works when the filtering column is omitted from the projection.
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in, {2}, true));

    const std::shared_ptr<Predicate> unsupported = PredicateBuilder::NotIn(
        1, "value", FieldType::INT, {Literal(int32_t{7}), Literal(FieldType::INT)});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_partial,
                         PredicateBuilder::And({not_in, unsupported}));
    // The residual must evaluate the NULL-containing list; native AND keeps only the safe child.
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, and_partial, {2}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_partial,
                         PredicateBuilder::Or({not_in, unsupported}));
    ASSERT_FALSE(Pushed(or_partial, arrow::schema({arrow::field("rowid", arrow::int64()),
                                                   arrow::field("value", arrow::int32())})));
}

TEST(VortexPredicateConverterTest, NotInUnsafeLiteralsAreNotPushed) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, {}), schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, {Literal(FieldType::INT)}), schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT,
                                                {Literal(int32_t{1}), Literal(FieldType::INT)}),
                        schema));
    ASSERT_FALSE(Pushed(
        PredicateBuilder::NotIn(0, "i", FieldType::INT, {Literal(int32_t{1}), Literal(int64_t{2})}),
        schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::NotIn(0, "i", FieldType::BIGINT, {Literal(int64_t{1})}), schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "absent", FieldType::INT, {Literal(int32_t{1})}),
                        schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, {Literal(int32_t{1})}), nullptr));
    ASSERT_FALSE(Pushed(
        PredicateBuilder::NotIn(0, "bin", FieldType::BINARY, {Literal(FieldType::BINARY, "x", 1)}),
        arrow::schema({arrow::field("bin", arrow::binary())})));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "f", FieldType::FLOAT, {Literal(1.5F)}),
                        arrow::schema({arrow::field("f", arrow::float32())})));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "d", FieldType::DOUBLE, {Literal(1.5)}),
                        arrow::schema({arrow::field("d", arrow::float64())})));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "dec", FieldType::DECIMAL,
                                                {Literal(Decimal::FromUnscaledLong(123, 10, 3))}),
                        arrow::schema({arrow::field("dec", arrow::decimal128(10, 2))})));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::NotIn(0, "ts", FieldType::TIMESTAMP,
                                       {Literal(Timestamp::FromEpochMillis(1, 1))}),
               arrow::schema({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MILLI))})));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::NotIn(
                   0, "ts", FieldType::TIMESTAMP,
                   {Literal(Timestamp::FromEpochMillis(std::numeric_limits<int64_t>::max()))}),
               arrow::schema({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::NANO))})));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "ts", FieldType::TIMESTAMP,
                                                {Literal(Timestamp::FromEpochMillis(1000))}),
                        arrow::schema({arrow::field(
                            "ts", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai"))})));
}

TEST(VortexPredicateConverterTest, InUnsupportedOrInconsistentLiteralsAreNotPushed) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, {}), schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::In(0, "i", FieldType::INT, {Literal(FieldType::INT)}), schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT,
                                             {Literal(int32_t{1}), Literal(FieldType::INT)}),
                        schema));
    ASSERT_FALSE(Pushed(
        PredicateBuilder::In(0, "i", FieldType::INT, {Literal(int32_t{1}), Literal(int64_t{2})}),
        schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::In(0, "i", FieldType::BIGINT, {Literal(int64_t{1})}), schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::In(0, "absent", FieldType::INT, {Literal(int32_t{1})}), schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::In(0, "i", FieldType::INT, {Literal(int32_t{1})}), nullptr));
    ASSERT_FALSE(Pushed(
        PredicateBuilder::In(0, "bin", FieldType::BINARY, {Literal(FieldType::BINARY, "x", 1)}),
        arrow::schema({arrow::field("bin", arrow::binary())})));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "f", FieldType::FLOAT, {Literal(1.5F)}),
                        arrow::schema({arrow::field("f", arrow::float32())})));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "d", FieldType::DOUBLE, {Literal(1.5)}),
                        arrow::schema({arrow::field("d", arrow::float64())})));
    const std::shared_ptr<arrow::Schema> decimal_schema =
        arrow::schema({arrow::field("dec", arrow::decimal128(10, 2))});
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "dec", FieldType::DECIMAL,
                                             {Literal(Decimal::FromUnscaledLong(123, 10, 2)),
                                              Literal(Decimal::FromUnscaledLong(123, 10, 3))}),
                        decimal_schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "dec", FieldType::DECIMAL,
                                             {Literal(Decimal::FromUnscaledLong(123, 11, 2))}),
                        decimal_schema));
}

TEST(VortexPredicateConverterTest, InTimestampsRequireMatchingUnitsAndLosslessValues) {
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("seconds", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("millis", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("micros", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
        arrow::field("nanos", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("local", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai")),
    });
    for (const char* name : {"seconds", "millis", "micros", "nanos"}) {
        ASSERT_TRUE(Pushed(PredicateBuilder::In(0, name, FieldType::TIMESTAMP,
                                                {Literal(Timestamp::FromEpochMillis(-1000)),
                                                 Literal(Timestamp::FromEpochMillis(2000))}),
                           schema));
    }
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "micros", FieldType::TIMESTAMP,
                                            {Literal(Timestamp::FromEpochMillis(-1, 1000))}),
                       schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "nanos", FieldType::TIMESTAMP,
                                            {Literal(Timestamp::FromEpochMillis(-1, 1))}),
                       schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "seconds", FieldType::TIMESTAMP,
                                             {Literal(Timestamp::FromEpochMillis(1))}),
                        schema));
    for (const char* name : {"millis", "micros"}) {
        ASSERT_FALSE(Pushed(PredicateBuilder::In(0, name, FieldType::TIMESTAMP,
                                                 {Literal(Timestamp::FromEpochMillis(1, 1))}),
                            schema));
    }
    for (const char* name : {"micros", "nanos"}) {
        ASSERT_FALSE(
            Pushed(PredicateBuilder::In(
                       0, name, FieldType::TIMESTAMP,
                       {Literal(Timestamp::FromEpochMillis(std::numeric_limits<int64_t>::max()))}),
                   schema));
        ASSERT_FALSE(
            Pushed(PredicateBuilder::In(
                       0, name, FieldType::TIMESTAMP,
                       {Literal(Timestamp::FromEpochMillis(std::numeric_limits<int64_t>::min()))}),
                   schema));
    }
    // The millisecond multiplication can fit while adding the fractional part overflows.
    const int64_t maximum = std::numeric_limits<int64_t>::max();
    ASSERT_FALSE(
        Pushed(PredicateBuilder::In(
                   0, "micros", FieldType::TIMESTAMP,
                   {Literal(Timestamp::FromEpochMillis(
                       maximum / 1000, static_cast<int32_t>((maximum % 1000 + 1) * 1000)))}),
               schema));
    ASSERT_FALSE(Pushed(
        PredicateBuilder::In(0, "nanos", FieldType::TIMESTAMP,
                             {Literal(Timestamp::FromEpochMillis(
                                 maximum / 1000000, static_cast<int32_t>(maximum % 1000000 + 1)))}),
        schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "local", FieldType::TIMESTAMP,
                                             {Literal(Timestamp::FromEpochMillis(1000))}),
                        schema));
}

TEST(VortexPredicateConverterTest, AndDropsUnsupportedChildrenButOrRejects) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    const std::shared_ptr<Predicate> supported =
        PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(static_cast<int32_t>(1)));
    const std::shared_ptr<Predicate> other =
        PredicateBuilder::In(0, "i", FieldType::INT,
                             {Literal(static_cast<int32_t>(2)), Literal(static_cast<int32_t>(3))});
    const std::shared_ptr<Predicate> unsupported = PredicateBuilder::In(
        0, "i", FieldType::INT, {Literal(static_cast<int32_t>(1)), Literal(FieldType::INT)});

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_partial,
                         PredicateBuilder::And({supported, unsupported}));
    ASSERT_TRUE(Pushed(and_partial, schema));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_none,
                         PredicateBuilder::And({unsupported, unsupported}));
    ASSERT_FALSE(Pushed(and_none, schema));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_full,
                         PredicateBuilder::Or({supported, other}));
    ASSERT_TRUE(Pushed(or_full, schema));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_partial,
                         PredicateBuilder::Or({supported, unsupported}));
    ASSERT_FALSE(Pushed(or_partial, schema));
}

TEST(VortexPredicateConverterTest, MembershipListBudgetCountsBooleanNodesAndNullGuards) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> literals;
    for (int32_t value = 0; value < 33; ++value) {
        const std::string text = fmt::format("value-{}", value);
        literals.emplace_back(FieldType::STRING, text.data(), text.size());
    }
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(1, "s", FieldType::STRING, literals)
                   : PredicateBuilder::In(1, "s", FieldType::STRING, literals);
        // ListContains lowers to 33 EQ + 32 OR nodes. NOT IN adds NOT plus the 3-node null guard.
        const uint32_t nodes = 65 + (negate ? 4 : 0);
        ASSERT_FALSE(Pushed(predicate, schema));
        ASSERT_FALSE(Pushed(predicate, schema, {MembershipPushdownMode::AUTO, nodes - 1}));
        ASSERT_TRUE(Pushed(predicate, schema, {MembershipPushdownMode::AUTO, nodes}));
        ASSERT_TRUE(Pushed(predicate, schema, {MembershipPushdownMode::FORCE, 0}));
        ASSERT_FALSE(Pushed(predicate, schema, {MembershipPushdownMode::OFF, nodes}));
    }
    literals.erase(literals.begin() + 32, literals.end());
    ASSERT_TRUE(Pushed(PredicateBuilder::In(1, "s", FieldType::STRING, literals), schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(1, "s", FieldType::STRING, literals), schema));
    literals.erase(literals.begin() + 31, literals.end());
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(1, "s", FieldType::STRING, literals), schema));
    literals.erase(literals.begin() + 30, literals.end());
    ASSERT_TRUE(Pushed(PredicateBuilder::NotIn(1, "s", FieldType::STRING, literals), schema));
    literals.erase(literals.begin() + 21, literals.end());
    ASSERT_TRUE(Pushed(PredicateBuilder::In(1, "s", FieldType::STRING, literals), schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::NotIn(1, "s", FieldType::STRING, literals), schema));
}

TEST(VortexPredicateConverterTest, MembershipDeduplicatesBeforeApplyingBudget) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> strings(4096, Literal(FieldType::STRING, "same", 4));
    strings.emplace_back(FieldType::STRING, "other", 5);
    const std::shared_ptr<Predicate> in = PredicateBuilder::In(1, "s", FieldType::STRING, strings);
    const std::shared_ptr<Predicate> not_in =
        PredicateBuilder::NotIn(1, "s", FieldType::STRING, strings);
    ASSERT_FALSE(Pushed(in, schema, {MembershipPushdownMode::AUTO, 2}));
    ASSERT_TRUE(Pushed(in, schema, {MembershipPushdownMode::AUTO, 3}));
    ASSERT_FALSE(Pushed(not_in, schema, {MembershipPushdownMode::AUTO, 6}));
    ASSERT_TRUE(Pushed(not_in, schema, {MembershipPushdownMode::AUTO, 7}));

    // Raw list length is not the decision: 4096 duplicates do not consume native nodes.
    for (int32_t unique_count : {21, 32, 33}) {
        std::vector<Literal> repeated(4096, Literal(FieldType::STRING, "same", 4));
        for (int32_t value = 1; value < unique_count; ++value) {
            const std::string text = fmt::format("value-{}", value);
            repeated.emplace_back(FieldType::STRING, text.data(), text.size());
        }
        ASSERT_EQ(Pushed(PredicateBuilder::In(1, "s", FieldType::STRING, repeated), schema),
                  unique_count <= 32);
        ASSERT_EQ(Pushed(PredicateBuilder::NotIn(1, "s", FieldType::STRING, repeated), schema),
                  unique_count <= 30);
    }

    const std::vector<Literal> integers(4096, Literal(int32_t{7}));
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, integers), schema,
                       {MembershipPushdownMode::AUTO, 1}));
    // The integer complement is one NEQ plus the 3-node null guard.
    ASSERT_TRUE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, integers), schema,
                       {MembershipPushdownMode::AUTO, 4}));
}

TEST(VortexPredicateConverterTest, MembershipKeepsHugeContinuousSetsButBoundsManyRanges) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> literals;
    for (int32_t value = 0; value < 65536; ++value) {
        literals.emplace_back(value);
    }
    const std::shared_ptr<Predicate> in = PredicateBuilder::In(0, "i", FieldType::INT, literals);
    const std::shared_ptr<Predicate> not_in =
        PredicateBuilder::NotIn(0, "i", FieldType::INT, literals);
    ASSERT_TRUE(Pushed(in, schema));
    ASSERT_TRUE(Pushed(not_in, schema));
    ASSERT_FALSE(Pushed(in, schema, {MembershipPushdownMode::AUTO, 2}));
    ASSERT_TRUE(Pushed(in, schema, {MembershipPushdownMode::AUTO, 3}));
    ASSERT_FALSE(Pushed(not_in, schema, {MembershipPushdownMode::AUTO, 5}));
    ASSERT_TRUE(Pushed(not_in, schema, {MembershipPushdownMode::AUTO, 6}));

    literals.clear();
    for (int32_t start = 0; start < 200; start += 10) {
        for (int32_t value = start; value < start + 8; ++value) {
            literals.emplace_back(value);
        }
    }
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, literals)
                   : PredicateBuilder::In(0, "i", FieldType::INT, literals);
        // 20 ranges, each 3 nodes, joined by 19 connectives; NOT IN adds the null guard.
        const uint32_t nodes = 79 + (negate ? 3 : 0);
        ASSERT_FALSE(Pushed(predicate, schema));
        ASSERT_FALSE(Pushed(predicate, schema, {MembershipPushdownMode::AUTO, nodes - 1}));
        ASSERT_TRUE(Pushed(predicate, schema, {MembershipPushdownMode::AUTO, nodes}));
        ASSERT_TRUE(Pushed(predicate, schema, {MembershipPushdownMode::FORCE, 0}));
    }
}

TEST(VortexPredicateConverterTest, MembershipBudgetCountsMixedRangesAndSingletons) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> literals = {Literal(int32_t{20}), Literal(int32_t{50})};
    for (int32_t value = 1; value <= 10; ++value) {
        literals.emplace_back(value);
        literals.emplace_back(value + 29);
    }
    // Two ranges + two singletons + three connectives = 11 nodes.
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, literals)
                   : PredicateBuilder::In(0, "i", FieldType::INT, literals);
        const uint32_t nodes = 11 + (negate ? 3 : 0);
        ASSERT_FALSE(Pushed(predicate, schema, {MembershipPushdownMode::AUTO, nodes - 1}));
        ASSERT_TRUE(Pushed(predicate, schema, {MembershipPushdownMode::AUTO, nodes}));
    }
}

TEST(VortexPredicateConverterTest, ForceMembershipStillRejectsUnsafeLiterals) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    const VortexPredicateOptions force{MembershipPushdownMode::FORCE, 0};
    std::vector<Literal> literals;
    for (int32_t value = 0; value < 1024; ++value) {
        literals.emplace_back(value);
    }
    for (const Literal& unsafe : {Literal(FieldType::INT), Literal(int64_t{1024})}) {
        std::vector<Literal> invalid = literals;
        invalid.push_back(unsafe);
        ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, invalid), schema, force));
        ASSERT_FALSE(
            Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, invalid), schema, force));
    }
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, literals),
                        arrow::schema({arrow::field("i", arrow::int64())}), force));
    for (bool negate : {false, true}) {
        const std::vector<Literal> timestamps = {
            Literal(Timestamp::FromEpochMillis(0)),
            Literal(Timestamp::FromEpochMillis(std::numeric_limits<int64_t>::max()))};
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "ts", FieldType::TIMESTAMP, timestamps)
                   : PredicateBuilder::In(0, "ts", FieldType::TIMESTAMP, timestamps);
        ASSERT_FALSE(Pushed(
            predicate, arrow::schema({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::NANO))}),
            force));
    }
}

TEST(VortexPredicateConverterTest, MembershipOffPreservesOtherNativePredicates) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    const VortexPredicateOptions off{MembershipPushdownMode::OFF, 0};
    const std::shared_ptr<Predicate> equal =
        PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(int32_t{1}));
    const std::shared_ptr<Predicate> in =
        PredicateBuilder::In(0, "i", FieldType::INT, {Literal(int32_t{1})});
    ASSERT_FALSE(Pushed(in, schema, off));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, {Literal(int32_t{1})}),
                        schema, off));
    ASSERT_TRUE(Pushed(equal, schema, off));
    ASSERT_TRUE(Pushed(PredicateBuilder::IsNull(0, "i", FieldType::INT), schema, off));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({in, equal}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({in, equal}));
    ASSERT_TRUE(Pushed(and_predicate, schema, off));
    ASSERT_FALSE(Pushed(or_predicate, schema, off));
}

TEST(VortexPredicateConverterTest, SparseMembershipModesReturnNativeOrCandidateRows) {
    NativeInCase test_case = {"sparse_budget", arrow::int32(), FieldType::INT, "[0,null,63,2]", {}};
    for (int32_t value = 0; value <= 64; value += 2) {
        test_case.literals.emplace_back(value);
    }
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(1, "value", FieldType::INT, test_case.literals)
                   : PredicateBuilder::In(1, "value", FieldType::INT, test_case.literals);
        const std::vector<int64_t> expected =
            negate ? std::vector<int64_t>{2} : std::vector<int64_t>{0, 3};
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeRows(test_case, predicate, {0, 1, 2, 3}, true, {}, false));
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, predicate, expected, true,
                                                 {MembershipPushdownMode::FORCE, 0}));
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, predicate, {0, 1, 2, 3}, true,
                                                 {MembershipPushdownMode::OFF}, false));
    }
}

TEST(VortexPredicateConverterTest, StringMembershipModesAndPartialAndOrUseSafeCandidates) {
    NativeInCase test_case = {
        "string_budget", arrow::utf8(), FieldType::STRING, R"(["s0",null,"absent","s32"])", {}};
    for (int32_t value = 0; value < 33; ++value) {
        const std::string text = fmt::format("s{}", value);
        test_case.literals.emplace_back(FieldType::STRING, text.data(), text.size());
    }
    const std::shared_ptr<Predicate> in =
        PredicateBuilder::In(1, "value", FieldType::STRING, test_case.literals);
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, in, {0, 1, 2, 3}, true, {}, false));
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeRows(test_case, in, {0, 3}, true, {MembershipPushdownMode::FORCE, 0}));
    const std::shared_ptr<Predicate> less_than_three =
        PredicateBuilder::LessThan(0, "rowid", FieldType::BIGINT, Literal(int64_t{3}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({in, less_than_three}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({in, less_than_three}));
    // AND keeps the other child, so the residual receives a safe superset, including null rows.
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, and_predicate, {0, 1, 2}, true));
    // Dropping only the IN child of OR would lose row 3; reject the entire native OR instead.
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeRows(test_case, or_predicate, {0, 1, 2, 3}, true, {}, false));
}

TEST(VortexPredicateConverterTest, NullPredicateIsNotPushed) {
    ASSERT_FALSE(Pushed(nullptr, TestSchema()));
}

}  // namespace
}  // namespace paimon::vortex::test
