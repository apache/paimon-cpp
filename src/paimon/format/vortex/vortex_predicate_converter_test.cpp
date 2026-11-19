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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/vortex/vortex_defs.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::vortex::test {
namespace {

constexpr char kInvalidUtf8Error[] =
    "create Vortex predicate scalar: Other error: invalid utf-8: "
    "invalid utf-8 sequence of 1 bytes from index 0";

std::shared_ptr<arrow::Schema> TestSchema() {
    return arrow::schema({
        arrow::field("i", arrow::int32()),
        arrow::field("s", arrow::utf8()),
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
    const Result<VxExpressionPtr> result =
        VortexPredicateConverter::Convert(predicate, schema, options);
    EXPECT_OK(result.status());
    return result.ok() && result.value() != nullptr;
}

struct NativeScanCase {
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

// Check rows returned by the native scan, then optionally check the Paimon residual filter.
void AssertNativeRows(
    const NativeScanCase& test_case, const std::shared_ptr<Predicate>& predicate,
    const std::vector<int64_t>& expected_rows,
    const std::optional<std::vector<int64_t>>& expected_residual_rows = std::nullopt,
    const VortexPredicateOptions& options = {}, bool expect_native_filter = true,
    bool project_rowid_only = false, const vx_expression* native_filter_override = nullptr) {
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
    VxSessionPtr session(vx_session_new());
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
    VxArrayPtr array(vx_array_from_arrow(&exported_array, &exported_schema, false, &error));
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(array, nullptr);
    LocalFileSystem file_system;
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output,
                             file_system.Create(path, /*overwrite=*/false));
        auto output_context = std::make_shared<VortexOutputContext>(output);
        vx_callback_sink* sink = nullptr;
        bool output_closed = false;
        ScopeGuard release_sink([&sink, &output_context, &output, &output_closed]() {
            if (!output_closed) {
                output_context->MarkAborted();
                if (sink != nullptr) {
                    vx_callback_sink_abort(std::exchange(sink, nullptr));
                }
                output_context->WaitForIdleCallbacks();
                [[maybe_unused]] Status close_status = output->Close();
            }
        });
        // Use the production callback I/O path, keeping Rust's asynchronous filesystem adapter
        // out of this predicate test. The array owns the borrowed dtype until the sink closes.
        const vx_dtype* dtype = vx_array_dtype(array.get());
        ASSERT_NE(dtype, nullptr);
        sink = vx_callback_sink_open(
            session.get(), VortexOutputContext::MakeCallbacks(output_context), dtype, &error);
        ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
        ASSERT_NE(sink, nullptr);
        vx_callback_sink_push(sink, array.get(), &error);
        ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
        // Closing consumes the sink on both success and failure.
        vx_callback_sink_close(std::exchange(sink, nullptr), &error);
        ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
        ASSERT_OK(output_context->GetCallbackStatus());
        output_closed = true;
        ASSERT_OK(output->Close());
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system.Open(path));
    ASSERT_OK_AND_ASSIGN(int64_t length, input->Length());
    auto input_context = std::make_shared<VortexInputContext>(input);
    VxDataSourcePtr source(vx_data_source_new_callback_with_coalesce(
        session.get(), VortexInputContext::MakeCallbacks(input_context),
        static_cast<uint64_t>(length), kDefaultVortexReadIoCoalesceMaxGap,
        kDefaultVortexReadIoCoalesceMaxSize, &error));
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(source, nullptr);
    ASSERT_OK_AND_ASSIGN(VxExpressionPtr filter,
                         VortexPredicateConverter::Convert(predicate, schema, options));
    ASSERT_EQ(filter != nullptr, expect_native_filter);
    vx_scan_options scan_options{};
    scan_options.filter = native_filter_override == nullptr ? filter.get() : native_filter_override;
    VxExpressionPtr root;
    VxExpressionPtr projection;
    if (project_rowid_only) {
        root = VxExpressionPtr(vx_expression_root());
        ASSERT_NE(root, nullptr);
        const char* names[] = {"rowid"};
        projection = VxExpressionPtr(vx_expression_select(names, 1, root.get()));
        ASSERT_NE(projection, nullptr);
        scan_options.projection = projection.get();
    }
    VxScanPtr scan(vx_data_source_scan(source.get(), &scan_options, nullptr, &error));
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(scan, nullptr);
    std::vector<int64_t> row_ids;
    std::vector<int64_t> residual_row_ids;
    while (true) {
        VxPartitionPtr partition(vx_scan_next_partition(scan.get(), &error));
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
            std::vector<char> mask;
            if (expected_residual_rows) {
                ASSERT_FALSE(project_rowid_only);
                const std::shared_ptr<PredicateFilter> residual =
                    std::dynamic_pointer_cast<PredicateFilter>(predicate);
                ASSERT_NE(residual, nullptr);
                const auto rows =
                    arrow::StructArray::Make(batch->columns(), batch->schema()->fields());
                ASSERT_TRUE(rows.ok()) << rows.status().ToString();
                ASSERT_OK_AND_ASSIGN(
                    mask, residual->Test(*rows.ValueOrDie(), arrow::default_memory_pool()));
                ASSERT_EQ(mask.size(), static_cast<size_t>(batch->num_rows()));
            }
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                ASSERT_FALSE(typed_ids.IsNull(row));
                const int64_t row_id = typed_ids.Value(row);
                row_ids.push_back(row_id);
                if (expected_residual_rows) {
                    ASSERT_GE(row_id, 0);
                    ASSERT_LT(row_id, values_result.ValueOrDie()->length());
                    const auto actual = batch->column(1)->GetScalar(row);
                    const auto expected = values_result.ValueOrDie()->GetScalar(row_id);
                    ASSERT_TRUE(actual.ok());
                    ASSERT_TRUE(expected.ok());
                    ASSERT_TRUE(actual.ValueOrDie()->Equals(*expected.ValueOrDie()));
                    if (mask[row] != 0) {
                        residual_row_ids.push_back(row_id);
                    }
                }
            }
        }
    }
    std::sort(row_ids.begin(), row_ids.end());
    ASSERT_EQ(row_ids, expected_rows);
    if (expected_residual_rows) {
        std::sort(residual_row_ids.begin(), residual_row_ids.end());
        ASSERT_EQ(residual_row_ids, *expected_residual_rows);
    }
}

// Compare an unsafe direct native expression with the converter's conservative fallback.
void AssertNativeComparisonRows(const NativeScanCase& test_case,
                                const std::shared_ptr<Predicate>& predicate,
                                const vx_scalar* scalar, vx_binary_operator op,
                                const std::vector<int64_t>& expected_rows) {
    VxExpressionPtr root(vx_expression_root());
    ASSERT_NE(root, nullptr);
    VxExpressionPtr column(vx_expression_get_item("value", root.get()));
    ASSERT_NE(column, nullptr);
    vx_error* error = nullptr;
    ScopeGuard release_error([&error]() {
        if (error != nullptr) {
            vx_error_free(error);
        }
    });
    VxExpressionPtr literal(vx_expression_literal(scalar, &error));
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(literal, nullptr);
    VxExpressionPtr filter(vx_expression_binary(op, column.get(), literal.get()));
    ASSERT_NE(filter, nullptr);
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, predicate, expected_rows, std::nullopt, {},
                                             /*expect_native_filter=*/false,
                                             /*project_rowid_only=*/false, filter.get()));
}

template <typename T>
NativeScanCase IntegerRangeBoundaryCase(const char* name,
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
}  // namespace

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
    const auto floating_schema =
        arrow::schema({arrow::field("f", arrow::float32()), arrow::field("d", arrow::float64())});
    ASSERT_TRUE(Pushed(PredicateBuilder::IsNull(0, "f", FieldType::FLOAT), floating_schema));
    ASSERT_TRUE(Pushed(PredicateBuilder::IsNotNull(1, "d", FieldType::DOUBLE), floating_schema));
}

TEST(VortexPredicateConverterTest, UnknownOrAmbiguousFileColumnsFallBack) {
    const auto equal = PredicateBuilder::Equal(0, "value", FieldType::INT, Literal(int32_t{1}));
    const auto is_null = PredicateBuilder::IsNull(0, "value", FieldType::INT);
    const auto is_not_null = PredicateBuilder::IsNotNull(0, "value", FieldType::INT);
    ASSERT_FALSE(Pushed(equal, nullptr));
    ASSERT_FALSE(Pushed(is_null, arrow::schema({arrow::field("other", arrow::int32())})));
    ASSERT_FALSE(Pushed(is_not_null, arrow::schema({arrow::field("value", arrow::int32()),
                                                    arrow::field("value", arrow::int32())})));
}

TEST(VortexPredicateConverterTest, LosslessDecimalComparisonsNormalizeToFileType) {
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    struct DecimalCase {
        NativeScanCase native;
        ComparisonBuilder build;
        std::vector<int64_t> expected_rows;
    };
    constexpr char values[] = R"(["-1.20",null,"0.00","1.20"])";
    const DecimalCase cases[] = {
        {{"precision_only",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          values,
          {Literal(Decimal(19, 2, 120))}},
         PredicateBuilder::Equal,
         {3}},
        {{"scale_up_compact_to_wide",
          arrow::decimal128(19, 2),
          FieldType::DECIMAL,
          values,
          {Literal(Decimal(18, 1, 12))}},
         PredicateBuilder::LessOrEqual,
         {0, 2, 3}},
    };
    for (const DecimalCase& test_case : cases) {
        const auto predicate =
            test_case.build(1, "value", FieldType::DECIMAL, test_case.native.literals[0]);
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case.native, predicate,
                                                 test_case.expected_rows, test_case.expected_rows));
    }
}

TEST(VortexPredicateConverterTest, LosslessDecimalMembershipNormalizesEachLiteral) {
    const NativeScanCase test_case = {
        "mixed_precisions",
        arrow::decimal128(10, 2),
        FieldType::DECIMAL,
        R"(["-1.20",null,"7.89","4.56"])",
        {Literal(Decimal(19, 2, -120)), Literal(Decimal(20, 2, 456))}};
    const auto in = PredicateBuilder::In(1, "value", FieldType::DECIMAL, test_case.literals);
    const auto not_in = PredicateBuilder::NotIn(1, "value", FieldType::DECIMAL, test_case.literals);
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, in, {0, 3}, {{0, 3}}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in, {2}, {{2}}));
}

TEST(VortexPredicateConverterTest, UnrepresentableDecimalsPreserveNativeCandidatesAndResidualRows) {
    const Literal unrepresentable(Decimal(10, 3, 1234));
    const NativeScanCase test_case = {"lossy_decimal",
                                      arrow::decimal128(10, 2),
                                      FieldType::DECIMAL,
                                      R"(["-1.23",null,"1.23","1.24"])",
                                      {}};
    const std::vector<int64_t> all_rows = {0, 1, 2, 3};

    // Rounding 1.234 to 1.23 would exclude row 2 from both native filters.
    const auto less_than =
        PredicateBuilder::LessThan(1, "value", FieldType::DECIMAL, unrepresentable);
    const auto not_in = PredicateBuilder::NotIn(1, "value", FieldType::DECIMAL,
                                                {Literal(Decimal(10, 2, -123)), unrepresentable});
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, less_than, all_rows, {{0, 2}}, {},
                                             /*expect_native_filter=*/false));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in, all_rows, {{2, 3}}, {},
                                             /*expect_native_filter=*/false));
}

TEST(VortexPredicateConverterTest, DecimalNormalizationRejectsInvalidLiteralMetadata) {
    const auto schema = arrow::schema({arrow::field("value", arrow::decimal128(10, 2))});
    // The value fits the column's precision, but the literal's precision is outside Paimon's range.
    const auto predicate =
        PredicateBuilder::Equal(0, "value", FieldType::DECIMAL, Literal(Decimal(39, 2, 123)));
    ASSERT_FALSE(Pushed(predicate, schema));
}

TEST(VortexPredicateConverterTest, MismatchedFileTypesReturnInvalid) {
    const auto schema = arrow::schema({arrow::field("value", arrow::int32())});
    const auto predicate =
        PredicateBuilder::Equal(0, "value", FieldType::BIGINT, Literal(int64_t{1}));
    ASSERT_NOK_WITH_MSG(VortexPredicateConverter::Convert(predicate, schema, {}),
                        "Predicate type mismatch for field 'value': expected INT from file type "
                        "int32, actual BIGINT");
}

TEST(VortexPredicateConverterTest, TimestampComparisonsRequireLosslessRepresentableValues) {
    const auto schema = arrow::schema({
        arrow::field("seconds", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("millis", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("local", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Shanghai")),
    });
    const std::pair<const char*, Literal> cases[] = {
        {"seconds", Literal(Timestamp::FromEpochMillis(1))},
        {"seconds", Literal(Timestamp::FromEpochMillis(1000, 1))},
        {"millis", Literal(Timestamp::FromEpochMillis(0, 1))},
        {"local", Literal(Timestamp::FromEpochMillis(1000))},
    };
    for (const auto& [field, literal] : cases) {
        SCOPED_TRACE(field);
        ASSERT_FALSE(
            Pushed(PredicateBuilder::Equal(0, field, FieldType::TIMESTAMP, literal), schema));
    }
}

TEST(VortexPredicateConverterTest, LosslessTimestampComparisonsFilterNativeFiles) {
    const std::vector<NativeScanCase> cases = {
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
    for (const NativeScanCase& test_case : cases) {
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case,
            PredicateBuilder::Equal(1, "value", FieldType::TIMESTAMP, test_case.literals[0]), {3}));
    }
}

TEST(VortexPredicateConverterTest, TimestampComparisonsPreserveBoundaryDecisionsAndNativeValues) {
    const std::vector<NativeScanCase> cases = {
        {"micros_calendar_limits",
         arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(253402207200999, 999000)),
          Literal(Timestamp::FromEpochMillis(-62167219200000)),
          Literal(Timestamp::FromEpochMillis(-1, 999000)),
          Literal(Timestamp::FromEpochMillis(9223372036854775, 808000)),
          Literal(Timestamp::FromEpochMillis(-9223372036854776, 192000))}},
        {"nanos_boundaries",
         arrow::timestamp(arrow::TimeUnit::NANO),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(9223372036854, 775807)),
          Literal(Timestamp::FromEpochMillis(-9223372036854)),
          Literal(Timestamp::FromEpochMillis(-1, 999999)),
          Literal(Timestamp::FromEpochMillis(9223372036854, 775808)),
          Literal(Timestamp::FromEpochMillis(-9223372036855, 224192))}},
    };
    for (const NativeScanCase& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("value", test_case.type)});
        // Accepted expressions must also fit the native calendar range.
        // The microsecond values cover year 0000 through Jiff's year 9999 limit.
        ASSERT_TRUE(
            Pushed(PredicateBuilder::Equal(0, "value", FieldType::TIMESTAMP, test_case.literals[0]),
                   schema));
        ASSERT_TRUE(Pushed(
            PredicateBuilder::LessOrEqual(0, "value", FieldType::TIMESTAMP, test_case.literals[1]),
            schema));
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case,
            PredicateBuilder::Equal(1, "value", FieldType::TIMESTAMP, test_case.literals[2]), {0}));
        // One unit beyond INT64_MAX must fall back rather than overflow.
        ASSERT_FALSE(
            Pushed(PredicateBuilder::Equal(0, "value", FieldType::TIMESTAMP, test_case.literals[3]),
                   schema));
        // Although the final value is INT64_MIN, the intermediate millisecond product
        // overflows. Preserve the existing fallback before calling the common utility.
        ASSERT_FALSE(
            Pushed(PredicateBuilder::Equal(0, "value", FieldType::TIMESTAMP, test_case.literals[4]),
                   schema));
    }
}

TEST(VortexPredicateConverterTest, TruncatedTimestampComparisonFallsBackWithoutLosingRows) {
    const NativeScanCase test_case = {"micros_fraction",
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
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, less_than, {0, 1, 2, 3}, std::nullopt, {},
                                             /*expect_native_filter=*/false));

    vx_error* error = nullptr;
    ScopeGuard release_error([&error]() {
        if (error != nullptr) {
            vx_error_free(error);
        }
    });
    VxScalarPtr truncated(
        vx_scalar_new_timestamp(0, VX_TIME_UNIT_MICROSECOND, nullptr, false, &error));
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(truncated, nullptr);
    // Truncating 1ns to 0us loses row 0, which the exact comparison above must retain.
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeComparisonRows(test_case, less_than, truncated.get(), VX_OPERATOR_LT, {3}));
}

TEST(VortexPredicateConverterTest, NativeFloatingPointOrderRequiresConservativeFallback) {
    const auto float_schema = arrow::schema({arrow::field("f", arrow::float32())});
    ASSERT_FALSE(
        Pushed(PredicateBuilder::Equal(0, "f", FieldType::FLOAT, Literal(1.5F)), float_schema));

    const NativeScanCase test_case = {
        "floating_order", arrow::float64(), FieldType::DOUBLE, "[NaN,null,-0.0,0.0]", {}};
    const double nan = std::numeric_limits<double>::quiet_NaN();
    ASSERT_OK_AND_ASSIGN(int32_t nan_comparison, Literal(nan).CompareTo(Literal(-nan)));
    ASSERT_EQ(nan_comparison, 0);
    ASSERT_OK_AND_ASSIGN(int32_t zero_comparison, Literal(-0.0).CompareTo(Literal(0.0)));
    ASSERT_LT(zero_comparison, 0);
    const std::shared_ptr<Predicate> equal_nan =
        PredicateBuilder::Equal(1, "value", FieldType::DOUBLE, Literal(-nan));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, equal_nan, {0, 1, 2, 3}, std::nullopt, {},
                                             /*expect_native_filter=*/false));
    VxScalarPtr negative_nan(vx_scalar_new_f64(-nan, false));
    ASSERT_NE(negative_nan, nullptr);
    // Native totalOrder distinguishes NaN signs, while Paimon must retain row 0.
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeComparisonRows(test_case, equal_nan, negative_nan.get(), VX_OPERATOR_EQ, {}));

    const std::shared_ptr<Predicate> equal_negative_zero =
        PredicateBuilder::Equal(1, "value", FieldType::DOUBLE, Literal(-0.0));
    VxScalarPtr negative_zero(vx_scalar_new_f64(-0.0, false));
    ASSERT_NE(negative_zero, nullptr);
    // Signed-zero ordering agrees in this case; it does not establish compatible NaN semantics.
    ASSERT_NO_FATAL_FAILURE(AssertNativeComparisonRows(test_case, equal_negative_zero,
                                                       negative_zero.get(), VX_OPERATOR_EQ, {2}));
}

TEST(VortexPredicateConverterTest, MembershipScalarsFilterNativeFilesAndExcludeNullRows) {
    const Decimal::int128_t wide_value = static_cast<Decimal::int128_t>(999999999999999999LL) * 10;
    const std::vector<NativeScanCase> cases = {
        {"boolean",
         arrow::boolean(),
         FieldType::BOOLEAN,
         "[true,null,false,true]",
         {Literal(true)}},
        {"string",
         arrow::utf8(),
         FieldType::STRING,
         R"(["",null,"miss","last"])",
         {Literal(FieldType::STRING, "", 0), Literal(FieldType::STRING, "last", 4)}},
        {"date",
         arrow::date32(),
         FieldType::DATE,
         "[-1,null,7,19000]",
         {Literal(FieldType::DATE, -1), Literal(FieldType::DATE, 19000)}},
        {"wide_decimal",
         arrow::decimal128(19, 2),
         FieldType::DECIMAL,
         R"(["-99999999999999999.90",null,"7.00","99999999999999999.90"])",
         {Literal(Decimal(19, 2, -wide_value)), Literal(Decimal(19, 2, wide_value))}},
        {"timestamp_utc",
         arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
         FieldType::TIMESTAMP,
         "[-1,null,7000,2001]",
         {Literal(Timestamp::FromEpochMillis(-1, 999000)),
          Literal(Timestamp::FromEpochMillis(2, 1000))}},
    };
    for (const NativeScanCase& test_case : cases) {
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case, PredicateBuilder::In(1, "value", test_case.field_type, test_case.literals),
            {0, 3}));
    }

    // NOT IN excludes nulls even when all non-null values are in the list.
    const NativeScanCase all_booleans = {"all_booleans",
                                         arrow::boolean(),
                                         FieldType::BOOLEAN,
                                         "[true,null,false,true]",
                                         {Literal(false), Literal(true)}};
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        all_booleans,
        PredicateBuilder::NotIn(1, "value", FieldType::BOOLEAN, all_booleans.literals), {}));
}

TEST(VortexPredicateConverterTest, MembershipMaximumPreservesNativeAndResidualRows) {
    const NativeScanCase test_case = {
        "membership_list_sizes", arrow::int32(), FieldType::INT, "[0,null,2048,1]", {}};
    const std::vector<int64_t> expected_rows = {2};
    for (int32_t size : {32, 33}) {
        SCOPED_TRACE(size);
        std::vector<Literal> literals;
        for (int32_t value = 0; value < size; ++value) {
            literals.emplace_back(value);
        }
        const auto predicate = PredicateBuilder::NotIn(1, "value", FieldType::INT, literals);
        if (size == 32) {
            ASSERT_NO_FATAL_FAILURE(
                AssertNativeRows(test_case, predicate, expected_rows, expected_rows));
        } else {
            ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, predicate, {0, 1, 2, 3},
                                                     expected_rows, {},
                                                     /*expect_native_filter=*/false));
            ASSERT_NO_FATAL_FAILURE(
                AssertNativeRows(test_case, predicate, expected_rows, expected_rows, {33}));
        }
    }

    const auto schema = TestSchema();
    const auto singleton = PredicateBuilder::NotIn(0, "i", FieldType::INT, {Literal(int32_t{1})});
    ASSERT_TRUE(Pushed(singleton, schema));
    ASSERT_FALSE(Pushed(singleton, schema, {0}));
}

TEST(VortexPredicateConverterTest, IntegerListsAtTypeLimitsFilterNativeFiles) {
    const std::vector<NativeScanCase> cases = {
        IntegerRangeBoundaryCase<int8_t>("tinyint_range_limits", arrow::int8(), FieldType::TINYINT,
                                         "[-128,null,0,127]"),
        IntegerRangeBoundaryCase<int16_t>("smallint_range_limits", arrow::int16(),
                                          FieldType::SMALLINT, "[-32768,null,0,32767]"),
        IntegerRangeBoundaryCase<int32_t>("int_range_limits", arrow::int32(), FieldType::INT,
                                          "[-2147483648,null,0,2147483647]"),
        IntegerRangeBoundaryCase<int64_t>("bigint_range_limits", arrow::int64(), FieldType::BIGINT,
                                          "[-9223372036854775808,null,0,9223372036854775807]"),
    };
    for (const NativeScanCase& test_case : cases) {
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
            test_case, PredicateBuilder::In(1, "value", test_case.field_type, test_case.literals),
            {0, 3}));
    }
}

TEST(VortexPredicateConverterTest, IntegerListsComposeWithNativePredicatesAndProjection) {
    NativeScanCase test_case = {
        "integer_range_compound", arrow::int32(), FieldType::INT, "[-4,null,8,4]", {}};
    for (int32_t value = -4; value <= 4; ++value) {
        test_case.literals.emplace_back(value);
    }
    const std::shared_ptr<Predicate> in =
        PredicateBuilder::In(1, "value", FieldType::INT, test_case.literals);
    const std::shared_ptr<Predicate> not_in =
        PredicateBuilder::NotIn(1, "value", FieldType::INT, test_case.literals);
    const std::shared_ptr<Predicate> first_three =
        PredicateBuilder::LessThan(0, "rowid", FieldType::BIGINT, Literal(int64_t{3}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> in_and,
                         PredicateBuilder::And({in, first_three}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, in_and, {0}, std::nullopt, {},
                                             /*expect_native_filter=*/true,
                                             /*project_rowid_only=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Predicate> not_in_or_null,
        PredicateBuilder::Or({not_in, PredicateBuilder::IsNull(1, "value", FieldType::INT)}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, not_in_or_null, {1, 2}, std::nullopt, {},
                                             /*expect_native_filter=*/true,
                                             /*project_rowid_only=*/true));
}

TEST(VortexPredicateConverterTest, NotInDuplicatesMissesAndAllExcludedFilterNativeFiles) {
    const NativeScanCase test_case = {
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
    const NativeScanCase all_null = {
        "not_in_all_null", arrow::int32(), FieldType::INT, "[null,null,null,null]", {}};
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        all_null, PredicateBuilder::NotIn(1, "value", FieldType::INT, {Literal(int32_t{1})}), {}));
}

TEST(VortexPredicateConverterTest, MembershipUnsupportedInputsFallBack) {
    const auto schema = TestSchema();
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, {}), schema));
    ASSERT_FALSE(Pushed(
        PredicateBuilder::In(0, "value", FieldType::BINARY, {Literal(FieldType::BINARY, "x", 1)}),
        arrow::schema({arrow::field("value", arrow::binary())})));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "value", FieldType::FLOAT, {Literal(1.5F)}),
                        arrow::schema({arrow::field("value", arrow::float32())})));
}

TEST(VortexPredicateConverterTest, NestedCompoundsPreserveNativeCandidates) {
    const NativeScanCase test_case = {
        "nested-fallback", arrow::utf8(), FieldType::STRING, R"(["aa",null,"ab","ba"])", {}};
    const Literal ab(FieldType::STRING, "ab", 2);
    const std::shared_ptr<Predicate> supported =
        PredicateBuilder::GreaterOrEqual(1, "value", FieldType::STRING, ab);
    const std::shared_ptr<Predicate> equal =
        PredicateBuilder::Equal(1, "value", FieldType::STRING, Literal(FieldType::STRING, "aa", 2));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> unsupported,
                         PredicateBuilder::StartsWith(1, "value", FieldType::STRING,
                                                      Literal(FieldType::STRING, "a", 1)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> partial_and,
                         PredicateBuilder::And({supported, unsupported}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> fallback_or,
                         PredicateBuilder::Or({unsupported, supported}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> nested_and,
                         PredicateBuilder::And({fallback_or, equal}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> nested_or,
                         PredicateBuilder::Or({partial_and, equal}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, partial_and, {2, 3}, {{2}}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, fallback_or, {0, 1, 2, 3}, {{0, 2, 3}}, {},
                                             /*expect_native_filter=*/false));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, nested_and, {0}, {{0}}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, nested_or, {0, 2, 3}, {{0, 2}}));
}

TEST(VortexPredicateConverterTest, NativeMembershipErrorPropagatesThroughOr) {
    const auto schema = TestSchema();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> unsupported,
                         PredicateBuilder::StartsWith(1, "s", FieldType::STRING,
                                                      Literal(FieldType::STRING, "a", 1)));
    const auto in = PredicateBuilder::In(
        1, "s", FieldType::STRING,
        {Literal(FieldType::STRING, "valid", 5), Literal(FieldType::STRING, "\xff", 1)});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> disjunction,
                         PredicateBuilder::Or({unsupported, in}));
    ASSERT_NOK_WITH_MSG(VortexPredicateConverter::Convert(disjunction, schema, {}),
                        kInvalidUtf8Error);
}

TEST(VortexPredicateConverterTest, NativeTemporalScalarBoundsFallBackWithoutErrors) {
    const int64_t maximum = std::numeric_limits<int64_t>::max();
    struct TemporalCase {
        const char* name;
        std::shared_ptr<arrow::DataType> type;
        FieldType field_type;
        Literal literal;
    };
    const std::vector<TemporalCase> cases = {
        {"maximum-micros", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), FieldType::TIMESTAMP,
         Literal(Timestamp::FromEpochMillis(maximum / 1000,
                                            static_cast<int32_t>((maximum % 1000) * 1000)))},
        {"minimum-micros", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), FieldType::TIMESTAMP,
         Literal(Timestamp::FromEpochMillis(std::numeric_limits<int64_t>::min() / 1000))},
        {"native-epoch-seconds", arrow::timestamp(arrow::TimeUnit::SECOND, "UTC"),
         FieldType::TIMESTAMP, Literal(Timestamp::FromEpochMillis(253402207201000))},
        {"native-epoch-millis", arrow::timestamp(arrow::TimeUnit::MILLI), FieldType::TIMESTAMP,
         Literal(Timestamp::FromEpochMillis(253402207201000))},
        {"minimum-date", arrow::date32(), FieldType::DATE,
         Literal(FieldType::DATE, std::numeric_limits<int32_t>::min())}};
    for (const TemporalCase& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("value", test_case.type)});
        ASSERT_FALSE(Pushed(
            PredicateBuilder::Equal(0, "value", test_case.field_type, test_case.literal), schema));
    }
}

TEST(VortexPredicateConverterTest, UnsafeTimestampInListPreservesNativeAndResidualRows) {
    const NativeScanCase test_case = {
        "timestamp-precision-fallback",
        arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
        FieldType::TIMESTAMP,
        "[0,null,1,2]",
        {Literal(Timestamp::FromEpochMillis(0, 1000)), Literal(Timestamp::FromEpochMillis(0, 1))}};
    const auto predicate =
        PredicateBuilder::In(1, "value", FieldType::TIMESTAMP, test_case.literals);
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, predicate, {0, 1, 2, 3}, {{2}}, {},
                                             /*expect_native_filter=*/false));
}

TEST(VortexPredicateConverterTest, MembershipMaximumAppliesIndependentlyToEachLeaf) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> integers;
    std::vector<Literal> longs;
    for (int32_t value = 0; value < 32; ++value) {
        integers.emplace_back(value);
        longs.emplace_back(static_cast<int64_t>(value));
    }
    const auto in = PredicateBuilder::In(0, "i", FieldType::INT, integers);
    const auto not_in = PredicateBuilder::NotIn(2, "l", FieldType::BIGINT, longs);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({in, not_in}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({in, not_in}));
    ASSERT_TRUE(Pushed(and_predicate, schema));
    ASSERT_TRUE(Pushed(or_predicate, schema));
    ASSERT_FALSE(Pushed(and_predicate, schema, {31}));
    ASSERT_FALSE(Pushed(or_predicate, schema, {31}));
}

TEST(VortexPredicateConverterTest, DuplicateValuesCountTowardMembershipMaximum) {
    const auto schema = TestSchema();
    const std::vector<Literal> literals(33, Literal(FieldType::STRING, "same", 4));
    const auto predicate = PredicateBuilder::NotIn(1, "s", FieldType::STRING, literals);
    ASSERT_FALSE(Pushed(predicate, schema));
    ASSERT_TRUE(Pushed(predicate, schema, {33}));
}

}  // namespace paimon::vortex::test
