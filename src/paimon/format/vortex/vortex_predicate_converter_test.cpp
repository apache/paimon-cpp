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
#include "paimon/common/predicate/and.h"
#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/or.h"
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
    const Result<VxExpressionPtr> result =
        VortexPredicateConverter::Convert(predicate, schema, options);
    EXPECT_OK(result.status());
    return result.ok() && result.value() != nullptr;
}

void AssertInvalidConversion(const std::shared_ptr<Predicate>& predicate,
                             const std::shared_ptr<arrow::Schema>& schema,
                             const std::vector<std::string>& message_parts,
                             const VortexPredicateOptions& options = {}) {
    const Result<VxExpressionPtr> result =
        VortexPredicateConverter::Convert(predicate, schema, options);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsInvalid()) << result.status().ToString();
    for (const std::string& part : message_parts) {
        ASSERT_NE(result.status().ToString().find(part), std::string::npos)
            << result.status().ToString();
    }
}

// Simulate a malformed external Predicate whose function disagrees with its node shape.
class FunctionOnlyPredicate : public Predicate {
 public:
    explicit FunctionOnlyPredicate(const Function& function) : function_(function) {}

    bool operator==(const Predicate& other) const override {
        return this == &other;
    }
    const Function& GetFunction() const override {
        return function_;
    }
    std::shared_ptr<Predicate> Negate() const override {
        return nullptr;
    }
    std::string ToString() const override {
        return "FunctionOnlyPredicate";
    }

 private:
    const Function& function_;
};

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
                      const vx_expression* native_filter_override = nullptr,
                      const std::vector<int64_t>* expected_residual_rows = nullptr) {
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
                               kDefaultVortexReadIoCoalesceMaxSize, &error),
                           vx_data_source_free);
    ASSERT_EQ(error, nullptr) << NativeErrorMessage(error);
    ASSERT_NE(source, nullptr);
    ASSERT_OK_AND_ASSIGN(VxExpressionPtr filter,
                         VortexPredicateConverter::Convert(predicate, schema, options));
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
    std::vector<int64_t> residual_row_ids;
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
            std::vector<char> mask;
            if (expected_residual_rows != nullptr) {
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
                if (expected_residual_rows != nullptr) {
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
    if (expected_residual_rows != nullptr) {
        std::sort(residual_row_ids.begin(), residual_row_ids.end());
        ASSERT_EQ(residual_row_ids, *expected_residual_rows);
    }
}

void AssertNativeAndResidualRows(const NativeInCase& test_case,
                                 const std::shared_ptr<Predicate>& predicate,
                                 const std::vector<int64_t>& expected_candidates,
                                 const std::vector<int64_t>& expected_matches,
                                 const VortexPredicateOptions& options = {},
                                 bool expect_native_filter = true) {
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case, predicate, expected_candidates, false,
                                             options, expect_native_filter, nullptr,
                                             &expected_matches));
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
        ASSERT_NO_FATAL_FAILURE(
            AssertInvalidConversion(build(0, "i", FieldType::INT, Literal(int64_t{1})),
                                    TestSchema(), {"i", "expected INT", "actual BIGINT"}));
        ASSERT_NO_FATAL_FAILURE(
            AssertInvalidConversion(build(0, "i", FieldType::BIGINT, Literal(int64_t{1})),
                                    TestSchema(), {"i", "expected INT", "actual BIGINT"}));
        ASSERT_NO_FATAL_FAILURE(
            AssertInvalidConversion(build(0, "dt", FieldType::INT, Literal(int32_t{1})),
                                    TestSchema(), {"dt", "expected DATE", "actual INT"}));
        ASSERT_TRUE(Pushed(build(0, "dec_compact", FieldType::DECIMAL,
                                 Literal(Decimal::FromUnscaledLong(12345, 19, 2))),
                           TestSchema()));
        ASSERT_FALSE(Pushed(build(0, "dec_compact", FieldType::DECIMAL,
                                  Literal(Decimal::FromUnscaledLong(12345, 10, 3))),
                            TestSchema()));
    }
}

TEST(VortexPredicateConverterTest, LosslessDecimalComparisonsNormalizeToFileType) {
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    const std::vector<ComparisonBuilder> builders = {
        PredicateBuilder::Equal,       PredicateBuilder::NotEqual,
        PredicateBuilder::LessThan,    PredicateBuilder::LessOrEqual,
        PredicateBuilder::GreaterThan, PredicateBuilder::GreaterOrEqual};
    const std::vector<std::vector<int64_t>> positive_rows = {{3},       {0, 2}, {0, 2},
                                                             {0, 2, 3}, {},     {3}};
    struct DecimalCase {
        NativeInCase native;
        std::vector<std::vector<int64_t>> expected_rows;
    };
    Decimal::int128_t maximum = 1;
    for (int32_t digit = 0; digit < 37; ++digit) {
        maximum *= 10;
    }
    --maximum;
    const std::vector<DecimalCase> cases = {
        {{"precision_only",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","1.20"])",
          {Literal(Decimal(19, 2, 120))}},
         positive_rows},
        {{"scale_down",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","1.20"])",
          {Literal(Decimal(20, 4, 12000))}},
         positive_rows},
        {{"scale_up",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","1.20"])",
          {Literal(Decimal(10, 1, 12))}},
         positive_rows},
        {{"negative_scale_down",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","1.20"])",
          {Literal(Decimal(20, 4, -12000))}},
         {{0}, {2, 3}, {}, {0}, {2, 3}, {0, 2, 3}}},
        {{"zero_scale_down",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","1.20"])",
          {Literal(Decimal(38, 38, 0))}},
         {{2}, {0, 3}, {0}, {0, 2}, {3}, {2, 3}}},
        {{"compact_to_wide",
          arrow::decimal128(19, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","1.20"])",
          {Literal(Decimal(18, 1, 12))}},
         positive_rows},
        {{"38_digit_unscaled_value",
          arrow::decimal128(37, 0),
          FieldType::DECIMAL,
          R"(["-9999999999999999999999999999999999999",null,"0","9999999999999999999999999999999999999"])",
          {Literal(Decimal(38, 1, maximum * 10))}},
         positive_rows},
    };
    for (const DecimalCase& test_case : cases) {
        for (size_t operation = 0; operation < builders.size(); ++operation) {
            SCOPED_TRACE(operation);
            const std::shared_ptr<Predicate> predicate =
                builders[operation](1, "value", FieldType::DECIMAL, test_case.native.literals[0]);
            ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(
                test_case.native, predicate, test_case.expected_rows[operation],
                test_case.expected_rows[operation]));
        }
    }
}

TEST(VortexPredicateConverterTest, LosslessDecimalMembershipNormalizesEachLiteral) {
    struct DecimalCase {
        NativeInCase native;
        std::vector<int64_t> in_rows;
        std::vector<int64_t> not_in_rows;
    };
    Decimal::int128_t maximum = 1;
    for (int32_t digit = 0; digit < 37; ++digit) {
        maximum *= 10;
    }
    --maximum;
    const std::vector<DecimalCase> cases = {
        {{"mixed_parameters",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","4.56"])",
          {Literal(Decimal(19, 1, -12)), Literal(Decimal(20, 4, 45600))}},
         {0, 3},
         {2}},
        {{"zero",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","4.56"])",
          {Literal(Decimal(38, 38, 0))}},
         {2},
         {0, 3}},
        {{"compact_and_wide",
          arrow::decimal128(19, 2),
          FieldType::DECIMAL,
          R"(["-1.20",null,"0.00","1.20"])",
          {Literal(Decimal(18, 1, -12)), Literal(Decimal(38, 4, 12000))}},
         {0, 3},
         {2}},
        {{"38_digit_unscaled_values",
          arrow::decimal128(37, 0),
          FieldType::DECIMAL,
          R"(["-9999999999999999999999999999999999999",null,"0","9999999999999999999999999999999999999"])",
          {Literal(Decimal(38, 1, -maximum * 10)), Literal(Decimal(38, 1, maximum * 10))}},
         {0, 3},
         {2}},
    };
    for (const DecimalCase& test_case : cases) {
        for (bool negate : {false, true}) {
            const std::shared_ptr<Predicate> predicate =
                negate ? PredicateBuilder::NotIn(1, "value", FieldType::DECIMAL,
                                                 test_case.native.literals)
                       : PredicateBuilder::In(1, "value", FieldType::DECIMAL,
                                              test_case.native.literals);
            const std::vector<int64_t>& expected =
                negate ? test_case.not_in_rows : test_case.in_rows;
            ASSERT_NO_FATAL_FAILURE(
                AssertNativeAndResidualRows(test_case.native, predicate, expected, expected));
        }
    }
}

TEST(VortexPredicateConverterTest, UnrepresentableDecimalsPreserveNativeCandidatesAndResidualRows) {
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    const std::vector<ComparisonBuilder> builders = {
        PredicateBuilder::Equal,       PredicateBuilder::NotEqual,
        PredicateBuilder::LessThan,    PredicateBuilder::LessOrEqual,
        PredicateBuilder::GreaterThan, PredicateBuilder::GreaterOrEqual};
    struct DecimalCase {
        NativeInCase native;
        std::vector<std::vector<int64_t>> expected_rows;
    };
    Decimal::int128_t maximum = 1;
    for (int32_t digit = 0; digit < 38; ++digit) {
        maximum *= 10;
    }
    --maximum;
    const std::vector<DecimalCase> cases = {
        {{"lossy_positive",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.23",null,"1.23","1.24"])",
          {Literal(Decimal(10, 3, 1234))}},
         {{}, {0, 2, 3}, {0, 2}, {0, 2}, {3}, {3}}},
        {{"lossy_negative",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.23",null,"1.23","1.24"])",
          {Literal(Decimal(10, 3, -1234))}},
         {{}, {0, 2, 3}, {}, {}, {0, 2, 3}, {0, 2, 3}}},
        {{"precision_overflow",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.23",null,"1.23","1.24"])",
          {Literal(Decimal(19, 2, 10000000000LL))}},
         {{}, {0, 2, 3}, {0, 2, 3}, {0, 2, 3}, {}, {}}},
        {{"wide_value_cannot_be_truncated_to_compact",
          arrow::decimal128(10, 2),
          FieldType::DECIMAL,
          R"(["-1.23",null,"1.23","1.24"])",
          // Its low 64 bits look like 1.23; the full source value must be checked first.
          {Literal(Decimal(20, 2, (static_cast<Decimal::int128_t>(1) << 64) + 123))}},
         {{}, {0, 2, 3}, {0, 2, 3}, {0, 2, 3}, {}, {}}},
        {{"128_bit_scale_overflow",
          arrow::decimal128(38, 38),
          FieldType::DECIMAL,
          R"(["-0.12000000000000000000000000000000000000",null,"0.12000000000000000000000000000000000000","0.99000000000000000000000000000000000000"])",
          {Literal(Decimal(38, 0, maximum))}},
         {{}, {0, 2, 3}, {0, 2, 3}, {0, 2, 3}, {}, {}}},
    };
    const std::vector<int64_t> all_rows = {0, 1, 2, 3};
    for (const DecimalCase& test_case : cases) {
        for (size_t operation = 0; operation < builders.size(); ++operation) {
            SCOPED_TRACE(operation);
            const std::shared_ptr<Predicate> predicate =
                builders[operation](1, "value", FieldType::DECIMAL, test_case.native.literals[0]);
            // Rounding +/-1.234 to +/-1.23 would lose rows in strict inequalities.
            ASSERT_NO_FATAL_FAILURE(
                AssertNativeAndResidualRows(test_case.native, predicate, all_rows,
                                            test_case.expected_rows[operation], {}, false));
        }
        const auto& column_type =
            checked_cast<const arrow::Decimal128Type&>(*test_case.native.type);
        const std::vector<Literal> literals = {
            Literal(Decimal(column_type.precision(), column_type.scale(), 0)),
            test_case.native.literals[0]};
        for (bool negate : {false, true}) {
            const std::shared_ptr<Predicate> predicate =
                negate ? PredicateBuilder::NotIn(1, "value", FieldType::DECIMAL, literals)
                       : PredicateBuilder::In(1, "value", FieldType::DECIMAL, literals);
            const std::vector<int64_t> expected =
                negate ? std::vector<int64_t>{0, 2, 3} : std::vector<int64_t>{};
            ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case.native, predicate,
                                                                all_rows, expected, {}, false));
        }
    }
}

TEST(VortexPredicateConverterTest, DecimalNormalizationRejectsInvalidLiteralMetadata) {
    const std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("value", arrow::decimal128(10, 2))});
    const std::vector<Literal> literals = {
        Literal(Decimal(0, 0, 0)),     Literal(Decimal(39, 2, 123)),
        Literal(Decimal(10, -1, 123)), Literal(Decimal(10, 11, 123)),
        Literal(Decimal(2, 0, 123)),   Literal(Decimal(10, 2, 10000000000LL)),
    };
    for (const Literal& literal : literals) {
        ASSERT_FALSE(
            Pushed(PredicateBuilder::Equal(0, "value", FieldType::DECIMAL, literal), schema));
        ASSERT_FALSE(
            Pushed(PredicateBuilder::In(0, "value", FieldType::DECIMAL, {literal}), schema));
        ASSERT_FALSE(
            Pushed(PredicateBuilder::NotIn(0, "value", FieldType::DECIMAL, {literal}), schema));
    }
}

TEST(VortexPredicateConverterTest, SameParameterDecimalOverflowPreservesNativeCandidates) {
    const NativeInCase test_case = {"same-parameter-wide-overflow",
                                    arrow::decimal128(10, 2),
                                    FieldType::DECIMAL,
                                    R"(["-1.23",null,"1.23","1.24"])",
                                    {}};
    // The low 64 bits are 123, but the full value does not fit the advertised compact precision.
    const Decimal::int128_t unscaled = (static_cast<Decimal::int128_t>(1) << 64) + 123;
    const Literal invalid(Decimal(10, 2, unscaled));
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    for (ComparisonBuilder build :
         {PredicateBuilder::Equal, PredicateBuilder::NotEqual, PredicateBuilder::LessThan,
          PredicateBuilder::LessOrEqual, PredicateBuilder::GreaterThan,
          PredicateBuilder::GreaterOrEqual}) {
        ASSERT_NO_FATAL_FAILURE(AssertNativeRows(test_case,
                                                 build(1, "value", FieldType::DECIMAL, invalid),
                                                 {0, 1, 2, 3}, false, {}, false));
    }
    for (const std::shared_ptr<Predicate>& predicate :
         {PredicateBuilder::In(1, "value", FieldType::DECIMAL, {invalid}),
          PredicateBuilder::NotIn(1, "value", FieldType::DECIMAL, {invalid})}) {
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeRows(test_case, predicate, {0, 1, 2, 3}, false, {}, false));
    }
}

TEST(VortexPredicateConverterTest, DecimalMembershipMaximumPrecedesNormalization) {
    const NativeInCase test_case = {"decimal_raw_length",
                                    arrow::decimal128(10, 2),
                                    FieldType::DECIMAL,
                                    R"(["-1.23",null,"1.23","1.24"])",
                                    {}};
    const std::vector<Literal> literals(33, Literal(Decimal(38, 4, 12300)));
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(1, "value", FieldType::DECIMAL, literals)
                   : PredicateBuilder::In(1, "value", FieldType::DECIMAL, literals);
        const std::vector<int64_t> expected =
            negate ? std::vector<int64_t>{0, 3} : std::vector<int64_t>{2};
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeAndResidualRows(test_case, predicate, {0, 1, 2, 3}, expected, {}, false));
        ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case, predicate, expected,
                                                            expected, VortexPredicateOptions{33}));
    }
}

TEST(VortexPredicateConverterTest, MismatchedBasicTypesReturnInvalid) {
    struct TypeCase {
        std::shared_ptr<arrow::DataType> arrow_type;
        FieldType field_type;
        const char* type_name;
    };
    const std::vector<TypeCase> cases = {
        {arrow::boolean(), FieldType::BOOLEAN, "BOOLEAN"},
        {arrow::int8(), FieldType::TINYINT, "TINYINT"},
        {arrow::int16(), FieldType::SMALLINT, "SMALLINT"},
        {arrow::int32(), FieldType::INT, "INT"},
        {arrow::int64(), FieldType::BIGINT, "BIGINT"},
        {arrow::float32(), FieldType::FLOAT, "FLOAT"},
        {arrow::float64(), FieldType::DOUBLE, "DOUBLE"},
        {arrow::utf8(), FieldType::STRING, "STRING"},
        {arrow::binary(), FieldType::BINARY, "BINARY"},
        {arrow::date32(), FieldType::DATE, "DATE"},
        {arrow::decimal128(10, 2), FieldType::DECIMAL, "DECIMAL"},
        {arrow::timestamp(arrow::TimeUnit::MILLI), FieldType::TIMESTAMP, "TIMESTAMP"},
    };
    for (const TypeCase& test_case : cases) {
        SCOPED_TRACE(test_case.type_name);
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("value", test_case.arrow_type)});
        const bool int_field = test_case.field_type == FieldType::INT;
        const FieldType wrong_type = int_field ? FieldType::BIGINT : FieldType::INT;
        const Literal wrong_literal = int_field ? Literal(int64_t{1}) : Literal(int32_t{1});
        const std::string expected = fmt::format("expected {}", test_case.type_name);
        const std::string actual = int_field ? "actual BIGINT" : "actual INT";
        // Both the leaf-to-file contract and the literal-to-leaf contract are errors.
        ASSERT_NO_FATAL_FAILURE(
            AssertInvalidConversion(PredicateBuilder::Equal(0, "value", wrong_type, wrong_literal),
                                    schema, {"value", expected, actual}));
        ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
            PredicateBuilder::Equal(0, "value", test_case.field_type, wrong_literal), schema,
            {"value", expected, actual}));
    }
}

TEST(VortexPredicateConverterTest, UnboundPredicatesPreserveNativeCandidateRows) {
    const NativeInCase test_case = {"unbound", arrow::int32(), FieldType::INT, "[1,null,2,3]", {}};
    const std::vector<std::shared_ptr<Predicate>> predicates = {
        PredicateBuilder::Equal(2, "missing", FieldType::INT, Literal(int32_t{1})),
        PredicateBuilder::IsNull(2, "missing", FieldType::INT),
        PredicateBuilder::IsNotNull(2, "missing", FieldType::INT),
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
                for (uint32_t maximum : {uint32_t{32}, uint32_t{80}}) {
                    const VortexPredicateOptions options{maximum};
                    const bool pushed = literals.size() <= maximum;
                    ASSERT_EQ(Pushed(predicate, schema, options), pushed);
                    const std::vector<int64_t> candidates =
                        pushed ? expected : std::vector<int64_t>{0, 1, 2, 3};
                    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(
                        test_case, predicate, candidates, expected, options, pushed));
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

TEST(VortexPredicateConverterTest, TimestampComparisonsPreserveBoundaryDecisionsAndNativeValues) {
    const std::vector<NativeInCase> cases = {
        {"micros_calendar_limits",
         arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(253402207200999, 999000)),
          Literal(Timestamp::FromEpochMillis(-62167219200000)),
          Literal(Timestamp::FromEpochMillis(-1, 999000)),
          Literal(Timestamp::FromEpochMillis(9223372036854775, 808000)),
          Literal(Timestamp::FromEpochMillis(-9223372036854776, 192000)),
          Literal(Timestamp::FromEpochMillis(0, 2000))}},
        {"nanos_boundaries",
         arrow::timestamp(arrow::TimeUnit::NANO),
         FieldType::TIMESTAMP,
         "[-1,null,7,2]",
         {Literal(Timestamp::FromEpochMillis(9223372036854, 775807)),
          Literal(Timestamp::FromEpochMillis(-9223372036854)),
          Literal(Timestamp::FromEpochMillis(-1, 999999)),
          Literal(Timestamp::FromEpochMillis(9223372036854, 775808)),
          Literal(Timestamp::FromEpochMillis(-9223372036855, 224192)),
          Literal(Timestamp::FromEpochMillis(0, 2))}},
    };
    for (const NativeInCase& test_case : cases) {
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
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeRows(test_case,
                             PredicateBuilder::GreaterOrEqual(1, "value", FieldType::TIMESTAMP,
                                                              test_case.literals[5]),
                             {2, 3}));
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

TEST(VortexPredicateConverterTest, InSmallAndLargeListsRespectConfiguredMaximum) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    for (const int32_t size : {1, 2, 20, 21, 1024}) {
        std::vector<Literal> literals;
        for (int32_t value = 0; value < size; ++value) {
            literals.emplace_back(value);
        }
        const std::shared_ptr<Predicate> predicate =
            PredicateBuilder::In(0, "i", FieldType::INT, literals);
        ASSERT_EQ(Pushed(predicate, schema), size <= 32);
        ASSERT_TRUE(Pushed(predicate, schema, {1024}));
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
        const std::shared_ptr<Predicate> predicate =
            PredicateBuilder::NotIn(1, "value", FieldType::INT, literals);
        const bool pushed = size <= 32;
        ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(
            test_case, predicate, pushed ? expected_rows : std::vector<int64_t>{0, 1, 2, 3},
            expected_rows, {}, pushed));
        if (size <= 100) {
            ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(
                test_case, predicate, expected_rows, expected_rows, {static_cast<uint32_t>(size)}));
        }
    }
}

TEST(VortexPredicateConverterTest, IntegerListsAtTypeLimitsFilterNativeFiles) {
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

TEST(VortexPredicateConverterTest, IntegerListsPreserveGapsAndSingletonsInNativeFiles) {
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
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(
        test_case, PredicateBuilder::In(1, "value", FieldType::TINYINT, test_case.literals),
        {0, 1, 2, 3}, {0, 2, 3}, {}, false));
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(
        test_case, PredicateBuilder::NotIn(1, "value", FieldType::TINYINT, test_case.literals),
        {0, 1, 2, 3}, {}, {}, false));
    test_case.name = "duplicate_singleton";
    test_case.literals = std::vector<Literal>(8, Literal(int8_t{-128}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::In(1, "value", FieldType::TINYINT, test_case.literals), {0}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeRows(
        test_case, PredicateBuilder::NotIn(1, "value", FieldType::TINYINT, test_case.literals),
        {2, 3}));
}

TEST(VortexPredicateConverterTest, IntegerListsComposeWithNativePredicatesAndProjection) {
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

TEST(VortexPredicateConverterTest, IntegerListsValidateEveryLiteral) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> safe;
    for (int32_t value = 0; value < 16; ++value) {
        safe.emplace_back(value);
    }
    for (const Literal& unsafe : {Literal(FieldType::INT), Literal(int64_t{16})}) {
        for (size_t position : {size_t{0}, size_t{8}, safe.size()}) {
            std::vector<Literal> literals = safe;
            literals.insert(literals.begin() + position, unsafe);
            for (bool negate : {false, true}) {
                const std::shared_ptr<Predicate> predicate =
                    negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, literals)
                           : PredicateBuilder::In(0, "i", FieldType::INT, literals);
                if (unsafe.IsNull()) {
                    ASSERT_FALSE(Pushed(predicate, schema));
                } else {
                    ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                        predicate, schema, {"i", "expected INT", "actual BIGINT"}));
                }
            }
        }
    }
    const std::shared_ptr<arrow::Schema> bigint_schema =
        arrow::schema({arrow::field("i", arrow::int64())});
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, safe)
                   : PredicateBuilder::In(0, "i", FieldType::INT, safe);
        ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(predicate, bigint_schema,
                                                        {"i", "expected BIGINT", "actual INT"}));
    }
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

TEST(VortexPredicateConverterTest, NotInUnsafeLiteralsFallBackAndTypeMismatchesFail) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, {}), schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT, {Literal(FieldType::INT)}), schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::NotIn(0, "i", FieldType::INT,
                                                {Literal(int32_t{1}), Literal(FieldType::INT)}),
                        schema));
    ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
        PredicateBuilder::NotIn(0, "i", FieldType::INT, {Literal(int32_t{1}), Literal(int64_t{2})}),
        schema, {"i", "expected INT", "actual BIGINT"}));
    ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
        PredicateBuilder::NotIn(0, "i", FieldType::BIGINT, {Literal(int64_t{1})}), schema,
        {"i", "expected INT", "actual BIGINT"}));
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

TEST(VortexPredicateConverterTest, InUnsafeLiteralsFallBackAndTypeMismatchesFail) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT, {}), schema));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::In(0, "i", FieldType::INT, {Literal(FieldType::INT)}), schema));
    ASSERT_FALSE(Pushed(PredicateBuilder::In(0, "i", FieldType::INT,
                                             {Literal(int32_t{1}), Literal(FieldType::INT)}),
                        schema));
    ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
        PredicateBuilder::In(0, "i", FieldType::INT, {Literal(int32_t{1}), Literal(int64_t{2})}),
        schema, {"i", "expected INT", "actual BIGINT"}));
    ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
        PredicateBuilder::In(0, "i", FieldType::BIGINT, {Literal(int64_t{1})}), schema,
        {"i", "expected INT", "actual BIGINT"}));
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
    ASSERT_TRUE(Pushed(PredicateBuilder::In(0, "dec", FieldType::DECIMAL,
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

TEST(VortexPredicateConverterTest, CompoundPredicatesPropagateTypeErrorsRegardlessOfBranchOrder) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    const std::shared_ptr<Predicate> supported =
        PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(int32_t{1}));
    const std::shared_ptr<Predicate> invalid =
        PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(int64_t{1}));
    const std::shared_ptr<Predicate> unsupported =
        PredicateBuilder::In(0, "i", FieldType::INT, {Literal(FieldType::INT)});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({supported, invalid}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({supported, invalid}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_after_fallback,
                         PredicateBuilder::And({unsupported, invalid}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_before_fallback,
                         PredicateBuilder::Or({invalid, unsupported}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_after_fallback,
                         PredicateBuilder::Or({unsupported, invalid}));
    for (const std::shared_ptr<Predicate>& predicate :
         {and_predicate, or_predicate, and_after_fallback, or_before_fallback, or_after_fallback}) {
        ASSERT_NO_FATAL_FAILURE(
            AssertInvalidConversion(predicate, schema, {"i", "expected INT", "actual BIGINT"}));
    }
}

TEST(VortexPredicateConverterTest, MalformedPredicateStructuresReturnInvalid) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    const std::shared_ptr<Predicate> unsupported =
        PredicateBuilder::In(0, "i", FieldType::INT, {Literal(FieldType::INT)});
    const std::vector<const CompoundFunction*> functions = {&And::Instance(), &Or::Instance()};
    for (const CompoundFunction* function : functions) {
        ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
            std::make_shared<FunctionOnlyPredicate>(*function), schema, {"CompoundPredicate"}));
        ASSERT_NO_FATAL_FAILURE(
            AssertInvalidConversion(std::make_shared<CompoundPredicateImpl>(
                                        *function, std::vector<std::shared_ptr<Predicate>>{}),
                                    schema, {"at least one child"}));
        for (const std::vector<std::shared_ptr<Predicate>>& children :
             {std::vector<std::shared_ptr<Predicate>>{nullptr, unsupported},
              std::vector<std::shared_ptr<Predicate>>{unsupported, nullptr}}) {
            ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                std::make_shared<CompoundPredicateImpl>(*function, children), schema,
                {"null child"}));
        }
    }
    const std::shared_ptr<Predicate> comparison =
        PredicateBuilder::Equal(0, "i", FieldType::INT, Literal(int32_t{1}));
    ASSERT_NO_FATAL_FAILURE(
        AssertInvalidConversion(std::make_shared<FunctionOnlyPredicate>(comparison->GetFunction()),
                                schema, {"LeafPredicate"}));
}

TEST(VortexPredicateConverterTest, ComparisonArityReturnsInvalid) {
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    for (ComparisonBuilder build :
         {PredicateBuilder::Equal, PredicateBuilder::NotEqual, PredicateBuilder::LessThan,
          PredicateBuilder::LessOrEqual, PredicateBuilder::GreaterThan,
          PredicateBuilder::GreaterOrEqual}) {
        const auto valid = std::dynamic_pointer_cast<LeafPredicateImpl>(
            build(0, "i", FieldType::INT, Literal(int32_t{1})));
        ASSERT_NE(valid, nullptr);
        for (const std::vector<Literal>& literals :
             {std::vector<Literal>{},
              std::vector<Literal>{Literal(int32_t{1}), Literal(int32_t{2})}}) {
            const std::shared_ptr<Predicate> malformed = std::make_shared<LeafPredicateImpl>(
                valid->GetLeafFunction(), 0, "i", FieldType::INT, literals);
            ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(malformed, TestSchema(),
                                                            {"i", "requires exactly one literal"}));
            const std::shared_ptr<Predicate> unsupported =
                PredicateBuilder::In(0, "i", FieldType::INT, {Literal(FieldType::INT)});
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> disjunction,
                                 PredicateBuilder::Or({unsupported, malformed}));
            ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(disjunction, TestSchema(),
                                                            {"i", "requires exactly one literal"}));
        }
    }
}

TEST(VortexPredicateConverterTest, NestedCompoundsPreserveNativeCandidatesAndPropagateErrors) {
    const NativeInCase test_case = {
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
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case, partial_and, {2, 3}, {2}));
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeAndResidualRows(test_case, fallback_or, {0, 1, 2, 3}, {0, 2, 3}, {}, false));
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case, nested_and, {0}, {0}));
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case, nested_or, {0, 2, 3}, {0, 2}));

    const std::shared_ptr<Predicate> invalid =
        PredicateBuilder::Equal(1, "value", FieldType::STRING, Literal(int64_t{1}));
    using CompoundBuilder = decltype(&PredicateBuilder::And);
    const std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("value", arrow::utf8())});
    for (CompoundBuilder outer : {PredicateBuilder::And, PredicateBuilder::Or}) {
        for (CompoundBuilder inner : {PredicateBuilder::And, PredicateBuilder::Or}) {
            for (bool invalid_first : {false, true}) {
                ASSERT_OK_AND_ASSIGN(
                    std::shared_ptr<Predicate> nested,
                    inner(invalid_first
                              ? std::vector<std::shared_ptr<Predicate>>{invalid, unsupported}
                              : std::vector<std::shared_ptr<Predicate>>{unsupported, invalid}));
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                                     outer({unsupported, nested}));
                ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                    predicate, schema, {"value", "expected STRING", "actual BIGINT"}));
            }
        }
    }
}

TEST(VortexPredicateConverterTest, NativeScalarErrorsPropagateThroughAllPredicateBranches) {
    const Literal invalid_utf8(FieldType::STRING, "\xff", 1);
    std::vector<std::shared_ptr<Predicate>> invalid;
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    for (ComparisonBuilder build :
         {PredicateBuilder::Equal, PredicateBuilder::NotEqual, PredicateBuilder::LessThan,
          PredicateBuilder::LessOrEqual, PredicateBuilder::GreaterThan,
          PredicateBuilder::GreaterOrEqual}) {
        invalid.push_back(build(1, "s", FieldType::STRING, invalid_utf8));
    }
    invalid.push_back(PredicateBuilder::In(1, "s", FieldType::STRING,
                                           {Literal(FieldType::STRING, "valid", 5), invalid_utf8}));
    invalid.push_back(PredicateBuilder::NotIn(
        1, "s", FieldType::STRING, {Literal(FieldType::STRING, "valid", 5), invalid_utf8}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> unsupported,
                         PredicateBuilder::StartsWith(1, "s", FieldType::STRING,
                                                      Literal(FieldType::STRING, "v", 1)));
    for (const std::shared_ptr<Predicate>& predicate : invalid) {
        ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
            predicate, TestSchema(), {"create Vortex predicate scalar", "invalid utf-8"}));
        using CompoundBuilder = decltype(&PredicateBuilder::And);
        for (CompoundBuilder build : {PredicateBuilder::And, PredicateBuilder::Or}) {
            for (bool invalid_first : {false, true}) {
                ASSERT_OK_AND_ASSIGN(
                    std::shared_ptr<Predicate> compound,
                    build(invalid_first
                              ? std::vector<std::shared_ptr<Predicate>>{predicate, unsupported}
                              : std::vector<std::shared_ptr<Predicate>>{unsupported, predicate}));
                ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                    compound, TestSchema(), {"create Vortex predicate scalar", "invalid utf-8"}));
            }
        }
    }
    // The list budget is still checked before inspecting a declined native scalar.
    std::vector<Literal> over_budget(33, invalid_utf8);
    ASSERT_FALSE(
        Pushed(PredicateBuilder::In(1, "s", FieldType::STRING, over_budget), TestSchema()));
    ASSERT_FALSE(
        Pushed(PredicateBuilder::NotIn(1, "s", FieldType::STRING, over_budget), TestSchema()));
    ASSERT_TRUE(Pushed(
        PredicateBuilder::Equal(1, "s", FieldType::STRING, Literal(FieldType::STRING, "valid", 5)),
        TestSchema()));
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
        {"paimon-calendar-limit", arrow::timestamp(arrow::TimeUnit::MICRO), FieldType::TIMESTAMP,
         Literal(Timestamp::FromEpochMillis(253402300799999, 999000))},
        {"native-epoch-seconds", arrow::timestamp(arrow::TimeUnit::SECOND, "UTC"),
         FieldType::TIMESTAMP, Literal(Timestamp::FromEpochMillis(253402207201000))},
        {"native-epoch-millis", arrow::timestamp(arrow::TimeUnit::MILLI), FieldType::TIMESTAMP,
         Literal(Timestamp::FromEpochMillis(253402207201000))},
        {"maximum-date", arrow::date32(), FieldType::DATE,
         Literal(FieldType::DATE, std::numeric_limits<int32_t>::max())},
        {"minimum-date", arrow::date32(), FieldType::DATE,
         Literal(FieldType::DATE, std::numeric_limits<int32_t>::min())}};
    using ComparisonBuilder = decltype(&PredicateBuilder::Equal);
    for (const TemporalCase& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("value", test_case.type)});
        for (ComparisonBuilder build :
             {PredicateBuilder::Equal, PredicateBuilder::NotEqual, PredicateBuilder::LessThan,
              PredicateBuilder::LessOrEqual, PredicateBuilder::GreaterThan,
              PredicateBuilder::GreaterOrEqual}) {
            ASSERT_FALSE(
                Pushed(build(0, "value", test_case.field_type, test_case.literal), schema));
        }
        for (const std::shared_ptr<Predicate>& predicate :
             {PredicateBuilder::In(0, "value", test_case.field_type, {test_case.literal}),
              PredicateBuilder::NotIn(0, "value", test_case.field_type, {test_case.literal})}) {
            ASSERT_FALSE(Pushed(predicate, schema));
        }
    }
    const std::shared_ptr<arrow::Schema> nanos_schema =
        arrow::schema({arrow::field("value", arrow::timestamp(arrow::TimeUnit::NANO, "UTC"))});
    // INT64_MAX nanoseconds is in year 2262 and remains a valid native timestamp.
    ASSERT_TRUE(Pushed(
        PredicateBuilder::Equal(0, "value", FieldType::TIMESTAMP,
                                Literal(Timestamp::FromEpochMillis(
                                    maximum / 1000000, static_cast<int32_t>(maximum % 1000000)))),
        nanos_schema));
}

TEST(VortexPredicateConverterTest, TemporalRangeFallbacksDoNotHideRealErrors) {
    const std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("date", arrow::date32()), arrow::field("name", arrow::utf8())});
    const Literal extreme(FieldType::DATE, std::numeric_limits<int32_t>::max());
    const Literal wrong_type(int64_t{1});
    const std::shared_ptr<Predicate> unsupported =
        PredicateBuilder::LessThan(0, "date", FieldType::DATE, extreme);
    for (bool negate : {false, true}) {
        for (bool extreme_first : {false, true}) {
            const std::vector<Literal> literals = extreme_first
                                                      ? std::vector<Literal>{extreme, wrong_type}
                                                      : std::vector<Literal>{wrong_type, extreme};
            const std::shared_ptr<Predicate> predicate =
                negate ? PredicateBuilder::NotIn(0, "date", FieldType::DATE, literals)
                       : PredicateBuilder::In(0, "date", FieldType::DATE, literals);
            ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                predicate, schema, {"date", "expected DATE", "actual BIGINT"}));
        }
    }
    const auto valid =
        PredicateBuilder::Equal(0, "date", FieldType::DATE, Literal(FieldType::DATE, 0));
    const auto valid_leaf = std::dynamic_pointer_cast<LeafPredicateImpl>(valid);
    ASSERT_NE(valid_leaf, nullptr);
    const auto malformed = std::make_shared<LeafPredicateImpl>(
        valid_leaf->GetLeafFunction(), 0, "date", FieldType::DATE, std::vector<Literal>{});
    const std::vector<std::pair<std::shared_ptr<Predicate>, std::string>> errors = {
        {PredicateBuilder::Equal(0, "date", FieldType::DATE, wrong_type), "Literal type mismatch"},
        {PredicateBuilder::Equal(1, "name", FieldType::STRING,
                                 Literal(FieldType::STRING, "\xff", 1)),
         "invalid utf-8"},
        {malformed, "requires exactly one literal"}};
    using CompoundBuilder = decltype(&PredicateBuilder::And);
    for (const auto& [invalid, message] : errors) {
        for (CompoundBuilder build : {PredicateBuilder::And, PredicateBuilder::Or}) {
            for (bool invalid_first : {false, true}) {
                ASSERT_OK_AND_ASSIGN(
                    std::shared_ptr<Predicate> compound,
                    build(invalid_first
                              ? std::vector<std::shared_ptr<Predicate>>{invalid, unsupported}
                              : std::vector<std::shared_ptr<Predicate>>{unsupported, invalid}));
                ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(compound, schema, {message}));
            }
        }
    }
}

TEST(VortexPredicateConverterTest, MembershipFallbacksDoNotHideTypeErrors) {
    const Literal null_value(FieldType::INT);
    const Literal wrong_type(int64_t{1});
    const Literal valid(int32_t{1});
    for (bool negate : {false, true}) {
        for (bool invalid_first : {false, true}) {
            const std::vector<Literal> literals =
                invalid_first ? std::vector<Literal>{wrong_type, null_value}
                              : std::vector<Literal>{null_value, wrong_type};
            const std::shared_ptr<Predicate> predicate =
                negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, literals)
                       : PredicateBuilder::In(0, "i", FieldType::INT, literals);
            ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                predicate, TestSchema(), {"i", "expected INT", "actual BIGINT"}));
        }
        const std::shared_ptr<Predicate> fallback =
            negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, {null_value, valid})
                   : PredicateBuilder::In(0, "i", FieldType::INT, {null_value, valid});
        ASSERT_FALSE(Pushed(fallback, TestSchema()));
    }
}

TEST(VortexPredicateConverterTest, MembershipTypeErrorsAreIndependentOfMaximum) {
    struct TypeCase {
        std::shared_ptr<arrow::DataType> type;
        FieldType field_type;
        Literal valid;
        const char* expected_type;
    };
    const std::vector<TypeCase> cases = {
        {arrow::int32(), FieldType::INT, Literal(int32_t{1}), "INT"},
        {arrow::decimal128(10, 2), FieldType::DECIMAL, Literal(Decimal(38, 4, 12300)), "DECIMAL"}};
    for (const TypeCase& test_case : cases) {
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("value", test_case.type)});
        for (int32_t position : {0, 16, 32}) {
            std::vector<Literal> literals(33, test_case.valid);
            literals[position] = Literal(int64_t{2});
            for (uint32_t maximum : {uint32_t{0}, uint32_t{32}, uint32_t{33}}) {
                SCOPED_TRACE(fmt::format("type={}, maximum={}, position={}",
                                         test_case.expected_type, maximum, position));
                for (bool negate : {false, true}) {
                    SCOPED_TRACE(negate);
                    const std::shared_ptr<Predicate> predicate =
                        negate ? PredicateBuilder::NotIn(0, "value", test_case.field_type, literals)
                               : PredicateBuilder::In(0, "value", test_case.field_type, literals);
                    ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                        predicate, schema,
                        {"value", fmt::format("expected {}", test_case.expected_type),
                         "actual BIGINT"},
                        {maximum}));
                }
            }
        }
    }

    // File/predicate mismatches must also be independent of the membership maximum.
    const std::shared_ptr<arrow::Schema> bigint_schema =
        arrow::schema({arrow::field("i", arrow::int64())});
    const std::vector<Literal> literals(33, Literal(int32_t{1}));
    for (uint32_t maximum : {uint32_t{0}, uint32_t{32}, uint32_t{33}}) {
        for (bool negate : {false, true}) {
            const std::shared_ptr<Predicate> predicate =
                negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, literals)
                       : PredicateBuilder::In(0, "i", FieldType::INT, literals);
            ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                predicate, bigint_schema, {"i", "expected BIGINT", "actual INT"}, {maximum}));
        }
    }
}

TEST(VortexPredicateConverterTest, OversizedMembershipFallbacksDoNotHideTypeErrors) {
    struct TypeCase {
        std::shared_ptr<arrow::DataType> type;
        FieldType field_type;
        Literal valid;
        Literal first;
        const char* expected_type;
    };
    const std::vector<TypeCase> cases = {
        {arrow::int32(), FieldType::INT, Literal(int32_t{1}), Literal(FieldType::INT), "INT"},
        {arrow::float32(), FieldType::FLOAT, Literal(1.0f), Literal(1.0f), "FLOAT"},
        {arrow::float64(), FieldType::DOUBLE, Literal(1.0), Literal(1.0), "DOUBLE"}};
    for (const TypeCase& test_case : cases) {
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field("value", test_case.type)});
        std::vector<Literal> literals(33, test_case.valid);
        literals.front() = test_case.first;
        literals.back() = Literal(int64_t{2});
        for (uint32_t maximum : {uint32_t{0}, uint32_t{32}, uint32_t{33}}) {
            SCOPED_TRACE(fmt::format("type={}, maximum={}", test_case.expected_type, maximum));
            for (bool negate : {false, true}) {
                const std::shared_ptr<Predicate> predicate =
                    negate ? PredicateBuilder::NotIn(0, "value", test_case.field_type, literals)
                           : PredicateBuilder::In(0, "value", test_case.field_type, literals);
                ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                    predicate, schema,
                    {"value", fmt::format("expected {}", test_case.expected_type), "actual BIGINT"},
                    {maximum}));
            }
        }
    }
}

TEST(VortexPredicateConverterTest, MembershipTimestampValueChecksRemainWithinMaximum) {
    const std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("value", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))});
    const int64_t maximum = std::numeric_limits<int64_t>::max();
    const Literal native_out_of_range(
        Timestamp::FromEpochMillis(maximum / 1000, static_cast<int32_t>((maximum % 1000) * 1000)));
    const Literal precision_loss(Timestamp::FromEpochMillis(0, 1));
    for (bool negate : {false, true}) {
        for (bool unsafe_first : {false, true}) {
            std::vector<Literal> literals(33, native_out_of_range);
            literals[unsafe_first ? 0 : 32] = precision_loss;
            const std::shared_ptr<Predicate> predicate =
                negate ? PredicateBuilder::NotIn(0, "value", FieldType::TIMESTAMP, literals)
                       : PredicateBuilder::In(0, "value", FieldType::TIMESTAMP, literals);
            for (uint32_t limit : {uint32_t{0}, uint32_t{32}, uint32_t{33}}) {
                ASSERT_FALSE(Pushed(predicate, schema, {limit}));
            }
        }
        const std::vector<Literal> literals(33, native_out_of_range);
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "value", FieldType::TIMESTAMP, literals)
                   : PredicateBuilder::In(0, "value", FieldType::TIMESTAMP, literals);
        ASSERT_FALSE(Pushed(predicate, schema));
        ASSERT_FALSE(Pushed(predicate, schema, {33}));
    }
}

TEST(VortexPredicateConverterTest, MembershipMaximumCountsRawValuesForInAndNotIn) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    for (uint32_t maximum : {uint32_t{1}, uint32_t{7}, uint32_t{32}, uint32_t{64}}) {
        for (uint32_t size : {maximum - 1, maximum, maximum + 1}) {
            SCOPED_TRACE(fmt::format("maximum={}, size={}", maximum, size));
            std::vector<Literal> literals;
            for (uint32_t value = 0; value < size; ++value) {
                const std::string text = fmt::format("value-{}", value);
                literals.emplace_back(FieldType::STRING, text.data(), text.size());
            }
            for (bool negate : {false, true}) {
                const std::shared_ptr<Predicate> predicate =
                    negate ? PredicateBuilder::NotIn(1, "s", FieldType::STRING, literals)
                           : PredicateBuilder::In(1, "s", FieldType::STRING, literals);
                ASSERT_EQ(Pushed(predicate, schema, {maximum}), size != 0 && size <= maximum);
                ASSERT_EQ(Pushed(predicate, schema), size != 0 && size <= 32);
                ASSERT_FALSE(Pushed(predicate, schema, {0}));
            }
        }
    }
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
    const auto not_in = PredicateBuilder::NotIn(4, "l", FieldType::BIGINT, longs);
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
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    for (uint32_t size : {uint32_t{31}, uint32_t{32}, uint32_t{33}, uint32_t{4096}}) {
        const std::vector<Literal> strings(size, Literal(FieldType::STRING, "same", 4));
        const std::vector<Literal> integers(size, Literal(int32_t{7}));
        for (bool negate : {false, true}) {
            const std::shared_ptr<Predicate> string_predicate =
                negate ? PredicateBuilder::NotIn(1, "s", FieldType::STRING, strings)
                       : PredicateBuilder::In(1, "s", FieldType::STRING, strings);
            const std::shared_ptr<Predicate> integer_predicate =
                negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, integers)
                       : PredicateBuilder::In(0, "i", FieldType::INT, integers);
            ASSERT_EQ(Pushed(string_predicate, schema), size <= 32);
            ASSERT_EQ(Pushed(integer_predicate, schema), size <= 32);
            ASSERT_FALSE(Pushed(string_predicate, schema, {size - 1}));
            ASSERT_FALSE(Pushed(integer_predicate, schema, {size - 1}));
            if (size <= 33) {
                ASSERT_TRUE(Pushed(string_predicate, schema, {size}));
                ASSERT_TRUE(Pushed(integer_predicate, schema, {size}));
            }
        }
    }
}

TEST(VortexPredicateConverterTest,
     HugeContinuousMembershipListsAreRejectedBeforeNativeConstruction) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> literals;
    for (int32_t value = 0; value < 65536; ++value) {
        literals.emplace_back(value);
    }
    // Do not construct a giant native expression even though the list is one continuous range.
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, literals)
                   : PredicateBuilder::In(0, "i", FieldType::INT, literals);
        ASSERT_FALSE(Pushed(predicate, schema));
        ASSERT_FALSE(Pushed(predicate, schema, {65535}));
        ASSERT_FALSE(Pushed(predicate, schema, {0}));
    }
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
        ASSERT_FALSE(Pushed(predicate, schema));
        ASSERT_FALSE(Pushed(predicate, schema, {159}));
        ASSERT_TRUE(Pushed(predicate, schema, {160}));
    }
}

TEST(VortexPredicateConverterTest, MixedRangesAndSingletonsUseRawMembershipLength) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    std::vector<Literal> literals = {Literal(int32_t{20}), Literal(int32_t{50})};
    for (int32_t value = 1; value <= 10; ++value) {
        literals.emplace_back(value);
        literals.emplace_back(value + 29);
    }
    for (bool negate : {false, true}) {
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, literals)
                   : PredicateBuilder::In(0, "i", FieldType::INT, literals);
        ASSERT_FALSE(Pushed(predicate, schema, {21}));
        ASSERT_TRUE(Pushed(predicate, schema, {22}));
    }
}

TEST(VortexPredicateConverterTest, LargerMembershipMaximumStillRejectsUnsafeLiterals) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    const VortexPredicateOptions larger_maximum{2048};
    std::vector<Literal> literals;
    for (int32_t value = 0; value < 1024; ++value) {
        literals.emplace_back(value);
    }
    for (const Literal& unsafe : {Literal(FieldType::INT), Literal(int64_t{1024})}) {
        std::vector<Literal> invalid = literals;
        invalid.push_back(unsafe);
        for (bool negate : {false, true}) {
            const std::shared_ptr<Predicate> predicate =
                negate ? PredicateBuilder::NotIn(0, "i", FieldType::INT, invalid)
                       : PredicateBuilder::In(0, "i", FieldType::INT, invalid);
            if (unsafe.IsNull()) {
                ASSERT_FALSE(Pushed(predicate, schema, larger_maximum));
            } else {
                ASSERT_NO_FATAL_FAILURE(AssertInvalidConversion(
                    predicate, schema, {"i", "expected INT", "actual BIGINT"}, larger_maximum));
            }
        }
    }
    ASSERT_NO_FATAL_FAILURE(
        AssertInvalidConversion(PredicateBuilder::In(0, "i", FieldType::INT, literals),
                                arrow::schema({arrow::field("i", arrow::int64())}),
                                {"i", "expected BIGINT", "actual INT"}, larger_maximum));
    for (bool negate : {false, true}) {
        const std::vector<Literal> timestamps = {
            Literal(Timestamp::FromEpochMillis(0)),
            Literal(Timestamp::FromEpochMillis(std::numeric_limits<int64_t>::max()))};
        const std::shared_ptr<Predicate> predicate =
            negate ? PredicateBuilder::NotIn(0, "ts", FieldType::TIMESTAMP, timestamps)
                   : PredicateBuilder::In(0, "ts", FieldType::TIMESTAMP, timestamps);
        ASSERT_FALSE(Pushed(
            predicate, arrow::schema({arrow::field("ts", arrow::timestamp(arrow::TimeUnit::NANO))}),
            larger_maximum));
    }
}

TEST(VortexPredicateConverterTest, ZeroMembershipMaximumPreservesOtherNativePredicates) {
    const std::shared_ptr<arrow::Schema> schema = TestSchema();
    const VortexPredicateOptions off{0};
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

TEST(VortexPredicateConverterTest, SparseMembershipMaximumReturnsNativeOrCandidateRows) {
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
            AssertNativeAndResidualRows(test_case, predicate, {0, 1, 2, 3}, expected, {}, false));
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeAndResidualRows(test_case, predicate, expected, expected, {33}));
        ASSERT_NO_FATAL_FAILURE(
            AssertNativeAndResidualRows(test_case, predicate, {0, 1, 2, 3}, expected, {0}, false));
    }
}

TEST(VortexPredicateConverterTest, StringMembershipMaximumAndPartialAndOrUseSafeCandidates) {
    NativeInCase test_case = {
        "string_budget", arrow::utf8(), FieldType::STRING, R"(["s0",null,"absent","s32"])", {}};
    for (int32_t value = 0; value < 33; ++value) {
        const std::string text = fmt::format("s{}", value);
        test_case.literals.emplace_back(FieldType::STRING, text.data(), text.size());
    }
    const std::shared_ptr<Predicate> in =
        PredicateBuilder::In(1, "value", FieldType::STRING, test_case.literals);
    ASSERT_NO_FATAL_FAILURE(
        AssertNativeAndResidualRows(test_case, in, {0, 1, 2, 3}, {0, 3}, {}, false));
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case, in, {0, 3}, {0, 3}, {33}));
    const std::shared_ptr<Predicate> less_than_three =
        PredicateBuilder::LessThan(0, "rowid", FieldType::BIGINT, Literal(int64_t{3}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({in, less_than_three}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({in, less_than_three}));
    // AND keeps the other child, so the residual receives a safe superset, including null rows.
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case, and_predicate, {0, 1, 2}, {0}));
    // Dropping only the IN child of OR would lose row 3; reject the entire native OR instead.
    ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(test_case, or_predicate, {0, 1, 2, 3},
                                                        {0, 1, 2, 3}, {}, false));
}

TEST(VortexPredicateConverterTest, NullPredicateIsNotPushed) {
    ASSERT_FALSE(Pushed(nullptr, TestSchema()));
}

}  // namespace
}  // namespace paimon::vortex::test
