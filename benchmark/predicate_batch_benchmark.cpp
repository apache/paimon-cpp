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

// Incremental batch-predicate variants: original Literal conversion, cheap field bounds,
// typed equality, and candidate selection. Run with --benchmark_repetitions=6 to report
// aggregate timings. These are microbenchmarks, not end-to-end read latencies.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "benchmark/benchmark.h"
#include "fmt/format.h"
#include "paimon/common/predicate/equal.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/predicate_builder.h"

namespace paimon {
namespace {
class LiteralBatchEqual : public NullFalseLeafBinaryFunction {
 public:
    using NullFalseLeafBinaryFunction::Test;
    Result<bool> Test(const Literal& value, const Literal& literal) const override {
        return Equal::Instance().Test(value, literal);
    }
    Result<bool> Test(int64_t rows, const Literal& min, const Literal& max,
                      const std::optional<int64_t>& nulls, const Literal& literal) const override {
        return Equal::Instance().Test(rows, min, max, nulls, literal);
    }
    Type GetType() const override {
        return Type::EQUAL;
    }
    std::string ToString() const override {
        return "LiteralBatchEqual";
    }
    const LeafFunction* Negate() const override {
        return Equal::Instance().Negate();
    }
};

Result<std::vector<char>> TestEager(const std::shared_ptr<Predicate>& predicate,
                                    const arrow::Array& array, arrow::MemoryPool* pool,
                                    bool literal_equal = false, bool box_all_fields = false) {
    auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (!compound) {
        if (literal_equal || box_all_fields) {
            auto leaf = std::dynamic_pointer_cast<LeafPredicateImpl>(predicate);
            if (!leaf || leaf->GetFunction().GetType() != Function::Type::EQUAL) {
                return Status::Invalid("benchmark reference requires equality leaves");
            }
            const auto& batch = checked_cast<const arrow::StructArray&>(array);
            const int32_t field_count =
                box_all_fields ? static_cast<int32_t>(batch.fields().size()) : batch.num_fields();
            if (leaf->FieldIndex() >= field_count) {
                return Status::Invalid("benchmark field index out of bounds");
            }
            const auto& field = batch.field(leaf->FieldIndex());
            const LiteralBatchEqual reference;
            const LeafFunction& legacy = reference;
            const LeafFunction& typed = Equal::Instance();
            const LeafFunction& function = literal_equal ? legacy : typed;
            return function.Test(*field, leaf->Literals(), pool);
        }
        return std::dynamic_pointer_cast<PredicateFilter>(predicate)->Test(array, pool);
    }
    const bool is_and = predicate->GetFunction().GetType() == Function::Type::AND;
    std::vector<char> result(array.length(), is_and);
    for (const auto& child : compound->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<char> matches,
                               TestEager(child, array, pool, literal_equal, box_all_fields));
        for (size_t i = 0; i < result.size(); ++i) {
            result[i] = is_and ? (result[i] & matches[i]) : (result[i] | matches[i]);
        }
    }
    return result;
}

struct BatchInput {
    std::shared_ptr<arrow::Array> array;
    std::shared_ptr<Predicate> predicate;
};

Result<BatchInput> MakeInput(int64_t period) {
    constexpr int64_t kRows = 8192;
    arrow::Int64Builder keys;
    arrow::StringBuilder values;
    for (int64_t i = 0; i < kRows; ++i) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(keys.Append(i % period));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(values.Append("q15"));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> key_array, keys.Finish());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> value_array, values.Finish());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> array,
        arrow::StructArray::Make({key_array, value_array},
                                 std::vector<std::string>{"key", "value"}));
    std::vector<std::shared_ptr<Predicate>> alternatives;
    for (int32_t i = 0; i < 16; ++i) {
        const std::string value = fmt::format("q{}", i);
        alternatives.push_back(PredicateBuilder::Equal(
            1, "value", FieldType::STRING, Literal(FieldType::STRING, value.data(), value.size())));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> inner, PredicateBuilder::Or(alternatives));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Predicate> predicate,
        PredicateBuilder::And(
            {PredicateBuilder::Equal(0, "key", FieldType::BIGINT, Literal(int64_t{0})), inner}));
    return BatchInput{array, predicate};
}

void BatchEvaluation(benchmark::State& state, int32_t variant) {
    auto input_result = MakeInput(state.range(0));
    if (!input_result.ok()) {
        state.SkipWithError(input_result.status().ToString().c_str());
        return;
    }
    const auto& input = input_result.value();
    auto pool = arrow::default_memory_pool();
    auto reference = TestEager(input.predicate, *input.array, pool,
                               /*literal_equal=*/true, /*box_all_fields=*/true);
    if (!reference.ok()) {
        state.SkipWithError(reference.status().ToString().c_str());
        return;
    }
    auto filter = std::dynamic_pointer_cast<PredicateFilter>(input.predicate);
    for (auto _ : state) {
        auto actual =
            variant < 3 ? TestEager(input.predicate, *input.array, pool,
                                    /*literal_equal=*/variant < 2, /*box_all_fields=*/variant == 0)
                        : filter->Test(*input.array, pool);
        if (!actual.ok()) {
            state.SkipWithError(actual.status().ToString().c_str());
            break;
        }
        benchmark::DoNotOptimize(actual.value().data());
        if (actual.value() != reference.value()) {
            state.SkipWithError("predicate result differs from eager Literal evaluation");
            break;
        }
    }
    state.SetItemsProcessed(state.iterations() * input.array->length());
}

BENCHMARK_CAPTURE(BatchEvaluation, Original, 0)
    ->Arg(8192)
    ->Arg(16)
    ->Arg(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BatchEvaluation, FieldBounds, 1)
    ->Arg(8192)
    ->Arg(16)
    ->Arg(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BatchEvaluation, TypedEquality, 2)
    ->Arg(8192)
    ->Arg(16)
    ->Arg(1)
    ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BatchEvaluation, CandidateSelection, 3)
    ->Arg(8192)
    ->Arg(16)
    ->Arg(1)
    ->Unit(benchmark::kMillisecond);
}  // namespace
}  // namespace paimon

BENCHMARK_MAIN();
