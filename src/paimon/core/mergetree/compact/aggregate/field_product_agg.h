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

#pragma once

#include <functional>
#include <memory>
#include <string>

#include "paimon/common/data/data_define.h"
#include "paimon/core/mergetree/compact/aggregate/field_aggregator.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {
/// Multiplies non-null values of a field across rows.
///
/// Retraction divides the accumulator by the retracted value. Everything Java rejects is rejected
/// here as an error rather than by wrapping around: an integer product or quotient outside the
/// field type, an integer division by zero, and a decimal quotient with no finite decimal
/// expansion. A decimal result that no longer fits the field precision aggregates to null, which is
/// what Java's Decimal.fromBigDecimal() yields for it.
class FieldProductAgg : public FieldAggregator {
 public:
    static Result<std::unique_ptr<FieldProductAgg>> Create(
        const std::shared_ptr<arrow::DataType>& field_type,
        const std::shared_ptr<MemoryPool>& pool);

    Result<VariantType> Agg(const VariantType& accumulator,
                            const VariantType& input_field) override {
        bool accumulator_null = DataDefine::IsVariantNull(accumulator);
        bool input_null = DataDefine::IsVariantNull(input_field);
        if (accumulator_null || input_null) {
            return accumulator_null ? input_field : accumulator;
        }
        return multiply_func_(accumulator, input_field);
    }

    Result<VariantType> Retract(const VariantType& accumulator,
                                const VariantType& input_field) const override {
        if (DataDefine::IsVariantNull(accumulator) || DataDefine::IsVariantNull(input_field)) {
            return accumulator;
        }
        return divide_func_(accumulator, input_field);
    }

 public:
    static constexpr char NAME[] = "product";

 private:
    using FieldArithmeticFunc = std::function<Result<VariantType>(const VariantType& accumulator,
                                                                  const VariantType& input_field)>;

    FieldProductAgg(const std::shared_ptr<arrow::DataType>& field_type,
                    const FieldArithmeticFunc& multiply_func,
                    const FieldArithmeticFunc& divide_func, const std::shared_ptr<MemoryPool>& pool)
        : FieldAggregator(std::string(NAME), field_type, pool),
          multiply_func_(multiply_func),
          divide_func_(divide_func) {}

    static Result<FieldArithmeticFunc> CreateMultiplyFunc(
        const std::shared_ptr<arrow::DataType>& field_type);

    static Result<FieldArithmeticFunc> CreateDivideFunc(
        const std::shared_ptr<arrow::DataType>& field_type);

 private:
    FieldArithmeticFunc multiply_func_;
    FieldArithmeticFunc divide_func_;
};
}  // namespace paimon
