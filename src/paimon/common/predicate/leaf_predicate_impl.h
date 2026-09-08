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

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/predicate/compound_function.h"
#include "paimon/common/predicate/leaf_function.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/predicate/leaf_predicate.h"
namespace paimon {
class LeafPredicateImpl : public LeafPredicate, public PredicateFilter {
 public:
    LeafPredicateImpl(const LeafFunction& leaf_function, int32_t field_index,
                      const std::string& field_name, const FieldType& field_type,
                      const std::vector<Literal>& literals)
        : LeafPredicate(leaf_function, field_index, field_name, field_type, literals) {}

    const LeafFunction& GetLeafFunction() const {
        return leaf_function_;
    }

    Result<std::vector<char>> Test(const arrow::Array& array,
                                   arrow::MemoryPool* pool) const override {
        const auto& struct_array = checked_cast<const arrow::StructArray&>(array);
        if (field_index_ >= static_cast<int32_t>(struct_array.fields().size())) {
            return Status::Invalid(
                fmt::format("field index {} exceed field count {} in struct array", field_index_,
                            struct_array.fields().size()));
        }
        const auto& field_array = struct_array.field(field_index_);
        return leaf_function_.Test(*field_array, literals_, pool);
    }

    Result<std::vector<char>> TestSelected(const arrow::Array& array,
                                           const std::vector<int64_t>& selection,
                                           arrow::MemoryPool* pool) const override {
        const auto& struct_array = checked_cast<const arrow::StructArray&>(array);
        if (field_index_ >= static_cast<int32_t>(struct_array.fields().size())) {
            return Status::Invalid(
                fmt::format("field index {} exceed field count {} in struct array", field_index_,
                            struct_array.fields().size()));
        }
        const auto& field_array = struct_array.field(field_index_);
        // Preserve eager conversion/comparison errors, even when the offending row is not
        // selected. Dictionary and unsupported physical types also retain the original path.
        // LIKE validates escapes while examining values; temporal/decimal conversions may fail
        // on rows outside the candidate set as well.
        if (leaf_function_.GetType() == Function::Type::LIKE ||
            field_array->type_id() == arrow::Type::TIMESTAMP ||
            field_array->type_id() == arrow::Type::DECIMAL128) {
            return PredicateFilter::TestSelected(array, selection, pool);
        }
        auto field_type = FieldTypeUtils::ConvertToFieldType(field_array->type_id());
        if (!field_type.ok()) {
            return PredicateFilter::TestSelected(array, selection, pool);
        }
        for (const auto& literal : literals_) {
            if (!literal.IsNull() && literal.GetType() != field_type.value()) {
                return PredicateFilter::TestSelected(array, selection, pool);
            }
        }
        // Gather only the predicate column, not unrelated payload columns.
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> selected,
                               SelectRows(*field_array, selection, pool));
        return leaf_function_.Test(*selected, literals_, pool);
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema,
                      const InternalRow& row) const override {
        if (field_index_ >= row.GetFieldCount()) {
            return Status::Invalid(fmt::format("field index {} exceed field count {} in row",
                                               field_index_, row.GetFieldCount()));
        }
        PAIMON_ASSIGN_OR_RAISE(Literal value, LiteralConverter::ConvertLiteralsFromRow(
                                                  schema, row, field_index_, field_type_));
        return leaf_function_.Test(value, literals_);
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                      const InternalRow& min_values, const InternalRow& max_values,
                      const InternalArray& null_counts) const override {
        PAIMON_ASSIGN_OR_RAISE(Literal min_value,
                               LiteralConverter::ConvertLiteralsFromRow(schema, min_values,
                                                                        field_index_, field_type_));
        PAIMON_ASSIGN_OR_RAISE(Literal max_value,
                               LiteralConverter::ConvertLiteralsFromRow(schema, max_values,
                                                                        field_index_, field_type_));
        std::optional<int64_t> null_count = null_counts.IsNullAt(field_index_)
                                                ? std::optional<int64_t>()
                                                : null_counts.GetLong(field_index_);
        if (null_count == std::nullopt || row_count != null_count.value()) {
            // not all null
            // min or max is null
            // unknown stats
            if (min_value.IsNull() || max_value.IsNull()) {
                return true;
            }
        }
        return leaf_function_.Test(row_count, min_value, max_value, null_count, literals_);
    }

    std::shared_ptr<LeafPredicateImpl> NewLeafPredicate(int32_t new_field_index) const {
        return std::make_shared<LeafPredicateImpl>(leaf_function_, new_field_index, field_name_,
                                                   field_type_, literals_);
    }

    std::shared_ptr<LeafPredicateImpl> NewLeafPredicate(const std::string& new_field_name) const {
        return std::make_shared<LeafPredicateImpl>(leaf_function_, field_index_, new_field_name,
                                                   field_type_, literals_);
    }
};
}  // namespace paimon
