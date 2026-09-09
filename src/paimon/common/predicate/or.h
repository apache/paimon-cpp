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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "fmt/format.h"
#include "paimon/common/predicate/compound_function.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class InternalArray;
class InternalRow;

/// A `CompoundFunction` to eval or.
class Or : public CompoundFunction {
 public:
    static const Or& Instance();

    Result<std::vector<char>> Test(const arrow::Array& array,
                                   const std::vector<std::shared_ptr<Predicate>>& children,
                                   arrow::MemoryPool* pool) const override {
        std::vector<char> is_valid(array.length(), false);
        for (const auto& child : children) {
            auto child_filter = std::dynamic_pointer_cast<PredicateFilter>(child);
            if (!child_filter) {
                return Status::Invalid(
                    fmt::format("child filter {} does not support Test", child->ToString()));
            }
            PAIMON_ASSIGN_OR_RAISE(std::vector<char> child_valid, child_filter->Test(array, pool));
            for (size_t i = 0; i < is_valid.size(); i++) {
                is_valid[i] = (is_valid[i] | child_valid[i]);
            }
        }
        return is_valid;
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, const InternalRow& row,
                      const std::vector<std::shared_ptr<Predicate>>& children) const override {
        for (const auto& child : children) {
            auto child_filter = std::dynamic_pointer_cast<PredicateFilter>(child);
            if (!child_filter) {
                return Status::Invalid(
                    fmt::format("child filter {} does not support Test", child->ToString()));
            }
            PAIMON_ASSIGN_OR_RAISE(bool is_valid, child_filter->Test(schema, row));
            if (is_valid) {
                return true;
            }
        }
        return false;
    }

    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                      const InternalRow& min_values, const InternalRow& max_values,
                      const InternalArray& null_counts,
                      const std::vector<std::shared_ptr<Predicate>>& children) const override {
        for (const auto& child : children) {
            auto child_filter = std::dynamic_pointer_cast<PredicateFilter>(child);
            if (!child_filter) {
                return Status::Invalid(
                    fmt::format("child filter {} does not support Test", child->ToString()));
            }
            PAIMON_ASSIGN_OR_RAISE(bool is_valid, child_filter->Test(schema, row_count, min_values,
                                                                     max_values, null_counts));
            if (is_valid) {
                return true;
            }
        }
        return false;
    }
    Type GetType() const override {
        return Type::OR;
    }
    const CompoundFunction& Negate() const override;
    std::string ToString() const override {
        return "Or";
    }

 private:
    Or() = default;
};
}  // namespace paimon
