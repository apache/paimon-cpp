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

#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/type_fwd.h"
#include "fmt/format.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"

namespace paimon {
class CompoundFunction : public Function {
 public:
    // input array is the struct array of all fields
    // `pool` is where any arrow buffer the evaluation allocates comes from, it must not be null.
    virtual Result<std::vector<char>> Test(const arrow::Array& array,
                                           const std::vector<std::shared_ptr<Predicate>>& children,
                                           arrow::MemoryPool* pool) const = 0;

    virtual Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, const InternalRow& row,
                              const std::vector<std::shared_ptr<Predicate>>& children) const = 0;

    virtual Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                              const InternalRow& min_values, const InternalRow& max_values,
                              const InternalArray& null_counts,
                              const std::vector<std::shared_ptr<Predicate>>& children) const = 0;

    virtual Result<std::vector<char>> TestSelected(
        const arrow::Array& array, const std::vector<std::shared_ptr<Predicate>>& children,
        const std::vector<int64_t>& selection, arrow::MemoryPool* pool) const = 0;

    virtual const CompoundFunction& Negate() const = 0;

 protected:
    static Result<std::vector<char>> TestWithSelection(
        const arrow::Array& array, const std::vector<std::shared_ptr<Predicate>>& children,
        arrow::MemoryPool* pool, bool is_and,
        const std::vector<int64_t>* input_selection = nullptr) {
        std::vector<int64_t> selection;
        if (input_selection) {
            selection = *input_selection;
        } else {
            selection.resize(array.length());
            std::iota(selection.begin(), selection.end(), int64_t{0});
        }
        std::vector<char> results(selection.size(), is_and);
        std::vector<size_t> positions(selection.size());
        std::iota(positions.begin(), positions.end(), size_t{0});
        for (const auto& child : children) {
            auto filter = std::dynamic_pointer_cast<PredicateFilter>(child);
            if (!filter) {
                return Status::Invalid(
                    fmt::format("child filter {} does not support Test", child->ToString()));
            }
            PAIMON_ASSIGN_OR_RAISE(std::vector<char> child_results,
                                   filter->TestSelected(array, selection, pool));
            if (child_results.size() != selection.size()) {
                return Status::Invalid("predicate result size does not match selection size");
            }
            // Avoid rewriting indices and results when every candidate needs the next child.
            if (std::all_of(child_results.begin(), child_results.end(),
                            [is_and](char matched) { return (matched != 0) == is_and; })) {
                continue;
            }
            size_t remaining = 0;
            for (size_t i = 0; i < selection.size(); ++i) {
                const int64_t row = selection[i];
                const bool matched = child_results[i] != 0;
                results[positions[i]] = matched;
                // AND continues only on matches; OR continues only on non-matches.
                if (matched == is_and) {
                    positions[remaining] = positions[i];
                    selection[remaining++] = row;
                }
            }
            selection.resize(remaining);
        }
        return results;
    }
};
}  // namespace paimon
