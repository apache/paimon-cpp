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
#include <vector>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/predicate/predicate.h"

namespace paimon {
class PredicateFilter : virtual public Predicate {
 public:
    /// @param array is the struct array of all fields
    /// @param pool is where any arrow buffer the evaluation allocates comes from, it must not be
    ///        null
    virtual Result<std::vector<char>> Test(const arrow::Array& array,
                                           arrow::MemoryPool* pool) const = 0;
    /// Evaluate only the specified batch-local rows, returning results in selection order.
    /// The selection must contain valid, strictly increasing indices. The default retains eager
    /// evaluation, including errors on unselected rows. Implementations may specialize this only
    /// when skipping rows does not bypass validation.
    virtual Result<std::vector<char>> TestSelected(const arrow::Array& array,
                                                   const std::vector<int64_t>& selection,
                                                   arrow::MemoryPool* pool) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<char> all_results, Test(array, pool));
        if (all_results.size() != static_cast<size_t>(array.length())) {
            return Status::Invalid("predicate result size does not match array length");
        }
        std::vector<char> results;
        results.reserve(selection.size());
        for (int64_t row : selection) {
            results.push_back(all_results[row]);
        }
        return results;
    }
    virtual Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema,
                              const InternalRow& row) const = 0;
    virtual Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                              const InternalRow& min_values, const InternalRow& max_values,
                              const InternalArray& null_counts) const = 0;

 protected:
    static Result<std::shared_ptr<arrow::Array>> SelectRows(const arrow::Array& array,
                                                            const std::vector<int64_t>& selection,
                                                            arrow::MemoryPool* pool) {
        if (selection.empty()) {
            return array.Slice(0, 0);
        }
        if (selection.size() == static_cast<size_t>(array.length())) {
            return arrow::MakeArray(array.data());
        }
        arrow::Int64Builder builder(pool);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendValues(selection));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> indices, builder.Finish());
        arrow::compute::ExecContext context(pool);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            arrow::Datum selected,
            arrow::compute::Take(arrow::Datum(array), arrow::Datum(indices),
                                 arrow::compute::TakeOptions::Defaults(), &context));
        return selected.make_array();
    }
};
}  // namespace paimon
