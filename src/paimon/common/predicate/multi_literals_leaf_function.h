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

#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "paimon/common/predicate/leaf_function.h"
#include "paimon/status.h"

namespace paimon {

class MultiLiteralsLeafFunction : public LeafFunction {
 public:
    /// Probes the whole batch with `arrow::compute::is_in` when the literals allow it, and falls
    /// back to comparing every row against every literal otherwise. Every `LeafFunction` is a
    /// shared stateless singleton, so the value set is built per batch; that costs `O(literals)`
    /// and buys an `O(rows)` probe.
    Result<std::vector<char>> Test(const arrow::Array& array,
                                   const std::vector<Literal>& literals) const override;

    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count,
                      const std::vector<Literal>& literals) const override {
        if (null_count != std::nullopt && row_count == null_count.value()) {
            return false;
        }
        return InnerTest(row_count, min_value, max_value, null_count, literals);
    }

    Result<bool> Test(const Literal& field, const std::vector<Literal>& literals) const override {
        if (field.IsNull()) {
            return false;
        }
        return InnerTest(field, literals);
    }

    // Precondition: field is not empty
    virtual Result<bool> InnerTest(const Literal& field,
                                   const std::vector<Literal>& literals) const = 0;

    virtual Result<bool> InnerTest(int64_t row_count, const Literal& min_value,
                                   const Literal& max_value,
                                   const std::optional<int64_t>& null_count,
                                   const std::vector<Literal>& literals) const = 0;
};
}  // namespace paimon
