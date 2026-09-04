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

#include <cstdint>
#include <optional>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/type_fwd.h"
#include "paimon/common/predicate/leaf_function.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class NullFalseLeafBinaryFunction : public LeafFunction {
 public:
    /// Compares the whole batch against the literal with one `arrow::compute` comparison kernel
    /// when the literal and the column allow it, and falls back to materializing every row into a
    /// `Literal` and comparing one at a time otherwise. Every `LeafFunction` is a shared stateless
    /// singleton, so the scalar the kernel compares against is built per batch; that costs `O(1)`
    /// and buys an `O(rows)` compare with no `Literal` per row in between.
    Result<std::vector<char>> Test(const arrow::Array& array, const std::vector<Literal>& literals,
                                   arrow::MemoryPool* pool) const override;

    Result<bool> Test(const Literal& value, const std::vector<Literal>& literals) const override {
        if (literals.size() < LITERAL_LIMIT) {
            return Status::Invalid("NullFalseLeafBinaryFunction needs single literal for field");
        }
        if (literals[0].IsNull() || value.IsNull()) {
            return false;
        }
        return Test(value, literals[0]);
    }

    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count,
                      const std::vector<Literal>& literals) const override {
        if (literals.size() < LITERAL_LIMIT) {
            return Status::Invalid("NullFalseLeafBinaryFunction needs single literal for field");
        }
        if (null_count != std::nullopt) {
            if (row_count == null_count.value() || literals[0].IsNull()) {
                return false;
            }
        }
        return Test(row_count, min_value, max_value, null_count, literals[0]);
    }

    // Precondition: field and literals are not empty
    virtual Result<bool> Test(const Literal& field, const Literal& literal) const = 0;
    virtual Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                              const std::optional<int64_t>& null_count,
                              const Literal& literal) const = 0;

 private:
    static constexpr size_t LITERAL_LIMIT = 1;
};
}  // namespace paimon
