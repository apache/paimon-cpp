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

#include "paimon/common/predicate/leaf_function.h"

namespace paimon {

/// Identifies an index-dependent predicate without implementing scalar evaluation.
class FullTextMatchFunction : public LeafFunction {
 public:
    static const FullTextMatchFunction& Instance() {
        static const FullTextMatchFunction instance;
        return instance;
    }

    Result<std::vector<char>> Test(const arrow::Array& array, const std::vector<Literal>& literals,
                                   arrow::MemoryPool* pool) const override {
        return Status::NotImplemented(
            "FullTextMatch does not support scalar Test; a full-text index is required");
    }

    Result<bool> Test(const Literal& value, const std::vector<Literal>& literals) const override {
        return Status::NotImplemented(
            "FullTextMatch does not support scalar Test; a full-text index is required");
    }

    Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                      const std::optional<int64_t>& null_count,
                      const std::vector<Literal>& literals) const override {
        return Status::NotImplemented(
            "FullTextMatch does not support statistics Test; a full-text index is required");
    }

    const LeafFunction* Negate() const override {
        return nullptr;
    }

    Type GetType() const override {
        return Type::FULL_TEXT_MATCH;
    }

    std::string ToString() const override {
        return "FullTextMatch";
    }

 private:
    FullTextMatchFunction() = default;
};

}  // namespace paimon
