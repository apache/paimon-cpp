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

#include "arrow/array/array_base.h"
#include "arrow/type_fwd.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"
namespace paimon {
class LeafFunction : public Function {
 public:
    // input array is the exact single field array
    // `pool` is where any arrow buffer the evaluation allocates comes from, it must not be null.
    virtual Result<std::vector<char>> Test(const arrow::Array& array,
                                           const std::vector<Literal>& literals,
                                           arrow::MemoryPool* pool) const = 0;

    virtual Result<bool> Test(const Literal& value, const std::vector<Literal>& literals) const = 0;

    virtual Result<bool> Test(int64_t row_count, const Literal& min_value, const Literal& max_value,
                              const std::optional<int64_t>& null_count,
                              const std::vector<Literal>& literals) const = 0;

    virtual const LeafFunction* Negate() const = 0;
};
}  // namespace paimon
