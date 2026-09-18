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

#pragma once

#include <memory>

#include "arrow/api.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
namespace paimon {

/// Provides core methods to serialize, deserialize, and compare global index keys.
class KeySerializer {
 public:
    ~KeySerializer() = default;

    static Result<std::shared_ptr<KeySerializer>> Create(
        const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<MemoryPool>& pool);

    Result<std::shared_ptr<Bytes>> Serialize(const Literal& literal) const;

    Result<Literal> Deserialize(const MemorySlice& slice) const;

    Status ValidateSerializedKey(const MemorySlice& slice) const;

    MemorySlice::SliceComparator CreateComparator() const;

    MemoryPool* GetMemoryPool() const {
        return pool_.get();
    }

 private:
    KeySerializer(const std::shared_ptr<arrow::DataType>& type,
                  const std::shared_ptr<MemoryPool>& pool)
        : type_(type), pool_(pool) {}

    std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon
