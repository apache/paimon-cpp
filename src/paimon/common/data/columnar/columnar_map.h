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
#include <memory>

#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon {
class MemoryPool;

/// Columnar map to support access to vector column data.
class ColumnarMap : public InternalMap {
 public:
    ColumnarMap(const std::shared_ptr<arrow::Array>& key_array,
                const std::shared_ptr<arrow::Array>& value_array,
                const std::shared_ptr<MemoryPool>& pool, int32_t offset, int32_t length);

    int32_t Size() const override {
        return length_;
    }
    std::shared_ptr<InternalArray> KeyArray() const override;
    std::shared_ptr<InternalArray> ValueArray() const override;

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::Array> key_array_;
    std::shared_ptr<arrow::Array> value_array_;
    int32_t offset_;
    int32_t length_;
};
}  // namespace paimon
