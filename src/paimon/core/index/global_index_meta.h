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
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/memory/bytes.h"
namespace paimon {
/// Schema for global index.
struct GlobalIndexMeta {
    static constexpr int32_t NUM_FIELDS = 6;

    GlobalIndexMeta(int64_t _row_range_start, int64_t _row_range_end, int32_t _index_field_id,
                    const std::optional<std::vector<int32_t>>& _extra_field_ids,
                    const std::shared_ptr<Bytes>& _index_meta);

    GlobalIndexMeta(int64_t _row_range_start, int64_t _row_range_end, int32_t _index_field_id,
                    const std::optional<std::vector<int32_t>>& _extra_field_ids,
                    const std::shared_ptr<Bytes>& _index_meta,
                    const std::shared_ptr<Bytes>& _source_meta);

    bool operator==(const GlobalIndexMeta& other) const;

    std::string ToString() const;

    BinaryRow ToRow(MemoryPool* pool) const;

    static Result<GlobalIndexMeta> FromRow(const InternalRow& row);

    static const std::shared_ptr<arrow::DataType>& DataType();

    int64_t row_range_start;
    int64_t row_range_end;
    int32_t index_field_id;
    std::optional<std::vector<int32_t>> extra_field_ids;
    std::shared_ptr<Bytes> index_meta;
    std::shared_ptr<Bytes> source_meta;
};

}  // namespace paimon
