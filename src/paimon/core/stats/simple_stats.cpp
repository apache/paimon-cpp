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

#include "paimon/core/stats/simple_stats.h"

#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_section.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/murmurhash_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/macros.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
class Bytes;

const SimpleStats& SimpleStats::EmptyStats() {
    static const SimpleStats kEmptyStats(
        BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
        BinaryArray::FromLongArray(std::vector<int64_t>(), GetDefaultPool().get()));
    return kEmptyStats;
}

BinaryRow SimpleStats::ToRow() const {
    BinaryRow row(3);
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    BinaryRowWriter writer(&row, 32 * 1024, pool.get());
    auto min_value_bytes = SerializationUtils::SerializeBinaryRow(min_values_, pool.get());
    writer.WriteBinary(0, *min_value_bytes);
    auto max_value_bytes = SerializationUtils::SerializeBinaryRow(max_values_, pool.get());
    writer.WriteBinary(1, *max_value_bytes);
    writer.WriteArray(2, null_counts_);
    writer.Complete();
    return row;
}

Result<SimpleStats> SimpleStats::FromRow(const InternalRow* row, MemoryPool* pool) {
    if (PAIMON_UNLIKELY(row == nullptr)) {
        return Status::Invalid("internal row is null pointer");
    }
    if (PAIMON_UNLIKELY(pool == nullptr)) {
        return Status::Invalid("memory pool is null pointer");
    }
    std::shared_ptr<Bytes> min_value = row->GetBinary(0);
    if (PAIMON_UNLIKELY(min_value == nullptr)) {
        return Status::Invalid("get min value from internal row failed.");
    }
    std::shared_ptr<Bytes> max_value = row->GetBinary(1);
    if (PAIMON_UNLIKELY(max_value == nullptr)) {
        return Status::Invalid("get max value from internal row failed.");
    }
    std::shared_ptr<InternalArray> null_counts = row->GetArray(2);
    if (PAIMON_UNLIKELY(null_counts == nullptr)) {
        return Status::Invalid("get null counts from internal row failed.");
    }
    PAIMON_ASSIGN_OR_RAISE(BinaryRow min_values,
                           SerializationUtils::DeserializeBinaryRow(min_value));
    PAIMON_ASSIGN_OR_RAISE(BinaryRow max_values,
                           SerializationUtils::DeserializeBinaryRow(max_value));
    if (min_values.GetFieldCount() == 0 && max_values.GetFieldCount() == 0 &&
        null_counts->Size() == 0) {
        return SimpleStats::EmptyStats();
    }
    return SimpleStats(min_values, max_values, BinaryArray::FromLongArray(null_counts.get(), pool));
}

int32_t SimpleStats::HashCode() const {
    int32_t min_hash = min_values_.HashCode();
    int32_t max_hash = max_values_.HashCode();
    int32_t null_hash = null_counts_.HashCode();
    int32_t hash = MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<void*>(&max_hash), 0,
                                                    sizeof(max_hash), min_hash);
    return MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<void*>(&null_hash), 0,
                                            sizeof(null_hash), hash);
}

bool SimpleStats::operator==(const SimpleStats& other) const {
    if (this == &other) {
        return true;
    }
    return min_values_ == other.min_values_ && max_values_ == other.max_values_ &&
           null_counts_ == other.null_counts_;
}

const std::shared_ptr<arrow::DataType>& SimpleStats::DataType() {
    static std::shared_ptr<arrow::DataType> data_type = arrow::struct_(
        {arrow::field("_MIN_VALUES", arrow::binary(), /*nullable=*/false),
         arrow::field("_MAX_VALUES", arrow::binary(), /*nullable=*/false),
         arrow::field("_NULL_COUNTS", arrow::list(arrow::field("item", arrow::int64(), true)),
                      /*nullable=*/true)});
    return data_type;
}

}  // namespace paimon
