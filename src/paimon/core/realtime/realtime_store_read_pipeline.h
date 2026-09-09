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

#include <map>
#include <memory>
#include <string>

#include "paimon/common/data/shredding/shredding_read_plan.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/result.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {

/// Builds the schema requested from a `RealtimeStore` and converts a store reader into the
/// logical representation expected by table read.
class RealtimeStoreReadPipeline {
 public:
    /// `realtime_write_schema` is the complete schema written to `RealtimeStore`, including its
    /// system fields.
    static Result<std::unique_ptr<RealtimeStoreReadPipeline>> Create(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::shared_ptr<arrow::Schema>& realtime_write_schema,
        const std::shared_ptr<MemoryPool>& memory_pool,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    const std::shared_ptr<arrow::Schema>& StoreReadSchema() const {
        return store_read_schema_;
    }

    /// Wraps a store reader with offset filtering followed by physical-to-logical conversion.
    Result<std::unique_ptr<BatchReader>> Wrap(std::unique_ptr<BatchReader>&& store_reader,
                                              const OffsetRange& visible_offsets) const;

 private:
    RealtimeStoreReadPipeline(std::shared_ptr<arrow::Schema> logical_schema,
                              std::shared_ptr<arrow::Schema> store_read_schema,
                              std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans,
                              bool needs_conversion, std::shared_ptr<arrow::MemoryPool> arrow_pool);

    std::shared_ptr<arrow::Schema> logical_schema_;
    std::shared_ptr<arrow::Schema> store_read_schema_;
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans_;
    bool needs_conversion_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

}  // namespace paimon
