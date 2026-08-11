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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/type_fwd.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;

/// Combines predicate (probe) columns with selectively read payload columns.
///
/// Probe rows and payload rows must have a one-to-one positional correspondence. The reader
/// validates this invariant while consuming the payload reader.
class LateMaterializationBatchReader : public BatchReader {
 public:
    static Result<std::unique_ptr<LateMaterializationBatchReader>> Create(
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<arrow::Schema>& probe_schema,
        std::shared_ptr<arrow::StructArray> probe_data,
        const std::shared_ptr<arrow::Schema>& payload_schema,
        std::unique_ptr<BatchReader>&& payload_reader, int32_t read_batch_size,
        const std::shared_ptr<MemoryPool>& pool,
        std::unique_ptr<arrow::MemoryPool> arrow_pool = nullptr);

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;

 private:
    enum class Source { PROBE, PAYLOAD };

    struct FieldSource {
        Source source;
        int32_t index;
    };

    LateMaterializationBatchReader(const std::shared_ptr<arrow::Schema>& read_schema,
                                   std::shared_ptr<arrow::StructArray> probe_data,
                                   std::unique_ptr<BatchReader>&& payload_reader,
                                   std::vector<FieldSource>&& field_sources,
                                   int32_t read_batch_size, const std::shared_ptr<MemoryPool>& pool,
                                   std::unique_ptr<arrow::MemoryPool> arrow_pool);

    Result<ReadBatchWithBitmap> MakeBatch(const std::shared_ptr<arrow::StructArray>& payload_data);

    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::StructArray> probe_data_;
    std::unique_ptr<BatchReader> payload_reader_;
    std::vector<FieldSource> field_sources_;
    int32_t read_batch_size_;
    int64_t probe_offset_ = 0;
};

}  // namespace paimon
