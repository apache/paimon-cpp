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
#include <string>
#include <vector>

#include "paimon/reader/batch_reader.h"
#include "paimon/status.h"

namespace arrow {
class Array;
class DataType;
class MemoryPool;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {
class InternalReadContext;

/// Selects and sorts the global TopK from all rows produced by the input reader.
class TopKBatchReader : public BatchReader {
 public:
    static std::unique_ptr<BatchReader> ApplyTopkReaderIfNeeded(std::unique_ptr<BatchReader> reader,
                                                                const InternalReadContext& context);

    enum class ScoreOrder { ASCENDING, DESCENDING };

    TopKBatchReader(std::unique_ptr<BatchReader> reader, const std::string& score_field_name,
                    ScoreOrder score_order, int32_t limit, int32_t batch_size,
                    const std::shared_ptr<arrow::Schema>& read_schema,
                    const std::shared_ptr<arrow::MemoryPool>& pool);
    ~TopKBatchReader() override = default;

    Result<ReadBatch> NextBatch() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;
    void Close() override;

 private:
    struct Candidate {
        float score;
        uint64_t ordinal;
        std::shared_ptr<arrow::Array> values;
        int64_t row;
    };

    Status CompactCandidates(std::vector<Candidate>* candidates) const;
    Status ReadTopK();
    Result<ReadBatch> ReadOutput();

 private:
    std::shared_ptr<arrow::MemoryPool> pool_;
    std::unique_ptr<BatchReader> reader_;
    std::string score_name_;
    ScoreOrder score_order_;
    int32_t limit_;
    int32_t batch_size_;
    std::shared_ptr<arrow::DataType> read_type_;
    bool remove_score_;

    bool initialized_ = false;
    std::shared_ptr<arrow::StructArray> output_;
    int64_t position_ = 0;
};
}  // namespace paimon
