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
#include <string>
#include <utility>
#include <vector>

#include "paimon/reader/batch_reader.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace paimon {

/// Drops dependency columns after filtering/merging, preserving the actual batch fields and
/// the leading _VALUE_KIND. Retains implicit index scores until the outer TopK reader consumes
/// them and removes scores absent from the output schema.
class ProjectionBatchReader : public BatchReader {
 public:
    ProjectionBatchReader(std::unique_ptr<BatchReader> reader,
                          std::vector<std::string> output_fields,
                          const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : reader_(std::move(reader)),
          output_fields_(std::move(output_fields)),
          arrow_pool_(arrow_pool) {}

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }
    void Close() override {
        reader_->Close();
    }

 private:
    Status Project(ReadBatch* batch) const;

    std::unique_ptr<BatchReader> reader_;
    std::vector<std::string> output_fields_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};
}  // namespace paimon
