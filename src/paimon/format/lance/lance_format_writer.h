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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "arrow/memory_pool.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/lance/lance_ffi.h"
#include "paimon/format/lance/lance_utils.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::lance {

class LanceFormatWriter : public FormatWriter {
 public:
    static Result<std::unique_ptr<LanceFormatWriter>> Create(
        const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
        const LanceStorageOptions& storage_options,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~LanceFormatWriter() override;

    Status AddBatch(::ArrowArray* batch) override;
    Status Flush() override;
    Status Finish() override;
    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;
    std::shared_ptr<Metrics> GetWriterMetrics() const override;
    Status AddMetadata(const std::map<std::string, std::string>& metadata) override;

 private:
    LanceFormatWriter(const std::shared_ptr<arrow::Schema>& schema,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                      PaimonLanceWriter* writer);

    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    PaimonLanceWriter* writer_;
    std::shared_ptr<Metrics> metrics_;
    uint64_t rows_written_ = 0;
    bool finished_ = false;
};

}  // namespace paimon::lance
