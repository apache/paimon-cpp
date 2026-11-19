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

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/metrics.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class MetricsImpl;

/// Opens one source at a time in input order. Returned batches own their resources, so each
/// reader can close at EOF. Failures identify the source and terminate the read.
class LazyConcatBatchReader : public BatchReader {
 public:
    /// Called once when its source is reached. A null reader skips the source. Each returned
    /// reader owns the dependencies it needs after the factory is released.
    using ReaderFactory = std::function<Result<std::unique_ptr<BatchReader>>()>;

    struct Source {
        std::string name;
        ReaderFactory open;
    };

    LazyConcatBatchReader(std::vector<Source>&& sources,
                          const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~LazyConcatBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    void Close() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    void DoClose();

    void CloseCurrent();

    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::vector<Source> sources_;
    size_t next_source_ = 0;
    std::unique_ptr<BatchReader> current_reader_;
    std::string current_name_;
    Status failure_;
    std::shared_ptr<MetricsImpl> closed_metrics_;
};

}  // namespace paimon
