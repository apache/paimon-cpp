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

class MemoryPool;
class MetricsImpl;

/// Reads a sequence of readers one after another, opening each only when it is reached.
///
/// A split holds a whole partition's files and a reader holds its file open, so building them all
/// up front could exhaust the file descriptors before returning a row. Here one file is open at a
/// time, closed as soon as it runs out. A closed reader is kept, and its metrics with it: a batch
/// it handed out is allocated from a pool that reader owns.
///
/// Every failure names the file it came from, which is the only thing telling one file of a
/// partition from another, and is terminal as `BatchReader` requires.
class LazyConcatBatchReader : public BatchReader {
 public:
    /// Opens one reader, at most once, when the previous reader runs out.
    using ReaderFactory = std::function<Result<std::unique_ptr<BatchReader>>()>;

    /// One reader and the file it reads, named in every error that reader produces.
    struct Source {
        std::string name;
        ReaderFactory open;
    };

    LazyConcatBatchReader(std::vector<Source>&& sources, const std::shared_ptr<MemoryPool>& pool);

    ~LazyConcatBatchReader() override;

    Result<ReadBatch> NextBatch() override;
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override;
    void Close() override;
    std::shared_ptr<Metrics> GetReaderMetrics() const override;

 private:
    /// Closes every reader; called by both `Close` and the destructor.
    void DoClose();

    /// Closes the reader in hand, keeping its metrics.
    void CloseCurrent();

    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::vector<Source> sources_;
    size_t next_source_ = 0;
    std::unique_ptr<BatchReader> current_reader_;
    /// The readers already closed, held so the batches they handed out stay valid.
    std::vector<std::unique_ptr<BatchReader>> closed_readers_;
    /// The file `current_reader_` reads, kept so a failure part way through can name it.
    std::string current_name_;
    /// The failure this reader stopped at, or OK while it has not failed.
    Status failure_;
    std::shared_ptr<MetricsImpl> closed_metrics_;
};

}  // namespace paimon
