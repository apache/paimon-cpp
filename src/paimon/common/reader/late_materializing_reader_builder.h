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
#include <utility>

#include "paimon/common/reader/late_materializing_file_batch_reader.h"
#include "paimon/format/reader_builder.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/prefetch_file_batch_reader.h"
#include "paimon/result.h"

namespace paimon {

class LateMaterializingReaderBuilder : public ReaderBuilder {
 public:
    LateMaterializingReaderBuilder(std::unique_ptr<ReaderBuilder> inner,
                                   std::shared_ptr<MemoryPool> pool)
        : inner_(std::move(inner)), pool_(std::move(pool)) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        inner_->WithMemoryPool(pool);
        return this;
    }

    ReaderBuilder* WithCache(const std::shared_ptr<Cache>& cache) override {
        inner_->WithCache(cache);
        return this;
    }

    ReaderBuilder* WithReadHints(const std::optional<ReadHints>& hints) override {
        inner_->WithReadHints(hints);
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& stream) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> base, inner_->Build(stream));
        auto* prefetch = dynamic_cast<PrefetchFileBatchReader*>(base.get());
        if (prefetch == nullptr) {
            return Status::Invalid("Late materialization requires prefetch interface");
        }
        base.release();
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LateMaterializingFileBatchReader> reader,
            LateMaterializingFileBatchReader::Create(
                std::unique_ptr<PrefetchFileBatchReader>(prefetch), pool_));
        return std::unique_ptr<FileBatchReader>(std::move(reader));
    }

 private:
    std::unique_ptr<ReaderBuilder> inner_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
