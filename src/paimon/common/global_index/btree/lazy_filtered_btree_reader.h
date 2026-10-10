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
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/common/global_index/sorted_file_global_index_reader.h"
#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/sst/block_cache.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
class LazyFilteredBTreeReader : public SortedFileGlobalIndexReader {
 public:
    static Result<std::shared_ptr<LazyFilteredBTreeReader>> Create(
        std::optional<int32_t> read_buffer_size, const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<arrow::DataType>& key_type,
        const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::shared_ptr<CacheManager>& cache_manager, int64_t fallback_scan_max_size,
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor);

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    std::string GetIndexType() const override {
        return BtreeDefs::kIdentifier;
    }

 private:
    LazyFilteredBTreeReader(std::optional<int32_t> read_buffer_size,
                            std::unique_ptr<SortedFileMetaSelector> file_selector,
                            const std::shared_ptr<KeySerializer>& key_serializer,
                            const std::shared_ptr<GlobalIndexFileReader>& file_reader,
                            const std::shared_ptr<CacheManager>& cache_manager,
                            int64_t fallback_scan_max_size, const std::shared_ptr<MemoryPool>& pool,
                            const std::shared_ptr<Executor>& executor);

    Result<std::shared_ptr<GlobalIndexReader>> OpenReader(const GlobalIndexIOMeta& meta) override;
    Result<RoaringBitmap64> ReadNullBitmap(const std::shared_ptr<BlockCache>& cache,
                                           const std::optional<BlockHandle>& block_handle);

 private:
    std::optional<int32_t> read_buffer_size_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<KeySerializer> key_serializer_;
    std::shared_ptr<GlobalIndexFileReader> file_reader_;
    std::shared_ptr<CacheManager> cache_manager_;
};

}  // namespace paimon
