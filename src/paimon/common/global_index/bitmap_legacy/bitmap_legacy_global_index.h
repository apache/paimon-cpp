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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/global_index/wrap/file_index_reader_wrapper.h"
#include "paimon/global_index/global_indexer.h"

namespace paimon {
/// Exposes the bitmap file index as a global index with one file per shard.
// TODO(xinyu.lxy): Address the following limitations:
// 1. Define dedicated option forwarding for bitmap-legacy. The factory currently passes the full
//    global index options map unchanged to BitmapFileIndex, which reads unprefixed "version" and
//    "index-block-size" keys.
// 2. Add cache limits or eviction. Queried bitmaps, including empty results, are retained for the
//    reader's lifetime.
// 3. Support concurrent reads. Lazy metadata initialization, bitmap caches and shared seek/read
//    state are currently unsynchronized.
// 4. Support spilling or streaming writes. All keys and bitmaps for a shard are accumulated in
//    memory, and the complete serialized index is also buffered in memory before being written.
class BitmapLegacyGlobalIndex : public GlobalIndexer {
 public:
    explicit BitmapLegacyGlobalIndex(const std::shared_ptr<BitmapFileIndex>& index)
        : index_(index) {}

    Result<std::optional<std::vector<std::string>>> GetExtraFieldNames() const override;

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files, int64_t total_row_count,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    std::shared_ptr<BitmapFileIndex> index_;
};

class BitmapLegacyGlobalIndexReader : public FileIndexReaderWrapper {
 public:
    BitmapLegacyGlobalIndexReader(const std::shared_ptr<FileIndexReader>& reader,
                                  const std::function<Result<std::shared_ptr<GlobalIndexResult>>(
                                      const std::shared_ptr<FileIndexResult>&)>& transform)
        : FileIndexReaderWrapper(reader, transform) {}

    static inline const char kIdentifier[] = "bitmap-legacy";

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return kIdentifier;
    }
};

}  // namespace paimon
