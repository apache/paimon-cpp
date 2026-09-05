/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <string>

#include "paimon/cache/cache.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/memory/memory_pool.h"
#include "parquet/page_index.h"

namespace paimon::parquet {

inline MemorySegment AllocateParquetCacheSegment(int32_t size,
                                                 const std::shared_ptr<MemoryPool>& pool) {
    struct PooledBytes {
        PooledBytes(int32_t size, const std::shared_ptr<MemoryPool>& memory_pool)
            : pool(memory_pool), bytes(size, memory_pool.get()) {}
        // Destroy bytes before releasing their allocator.
        std::shared_ptr<MemoryPool> pool;
        Bytes bytes;
    };
    auto owner = std::make_shared<PooledBytes>(size, pool);
    auto* bytes = &owner->bytes;
    return MemorySegment::Wrap(std::shared_ptr<Bytes>(std::move(owner), bytes));
}

// Cache immutable page-index bytes, not Arrow readers: the latter borrow their
// input stream, reader properties and decryptor from the current file reader.
class ParquetInputStream : public ArrowInputStreamAdapter {
 public:
    ParquetInputStream(const std::shared_ptr<paimon::InputStream>& input, int64_t file_size,
                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                       const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Cache>& cache,
                       const std::string& file_uri)
        : ArrowInputStreamAdapter(input, file_size, arrow_pool),
          pool_(pool),
          cache_(cache),
          file_uri_(file_uri) {}

    // Called before the stream is published to the file reader. Only index
    // ranges described by its footer are eligible; data pages remain uncached.
    void SetPageIndexRanges(const ::parquet::FileMetaData& metadata) {
        for (int32_t rg = 0; rg < metadata.num_row_groups(); ++rg) {
            auto ranges = ::parquet::PageIndexReader::DeterminePageIndexRangesInRowGroup(
                *metadata.RowGroup(rg), {});
            for (const auto& range : {ranges.column_index, ranges.offset_index}) {
                if (range.has_value()) {
                    index_ranges_.emplace(range->offset, range->length);
                }
            }
        }
    }

    using ArrowInputStreamAdapter::ReadAt;

    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (!cache_ || file_uri_.empty() || nbytes <= 0 ||
            nbytes > std::numeric_limits<int32_t>::max()) {
            return ArrowInputStreamAdapter::ReadAt(position, nbytes, out);
        }
        auto range = index_ranges_.upper_bound(position);
        if (range == index_ranges_.begin()) {
            return ArrowInputStreamAdapter::ReadAt(position, nbytes, out);
        }
        --range;
        if (position - range->first > range->second ||
            nbytes > range->second - (position - range->first)) {
            return ArrowInputStreamAdapter::ReadAt(position, nbytes, out);
        }
        auto key = CacheKey::ForKind(file_uri_, position, static_cast<int32_t>(nbytes),
                                     CacheKind::DATA_FILE_FOOTER);
        auto value = cache_->Get(
            key,
            [this, position,
             nbytes](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
                MemorySegment segment =
                    AllocateParquetCacheSegment(static_cast<int32_t>(nbytes), pool_);
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    int64_t size,
                    ArrowInputStreamAdapter::ReadAt(position, nbytes, segment.MutableData()));
                if (size != nbytes) {
                    return Status::IOError("Short read of Parquet page index");
                }
                return std::make_shared<CacheValue>(segment, CacheCallback());
            });
        if (!value.ok()) {
            return ToArrowStatus(value.status());
        }
        if (!value.value() || value.value()->GetSegment().Size() != nbytes) {
            return arrow::Status::IOError("Invalid Parquet page-index cache value");
        }
        std::memcpy(out, value.value()->GetSegment().Data(), nbytes);
        return nbytes;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Cache> cache_;
    std::string file_uri_;
    std::map<int64_t, int64_t> index_ranges_;
};

}  // namespace paimon::parquet
