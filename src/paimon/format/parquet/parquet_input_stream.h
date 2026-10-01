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

#include <atomic>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <string>

#include "paimon/cache/cache.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/memory/memory_pool.h"
#include "parquet/metadata.h"
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

// Shared independently of the stream so in-flight reads can finish after the reader closes.
// Counters describe requests, not cache residency; bytes retained by consumers may outlive
// eviction.
struct ParquetDataCacheMetrics {
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> bypasses{0};
    std::atomic<uint64_t> hit_bytes{0};
    std::atomic<uint64_t> admission_bytes{0};
    std::atomic<uint64_t> admission_failures{0};

    void Collect(Metrics* metrics) const {
        metrics->SetCounter(ParquetMetrics::DATA_CACHE_HITS, hits.load(std::memory_order_relaxed));
        metrics->SetCounter(ParquetMetrics::DATA_CACHE_MISSES,
                            misses.load(std::memory_order_relaxed));
        metrics->SetCounter(ParquetMetrics::DATA_CACHE_BYPASSES,
                            bypasses.load(std::memory_order_relaxed));
        metrics->SetCounter(ParquetMetrics::DATA_CACHE_HIT_BYTES,
                            hit_bytes.load(std::memory_order_relaxed));
        metrics->SetCounter(ParquetMetrics::DATA_CACHE_ADMISSION_BYTES,
                            admission_bytes.load(std::memory_order_relaxed));
        metrics->SetCounter(ParquetMetrics::DATA_CACHE_ADMISSION_FAILURES,
                            admission_failures.load(std::memory_order_relaxed));
    }
};

// Cache immutable page-index bytes, not Arrow readers: the latter borrow their
// input stream, reader properties and decryptor from the current file reader.
class ParquetInputStream : public ArrowInputStreamAdapter {
 public:
    ParquetInputStream(const std::shared_ptr<paimon::InputStream>& input, int64_t file_size,
                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                       const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Cache>& cache,
                       const std::string& file_uri, bool enable_data_cache = false,
                       int64_t max_cache_range_bytes = 4 * 1024 * 1024)
        : ArrowInputStreamAdapter(input, file_size, arrow_pool),
          pool_(pool),
          cache_(cache),
          file_uri_(file_uri),
          enable_data_cache_(enable_data_cache),
          max_cache_range_bytes_(max_cache_range_bytes) {}

    const std::shared_ptr<ParquetDataCacheMetrics>& DataCacheMetrics() const {
        return data_cache_metrics_;
    }

    // Called before the stream is published to the file reader. Only index
    // ranges described by its footer are eligible for the metadata cache.
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

    // Data-file paths must identify immutable bytes. Only asynchronous pre-buffer ranges
    // are cached; cold reads retain the underlying filesystem's asynchronous behavior.
    arrow::Future<std::shared_ptr<arrow::Buffer>> ReadAsync(const arrow::io::IOContext& io_context,
                                                            int64_t position,
                                                            int64_t nbytes) override {
        if (!enable_data_cache_ || !cache_ || file_uri_.empty() || position < 0 || nbytes <= 0 ||
            nbytes > max_cache_range_bytes_ || nbytes > std::numeric_limits<int32_t>::max()) {
            data_cache_metrics_->bypasses.fetch_add(1, std::memory_order_relaxed);
            return ArrowInputStreamAdapter::ReadAsync(io_context, position, nbytes);
        }
        auto key = CacheKey::ForPosition(file_uri_, position, static_cast<int32_t>(nbytes),
                                         /*is_index=*/false);
        auto lookup = cache_->GetIfPresent(key);
        if (!lookup.ok()) {
            data_cache_metrics_->bypasses.fetch_add(1, std::memory_order_relaxed);
            return ArrowInputStreamAdapter::ReadAsync(io_context, position, nbytes);
        }
        std::shared_ptr<CacheValue> cached = std::move(lookup).value();
        if (cached && cached->GetSegment().Data() && cached->GetSegment().Size() == nbytes) {
            data_cache_metrics_->hits.fetch_add(1, std::memory_order_relaxed);
            data_cache_metrics_->hit_bytes.fetch_add(nbytes, std::memory_order_relaxed);
            struct CachedBuffer {
                explicit CachedBuffer(std::shared_ptr<CacheValue> value)
                    : owner(std::move(value)),
                      buffer(reinterpret_cast<const uint8_t*>(owner->GetSegment().Data()),
                             owner->GetSegment().Size()) {}
                std::shared_ptr<CacheValue> owner;
                arrow::Buffer buffer;
            };
            auto owner = std::make_shared<CachedBuffer>(std::move(cached));
            auto* buffer = &owner->buffer;
            return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(
                std::shared_ptr<arrow::Buffer>(std::move(owner), buffer));
        }
        data_cache_metrics_->misses.fetch_add(1, std::memory_order_relaxed);
        // Capture owners rather than this: completion may run after this adapter is released.
        return ArrowInputStreamAdapter::ReadAsync(io_context, position, nbytes)
            .Then([cache = cache_, pool = pool_, metrics = data_cache_metrics_, key,
                   nbytes](const std::shared_ptr<arrow::Buffer>& buffer)
                      -> std::shared_ptr<arrow::Buffer> {
                if (buffer->size() == nbytes) {
                    MemorySegment segment =
                        AllocateParquetCacheSegment(static_cast<int32_t>(nbytes), pool);
                    std::memcpy(segment.MutableData(), buffer->data(), nbytes);
                    // Admission failure (including an undersized cache) does not fail a read.
                    metrics->admission_bytes.fetch_add(nbytes, std::memory_order_relaxed);
                    Status status =
                        cache->Put(key, std::make_shared<CacheValue>(segment, CacheCallback()));
                    if (!status.ok()) {
                        metrics->admission_failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                return buffer;
            });
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
    std::shared_ptr<ParquetDataCacheMetrics> data_cache_metrics_ =
        std::make_shared<ParquetDataCacheMetrics>();
    bool enable_data_cache_;
    int64_t max_cache_range_bytes_;
};

}  // namespace paimon::parquet
