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

#include "paimon/format/parquet/parquet_input_stream.h"

#include <functional>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/fs/file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::parquet::test {
namespace {
class DeferredInputStream : public InputStream {
 public:
    Status Seek(int64_t, SeekOrigin) override {
        return Status::OK();
    }
    Result<int64_t> GetPos() const override {
        return 0;
    }
    Result<int64_t> Read(char* out, int64_t size) override {
        return Read(out, size, 0);
    }
    Result<int64_t> Read(char* out, int64_t size, int64_t offset) override {
        if (offset < 0 || size < 0 || offset > 16 || size > 16 - offset) {
            return Status::IOError("outside test file");
        }
        std::memcpy(out, "0123456789abcdef" + offset, size);
        return size;
    }
    void ReadAsync(char* out, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        ++reads;
        pending = [this, out, size, offset, cb = std::move(callback)](bool fail) mutable {
            if (fail) {
                cb(Status::IOError("injected failure"));
                return;
            }
            auto result = Read(out, size, offset);
            cb(result.ok() ? Status::OK() : result.status());
        };
    }
    void Complete(bool fail = false) {
        auto cb = std::move(pending);
        cb(fail);
    }
    Result<std::string> GetUri() const override {
        return std::string("immutable-file");
    }
    Result<int64_t> Length() const override {
        return 16;
    }
    Status Close() override {
        return Status::OK();
    }
    int32_t reads = 0;
    std::function<void(bool)> pending;
};

// Models a caller-owned implementation that has not opted into the new lookup operation.
class LegacyCache : public LruCache {
 public:
    LegacyCache() : LruCache(64) {}
    Result<std::shared_ptr<CacheValue>> GetIfPresent(
        const std::shared_ptr<CacheKey>& key) override {
        return Cache::GetIfPresent(key);
    }
};
}  // namespace

TEST(ParquetInputStreamTest, AsyncMissDoesNotBlockAndHitSurvivesEviction) {
    auto pool = GetDefaultPool();
    auto cache = std::make_shared<LruCache>(16);
    auto input = std::make_shared<DeferredInputStream>();
    auto stream = std::make_shared<ParquetInputStream>(input, 16, GetArrowPool(pool), pool, cache,
                                                       "immutable-file", true);
    auto cold = stream->ReadAsync(arrow::io::IOContext(), 2, 8);
    ASSERT_FALSE(cold.is_finished());
    ASSERT_EQ(cache->Size(), 0);
    input->Complete();
    ASSERT_TRUE(cold.status().ok());
    ASSERT_EQ(cold.result().ValueOrDie()->ToString(), "23456789");
    ASSERT_EQ(stream->StorageReadBytes()->load(), 8);
    auto warm = stream->ReadAsync(arrow::io::IOContext(), 2, 8);
    ASSERT_TRUE(warm.is_finished());
    ASSERT_TRUE(warm.status().ok());
    ASSERT_EQ(input->reads, 1);
    auto stats = stream->DataCacheMetrics();
    auto metrics = std::make_shared<MetricsImpl>();
    stats->Collect(metrics.get());
    ASSERT_OK_AND_ASSIGN(uint64_t hits, metrics->GetCounter(ParquetMetrics::DATA_CACHE_HITS));
    ASSERT_OK_AND_ASSIGN(uint64_t misses, metrics->GetCounter(ParquetMetrics::DATA_CACHE_MISSES));
    ASSERT_OK_AND_ASSIGN(uint64_t hit_bytes,
                         metrics->GetCounter(ParquetMetrics::DATA_CACHE_HIT_BYTES));
    ASSERT_EQ(1, hits);
    ASSERT_EQ(1, misses);
    ASSERT_EQ(8, hit_bytes);
    ASSERT_EQ(8, stats->admission_bytes.load());
    cache->InvalidateAll();
    stream.reset();
    cache.reset();
    ASSERT_EQ(warm.result().ValueOrDie()->ToString(), "23456789");
}

TEST(ParquetInputStreamTest, FailureIsNotCachedAndAdmissionFailureDoesNotFailRead) {
    auto pool = GetDefaultPool();
    auto cache = std::make_shared<LruCache>(4);
    auto input = std::make_shared<DeferredInputStream>();
    ParquetInputStream stream(input, 16, GetArrowPool(pool), pool, cache, "file", true);
    auto failed = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
    input->Complete(true);
    ASSERT_FALSE(failed.status().ok());
    ASSERT_EQ(cache->Size(), 0);
    for (int32_t i = 0; i < 2; ++i) {
        auto read = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
        ASSERT_FALSE(read.is_finished());
        input->Complete();
        ASSERT_TRUE(read.status().ok());
        ASSERT_EQ(read.result().ValueOrDie()->ToString(), "01234567");
        ASSERT_EQ(cache->GetCurrentWeight(), 0);
    }
    ASSERT_EQ(input->reads, 3);
}

TEST(ParquetInputStreamTest, AdmissionErrorsRemainOptionalAndAreCounted) {
    class RejectingCache : public LruCache {
     public:
        RejectingCache() : LruCache(64) {}
        Status Put(const std::shared_ptr<CacheKey>&, const std::shared_ptr<CacheValue>&) override {
            return Status::IOError("injected admission failure");
        }
    };
    auto pool = GetDefaultPool();
    auto cache = std::make_shared<RejectingCache>();
    auto input = std::make_shared<DeferredInputStream>();
    ParquetInputStream stream(input, 16, GetArrowPool(pool), pool, cache, "file", true);
    auto result = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
    input->Complete();
    ASSERT_TRUE(result.status().ok());
    ASSERT_EQ("01234567", result.result().ValueOrDie()->ToString());
    ASSERT_EQ(1, stream.DataCacheMetrics()->admission_failures.load());
    ASSERT_EQ(8, stream.DataCacheMetrics()->admission_bytes.load());
    ASSERT_EQ(0, cache->Size());
}

TEST(ParquetInputStreamTest, DisabledUnsupportedAndLargeRangesBypassCache) {
    auto pool = GetDefaultPool();
    for (int32_t scenario = 0; scenario < 4; ++scenario) {
        std::shared_ptr<LruCache> cache =
            scenario == 3 ? checked_pointer_cast<LruCache>(std::make_shared<LegacyCache>())
                          : std::make_shared<LruCache>(64);
        auto input = std::make_shared<DeferredInputStream>();
        ParquetInputStream stream(input, 16, GetArrowPool(pool), pool, cache,
                                  scenario == 1 ? "" : "file", scenario != 0,
                                  scenario == 2 ? 4 : 64);
        for (int32_t i = 0; i < 2; ++i) {
            auto read = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
            ASSERT_FALSE(read.is_finished());
            input->Complete();
            ASSERT_TRUE(read.status().ok());
            ASSERT_EQ(cache->Size(), 0);
        }
        ASSERT_EQ(input->reads, 2);
        ASSERT_EQ(2, stream.DataCacheMetrics()->bypasses.load());
        ASSERT_EQ(0, stream.DataCacheMetrics()->hits.load());
        ASSERT_EQ(0, stream.DataCacheMetrics()->misses.load());
    }
}

TEST(ParquetInputStreamTest, ReuseAcrossReadersKeepsAllocatorAndSeparatesFilesAndRanges) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    std::weak_ptr<MemoryPool> weak_pool = pool;
    auto cache = std::make_shared<LruCache>(16);
    auto input = std::make_shared<DeferredInputStream>();
    {
        ParquetInputStream stream(input, 16, GetArrowPool(pool), pool, cache, "file", true);
        auto read = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
        input->Complete();
        ASSERT_TRUE(read.status().ok());
    }
    pool.reset();
    ASSERT_FALSE(weak_pool.expired());
    pool = GetDefaultPool();
    {
        ParquetInputStream stream(input, 16, GetArrowPool(pool), pool, cache, "file", true);
        auto hit = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
        ASSERT_TRUE(hit.is_finished());
        ASSERT_EQ(stream.StorageReadBytes()->load(), 0);
        auto miss = stream.ReadAsync(arrow::io::IOContext(), 1, 8);
        ASSERT_FALSE(miss.is_finished());
        input->Complete();
        ASSERT_TRUE(miss.status().ok());
    }
    {
        ParquetInputStream stream(input, 16, GetArrowPool(pool), pool, cache, "other", true);
        auto miss = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
        ASSERT_FALSE(miss.is_finished());
        input->Complete();
        ASSERT_TRUE(miss.status().ok());
    }
    ASSERT_LE(cache->GetCurrentWeight(), 16);
    cache->InvalidateAll();
    ASSERT_TRUE(weak_pool.expired());
}

TEST(ParquetInputStreamTest, ConcurrentWarmReadersReuseBytes) {
    auto pool = GetDefaultPool();
    auto cache = std::make_shared<LruCache>(64);
    auto input = std::make_shared<DeferredInputStream>();
    ParquetInputStream stream(input, 16, GetArrowPool(pool), pool, cache, "file", true);
    auto read = stream.ReadAsync(arrow::io::IOContext(), 0, 8);
    input->Complete();
    ASSERT_TRUE(read.status().ok());
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < 8; ++i) {
        threads.emplace_back([&]() {
            ParquetInputStream reader(input, 16, GetArrowPool(pool), pool, cache, "file", true);
            auto hit = reader.ReadAsync(arrow::io::IOContext(), 0, 8);
            ASSERT_TRUE(hit.is_finished());
            ASSERT_TRUE(hit.status().ok());
            ASSERT_EQ(hit.result().ValueOrDie()->ToString(), "01234567");
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(input->reads, 1);
}

}  // namespace paimon::parquet::test
