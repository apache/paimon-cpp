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

#include "paimon/common/utils/read_ahead_cache.h"

#include <chrono>
#include <fstream>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/utils/gated_async_input_stream.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

// Helper to create a test file, write content, and return a ready ReadAheadCache.
struct TestCacheEnv {
    std::string path;
    std::shared_ptr<paimon::ReadAheadCache> cache;
    std::shared_ptr<paimon::MemoryPool> pool;
};

// `file_size` defaults to 0, i.e. unknown, which keeps the block cache off: a
// read that no registered range covers stays a plain miss. The block cache
// tests pass the real size of the file.
TestCacheEnv CreateTestFileAndCache(const std::string& filename, const std::string& content,
                                    const paimon::CacheConfig& config,
                                    std::vector<paimon::ByteRange> ranges, uint64_t file_size = 0) {
    auto dir = UniqueTestDirectory::Create();
    EXPECT_TRUE(dir);
    std::string path = dir->Str() + "/" + filename;
    std::ofstream file(path, std::ios::binary);
    EXPECT_TRUE(file.is_open());
    file.write(content.data(), content.size());
    EXPECT_FALSE(file.fail());
    file.close();

    auto fs_result = FileSystemFactory::Get("local", path, {});
    EXPECT_TRUE(fs_result.ok());
    auto fs = std::move(fs_result).value();
    auto in_result = fs->Open(path);
    EXPECT_TRUE(in_result.ok());
    auto in = std::move(in_result).value();

    auto pool = GetDefaultPool();
    auto cache = std::make_shared<ReadAheadCache>(std::move(in), config, file_size, pool);
    EXPECT_OK(cache->Init(std::move(ranges)));
    return {path, cache, pool};
}

// Assert that reading the range is a cache hit filling the destination with
// the expected content.
void AssertReadEquals(const ByteRange& range, const std::string& expected, ReadAheadCache* cache) {
    std::string dest(std::max<size_t>(range.length, 1), 'X');
    bool hit = false;
    ASSERT_OK_AND_ASSIGN(hit, cache->Read(range, dest.data()));
    ASSERT_TRUE(hit) << expected;
    EXPECT_EQ(expected, std::string_view(dest.data(), range.length));
}

// Assert that reading the range misses and leaves the destination untouched.
void AssertReadMiss(const ByteRange& range, ReadAheadCache* cache) {
    std::string dest(std::max<size_t>(range.length, 1), 'X');
    bool hit = true;
    ASSERT_OK_AND_ASSIGN(hit, cache->Read(range, dest.data()));
    ASSERT_FALSE(hit);
    EXPECT_EQ(std::string(dest.size(), 'X'), dest);
}

TEST(TestReadAheadCache, TestBasics) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/128 * 1024 * 1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache(
        "data_file", content, config,
        {{1, 2}, {3, 2}, {8, 2}, {10, 4}, {14, 0}, {15, 4}, {20, 2}, {25, 0}});
    auto& cache = *env.cache;

    AssertReadEquals({20, 2}, "uv", &cache);
    AssertReadEquals({1, 2}, "bc", &cache);
    AssertReadEquals({3, 2}, "de", &cache);
    AssertReadEquals({8, 2}, "ij", &cache);
    AssertReadEquals({10, 4}, "klmn", &cache);
    AssertReadEquals({15, 4}, "pqrs", &cache);
    AssertReadEquals({19, 3}, "tuv", &cache);

    // Zero-sized reads are immediate hits touching nothing.
    AssertReadEquals({14, 0}, "", &cache);
    AssertReadEquals({25, 0}, "", &cache);

    // Non-cached ranges miss and leave the destination untouched.
    AssertReadMiss({20, 3}, &cache);
    AssertReadMiss({0, 3}, &cache);
    AssertReadMiss({25, 2}, &cache);
}

// Test that a read spanning several adjacent cache entries is served from the
// contiguous run of entries and counted as a single hit.
TEST(TestReadAheadCache, TestMultiSegmentContiguousHit) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    // A single 25-byte range exceeds range_size_limit, so Init() coalesces it
    // into three adjacent entries: {0,10}, {10,10} and {20,5}.
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 25}});
    auto& cache = *env.cache;

    // Spans all three entries.
    AssertReadEquals({5, 20}, "fghijklmnopqrstuvwxy", &cache);

    // Spans the first two entries only, trimming both ends of the run.
    AssertReadEquals({5, 10}, "fghijklmno", &cache);

    // Runs past the end of the last entry: no contiguous cover, a miss.
    AssertReadMiss({5, 21}, &cache);

    // A multi-segment hit counts once with the full requested length.
    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache.CollectMetrics(&metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t hits, metrics->GetCounter(ReadAheadCacheMetrics::READ_HITS));
    ASSERT_EQ(hits, 2u);
    ASSERT_OK_AND_ASSIGN(uint64_t hit_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_HIT_BYTES));
    ASSERT_EQ(hit_bytes, 30u);
    ASSERT_OK_AND_ASSIGN(uint64_t misses, metrics->GetCounter(ReadAheadCacheMetrics::READ_MISSES));
    ASSERT_EQ(misses, 1u);
    ASSERT_OK_AND_ASSIGN(uint64_t miss_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_MISS_BYTES));
    ASSERT_EQ(miss_bytes, 21u);
}

// Test repeated reads to the same range to ensure cache reuse.
TEST(TestReadAheadCache, TestRepeatedReadCacheReuse) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/64);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}, {7, 5}});
    auto& cache = *env.cache;

    AssertReadEquals({0, 5}, "abcde", &cache);
    AssertReadEquals({0, 5}, "abcde", &cache);
}

// The cache never evicts: every prefetched range stays cached until
// ReleaseBuffers()/Reset(), regardless of how much data accumulates.
TEST(TestReadAheadCache, TestNoEvictionKeepsAllRanges) {
    CacheConfig config(/*range_size_limit=*/5, /*hole_size_limit=*/2,
                       /*pre_buffer_limit=*/10);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}, {8, 5}, {16, 5}});
    auto& cache = *env.cache;

    AssertReadEquals({0, 5}, "abcde", &cache);

    // Reading further ranges keeps the earlier ones cached.
    AssertReadEquals({8, 5}, "ijklm", &cache);
    AssertReadEquals({16, 5}, "qrstu", &cache);
    AssertReadEquals({0, 5}, "abcde", &cache);
}

// Test that Read() hits and misses are recorded in the cache metrics.
TEST(TestReadAheadCache, TestMetrics) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/128 * 1024 * 1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}, {8, 5}});
    auto& cache = *env.cache;

    AssertReadEquals({0, 5}, "abcde", &cache);
    // Out of any cached range: a miss.
    AssertReadMiss({20, 3}, &cache);

    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache.CollectMetrics(&metrics);
    // Both Read() requests are counted, regardless of hit or miss.
    ASSERT_OK_AND_ASSIGN(uint64_t read_count,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_COUNT));
    ASSERT_EQ(read_count, 2u);
    ASSERT_OK_AND_ASSIGN(uint64_t read_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_BYTES));
    ASSERT_EQ(read_bytes, 8u);
    ASSERT_OK_AND_ASSIGN(uint64_t hits, metrics->GetCounter(ReadAheadCacheMetrics::READ_HITS));
    ASSERT_EQ(hits, 1u);
    ASSERT_OK_AND_ASSIGN(uint64_t hit_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_HIT_BYTES));
    ASSERT_EQ(hit_bytes, 5u);
    ASSERT_OK_AND_ASSIGN(uint64_t misses, metrics->GetCounter(ReadAheadCacheMetrics::READ_MISSES));
    ASSERT_EQ(misses, 1u);
    ASSERT_OK_AND_ASSIGN(uint64_t miss_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_MISS_BYTES));
    ASSERT_EQ(miss_bytes, 3u);
    // The hit prefetches both pending ranges in one window: two IO requests
    // for 10 bytes in total; the miss issues no further fetch.
    ASSERT_OK_AND_ASSIGN(uint64_t io_count, metrics->GetCounter(ReadAheadCacheMetrics::IO_COUNT));
    ASSERT_EQ(io_count, 2u);
    ASSERT_OK_AND_ASSIGN(uint64_t io_bytes, metrics->GetCounter(ReadAheadCacheMetrics::IO_BYTES));
    ASSERT_EQ(io_bytes, 10u);
}

// Test that ReleaseBuffers() drops the cached data but keeps the hit/miss counters
// readable, while Reset() zeroes them as well.
TEST(TestReadAheadCache, TestReleaseBuffersKeepsMetrics) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/128 * 1024 * 1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}});
    auto& cache = *env.cache;

    AssertReadEquals({0, 5}, "abcde", &cache);

    cache.ReleaseBuffers();

    // The previously cached range is gone: the read now misses.
    AssertReadMiss({0, 5}, &cache);

    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache.CollectMetrics(&metrics);
    // The read counters survive ReleaseBuffers() as well.
    ASSERT_OK_AND_ASSIGN(uint64_t read_count,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_COUNT));
    ASSERT_EQ(read_count, 2u);
    ASSERT_OK_AND_ASSIGN(uint64_t read_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_BYTES));
    ASSERT_EQ(read_bytes, 10u);
    ASSERT_OK_AND_ASSIGN(uint64_t hits, metrics->GetCounter(ReadAheadCacheMetrics::READ_HITS));
    ASSERT_EQ(hits, 1u);
    ASSERT_OK_AND_ASSIGN(uint64_t hit_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_HIT_BYTES));
    ASSERT_EQ(hit_bytes, 5u);
    ASSERT_OK_AND_ASSIGN(uint64_t misses, metrics->GetCounter(ReadAheadCacheMetrics::READ_MISSES));
    ASSERT_EQ(misses, 1u);
    // The io counters survive ReleaseBuffers() as well.
    ASSERT_OK_AND_ASSIGN(uint64_t io_count, metrics->GetCounter(ReadAheadCacheMetrics::IO_COUNT));
    ASSERT_EQ(io_count, 1u);
    ASSERT_OK_AND_ASSIGN(uint64_t io_bytes, metrics->GetCounter(ReadAheadCacheMetrics::IO_BYTES));
    ASSERT_EQ(io_bytes, 5u);

    // Reset() clears the counters too.
    cache.Reset();
    std::shared_ptr<Metrics> reset_metrics = std::make_shared<MetricsImpl>();
    cache.CollectMetrics(&reset_metrics);
    ASSERT_OK_AND_ASSIGN(read_count, reset_metrics->GetCounter(ReadAheadCacheMetrics::READ_COUNT));
    ASSERT_EQ(read_count, 0u);
    ASSERT_OK_AND_ASSIGN(hits, reset_metrics->GetCounter(ReadAheadCacheMetrics::READ_HITS));
    ASSERT_EQ(hits, 0u);
    ASSERT_OK_AND_ASSIGN(misses, reset_metrics->GetCounter(ReadAheadCacheMetrics::READ_MISSES));
    ASSERT_EQ(misses, 0u);
    ASSERT_OK_AND_ASSIGN(io_count, reset_metrics->GetCounter(ReadAheadCacheMetrics::IO_COUNT));
    ASSERT_EQ(io_count, 0u);
    ASSERT_OK_AND_ASSIGN(io_bytes, reset_metrics->GetCounter(ReadAheadCacheMetrics::IO_BYTES));
    ASSERT_EQ(io_bytes, 0u);
}

// Test that a failed prefetch surfaces as an error Status from Read(), not as
// a miss: the entry exists from the moment its fetch is submitted and its
// future carries the IO error.
TEST(TestReadAheadCache, TestPrefetchIOErrorPropagation) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto io_hook = paimon::IOHook::GetInstance();

    // Single entry: the prefetch is the first IO after the hook is armed.
    {
        auto env = CreateTestFileAndCache("data_file", content, config, {{0, 10}});
        paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(0, paimon::IOHook::Mode::RETURN_ERROR);
        std::string dest(5, 'X');
        ASSERT_NOK_WITH_MSG(env.cache->Read({0, 5}, dest.data()),
                            "io hook triggered io error at position");
    }

    // Several adjacent entries: the error of any segment aborts the read.
    {
        auto env = CreateTestFileAndCache("data_file", content, config, {{0, 25}});
        paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(1, paimon::IOHook::Mode::RETURN_ERROR);
        std::string dest(20, 'X');
        ASSERT_NOK_WITH_MSG(env.cache->Read({0, 20}, dest.data()),
                            "io hook triggered io error at position");
    }
}

// Test that Warmup() fetches the pending ranges up front so the first Read()
// issues no further IO, while without Warmup() the first Read() triggers the
// prefetch itself.
TEST(TestReadAheadCache, TestWarmupPrefetchesBeforeFirstRead) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env1 = CreateTestFileAndCache("data_file", content, config, {{0, 5}, {8, 5}});
    env1.cache->Warmup();
    auto env2 = CreateTestFileAndCache("data_file", content, config, {{0, 5}});

    auto io_hook = paimon::IOHook::GetInstance();
    paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
    // Any new IO fails: the warmed-up reads must be served without fetching.
    io_hook->Reset(0, paimon::IOHook::Mode::RETURN_ERROR);

    AssertReadEquals({0, 5}, "abcde", env1.cache.get());
    AssertReadEquals({8, 5}, "ijklm", env1.cache.get());

    // Without Warmup() the first Read() starts the prefetch and sees the error.
    std::string dest(5, 'X');
    ASSERT_NOK(env2.cache->Read({0, 5}, dest.data()));
}

// Warmup() without any pending ranges is a safe no-op.
TEST(TestReadAheadCache, TestWarmupWithEmptyRanges) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {});
    env.cache->Warmup();
    AssertReadMiss({0, 5}, env.cache.get());
}

// A reader racing an in-flight prefetch must find the published entry and wait
// on its future instead of missing and re-fetching the same bytes: entries are
// published under the lock before their fetch is dispatched.
TEST(TestReadAheadCache, TestInFlightEntryServesRacingReader) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string path = dir->Str() + "/data_file";
    std::ofstream file(path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.write(content.data(), content.size());
    ASSERT_FALSE(file.fail());
    file.close();
    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", path, {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs->Open(path));
    auto gated = std::make_shared<GatedAsyncInputStream>(std::move(in));

    ReadAheadCache cache(gated, config, /*file_size=*/0, GetDefaultPool());
    ASSERT_OK(cache.Init({{0, 5}}));
    cache.Warmup();

    // The prefetch entry is published, but its fetch is still held.
    ASSERT_EQ(gated->AsyncReadCount(), 1);

    // A racing reader blocks on the in-flight entry's future and is served
    // from it once the fetch completes, without triggering a second fetch.
    std::thread reader([&cache, &gated]() {
        std::string dest(5, 'X');
        Result<bool> res = cache.Read({0, 5}, dest.data());
        EXPECT_TRUE(res.ok());
        if (res.ok()) {
            EXPECT_TRUE(res.value());
        }
        EXPECT_EQ("abcde", std::string_view(dest.data(), 5));
        EXPECT_EQ(gated->AsyncReadCount(), 1);
    });
    // Give the reader time to block on the in-flight entry's future before
    // completing the fetch.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    gated->ReleaseAll();
    reader.join();

    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache.CollectMetrics(&metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t misses, metrics->GetCounter(ReadAheadCacheMetrics::READ_MISSES));
    ASSERT_EQ(misses, 0u);
    ASSERT_OK_AND_ASSIGN(uint64_t io_count, metrics->GetCounter(ReadAheadCacheMetrics::IO_COUNT));
    ASSERT_EQ(io_count, 1u);
}

// Test that pre_buffer_limit truncates the prefetch window: only ranges within
// the window are fetched at once, later reads fetch the remaining batches.
TEST(TestReadAheadCache, TestPreBufferWindowLimit) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/0, /*pre_buffer_limit=*/10);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 10}, {16, 10}});
    auto& cache = *env.cache;

    auto io_hook = paimon::IOHook::GetInstance();
    paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
    // IOCount() only counts while armed; INT64_MAX never triggers the error mode.
    io_hook->Reset(INT64_MAX, paimon::IOHook::Mode::RETURN_ERROR);

    AssertReadEquals({0, 10}, "abcdefghij", &cache);
    // The second range did not fit into the window: only one prefetch IO.
    ASSERT_EQ(io_hook->IOCount(), 1);

    AssertReadEquals({16, 10}, "qrstuvwxyz", &cache);
    // The second read triggered exactly one more prefetch IO.
    ASSERT_EQ(io_hook->IOCount(), 2);

    // The range is cached now: re-reading it issues no IO at all.
    io_hook->Reset(INT64_MAX, paimon::IOHook::Mode::RETURN_ERROR);
    AssertReadEquals({16, 10}, "qrstuvwxyz", &cache);
    ASSERT_EQ(io_hook->IOCount(), 0);
}

// Test that Init() rejects a second call until the cache is reset.
TEST(TestReadAheadCache, TestDoubleInit) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}});
    auto& cache = *env.cache;

    Status status = cache.Init({{8, 5}});
    ASSERT_FALSE(status.ok());

    // The original ranges still work.
    AssertReadEquals({0, 5}, "abcde", &cache);
}

// Test that the cache can be re-initialized after Reset() and serves the new ranges.
TEST(TestReadAheadCache, TestReinitAfterReset) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}});
    auto& cache = *env.cache;

    AssertReadEquals({0, 5}, "abcde", &cache);

    cache.Reset();
    ASSERT_OK(cache.Init({{3, 4}}));
    AssertReadEquals({3, 4}, "defg", &cache);

    // The old ranges are gone.
    AssertReadMiss({20, 2}, &cache);
}

// Test that Init() merges ranges separated by a small hole, so a read
// spanning the hole is served by the single coalesced entry.
TEST(TestReadAheadCache, TestInitCoalescesSmallHoles) {
    CacheConfig config(/*range_size_limit=*/1024,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    // Byte 5 sits in a 1-byte hole, within hole_size_limit: one entry {0,11}.
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}, {6, 5}});
    auto& cache = *env.cache;

    AssertReadEquals({4, 3}, "efg", &cache);
}

// CollectMetrics() with a null metrics output is a safe no-op.
TEST(TestReadAheadCache, TestCollectMetricsWithNullMetrics) {
    CacheConfig config(/*range_size_limit=*/10,
                       /*hole_size_limit=*/2, /*pre_buffer_limit=*/1024);
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    auto env = CreateTestFileAndCache("data_file", content, config, {{0, 5}});
    env.cache->CollectMetrics(/*metrics=*/nullptr);
    std::shared_ptr<Metrics> null_metrics;
    env.cache->CollectMetrics(&null_metrics);
}

// Read the io counters of the cache.
void GetIOCounters(ReadAheadCache* cache, uint64_t* io_count, uint64_t* io_bytes) {
    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache->CollectMetrics(&metrics);
    ASSERT_OK_AND_ASSIGN(*io_count, metrics->GetCounter(ReadAheadCacheMetrics::IO_COUNT));
    ASSERT_OK_AND_ASSIGN(*io_bytes, metrics->GetCounter(ReadAheadCacheMetrics::IO_BYTES));
}

// Counters of the block cache, the companion of GetIOCounters().
struct BlockCounters {
    uint64_t hits = 0;
    uint64_t hit_bytes = 0;
    uint64_t fetches = 0;
    uint64_t fetch_bytes = 0;
};

void GetBlockCounters(ReadAheadCache* cache, BlockCounters* counters) {
    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache->CollectMetrics(&metrics);
    ASSERT_OK_AND_ASSIGN(counters->hits, metrics->GetCounter(ReadAheadCacheMetrics::BLOCK_HITS));
    ASSERT_OK_AND_ASSIGN(counters->hit_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::BLOCK_HIT_BYTES));
    ASSERT_OK_AND_ASSIGN(counters->fetches,
                         metrics->GetCounter(ReadAheadCacheMetrics::BLOCK_FETCHES));
    ASSERT_OK_AND_ASSIGN(counters->fetch_bytes,
                         metrics->GetCounter(ReadAheadCacheMetrics::BLOCK_FETCH_BYTES));
}

// A cache configuration with the block cache enabled at the given granularity.
// The registered ranges are irrelevant to the block cache tests: they read the
// bytes the parquet reader reads before any range is registered.
CacheConfig BlockCacheConfig(uint64_t block_size, uint64_t block_cache_limit) {
    CacheConfig config(/*range_size_limit=*/10, /*hole_size_limit=*/2,
                       /*pre_buffer_limit=*/1024);
    config.SetBlockSize(block_size);
    config.SetBlockCacheLimit(block_cache_limit);
    return config;
}

// The block cache serves the reads that no registered range covers, and such a
// read is counted as a block hit rather than as a miss. See FileBlockCache and
// its own test for the block semantics themselves.
TEST(TestReadAheadCache, TestBlockCacheServesUncoveredReads) {
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    // Blocks are aligned to the end of the file: block 0 is [18, 26).
    CacheConfig config = BlockCacheConfig(/*block_size=*/8, /*block_cache_limit=*/1024);
    auto env = CreateTestFileAndCache("data_file", content, config, {}, content.size());
    auto& cache = *env.cache;

    // The whole last block, as the footer read of a parquet file does, then two
    // small reads inside it, as the page index reads of the other readers do.
    AssertReadEquals({18, 8}, "stuvwxyz", &cache);
    AssertReadEquals({20, 2}, "uv", &cache);
    AssertReadEquals({25, 1}, "z", &cache);

    BlockCounters blocks;
    GetBlockCounters(&cache, &blocks);
    ASSERT_EQ(blocks.fetches, 1u);
    ASSERT_EQ(blocks.fetch_bytes, 8u);
    ASSERT_EQ(blocks.hits, 3u);
    ASSERT_EQ(blocks.hit_bytes, 8u + 2u + 1u);

    // The block fetches are issued to the underlying stream too, so they are
    // part of the io counters.
    uint64_t io_count = 0;
    uint64_t io_bytes = 0;
    GetIOCounters(&cache, &io_count, &io_bytes);
    ASSERT_EQ(io_count, 1u);
    ASSERT_EQ(io_bytes, 8u);

    // A block hit is neither a hit of a registered range nor a miss.
    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache.CollectMetrics(&metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t read_count,
                         metrics->GetCounter(ReadAheadCacheMetrics::READ_COUNT));
    ASSERT_EQ(read_count, 3u);
    ASSERT_OK_AND_ASSIGN(uint64_t hits, metrics->GetCounter(ReadAheadCacheMetrics::READ_HITS));
    ASSERT_EQ(hits, 0u);
    ASSERT_OK_AND_ASSIGN(uint64_t misses, metrics->GetCounter(ReadAheadCacheMetrics::READ_MISSES));
    ASSERT_EQ(misses, 0u);
}

// A read the block cache declines stays a plain miss left to the caller.
TEST(TestReadAheadCache, TestBlockCacheDeclinedReadIsAMiss) {
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    CacheConfig config = BlockCacheConfig(/*block_size=*/8, /*block_cache_limit=*/1024);
    auto env = CreateTestFileAndCache("data_file", content, config, {}, content.size());
    auto& cache = *env.cache;

    // Larger than one block, so the block cache leaves it to the caller.
    AssertReadMiss({0, 12}, &cache);

    BlockCounters blocks;
    GetBlockCounters(&cache, &blocks);
    ASSERT_EQ(blocks.fetches, 0u);
    ASSERT_EQ(blocks.hits, 0u);
    std::shared_ptr<Metrics> metrics = std::make_shared<MetricsImpl>();
    cache.CollectMetrics(&metrics);
    ASSERT_OK_AND_ASSIGN(uint64_t misses, metrics->GetCounter(ReadAheadCacheMetrics::READ_MISSES));
    ASSERT_EQ(misses, 1u);
}

// The blocks belong to the file rather than to a registration round: they
// survive Reset() and are only released by ReleaseBuffers().
TEST(TestReadAheadCache, TestBlockCacheSurvivesReset) {
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    CacheConfig config = BlockCacheConfig(/*block_size=*/8, /*block_cache_limit=*/1024);
    auto env = CreateTestFileAndCache("data_file", content, config, {}, content.size());
    auto& cache = *env.cache;

    AssertReadEquals({18, 4}, "stuv", &cache);

    // Reset() drops the registered ranges and zeroes the counters.
    cache.Reset();
    AssertReadEquals({18, 4}, "stuv", &cache);
    BlockCounters blocks;
    GetBlockCounters(&cache, &blocks);
    ASSERT_EQ(blocks.hits, 1u);
    ASSERT_EQ(blocks.fetches, 0u);
    uint64_t io_count = 0;
    uint64_t io_bytes = 0;
    GetIOCounters(&cache, &io_count, &io_bytes);
    ASSERT_EQ(io_count, 0u);

    // ReleaseBuffers() drops the blocks, so the same read fetches again.
    cache.ReleaseBuffers();
    AssertReadEquals({18, 4}, "stuv", &cache);
    GetBlockCounters(&cache, &blocks);
    ASSERT_EQ(blocks.hits, 2u);
    ASSERT_EQ(blocks.fetches, 1u);
}

// A zero block cache limit or an unknown file size leaves the block cache out
// entirely: an uncovered read is a plain miss left to the caller.
TEST(TestReadAheadCache, TestBlockCacheDisabled) {
    std::string content = "abcdefghijklmnopqrstuvwxyz";
    CacheConfig zero_limit = BlockCacheConfig(/*block_size=*/8, /*block_cache_limit=*/0);
    auto env = CreateTestFileAndCache("data_file", content, zero_limit, {}, content.size());
    AssertReadMiss({18, 4}, env.cache.get());
    AssertReadMiss({18, 4}, env.cache.get());
    BlockCounters blocks;
    GetBlockCounters(env.cache.get(), &blocks);
    ASSERT_EQ(blocks.fetches, 0u);
    ASSERT_EQ(blocks.hits, 0u);

    // An unknown file size cannot be aligned to, so it disables the cache too.
    CacheConfig enabled = BlockCacheConfig(/*block_size=*/8, /*block_cache_limit=*/1024);
    auto unknown_size = CreateTestFileAndCache("data_file", content, enabled, {}, /*file_size=*/0);
    AssertReadMiss({18, 4}, unknown_size.cache.get());
    GetBlockCounters(unknown_size.cache.get(), &blocks);
    ASSERT_EQ(blocks.fetches, 0u);
    ASSERT_EQ(blocks.hits, 0u);
}

}  // namespace paimon::test
