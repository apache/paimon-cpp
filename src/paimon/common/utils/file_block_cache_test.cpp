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

#include "paimon/common/utils/file_block_cache.h"

#include <chrono>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/utils/gated_async_input_stream.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

constexpr char kContent[] = "abcdefghijklmnopqrstuvwxyz";
// 26 bytes in blocks of 8: block 0 = [18, 26), block 1 = [10, 18),
// block 2 = [2, 10) and block 3 = [0, 2), truncated at the start of the file.
constexpr uint64_t kBlockSize = 8;

// A pool of its own for every cache, so that a buffer outliving the pool it was
// allocated from shows up instead of being covered by the global pool, which
// never goes away.
std::shared_ptr<MemoryPool> TestPool() {
    return std::shared_ptr<MemoryPool>(GetMemoryPool());
}

// Write the test content into a fresh directory and open it for reading. The
// directory is returned so that it outlives the stream.
std::shared_ptr<InputStream> OpenTestFile(std::unique_ptr<UniqueTestDirectory>* dir) {
    *dir = UniqueTestDirectory::Create();
    EXPECT_TRUE(*dir);
    std::string path = (*dir)->Str() + "/data_file";
    std::ofstream file(path, std::ios::binary);
    EXPECT_TRUE(file.is_open());
    file.write(kContent, sizeof(kContent) - 1);
    EXPECT_FALSE(file.fail());
    file.close();

    Result<std::unique_ptr<FileSystem>> fs = FileSystemFactory::Get("local", path, {});
    EXPECT_OK(fs.status());
    Result<std::unique_ptr<InputStream>> in = fs.value()->Open(path);
    EXPECT_OK(in.status());
    return std::move(in).value();
}

// Assert that the range is served out of a block with the expected content.
void AssertServed(const ByteRange& range, const std::string& expected, FileBlockCache* cache) {
    std::string dest(range.length, 'X');
    ASSERT_TRUE(cache->Read(range, dest.data())) << expected;
    ASSERT_EQ(expected, std::string_view(dest.data(), range.length));
}

// Assert that the cache declines the range and leaves the destination untouched,
// so that the caller can read the bytes itself.
void AssertDeclined(const ByteRange& range, FileBlockCache* cache) {
    std::string dest(range.length, 'X');
    ASSERT_FALSE(cache->Read(range, dest.data()));
    ASSERT_EQ(std::string(dest.size(), 'X'), dest);
}

// Assert that ReadAsync() serves the range with the expected content without
// leaving the caller: either the block is cached already or its fetch completes
// inline, the way the local filesystem completes its ReadAsync().
void AssertServedAsyncInline(const ByteRange& range, const std::string& expected,
                             FileBlockCache* cache) {
    std::string dest(range.length, 'X');
    bool served = false;
    bool called = false;
    cache->ReadAsync(range, dest.data(), [&](bool hit) {
        called = true;
        served = hit;
    });
    ASSERT_TRUE(called) << expected;
    ASSERT_TRUE(served) << expected;
    ASSERT_EQ(expected, std::string_view(dest.data(), range.length));
}

// Assert that ReadAsync() declines the range inline, leaving the destination
// untouched, the way AssertDeclined() does for Read().
void AssertDeclinedAsync(const ByteRange& range, FileBlockCache* cache) {
    std::string dest(range.length, 'X');
    bool served = true;
    bool called = false;
    cache->ReadAsync(range, dest.data(), [&](bool hit) {
        called = true;
        served = hit;
    });
    ASSERT_TRUE(called);
    ASSERT_FALSE(served);
    ASSERT_EQ(std::string(dest.size(), 'X'), dest);
}

}  // namespace

// The tail of the file is fetched once and then serves every later read falling
// into it, the way the footer read of a parquet file serves the page index reads
// of the readers sharing the cache.
TEST(TestFileBlockCache, TestServesRepeatedTailReads) {
    std::unique_ptr<UniqueTestDirectory> dir;
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/1024, TestPool());

    // The whole last block, as the footer read of a parquet file does.
    AssertServed({18, 8}, "stuvwxyz", &cache);
    // Five small reads inside that block, as the page index reads do.
    AssertServed({25, 1}, "z", &cache);
    AssertServed({20, 2}, "uv", &cache);
    AssertServed({18, 1}, "s", &cache);
    AssertServed({22, 4}, "wxyz", &cache);
    AssertServed({19, 3}, "tuv", &cache);

    // One fetch of one block served all six reads.
    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.fetch_bytes, 8u);
    ASSERT_EQ(counters.hits, 6u);
    ASSERT_EQ(counters.hit_bytes, 8u + 1u + 2u + 1u + 4u + 3u);
}

// The lowest block is truncated at the start of the file, so no block fetch
// reads past either end of the file.
TEST(TestFileBlockCache, TestClampsLowestBlockAtFileStart) {
    std::unique_ptr<UniqueTestDirectory> dir;
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/1024, TestPool());

    AssertServed({0, 2}, "ab", &cache);
    AssertServed({1, 1}, "b", &cache);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.fetch_bytes, 2u);
    ASSERT_EQ(counters.hits, 2u);
}

// A reader racing the fetch of a block must wait on the published block instead
// of issuing a second fetch for the same bytes.
TEST(TestFileBlockCache, TestSingleFlightForConcurrentReads) {
    std::unique_ptr<UniqueTestDirectory> dir;
    auto gated = std::make_shared<GatedAsyncInputStream>(OpenTestFile(&dir));
    FileBlockCache cache(gated, sizeof(kContent) - 1, kBlockSize, /*capacity=*/1024, TestPool());

    // The first reader publishes block 0 = [18, 26) and blocks on its held fetch.
    std::thread first([&cache]() {
        std::string dest(4, 'X');
        EXPECT_TRUE(cache.Read({18, 4}, dest.data()));
        EXPECT_EQ("stuv", std::string_view(dest.data(), 4));
    });
    while (gated->AsyncReadCount() == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // The second reader finds the published block and waits for the same fetch.
    std::thread second([&cache]() {
        std::string dest(4, 'X');
        EXPECT_TRUE(cache.Read({22, 4}, dest.data()));
        EXPECT_EQ("wxyz", std::string_view(dest.data(), 4));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(gated->AsyncReadCount(), 1);
    gated->ReleaseAll();
    first.join();
    second.join();

    ASSERT_EQ(gated->AsyncReadCount(), 1);
    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.hits, 2u);
}

// Blocks are never evicted, so a read whose block does not fit into the capacity
// is declined instead of replacing a cached block.
TEST(TestFileBlockCache, TestExhaustedCapacityDeclinesNewBlocks) {
    std::unique_ptr<UniqueTestDirectory> dir;
    // The capacity holds exactly one block.
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/kBlockSize, TestPool());

    AssertServed({18, 4}, "stuv", &cache);
    // Block 1 = [10, 18) does not fit anymore.
    AssertDeclined({10, 4}, &cache);
    // The block already cached still serves its reads.
    AssertServed({20, 2}, "uv", &cache);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.fetch_bytes, 8u);
    ASSERT_EQ(counters.hits, 2u);
}

// Reads that one block cannot serve are declined: a read straddling two blocks,
// a read larger than a block and a read reaching past EOF.
TEST(TestFileBlockCache, TestDeclinesStraddlingAndOversizedReads) {
    std::unique_ptr<UniqueTestDirectory> dir;
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/1024, TestPool());

    // [16, 20) straddles block 1 = [10, 18) and block 0 = [18, 26).
    AssertDeclined({16, 4}, &cache);
    // Larger than one block.
    AssertDeclined({0, 12}, &cache);
    // Reaches past the end of the file.
    AssertDeclined({24, 4}, &cache);
    // Starts past the end of the file.
    AssertDeclined({26, 2}, &cache);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 0u);
    ASSERT_EQ(counters.hits, 0u);
}

// A zero capacity or a zero block size turns the cache off entirely.
TEST(TestFileBlockCache, TestDisabledByZeroCapacityOrBlockSize) {
    std::unique_ptr<UniqueTestDirectory> dir;
    std::shared_ptr<InputStream> in = OpenTestFile(&dir);

    FileBlockCache no_capacity(in, sizeof(kContent) - 1, kBlockSize, /*capacity=*/0, TestPool());
    AssertDeclined({18, 4}, &no_capacity);
    ASSERT_EQ(no_capacity.GetCounters().fetches, 0u);

    FileBlockCache no_block_size(in, sizeof(kContent) - 1, /*block_size=*/0, /*capacity=*/1024,
                                 TestPool());
    AssertDeclined({18, 4}, &no_block_size);
    ASSERT_EQ(no_block_size.GetCounters().fetches, 0u);
}

// ResetCounters() keeps the cached blocks, which cache the file rather than a
// round of reads, while Release() drops them and makes the next read fetch again.
TEST(TestFileBlockCache, TestResetCountersKeepsBlocksAndReleaseDropsThem) {
    std::unique_ptr<UniqueTestDirectory> dir;
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/1024, TestPool());

    AssertServed({18, 4}, "stuv", &cache);
    ASSERT_EQ(cache.GetCounters().fetches, 1u);

    cache.ResetCounters();
    ASSERT_EQ(cache.GetCounters().hits, 0u);
    ASSERT_EQ(cache.GetCounters().fetches, 0u);
    // The block is still there, so this read needs no fetch.
    AssertServed({18, 4}, "stuv", &cache);
    ASSERT_EQ(cache.GetCounters().hits, 1u);
    ASSERT_EQ(cache.GetCounters().fetches, 0u);

    cache.Release();
    AssertServed({18, 4}, "stuv", &cache);
    ASSERT_EQ(cache.GetCounters().hits, 2u);
    ASSERT_EQ(cache.GetCounters().fetches, 1u);
}

// A failed fetch is not reported to the caller, which reads its own bytes
// instead, and the failed block is not fetched again.
TEST(TestFileBlockCache, TestFetchErrorDeclinesTheReads) {
    std::unique_ptr<UniqueTestDirectory> dir;
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/1024, TestPool());
    auto io_hook = paimon::IOHook::GetInstance();
    paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
    io_hook->Reset(0, paimon::IOHook::Mode::RETURN_ERROR);

    AssertDeclined({18, 4}, &cache);
    // The failed block is kept, so the later reads of it are declined without a
    // second fetch, even once the reads would succeed again.
    io_hook->Clear();
    AssertDeclined({20, 2}, &cache);
    // The other blocks are unaffected.
    AssertServed({10, 4}, "klmn", &cache);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 2u);
    ASSERT_EQ(counters.hits, 1u);
}

// A file size larger than the physical file - the size recorded by the file
// metadata is not necessarily the size of the file - makes the fetch of the
// block reaching past the end of the file fail. That must cost no more than the
// caching of that block: the reads it would serve are declined and read by the
// caller itself.
TEST(TestFileBlockCache, TestFileSizeLargerThanTheFileOnlyLosesTheLastBlock) {
    std::unique_ptr<UniqueTestDirectory> dir;
    // 30 instead of 26, so block 0 = [22, 30) has 4 bytes the file does not have.
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1 + 4, kBlockSize,
                         /*capacity=*/1024, TestPool());

    AssertDeclined({22, 4}, &cache);
    // No second fetch of the block that cannot be read.
    AssertDeclined({23, 2}, &cache);
    // The blocks that the file does have still serve their reads.
    AssertServed({10, 4}, "klmn", &cache);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 2u);
    ASSERT_EQ(counters.hits, 1u);
}

// An object store stream destroys the callback of a read after it has resolved
// it, so the last reference to a block buffer can be dropped by an IO thread
// after the cache - and the memory pool it holds - is already gone. The buffer
// must keep its pool alive until then, or it frees its allocation against a
// destroyed pool.
TEST(TestFileBlockCache, TestBlockBufferKeepsPoolAliveAfterCacheIsGone) {
    std::unique_ptr<UniqueTestDirectory> dir;
    auto gated = std::make_shared<GatedAsyncInputStream>(OpenTestFile(&dir));
    std::weak_ptr<MemoryPool> weak_pool;
    {
        std::shared_ptr<MemoryPool> pool = TestPool();
        weak_pool = pool;
        // Declared after the pool, so the cache is destroyed before the last
        // reference of this test to the pool is dropped.
        FileBlockCache cache(gated, sizeof(kContent) - 1, kBlockSize, /*capacity=*/1024, pool);

        std::thread reader([&cache]() {
            std::string dest(4, 'X');
            EXPECT_TRUE(cache.Read({18, 4}, dest.data()));
            EXPECT_EQ("stuv", std::string_view(dest.data(), 4));
        });
        while (gated->AsyncReadCount() == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        // The fetch is completed, but the stream keeps its callback - and with it
        // a reference to the block buffer - alive.
        gated->ReleaseAllKeepingCallbacks();
        reader.join();
    }

    // The cache and the pool reference of this test are gone, so the callback
    // holds the last reference to the buffer, which must still hold the pool.
    ASSERT_FALSE(weak_pool.expired());
    gated->DropCompletedCallbacks();
    ASSERT_TRUE(weak_pool.expired());
}

// A caller whose own interface is asynchronous must not block on the fetch of a
// missing block: ReadAsync() returns while the fetch is in flight and resolves
// its callback when the fetch does.
TEST(TestFileBlockCache, TestReadAsyncAwaitsTheBlockFetch) {
    std::unique_ptr<UniqueTestDirectory> dir;
    auto gated = std::make_shared<GatedAsyncInputStream>(OpenTestFile(&dir));
    FileBlockCache cache(gated, sizeof(kContent) - 1, kBlockSize, /*capacity=*/1024, TestPool());

    std::string dest(8, 'X');
    bool called = false;
    bool served = false;
    cache.ReadAsync({18, 8}, dest.data(), [&](bool hit) {
        called = true;
        served = hit;
    });

    // The fetch is held and the caller was not blocked on it.
    ASSERT_FALSE(called);
    ASSERT_EQ(std::string(8, 'X'), dest);
    ASSERT_EQ(gated->AsyncReadCount(), 1);

    gated->ReleaseAll();
    ASSERT_TRUE(called);
    ASSERT_TRUE(served);
    ASSERT_EQ("stuvwxyz", std::string_view(dest.data(), 8));

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.fetch_bytes, 8u);
    ASSERT_EQ(counters.hits, 1u);
    ASSERT_EQ(counters.hit_bytes, 8u);
}

// A block that is cached already resolves the callback inline, and so does a
// fetch that the stream completes inline: the local filesystem reads inline, so
// a reader on it observes exactly what Read() would have returned.
TEST(TestFileBlockCache, TestReadAsyncCompletesInlineWhenTheBlockIsThere) {
    std::unique_ptr<UniqueTestDirectory> dir;
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/1024, TestPool());

    // The whole last block, as the footer read of a parquet file does.
    AssertServedAsyncInline({18, 8}, "stuvwxyz", &cache);
    // Small reads inside the block that is cached now, as the page index reads do.
    AssertServedAsyncInline({20, 2}, "uv", &cache);
    AssertServedAsyncInline({25, 1}, "z", &cache);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.fetch_bytes, 8u);
    ASSERT_EQ(counters.hits, 3u);
    ASSERT_EQ(counters.hit_bytes, 8u + 2u + 1u);
}

// Two asynchronous readers of the same missing block share the one fetch it
// dispatches, the way two blocking readers do: neither waits on a thread, and
// both callbacks run when the fetch is resolved.
TEST(TestFileBlockCache, TestReadAsyncSingleFlightForConcurrentReads) {
    std::unique_ptr<UniqueTestDirectory> dir;
    auto gated = std::make_shared<GatedAsyncInputStream>(OpenTestFile(&dir));
    FileBlockCache cache(gated, sizeof(kContent) - 1, kBlockSize, /*capacity=*/1024, TestPool());

    std::string first_dest(4, 'X');
    std::string second_dest(4, 'X');
    int32_t served = 0;
    cache.ReadAsync({18, 4}, first_dest.data(), [&served](bool hit) { served += hit ? 1 : 0; });
    cache.ReadAsync({22, 4}, second_dest.data(), [&served](bool hit) { served += hit ? 1 : 0; });

    // Both readers are waiting for the same held fetch, and neither was blocked.
    ASSERT_EQ(served, 0);
    ASSERT_EQ(gated->AsyncReadCount(), 1);
    ASSERT_EQ(std::string(4, 'X'), first_dest);
    ASSERT_EQ(std::string(4, 'X'), second_dest);

    gated->ReleaseAll();
    ASSERT_EQ(served, 2);
    ASSERT_EQ("stuv", std::string_view(first_dest.data(), 4));
    ASSERT_EQ("wxyz", std::string_view(second_dest.data(), 4));

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.hits, 2u);
}

// Every read the cache declines is declined inline, so an asynchronous caller
// can fall back to its own read right away instead of waiting for a fetch that
// was never dispatched.
TEST(TestFileBlockCache, TestReadAsyncDeclinesInline) {
    std::unique_ptr<UniqueTestDirectory> dir;
    auto gated = std::make_shared<GatedAsyncInputStream>(OpenTestFile(&dir));
    FileBlockCache cache(gated, sizeof(kContent) - 1, kBlockSize, /*capacity=*/1024, TestPool());

    // [16, 20) straddles block 1 = [10, 18) and block 0 = [18, 26).
    AssertDeclinedAsync({16, 4}, &cache);
    // Larger than one block.
    AssertDeclinedAsync({0, 12}, &cache);
    // Reaches past the end of the file.
    AssertDeclinedAsync({24, 4}, &cache);
    // Starts past the end of the file.
    AssertDeclinedAsync({26, 2}, &cache);

    ASSERT_EQ(gated->AsyncReadCount(), 0);
    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 0u);
    ASSERT_EQ(counters.hits, 0u);
}

// Blocks are never evicted, so a read whose block does not fit into the capacity
// is declined inline instead of replacing a cached block.
TEST(TestFileBlockCache, TestReadAsyncExhaustedCapacityDeclinesInline) {
    std::unique_ptr<UniqueTestDirectory> dir;
    // The capacity holds exactly one block.
    FileBlockCache cache(OpenTestFile(&dir), sizeof(kContent) - 1, kBlockSize,
                         /*capacity=*/kBlockSize, TestPool());

    AssertServedAsyncInline({18, 4}, "stuv", &cache);
    // Block 1 = [10, 18) does not fit anymore.
    AssertDeclinedAsync({10, 4}, &cache);
    // The block already cached still serves its reads.
    AssertServedAsyncInline({20, 2}, "uv", &cache);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.fetch_bytes, 8u);
    ASSERT_EQ(counters.hits, 2u);
}

// A failed fetch declines an asynchronous read the way it declines a blocking
// one: the failure is not reported to the caller, which reads its own bytes
// instead, and the failed block is not fetched again.
TEST(TestFileBlockCache, TestReadAsyncFetchErrorDeclinesTheReads) {
    std::unique_ptr<UniqueTestDirectory> dir;
    auto gated = std::make_shared<GatedAsyncInputStream>(OpenTestFile(&dir));
    FileBlockCache cache(gated, sizeof(kContent) - 1, kBlockSize, /*capacity=*/1024, TestPool());

    std::string dest(4, 'X');
    bool called = false;
    bool served = true;
    cache.ReadAsync({18, 4}, dest.data(), [&](bool hit) {
        called = true;
        served = hit;
    });
    ASSERT_FALSE(called);
    gated->FailAll(Status::IOError("fetch failed"));

    // The caller sees a declined read, not the failure of the fetch.
    ASSERT_TRUE(called);
    ASSERT_FALSE(served);
    ASSERT_EQ(std::string(4, 'X'), dest);

    // The failed block is kept, so a later read of it is declined inline without
    // a second fetch.
    AssertDeclinedAsync({20, 2}, &cache);
    ASSERT_EQ(gated->AsyncReadCount(), 1);

    const FileBlockCache::Counters counters = cache.GetCounters();
    ASSERT_EQ(counters.fetches, 1u);
    ASSERT_EQ(counters.hits, 0u);
}

}  // namespace paimon::test
