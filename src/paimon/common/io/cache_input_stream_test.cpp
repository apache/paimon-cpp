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

#include "paimon/common/io/cache_input_stream.h"

#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/gated_async_input_stream.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class CacheInputStreamTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(test_dir_);
        content_ = "abcdefghijklmnopqrstuvwxyz0123456789";
        file_path_ = test_dir_->Str() + "/test_data";
        std::ofstream file(file_path_, std::ios::binary);
        ASSERT_TRUE(file.is_open());
        file.write(content_.data(), content_.size());
        file.close();
    }

    std::unique_ptr<InputStream> OpenFile() const {
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                             FileSystemFactory::Get("local", file_path_, {}));
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<InputStream> in, fs->Open(file_path_));
        return in;
    }

    std::shared_ptr<ReadAheadCache> CreateCache(std::vector<ByteRange> ranges) {
        auto stream = OpenFile();
        CacheConfig config;
        config.SetRangeSizeLimit(1024);
        config.SetHoleSizeLimit(0);
        config.SetPreBufferLimit(1024 * 1024);
        // The file size is left unknown so the block cache stays off: these
        // tests exercise the fallback of CacheInputStream on a cache miss.
        auto cache =
            std::make_shared<ReadAheadCache>(std::move(stream), config, /*file_size=*/0, pool_);
        EXPECT_OK(cache->Init(std::move(ranges)));
        return cache;
    }

    // A cache reading from the given gated stream, with the block cache enabled
    // and no range registered, so that a test can observe a read falling to the
    // block cache while the fetch of a cold block is in flight.
    std::shared_ptr<ReadAheadCache> CreateGatedBlockCache(
        const std::shared_ptr<GatedAsyncInputStream>& gated) {
        CacheConfig config;
        config.SetRangeSizeLimit(1024);
        config.SetHoleSizeLimit(0);
        config.SetPreBufferLimit(1024 * 1024);
        config.SetBlockSize(8);
        config.SetBlockCacheLimit(1024);
        auto cache = std::make_shared<ReadAheadCache>(gated, config, content_.size(), pool_);
        EXPECT_OK(cache->Init(std::vector<ByteRange>{}));
        return cache;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::string content_;
    std::string file_path_;
};

// Test proxy methods: Seek, GetPos, Read (sequential), Close, GetUri, Length
TEST_F(CacheInputStreamTest, TestProxyMethods) {
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), /*cache=*/nullptr);

    // Length
    ASSERT_OK_AND_ASSIGN(int64_t length, stream.Length());
    ASSERT_EQ(length, content_.size());

    // GetUri
    ASSERT_OK_AND_ASSIGN(std::string uri, stream.GetUri());
    ASSERT_FALSE(uri.empty());

    // Seek + GetPos
    ASSERT_OK(stream.Seek(5, SeekOrigin::FS_SEEK_SET));
    ASSERT_OK_AND_ASSIGN(int64_t pos, stream.GetPos());
    ASSERT_EQ(pos, 5);

    // Read (sequential, no offset)
    std::string buffer(3, '\0');
    ASSERT_OK_AND_ASSIGN(int64_t bytes_read, stream.Read(buffer.data(), 3));
    ASSERT_EQ(bytes_read, 3);
    ASSERT_EQ(buffer, "fgh");

    // Close
    ASSERT_OK(stream.Close());
}

// Test Read(offset) with cache == nullptr → direct fallback to input_stream
TEST_F(CacheInputStreamTest, TestReadWithOffsetNullCache) {
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), /*cache=*/nullptr);

    std::string buffer(5, '\0');
    ASSERT_OK_AND_ASSIGN(int64_t bytes_read, stream.Read(buffer.data(), 5, /*offset=*/2));
    ASSERT_EQ(bytes_read, 5);
    ASSERT_EQ(buffer, "cdefg");
}

// Test Read(offset) with cache hit → memcpy from cache
TEST_F(CacheInputStreamTest, TestReadWithOffsetCacheHit) {
    // Cache range [2, 5) i.e. offset=2, length=5
    auto cache = CreateCache({{2, 5}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(5, '\0');
    ASSERT_OK_AND_ASSIGN(int64_t bytes_read, stream.Read(buffer.data(), 5, /*offset=*/2));
    ASSERT_EQ(bytes_read, 5);
    ASSERT_EQ(buffer, "cdefg");
}

// Test Read(offset) with cache miss → fallback to input_stream
TEST_F(CacheInputStreamTest, TestReadWithOffsetCacheMiss) {
    // Cache range [2, 5) but read from offset 10 which is not cached
    auto cache = CreateCache({{2, 5}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(3, '\0');
    ASSERT_OK_AND_ASSIGN(int64_t bytes_read, stream.Read(buffer.data(), 3, /*offset=*/10));
    ASSERT_EQ(bytes_read, 3);
    ASSERT_EQ(buffer, "klm");
}

// Test ReadAsync with cache == nullptr → direct fallback to input_stream->ReadAsync
TEST_F(CacheInputStreamTest, TestReadAsyncNullCache) {
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), /*cache=*/nullptr);

    std::string buffer(4, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 4, /*offset=*/0, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(buffer, "abcd");
}

// Test ReadAsync with cache hit → memcpy + callback(OK)
TEST_F(CacheInputStreamTest, TestReadAsyncCacheHit) {
    auto cache = CreateCache({{0, 10}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(4, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 4, /*offset=*/3, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(buffer, "defg");
}

// Test ReadAsync with cache miss → fallback to input_stream->ReadAsync
TEST_F(CacheInputStreamTest, TestReadAsyncCacheMiss) {
    // Cache range [0, 5) but read from offset 20 which is not cached
    auto cache = CreateCache({{0, 5}});
    auto underlying = OpenFile();
    CacheInputStream stream(std::move(underlying), cache);

    std::string buffer(4, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 4, /*offset=*/20, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(buffer, "uvwx");
}

// Test ReadAsync when cache_->Read() returns error status.
// Uses IOHook to inject IO error during ReadAheadCache's prefetch, causing cache_->Read() to fail.
TEST_F(CacheInputStreamTest, TestReadAsyncCacheReadError) {
    auto io_hook = paimon::IOHook::GetInstance();
    bool error_triggered = false;

    for (size_t i = 0; i < 20; i++) {
        // Open the cache stream and underlying stream BEFORE activating IOHook
        ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", file_path_, {}));
        ASSERT_OK_AND_ASSIGN(auto cache_stream, fs->Open(file_path_));
        ASSERT_OK_AND_ASSIGN(auto underlying, fs->Open(file_path_));
        CacheConfig config;
        config.SetRangeSizeLimit(1024);
        config.SetHoleSizeLimit(0);
        config.SetPreBufferLimit(1024 * 1024);
        auto cache = std::make_shared<ReadAheadCache>(std::move(cache_stream), config,
                                                      /*file_size=*/0, pool_);
        ASSERT_OK(cache->Init(std::vector<ByteRange>{{0, 10}}));

        // Now activate IOHook so that the prefetch IO (triggered by cache_->Read -> PreBuffer)
        // will fail at the i-th IO operation
        paimon::ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, paimon::IOHook::Mode::RETURN_ERROR);
        CacheInputStream stream(std::move(underlying), cache);

        std::string buffer(5, '\0');
        bool callback_called = false;
        Status callback_status = Status::OK();
        stream.ReadAsync(buffer.data(), 5, /*offset=*/0, [&](Status status) {
            callback_called = true;
            callback_status = status;
        });
        ASSERT_TRUE(callback_called);
        CHECK_HOOK_STATUS(callback_status, i);
        ASSERT_EQ(buffer, "abcde");
        error_triggered = true;
        break;
    }
    ASSERT_TRUE(error_triggered);
}

// A read no registered range covers falls to the block cache, whose fetch of a
// cold block must not block the asynchronous caller: ReadAsync() returns while
// the fetch is in flight and the callback runs when the fetch does.
TEST_F(CacheInputStreamTest, TestReadAsyncAwaitsTheBlockCacheFetch) {
    auto cache_stream = std::make_shared<GatedAsyncInputStream>(OpenFile());
    auto cache = CreateGatedBlockCache(cache_stream);
    // The stream of the reader is gated too, so that a test can tell its own
    // fallback read apart from the fetch the block cache issues.
    auto underlying_gated = std::make_unique<GatedAsyncInputStream>(OpenFile());
    GatedAsyncInputStream* underlying = underlying_gated.get();
    CacheInputStream stream(std::move(underlying_gated), cache);

    // Block 0 = [28, 36) of the 36-byte test file, which no range covers.
    std::string buffer(8, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 8, /*offset=*/28, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });

    // The block fetch is held, the reader was not blocked on it and issued no
    // fallback read of its own.
    ASSERT_FALSE(callback_called);
    ASSERT_EQ(cache_stream->AsyncReadCount(), 1);
    ASSERT_EQ(underlying->AsyncReadCount(), 0);
    ASSERT_EQ(std::string(8, '\0'), buffer);

    cache_stream->ReleaseAll();
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(content_.substr(28, 8), buffer);
    // The block cache served the read, so the stream of the reader was never
    // touched: no second read of bytes that are cached now.
    ASSERT_EQ(underlying->AsyncReadCount(), 0);
}

// A block fetch reads more than the caller asked for, so its failure stays a
// decline: the reader falls back to its own stream instead of reporting it.
TEST_F(CacheInputStreamTest, TestReadAsyncFallsBackWhenTheBlockFetchFails) {
    auto cache_stream = std::make_shared<GatedAsyncInputStream>(OpenFile());
    auto cache = CreateGatedBlockCache(cache_stream);
    auto underlying_gated = std::make_unique<GatedAsyncInputStream>(OpenFile());
    GatedAsyncInputStream* underlying = underlying_gated.get();
    CacheInputStream stream(std::move(underlying_gated), cache);

    std::string buffer(8, '\0');
    bool callback_called = false;
    Status callback_status = Status::Invalid("not called");
    stream.ReadAsync(buffer.data(), 8, /*offset=*/28, [&](Status status) {
        callback_called = true;
        callback_status = status;
    });
    ASSERT_FALSE(callback_called);
    cache_stream->FailAll(Status::IOError("fetch failed"));

    // The declined read fell back to the stream of the reader, whose own read is
    // held: the failure of the block fetch was not reported to the caller.
    ASSERT_FALSE(callback_called);
    ASSERT_EQ(underlying->AsyncReadCount(), 1);

    underlying->ReleaseAll();
    ASSERT_TRUE(callback_called);
    ASSERT_OK(callback_status);
    ASSERT_EQ(content_.substr(28, 8), buffer);
}

}  // namespace paimon::test
