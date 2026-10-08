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

#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/meta_to_arrow_array_converter.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/table/source/scan_metrics.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/row_range_index.h"

namespace paimon::test {
class RowRangeManifestFileTest : public ::testing::Test {
 protected:
    Result<std::unique_ptr<ManifestFile>> CreateManifest(
        const std::string& path, const std::shared_ptr<FileSystem>& fs, bool cache_enabled,
        const std::shared_ptr<Cache>& cache = nullptr,
        const std::shared_ptr<MemoryPool>& pool = GetDefaultPool()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileFormat> format,
                               FileFormatFactory::Get("avro", {}));
        auto schema = arrow::schema(arrow::FieldVector{});
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                               FileStorePathFactory::Create(path, schema, {}, "", "avro", "data-",
                                                            true, {}, std::nullopt, false, pool));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                               CoreOptions::FromMap({{Options::READ_BATCH_SIZE, "2"}}));
        if (cache) {
            options.WithCache(cache);
        } else if (cache_enabled) {
            options.WithCache(std::make_shared<LruCache>(64 * 1024 * 1024));
        }
        PAIMON_RETURN_NOT_OK(fs->Mkdirs(FileStorePathFactory::ManifestPath(path)));
        return ManifestFile::Create(fs, format, "null", path_factory, 1024, pool, options, schema);
    }

    Result<ManifestEntry> Entry(const std::string& name, std::optional<int64_t> first,
                                int64_t count, const FileKind& kind = FileKind::Add()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> meta,
                               DataFileMeta::ForAppend(name, 100, count, SimpleStats::EmptyStats(),
                                                       0, 0, 0, FileSource::Append(), std::nullopt,
                                                       std::nullopt, first, std::nullopt));
        return ManifestEntry(kind, BinaryRow::EmptyRow(), 0, 1, meta);
    }

    Status WriteArray(const std::string& path, const std::string& name,
                      const std::shared_ptr<arrow::StructArray>& array) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileFormat> format,
                               FileFormatFactory::Get("avro", {}));
        ArrowSchema schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*array->type(), &schema));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<WriterBuilder> builder,
                               format->CreateWriterBuilder(&schema, 2));
        LocalFileSystem fs;
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<OutputStream> output,
            fs.Create(FileStorePathFactory::ManifestPath(path) + "/" + name, false));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                               builder->Build(output, "null"));
        ArrowArray batch;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &batch));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&batch));
        PAIMON_RETURN_NOT_OK(writer->Flush());
        PAIMON_RETURN_NOT_OK(writer->Finish());
        return output->Close();
    }
};

TEST_F(RowRangeManifestFileTest, ArrowCacheReuseEvictionAndConcurrentReaders) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const auto cache_kind = CacheKind::MANIFEST;
    auto cache = std::make_shared<CountingRoutingCache>(cache_kind, 1024 * 1024);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
    ASSERT_OK_AND_ASSIGN(ManifestEntry a, Entry("a.parquet", 100, 10));
    ASSERT_OK_AND_ASSIGN(ManifestEntry b, Entry("b.parquet", 110, 10));
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling({a, b}));
    ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(110, 110)}));
    std::vector<ManifestEntry> expected{b};
    auto read = [&]() -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ManifestFile> reader,
                               CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
        std::vector<ManifestEntry> entries;
        PAIMON_RETURN_NOT_OK(
            reader->ReadRowRangeEntries(written.first, ranges, nullptr, written.second, &entries));
        if (entries != expected) {
            return Status::Invalid("cached manifest result differs from source");
        }
        return Status::OK();
    };
    ASSERT_OK(read());
    // The manifest has one decoded entry; no raw-byte copy is retained.
    ASSERT_EQ(1, cache->SupplierCallCount(cache_kind));
    // Readers own their decode state; the cache only shares immutable bytes.
    std::vector<std::future<Status>> readers;
    for (int32_t i = 0; i < 8; ++i) {
        readers.push_back(std::async(std::launch::async, read));
    }
    for (auto& reader : readers) {
        ASSERT_OK(reader.get());
    }
    ASSERT_EQ(1, cache->SupplierCallCount(cache_kind));
    cache->InvalidateAll();
    ASSERT_OK(read());
    ASSERT_EQ(2, cache->SupplierCallCount(cache_kind));
    // A different immutable manifest must not reuse the previous file's cached content.
    ASSERT_OK_AND_ASSIGN(WrittenFile next, manifest->WriteWithoutRolling({a}));
    std::vector<ManifestEntry> entries;
    ASSERT_OK(manifest->ReadRowRangeEntries(next.first, ranges, nullptr, next.second, &entries));
    ASSERT_TRUE(entries.empty());
    ASSERT_EQ(3, cache->SupplierCallCount(cache_kind));

    auto unsupported_cache =
        std::make_shared<CountingRoutingCache>(CacheKind::DATA_FILE_FOOTER, 1024 * 1024);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> unsupported,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true, unsupported_cache));
    ASSERT_OK(
        unsupported->ReadRowRangeEntries(written.first, ranges, nullptr, written.second, &entries));
    ASSERT_EQ(expected, entries);
    entries.clear();
    auto small_cache = std::make_shared<LruCache>(1);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> uncached,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true, small_cache));
    ASSERT_OK(
        uncached->ReadRowRangeEntries(written.first, ranges, nullptr, written.second, &entries));
    ASSERT_EQ(expected, entries);
    ASSERT_LE(small_cache->GetCurrentWeight(), 1);
    small_cache->InvalidateAll();
    uncached.reset();
    // Materialized entries must remain valid after both cache eviction and reader destruction.
    ASSERT_EQ(expected, entries);
}

TEST_F(RowRangeManifestFileTest, OrcCachePreservesManifestMetadata) {
    auto pool = GetDefaultPool();
    auto fs = std::make_shared<LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> format, FileFormatFactory::Get("orc", {}));
    auto schema = arrow::schema(arrow::FieldVector{});
    const std::string path = GetDataDir() + "/orc/append_09.db/append_09";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> paths,
                         FileStorePathFactory::Create(path, schema, {}, "", "orc", "data-", true,
                                                      {}, std::nullopt, false, pool));
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::READ_BATCH_SIZE, "2"}}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ManifestFile> uncached,
        ManifestFile::Create(fs, format, "zstd", paths, 1024, pool, options, schema));
    const std::string name = "manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1";
    std::vector<ManifestEntry> expected;
    ASSERT_OK(uncached->Read(name, nullptr, std::nullopt, &expected));
    ASSERT_EQ(5, expected.size());
    ASSERT_EQ(1721643142456LL, expected[0].File()->creation_time.GetMillisecond());
    // This legacy manifest has unknown row IDs, so every entry must be retained.
    ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(0, 0)}));
    std::vector<ManifestEntry> without_cache;
    ASSERT_OK(uncached->ReadRowRangeEntries(name, ranges, nullptr, std::nullopt, &without_cache));
    ASSERT_EQ(expected, without_cache);

    auto cache = std::make_shared<CountingRoutingCache>(CacheKind::MANIFEST, 1024 * 1024);
    options.WithCache(cache);
    for (int32_t attempt = 0; attempt < 2; ++attempt) {
        SCOPED_TRACE(attempt);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ManifestFile> reader,
            ManifestFile::Create(fs, format, "zstd", paths, 1024, pool, options, schema));
        std::vector<ManifestEntry> actual;
        ASSERT_OK(reader->ReadRowRangeEntries(name, ranges, nullptr, std::nullopt, &actual));
        ASSERT_EQ(expected, actual);
        ASSERT_EQ(1, cache->SupplierCallCount(CacheKind::MANIFEST));
        ASSERT_OK_AND_ASSIGN(uint64_t count,
                             reader->GetReadMetrics()->GetCounter(
                                 attempt == 0 ? ScanMetrics::MANIFEST_ARROW_CACHE_MISSES
                                              : ScanMetrics::MANIFEST_ARROW_CACHE_HITS));
        ASSERT_EQ(1, count);
    }
}

TEST_F(RowRangeManifestFileTest, EmptyManifestCacheReuse) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto cache = std::make_shared<CountingRoutingCache>(CacheKind::MANIFEST, 1024 * 1024);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling({}));
    ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(0, 0)}));
    for (int32_t attempt = 0; attempt < 2; ++attempt) {
        std::vector<ManifestEntry> entries;
        ASSERT_OK(manifest->ReadRowRangeEntries(written.first, ranges, nullptr, written.second,
                                                &entries));
        ASSERT_TRUE(entries.empty());
        ASSERT_EQ(1, cache->SupplierCallCount(CacheKind::MANIFEST));
    }
}

TEST_F(RowRangeManifestFileTest, AllReadPathsShareOneDecodedEntry) {
    for (int32_t first_reader : {0, 1, 2}) {
        SCOPED_TRACE(first_reader);
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        auto cache = std::make_shared<CountingRoutingCache>(CacheKind::MANIFEST, 1024 * 1024);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                             CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
        ASSERT_OK_AND_ASSIGN(ManifestEntry a, Entry("a.parquet", 100, 10));
        ASSERT_OK_AND_ASSIGN(ManifestEntry b, Entry("b.parquet", 110, 10));
        ASSERT_OK_AND_ASSIGN(ManifestEntry c, Entry("c.parquet", 120, 10));
        std::vector<ManifestEntry> source = {a, b, c};
        using WrittenFile = std::pair<std::string, int64_t>;
        ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling(source));
        ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(110, 110)}));
        auto read = [&](int32_t mode) {
            std::vector<ManifestEntry> actual;
            if (mode == 0) {
                ASSERT_OK(manifest->Read(written.first, nullptr, written.second, &actual));
                ASSERT_EQ(source, actual);
            } else if (mode == 1) {
                ASSERT_OK(manifest->ReadBucketEntries(written.first, 0, std::nullopt,
                                                      written.second, &actual));
                ASSERT_EQ(source, actual);
            } else {
                ASSERT_OK(manifest->ReadRowRangeEntries(written.first, ranges, nullptr,
                                                        written.second, &actual));
                ASSERT_EQ(std::vector<ManifestEntry>{b}, actual);
            }
        };
        read(first_reader);
        ASSERT_EQ(1, cache->Size());
        ASSERT_EQ(1, cache->SupplierCallCount());
        auto key = CacheKey::ForKind(
            PathUtil::JoinPath(FileStorePathFactory::ManifestPath(dir->Str()), written.first),
            /*position=*/0, /*length=*/-1, CacheKind::MANIFEST);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<CacheValue> cached,
            cache->Get(
                key, [](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
                    return Status::Invalid("manifest is missing under the existing cache key");
                }));
        ASSERT_TRUE(cached);
        // All later paths must work entirely from the same decoded cache entry.
        manifest->DeleteQuietly(written.first);
        for (int32_t mode : {0, 1, 2}) {
            read(mode);
        }
        ASSERT_EQ(1, cache->Size());
        ASSERT_EQ(1, cache->SupplierCallCount());
        // Existing callers can still invalidate a manifest by its whole-file key.
        cache->Invalidate(key);
        ASSERT_EQ(0, cache->Size());
    }
}

TEST_F(RowRangeManifestFileTest, ColdFilterFailureDoesNotPoisonCacheOrRepeatCallbacks) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto cache = std::make_shared<CountingRoutingCache>(CacheKind::MANIFEST, 1024 * 1024);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
    ASSERT_OK_AND_ASSIGN(ManifestEntry a, Entry("a.parquet", 100, 10));
    ASSERT_OK_AND_ASSIGN(ManifestEntry b, Entry("b.parquet", 110, 10));
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling({a, b}));
    int32_t calls = 0;
    auto filter = [&](const ManifestEntry&) -> Result<bool> {
        ++calls;
        return Status::IOError("consumer failed");
    };
    std::vector<ManifestEntry> entries;
    ASSERT_NOK_WITH_MSG(manifest->Read(written.first, filter, written.second, &entries),
                        "consumer failed");
    ASSERT_EQ(1, calls);
    ASSERT_TRUE(entries.empty());
    manifest->DeleteQuietly(written.first);
    ASSERT_OK(manifest->Read(written.first, nullptr, written.second, &entries));
    ASSERT_EQ(std::vector<ManifestEntry>({a, b}), entries);
    ASSERT_EQ(1, cache->SupplierCallCount());
}

TEST_F(RowRangeManifestFileTest, CachedBufferRetainsAllocatorUntilEviction) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto cache = std::make_shared<CountingRoutingCache>(CacheKind::MANIFEST, 1024 * 1024);
    std::weak_ptr<MemoryPool> weak_pool;
    ASSERT_OK_AND_ASSIGN(ManifestEntry entry, Entry("a.parquet", 100, 10));
    ASSERT_OK_AND_ASSIGN(ManifestEntry next, Entry("b.parquet", 110, 10));
    ASSERT_OK_AND_ASSIGN(ManifestEntry last, Entry("c.parquet", 120, 10));
    const std::vector<ManifestEntry> expected = {entry, next, last};
    using WrittenFile = std::pair<std::string, int64_t>;
    WrittenFile written;
    {
        std::shared_ptr<MemoryPool> pool = GetMemoryPool();
        weak_pool = pool;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                             CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache, pool));
        ASSERT_OK_AND_ASSIGN(written, manifest->WriteWithoutRolling(expected));
        std::vector<ManifestEntry> entries;
        ASSERT_OK(manifest->Read(written.first, nullptr, written.second, &entries));
    }
    ASSERT_FALSE(weak_pool.expired());
    std::vector<ManifestEntry> entries;
    {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> reader,
                             CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
        reader->DeleteQuietly(written.first);
        auto filter = [&](const ManifestEntry&) -> Result<bool> {
            cache->InvalidateAll();
            EXPECT_FALSE(weak_pool.expired());
            return true;
        };
        // Eviction during the first batch must not invalidate later IPC batches.
        ASSERT_OK(reader->Read(written.first, filter, written.second, &entries));
    }
    ASSERT_TRUE(weak_pool.expired());
    ASSERT_EQ(expected, entries);
}

TEST_F(RowRangeManifestFileTest, CacheAdmissionFailureReusesAlreadyDecodedBatches) {
    class FailingCache : public CountingRoutingCache {
     public:
        FailingCache() : CountingRoutingCache(CacheKind::MANIFEST, 1024 * 1024) {}

        Result<std::shared_ptr<CacheValue>> Get(
            const std::shared_ptr<CacheKey>& key,
            std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
                supplier) override {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CacheValue> value, supplier(key));
            after_load();
            return Status::IOError("cache admission failed");
        }

        std::function<void()> after_load;
    };
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto cache = std::make_shared<FailingCache>();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
    ASSERT_OK_AND_ASSIGN(ManifestEntry entry, Entry("a.parquet", 100, 10));
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling({entry}));
    // Reopening the source after an optional cache failure would now fail.
    cache->after_load = [&]() { manifest->DeleteQuietly(written.first); };
    int32_t calls = 0;
    auto filter = [&](const ManifestEntry&) -> Result<bool> {
        ++calls;
        return true;
    };
    std::vector<ManifestEntry> entries;
    ASSERT_OK(manifest->Read(written.first, filter, written.second, &entries));
    ASSERT_EQ(1, calls);
    ASSERT_EQ(std::vector<ManifestEntry>{entry}, entries);
    ASSERT_OK_AND_ASSIGN(uint64_t fallbacks, manifest->GetReadMetrics()->GetCounter(
                                                 ScanMetrics::MANIFEST_ARROW_CACHE_FALLBACKS));
    ASSERT_EQ(1, fallbacks);
}

class BlockingManifestCache : public CountingRoutingCache {
 public:
    BlockingManifestCache() : CountingRoutingCache(CacheKind::MANIFEST, 1024 * 1024) {}

    Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) override {
        return CountingRoutingCache::Get(key, [&](const std::shared_ptr<CacheKey>& supplier_key) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                ++loads_;
                cv_.notify_all();
                cv_.wait(lock, [&]() { return released_; });
            }
            return supplier(supplier_key);
        });
    }

    bool WaitForLoads(int32_t count) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(10), [&]() { return loads_ >= count; });
    }

    void Release() {
        std::lock_guard<std::mutex> lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

 private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int32_t loads_ = 0;
    bool released_ = false;
};

TEST_F(RowRangeManifestFileTest, ConcurrentColdReadersShareLoadAcrossInstances) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto cache = std::make_shared<BlockingManifestCache>();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
    ASSERT_OK_AND_ASSIGN(ManifestEntry entry, Entry("a.parquet", 100, 10));
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling({entry}));
    ASSERT_OK_AND_ASSIGN(WrittenFile other, manifest->WriteWithoutRolling({entry}));
    std::vector<std::unique_ptr<ManifestFile>> instances;
    for (int32_t i = 0; i < 9; ++i) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> reader,
                             CreateManifest(dir->Str(), dir->GetFileSystem(), true, cache));
        instances.push_back(std::move(reader));
    }
    std::vector<std::future<Status>> readers;
    // Release blocked suppliers before future destructors wait, including on assertion failure.
    ScopeGuard release([&]() { cache->Release(); });
    std::promise<void> start;
    auto gate = start.get_future().share();
    for (int32_t i = 0; i < 9; ++i) {
        readers.push_back(std::async(std::launch::async, [&, i]() -> Status {
            gate.wait();
            const auto& file = i == 8 ? other : written;
            std::vector<ManifestEntry> entries;
            PAIMON_RETURN_NOT_OK(instances[i]->Read(file.first, nullptr, file.second, &entries));
            if (entries != std::vector<ManifestEntry>{entry}) {
                return Status::Invalid("concurrent manifest result differs");
            }
            return Status::OK();
        }));
    }
    start.set_value();
    // An unrelated manifest must load while the first one is blocked.
    ASSERT_TRUE(cache->WaitForLoads(2));
    cache->Release();
    for (auto& reader : readers) {
        ASSERT_OK(reader.get());
    }
    ASSERT_EQ(2, cache->SupplierCallCount());
    ASSERT_EQ(2, cache->Size());
}

TEST_F(RowRangeManifestFileTest, MetricsCountPruningCacheReuseAndFilterErrors) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true));
    ASSERT_OK_AND_ASSIGN(ManifestEntry a, Entry("a.parquet", 100, 10));
    ASSERT_OK_AND_ASSIGN(ManifestEntry b, Entry("b.parquet", 110, 10));
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling({a, b}));
    ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(110, 110)}));
    for (int32_t i = 0; i < 2; ++i) {
        std::vector<ManifestEntry> entries;
        ASSERT_OK(manifest->ReadRowRangeEntries(written.first, ranges, nullptr, written.second,
                                                &entries));
        ASSERT_EQ(std::vector<ManifestEntry>{b}, entries);
    }
    auto metrics = manifest->GetReadMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t scanned,
                         metrics->GetCounter(ScanMetrics::ROW_RANGE_MANIFEST_ENTRIES_SCANNED));
    ASSERT_OK_AND_ASSIGN(uint64_t pruned,
                         metrics->GetCounter(ScanMetrics::ROW_RANGE_MANIFEST_ENTRIES_PRUNED));
    ASSERT_OK_AND_ASSIGN(uint64_t materialized,
                         metrics->GetCounter(ScanMetrics::ROW_RANGE_MANIFEST_ENTRIES_MATERIALIZED));
    ASSERT_EQ(4, scanned);
    ASSERT_EQ(2, pruned);
    ASSERT_EQ(2, materialized);
    ASSERT_OK_AND_ASSIGN(uint64_t hits,
                         metrics->GetCounter(ScanMetrics::MANIFEST_ARROW_CACHE_HITS));
    ASSERT_OK_AND_ASSIGN(uint64_t misses,
                         metrics->GetCounter(ScanMetrics::MANIFEST_ARROW_CACHE_MISSES));
    ASSERT_EQ(1, hits);
    ASSERT_EQ(1, misses);
    ASSERT_OK_AND_ASSIGN(HistogramStats stats,
                         metrics->GetHistogramStats(ScanMetrics::ROW_RANGE_MANIFEST_READ_DURATION));
    ASSERT_EQ(2, stats.count);
    std::vector<ManifestEntry> entries;
    ASSERT_NOK(manifest->ReadRowRangeEntries(
        written.first, ranges,
        [](const ManifestEntry&) -> Result<bool> { return Status::IOError("filter failure"); },
        written.second, &entries));
    ASSERT_OK_AND_ASSIGN(HistogramStats after_error,
                         manifest->GetReadMetrics()->GetHistogramStats(
                             ScanMetrics::ROW_RANGE_MANIFEST_READ_DURATION));
    ASSERT_EQ(3, after_error.count);
    // Previously returned snapshots remain unchanged after subsequent reads.
    ASSERT_OK_AND_ASSIGN(HistogramStats old_snapshot,
                         metrics->GetHistogramStats(ScanMetrics::ROW_RANGE_MANIFEST_READ_DURATION));
    ASSERT_EQ(2, old_snapshot.count);
}

TEST_F(RowRangeManifestFileTest, BoundariesUnknownRangesAndDeleteMerging) {
    for (bool cache_enabled : {false, true}) {
        SCOPED_TRACE(cache_enabled);
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                             CreateManifest(dir->Str(), dir->GetFileSystem(), cache_enabled));
        ASSERT_OK_AND_ASSIGN(ManifestEntry a, Entry("a.parquet", 100, 10));
        ASSERT_OK_AND_ASSIGN(ManifestEntry b, Entry("b.parquet", 110, 10));
        ASSERT_OK_AND_ASSIGN(ManifestEntry unknown, Entry("unknown.parquet", std::nullopt, 10));
        ManifestEntry deleted(FileKind::Delete(), a.Partition(), a.Bucket(), a.TotalBuckets(),
                              a.File());
        std::vector<ManifestEntry> source = {a, b, deleted, unknown};
        using WrittenFile = std::pair<std::string, int64_t>;
        ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling(source));
        const std::vector<std::vector<Range>> queries = {{Range(100, 100)},
                                                         {Range(109, 109)},
                                                         {Range(110, 110)},
                                                         {Range(119, 119)},
                                                         {Range(99, 99)},
                                                         {Range(120, 120)},
                                                         {Range(109, 110)},
                                                         {Range(100, 100), Range(119, 119)},
                                                         {}};
        for (const auto& query : queries) {
            ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create(query));
            auto filter = [&ranges](const ManifestEntry& entry) -> Result<bool> {
                const auto& meta = entry.File();
                return !meta->first_row_id ||
                       ranges.Intersects(*meta->first_row_id,
                                         *meta->first_row_id + meta->row_count - 1);
            };
            std::vector<ManifestEntry> ordinary;
            ASSERT_OK(manifest->Read(written.first, filter, written.second, &ordinary));
            std::vector<ManifestEntry> selected;
            ASSERT_OK(manifest->ReadRowRangeEntries(written.first, ranges, filter, written.second,
                                                    &selected));
            ASSERT_EQ(ordinary, selected);
            std::vector<ManifestEntry> ordinary_live;
            std::vector<ManifestEntry> selected_live;
            ASSERT_OK(FileStoreScan::MergeLiveEntries(ordinary, &ordinary_live));
            ASSERT_OK(FileStoreScan::MergeLiveEntries(selected, &selected_live));
            ASSERT_EQ(ordinary_live, selected_live);
            for (const auto& entry : selected_live) {
                ASSERT_NE("a.parquet", entry.File()->file_name);
            }
        }
        ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(100, 100)}));
        std::vector<ManifestEntry> entries;
        ASSERT_NOK_WITH_MSG(manifest->ReadRowRangeEntries(
                                written.first, ranges,
                                [](const ManifestEntry&) -> Result<bool> {
                                    return Status::IOError("filter error");
                                },
                                written.second, &entries),
                            "filter error");
        ASSERT_NOK(manifest->ReadRowRangeEntries("missing-manifest", ranges, nullptr, std::nullopt,
                                                 &entries));
    }
}

TEST_F(RowRangeManifestFileTest, UncertainAndOverflowingRangesAreRetained) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), false));
    std::vector<ManifestEntry> source;
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    for (const auto& range : std::vector<std::pair<int64_t, int64_t>>{
             {kMax, 2}, {kMax, 1}, {0, 0}, {-1, 10}, {0, -1}}) {
        ASSERT_OK_AND_ASSIGN(ManifestEntry entry,
                             Entry(fmt::format("file-{}-{}.parquet", range.first, range.second),
                                   range.first, range.second));
        source.push_back(entry);
    }
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written, manifest->WriteWithoutRolling(source));
    ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(0, 0)}));
    std::vector<ManifestEntry> actual;
    ASSERT_OK(
        manifest->ReadRowRangeEntries(written.first, ranges, nullptr, written.second, &actual));
    source.erase(source.begin() + 1);
    ASSERT_EQ(source, actual);
}

TEST_F(RowRangeManifestFileTest, SchemaEvolutionAndVersionValidation) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ManifestFile> manifest,
                         CreateManifest(dir->Str(), dir->GetFileSystem(), true));
    ASSERT_OK_AND_ASSIGN(ManifestEntry entry, Entry("a.parquet", 100, 10));
    ManifestEntrySerializer serializer(pool);
    ASSERT_OK_AND_ASSIGN(BinaryRow row, serializer.ToRow(entry));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MetaToArrowArrayConverter> converter,
                         MetaToArrowArrayConverter::Create(serializer.GetDataType(), pool));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, converter->NextBatch({row}));
    auto batch = checked_pointer_cast<arrow::StructArray>(array);
    ASSERT_OK_AND_ASSIGN(RowRangeIndex ranges, RowRangeIndex::Create({Range(1000, 1000)}));
    for (int32_t mode : {0, 1, 2, 3}) {
        SCOPED_TRACE(mode);
        auto fields = serializer.GetDataType()->fields();
        auto columns = batch->fields();
        auto file = checked_pointer_cast<arrow::StructArray>(columns[5]);
        auto file_fields = checked_pointer_cast<arrow::StructType>(file->type())->fields();
        auto file_columns = file->fields();
        if (mode == 0) {
            file_fields.erase(file_fields.begin() + 18);
            file_columns.erase(file_columns.begin() + 18);
        } else if (mode == 1) {
            std::swap(file_fields[2], file_fields[18]);
            std::swap(file_columns[2], file_columns[18]);
        } else {
            arrow::Int32Builder versions;
            ASSERT_TRUE(versions.Append(mode == 2 ? 1 : 999).ok());
            ASSERT_TRUE(versions.Finish(&columns[0]).ok());
        }
        auto updated_file = arrow::StructArray::Make(file_columns, file_fields);
        ASSERT_TRUE(updated_file.ok()) << updated_file.status().ToString();
        columns[5] = updated_file.ValueOrDie();
        fields[5] = fields[5]->WithType(arrow::struct_(file_fields));
        std::swap(fields[1], fields[5]);
        std::swap(columns[1], columns[5]);
        auto evolved_result = arrow::StructArray::Make(columns, fields);
        ASSERT_TRUE(evolved_result.ok()) << evolved_result.status().ToString();
        std::shared_ptr<arrow::StructArray> evolved = evolved_result.ValueOrDie();
        const std::string name = fmt::format("manifest-evolved-{}", mode);
        ASSERT_OK(WriteArray(dir->Str(), name, evolved));
        std::vector<ManifestEntry> actual;
        if (mode >= 2) {
            ASSERT_NOK_WITH_MSG(
                manifest->ReadRowRangeEntries(name, ranges, nullptr, std::nullopt, &actual),
                (mode == 2 ? "not compatible" : "Unsupported version: 999"));
        } else {
            ASSERT_OK(manifest->ReadRowRangeEntries(name, ranges, nullptr, std::nullopt, &actual));
            ASSERT_EQ(mode == 0 ? 1 : 0, actual.size());
            if (mode == 0) {
                ASSERT_FALSE(actual[0].File()->first_row_id.has_value());
            }
        }
    }
}

}  // namespace paimon::test
