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

#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/checked_cast.h"
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
        const std::shared_ptr<Cache>& cache = nullptr) {
        auto pool = GetDefaultPool();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileFormat> format,
                               FileFormatFactory::Get("avro", {}));
        auto schema = arrow::schema(arrow::FieldVector{});
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                               FileStorePathFactory::Create(path, schema, {}, "", "avro", "data-",
                                                            true, {}, std::nullopt, false, pool));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap({}));
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
    // One raw entry and one IPC entry use distinct keys in the same budget.
    ASSERT_EQ(2, cache->SupplierCallCount(cache_kind));
    // Readers own their decode state; the cache only shares immutable bytes.
    std::vector<std::future<Status>> readers;
    for (int32_t i = 0; i < 8; ++i) {
        readers.push_back(std::async(std::launch::async, read));
    }
    for (auto& reader : readers) {
        ASSERT_OK(reader.get());
    }
    ASSERT_EQ(2, cache->SupplierCallCount(cache_kind));
    cache->InvalidateAll();
    ASSERT_OK(read());
    ASSERT_EQ(4, cache->SupplierCallCount(cache_kind));
    // A different immutable manifest must not reuse the previous file's cached content.
    ASSERT_OK_AND_ASSIGN(WrittenFile next, manifest->WriteWithoutRolling({a}));
    std::vector<ManifestEntry> entries;
    ASSERT_OK(manifest->ReadRowRangeEntries(next.first, ranges, nullptr, next.second, &entries));
    ASSERT_TRUE(entries.empty());
    ASSERT_EQ(6, cache->SupplierCallCount(cache_kind));

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
    cache->InvalidateAll();
    manifest.reset();
    // Materialized entries must remain valid after both cache eviction and reader destruction.
    ASSERT_EQ(expected, entries);
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
