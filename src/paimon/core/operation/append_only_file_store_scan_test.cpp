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

#include "paimon/core/operation/append_only_file_store_scan.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/cache/cache.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/serializer/binary_row_serializer.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/math.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/io/meta_to_arrow_array_converter.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/partition_entry.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/scan_metrics.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_format_reader_builder.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"
namespace paimon::test {

class AppendBucketPruningTest : public testing::Test {
 public:
    Result<std::unique_ptr<AppendOnlyFileStoreScan>> CreateScan(
        const std::shared_ptr<Predicate>& predicate,
        const std::optional<int32_t>& bucket = std::nullopt) const {
        std::vector<DataField> fields = {DataField(0, arrow::field("rowkey", rowkey_type_)),
                                         DataField(1, arrow::field("value", arrow::int32()))};
        if (extra_field_) {
            fields.emplace_back(2, arrow::field("extra", arrow::int32()));
        }
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                               TableSchema::Create(schema_id_, arrow_schema, {}, {}, options_));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_));
        auto filters = std::make_shared<ScanFilter>(
            predicate, std::vector<std::map<std::string, std::string>>(), bucket);
        return AppendOnlyFileStoreScan::Create(nullptr, schema_manager_, nullptr, nullptr, schema,
                                               arrow_schema, filters, options,
                                               CreateDefaultExecutor(), pool_);
    }

    void CheckBuckets(const std::shared_ptr<Predicate>& predicate,
                      const std::optional<int32_t>& expected_bucket,
                      const std::optional<int32_t>& explicit_bucket = std::nullopt,
                      int32_t total_buckets = kNumBuckets) const {
        ASSERT_OK_AND_ASSIGN(auto scan, CreateScan(predicate, explicit_bucket));
        // Every file's stats match the lookup, so only bucket pruning can discard a file.
        SimpleStats stats = BinaryRowGenerator::GenerateStats(
            {std::string("a"), 0}, {std::string("z"), 100}, {0, 0}, pool_.get());
        ASSERT_OK_AND_ASSIGN(
            auto file,
            DataFileMeta::ForAppend("data.parquet", 100, 10, stats, 0, 9, 0, std::nullopt,
                                    std::nullopt, std::nullopt, std::nullopt, std::nullopt));
        for (int32_t bucket = 0; bucket < total_buckets; ++bucket) {
            ManifestEntry entry(FileKind::Add(), BinaryRow::EmptyRow(), bucket, total_buckets,
                                file);
            ASSERT_OK_AND_ASSIGN(auto stats_scan, CreateScan(predicate, bucket));
            ASSERT_OK_AND_ASSIGN(bool stats_match, stats_scan->FilterByStats(entry));
            ASSERT_TRUE(stats_match);
            ASSERT_OK_AND_ASSIGN(bool keep, scan->FilterManifestEntry(entry));
            ASSERT_EQ(keep, !expected_bucket.has_value() || bucket == expected_bucket.value());
        }
    }

    std::shared_ptr<Predicate> KeyEquals() const {
        return PredicateBuilder::Equal(0, "rowkey", FieldType::STRING,
                                       Literal(FieldType::STRING, "key", 3));
    }

    template <typename T>
    void CheckMatchingValue(FieldType field_type, const T& query_value, const T& stored_value) {
        options_[Options::BUCKET] = "17";
        auto predicate = PredicateBuilder::Equal(0, "rowkey", field_type, Literal(query_value));
        ASSERT_OK_AND_ASSIGN(auto comparison,
                             Literal(query_value).CompareTo(Literal(stored_value)));
        ASSERT_EQ(comparison, 0);
        BinaryRow stored_key = BinaryRowGenerator::GenerateRow({stored_value}, pool_.get());
        BinaryRow query_key = BinaryRowGenerator::GenerateRow({query_value}, pool_.get());
        int32_t bucket = DefaultBucketFunction().Bucket(stored_key, 17);
        ASSERT_NE(bucket, DefaultBucketFunction().Bucket(query_key, 17));
        SimpleStats stats = BinaryRowGenerator::GenerateStats(
            {stored_value, 0}, {stored_value, 100}, {0, 0}, pool_.get());
        ASSERT_OK_AND_ASSIGN(
            auto file,
            DataFileMeta::ForAppend("data.parquet", 100, 10, stats, 0, 9, 0, std::nullopt,
                                    std::nullopt, std::nullopt, std::nullopt, std::nullopt));
        ManifestEntry entry(FileKind::Add(), BinaryRow::EmptyRow(), bucket, 17, file);
        ASSERT_OK_AND_ASSIGN(auto stats_scan, CreateScan(predicate, bucket));
        ASSERT_OK_AND_ASSIGN(bool stats_match, stats_scan->FilterByStats(entry));
        ASSERT_TRUE(stats_match);
        ASSERT_OK_AND_ASSIGN(auto scan, CreateScan(predicate));
        ASSERT_OK_AND_ASSIGN(bool keep, scan->FilterManifestEntry(entry));
        ASSERT_TRUE(keep);
    }

    std::shared_ptr<arrow::DataType> rowkey_type_ = arrow::utf8();
    static constexpr int32_t kNumBuckets = 4;
    int64_t schema_id_ = 0;
    bool extra_field_ = false;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::map<std::string, std::string> options_ = {{Options::BUCKET, "4"},
                                                   {Options::BUCKET_KEY, "rowkey"}};
};

namespace {

class CountingSchemaFileSystem : public LocalFileSystem {
 public:
    Status ReadFile(const std::string& path, std::string* content) override {
        ++read_count;
        return LocalFileSystem::ReadFile(path, content);
    }

    int32_t read_count = 0;
};

class CountingManifestEntrySerializer : public ManifestEntrySerializer {
 public:
    explicit CountingManifestEntrySerializer(const std::shared_ptr<MemoryPool>& pool)
        : ManifestEntrySerializer(pool) {}

    Result<ManifestEntry> ConvertFrom(int32_t version, const InternalRow& row) const override {
        ++decoded_entries;
        return ManifestEntrySerializer::ConvertFrom(version, row);
    }

    mutable std::atomic<int64_t> decoded_entries{0};
};

class CountingManifestReaderBuilder : public MockFormatReaderBuilder {
 public:
    explicit CountingManifestReaderBuilder(const std::shared_ptr<arrow::Array>& data)
        : MockFormatReaderBuilder(data, data->type(), 2), data_(data) {}

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& input) const override {
        return std::make_unique<Reader>(data_, &rows_read);
    }

    mutable std::atomic<int64_t> rows_read{0};

 private:
    class Reader : public MockFileBatchReader {
     public:
        Reader(const std::shared_ptr<arrow::Array>& data, std::atomic<int64_t>* rows_read)
            : MockFileBatchReader(data, data->type(), 2), rows_read_(rows_read) {
            EnableRandomizeBatchSize(false);
        }

        bool SupportPreciseBitmapSelection() const override {
            return true;
        }

        Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
            PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch,
                                   MockFileBatchReader::NextBatchWithBitmap());
            if (!BatchReader::IsEofBatch(batch)) {
                *rows_read_ += batch.first.first->length;
            }
            return batch;
        }

     private:
        std::atomic<int64_t>* rows_read_;
    };

    std::shared_ptr<arrow::Array> data_;
};

}  // namespace

TEST_F(AppendBucketPruningTest, ReadsSingleBucketManifestOnce) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    // The converter owns the Arrow memory pool used by the mock reader's shared data.
    ManifestEntrySerializer serializer(pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MetaToArrowArrayConverter> converter,
                         MetaToArrowArrayConverter::Create(serializer.GetDataType(), pool_));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> initial, CreateScan(KeyEquals()));
    const int32_t bucket = initial->bucket_selector_->Bucket(kNumBuckets);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> format, FileFormatFactory::Get("avro", {}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> paths,
        FileStorePathFactory::Create(dir->Str(), initial->schema_, {}, "", "avro", "data-", true,
                                     {}, std::nullopt, false, pool_));
    ASSERT_OK(fs->Mkdirs(FileStorePathFactory::ManifestPath(dir->Str())));
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ManifestFile> manifest,
                         ManifestFile::Create(fs, format, "null", paths, 1024 * 1024, pool_,
                                              options, arrow::schema({})));
    SimpleStats stats = BinaryRowGenerator::GenerateStats(
        {std::string("a"), 0}, {std::string("z"), 100}, {0, 0}, pool_.get());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> file,
        DataFileMeta::ForAppend("data.avro", 100, 10, stats, 0, 9, schema_id_, std::nullopt,
                                std::nullopt, std::nullopt, std::nullopt, std::nullopt));
    std::vector<ManifestEntry> entries = {
        ManifestEntry(FileKind::Add(), BinaryRow::EmptyRow(), bucket, kNumBuckets, file)};
    ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> metas, manifest->Write(entries));
    ASSERT_EQ(metas.size(), 1);
    ASSERT_OK_AND_ASSIGN(BinaryRow row, serializer.ToRow(entries[0]));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> data, converter->NextBatch({row}));
    auto reader_builder = std::make_shared<CountingManifestReaderBuilder>(data);
    options_[Options::SCAN_MANIFEST_ENTRY_LAZY_DECODE_ENABLED] = "true";
    for (bool cache_enabled : {false, true}) {
        SCOPED_TRACE(cache_enabled);
        options.WithCache(cache_enabled ? std::make_shared<LruCache>(16 * 1024 * 1024) : nullptr);
        ASSERT_OK_AND_ASSIGN(manifest, ManifestFile::Create(fs, format, "null", paths, 1024 * 1024,
                                                            pool_, options, arrow::schema({})));
        manifest->reader_builder_ = reader_builder;
        bool cache_warm = false;
        for (bool inferred : {false, true}) {
            SCOPED_TRACE(inferred);
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<AppendOnlyFileStoreScan> scan,
                CreateScan(KeyEquals(), inferred ? std::nullopt : std::optional<int32_t>(bucket)));
            scan->manifest_file_ = manifest;
            for (bool known_bounds : {false, true}) {
                SCOPED_TRACE(known_bounds);
                auto meta = metas[0];
                meta.min_bucket_ = known_bounds ? std::optional<int32_t>(bucket) : std::nullopt;
                meta.max_bucket_ = meta.min_bucket_;
                reader_builder->rows_read = 0;
                std::vector<ManifestEntry> actual;
                ASSERT_OK(scan->ReadAndMergeBucketFileEntries({meta}, bucket, &actual));
                ASSERT_EQ(actual, entries);
                const int64_t expected_rows =
                    cache_enabled ? (cache_warm ? 0 : 1) : (known_bounds ? 1 : 2);
                ASSERT_EQ(reader_builder->rows_read.load(), expected_rows);
                cache_warm = cache_enabled;
            }
        }
    }
}

TEST_F(AppendBucketPruningTest, SelectivelyDecodesInferredBucketCandidates) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = dir->GetFileSystem();
    schema_manager_ = std::make_shared<SchemaManager>(fs, dir->Str());
    schema_id_ = 0;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> initial_scan,
                         CreateScan(nullptr));
    ASSERT_OK(schema_manager_->CreateTable(initial_scan->schema_, {}, {}, options_));
    schema_id_ = 1;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> inferred_scan,
                         CreateScan(KeyEquals()));
    ASSERT_FALSE(inferred_scan->bucket_filter_);
    ASSERT_TRUE(inferred_scan->bucket_selector_);
    const int32_t bucket = inferred_scan->bucket_selector_->Bucket(kNumBuckets);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> format, FileFormatFactory::Get("avro", {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> paths,
                         FileStorePathFactory::Create(
                             dir->Str(), initial_scan->schema_, /*partition_keys=*/{},
                             /*default_part_value=*/"", "avro", /*data_file_prefix=*/"data-",
                             /*legacy_partition_name_enabled=*/true, /*external_paths=*/{},
                             /*global_index_external_path=*/std::nullopt,
                             /*index_file_in_data_file_dir=*/false, pool_));
    ASSERT_OK(fs->Mkdirs(FileStorePathFactory::ManifestPath(dir->Str())));
    SimpleStats stats = BinaryRowGenerator::GenerateStats(
        {std::string("a"), 0}, {std::string("z"), 100}, {0, 0}, pool_.get());
    std::vector<ManifestEntry> entries;
    for (const auto& layout : std::vector<std::pair<int32_t, int64_t>>{
             {kNumBuckets, schema_id_}, {8, schema_id_}, {kNumBuckets, 0}, {-1, schema_id_}}) {
        for (int32_t id = 0; id < std::max(1, layout.first); ++id) {
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<DataFileMeta> file,
                DataFileMeta::ForAppend(fmt::format("data-{}.avro", entries.size()), 100, 10, stats,
                                        0, 9, layout.second, std::nullopt, std::nullopt,
                                        std::nullopt, std::nullopt, std::nullopt));
            entries.emplace_back(FileKind::Add(), BinaryRow::EmptyRow(), id, layout.first, file);
        }
    }
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> deleted_file,
        DataFileMeta::ForAppend("deleted.avro", 100, 10, stats, 0, 9, schema_id_, std::nullopt,
                                std::nullopt, std::nullopt, std::nullopt, std::nullopt));
    entries.emplace_back(FileKind::Add(), BinaryRow::EmptyRow(), bucket, kNumBuckets, deleted_file);
    ManifestEntry deletion(FileKind::Delete(), BinaryRow::EmptyRow(), bucket, kNumBuckets,
                           deleted_file);
    for (bool cache_enabled : {false, true}) {
        SCOPED_TRACE(cache_enabled);
        ASSERT_OK_AND_ASSIGN(CoreOptions manifest_options,
                             CoreOptions::FromMap({{Options::READ_BATCH_SIZE, "2"}}));
        if (cache_enabled) {
            manifest_options.WithCache(std::make_shared<LruCache>(16 * 1024 * 1024));
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ManifestFile> manifest,
                             ManifestFile::Create(fs, format, "null", paths, 1024 * 1024, pool_,
                                                  manifest_options, arrow::schema({})));
        auto serializer = std::make_unique<CountingManifestEntrySerializer>(pool_);
        auto* counter = serializer.get();
        manifest->serializer_ = std::move(serializer);
        using WrittenFile = std::pair<std::string, int64_t>;
        ASSERT_OK_AND_ASSIGN(WrittenFile additions, manifest->WriteWithoutRolling(entries));
        ASSERT_OK_AND_ASSIGN(WrittenFile deletions, manifest->WriteWithoutRolling({deletion}));
        std::vector<ManifestFileMeta> metas;
        for (const auto& file : {additions, deletions}) {
            metas.emplace_back(file.first, file.second, 0, 0, SimpleStats::EmptyStats(), schema_id_,
                               std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
                               std::nullopt);
        }
        std::vector<std::string> baseline_files;
        for (bool lazy_decode : {false, true}) {
            SCOPED_TRACE(lazy_decode);
            options_[Options::SCAN_MANIFEST_ENTRY_LAZY_DECODE_ENABLED] =
                lazy_decode ? "true" : "false";
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> scan,
                                 CreateScan(KeyEquals()));
            scan->manifest_file_ = manifest;
            counter->decoded_entries = 0;
            std::vector<ManifestEntry> candidates;
            ASSERT_OK(scan->ReadAndMergeBucketFileEntries(metas, bucket, &candidates));
            // Both schema versions have the same bucket layout: skip their unrelated
            // buckets, but retain different bucket counts and both sides of the deletion.
            ASSERT_EQ(counter->decoded_entries.load(),
                      entries.size() + 1 - (lazy_decode ? 2 * (kNumBuckets - 1) : 0));
            std::vector<std::string> files;
            for (const auto& entry : candidates) {
                ASSERT_NE(entry.FileName(), "deleted.avro");
                ASSERT_OK_AND_ASSIGN(bool keep, scan->FilterManifestEntry(entry));
                if (keep) {
                    files.push_back(entry.FileName());
                }
            }
            std::sort(files.begin(), files.end());
            ASSERT_EQ(files.size(), 4);
            if (!lazy_decode) {
                baseline_files = files;
            } else {
                ASSERT_EQ(files, baseline_files);
            }
        }
    }
}

TEST_F(AppendBucketPruningTest, PrunesStringKeyLookup) {
    BinaryRow key = BinaryRowGenerator::GenerateRow({std::string("key")}, pool_.get());
    int32_t expected_bucket = DefaultBucketFunction().Bucket(key, kNumBuckets);
    CheckBuckets(KeyEquals(), expected_bucket);
}

TEST_F(AppendBucketPruningTest, SelectivelyDecodesWithoutReadingHistoricalSchemas) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<CountingSchemaFileSystem>();
    schema_manager_ = std::make_shared<SchemaManager>(fs, dir->Str());
    schema_id_ = 0;
    rowkey_type_ = arrow::int32();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> initial, CreateScan(nullptr));
    ASSERT_OK(schema_manager_->CreateTable(initial->schema_, {}, {}, options_));
    schema_id_ = 1000;
    auto predicate = PredicateBuilder::Equal(0, "rowkey", FieldType::INT, Literal(int32_t{-1}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> current, CreateScan(predicate));
    // Only schema 0 has a schema file; unused historical versions must not be read.
    const int32_t bucket = current->bucket_selector_->Bucket(kNumBuckets);
    SimpleStats old_stats = BinaryRowGenerator::GenerateStats({int32_t{-100}, 0}, {int32_t{0}, 100},
                                                              {0, 0}, pool_.get());
    std::vector<ManifestEntry> entries;
    for (int32_t id = 0; id <= kNumBuckets; ++id) {
        const bool historical = id < kNumBuckets;
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<DataFileMeta> file,
            DataFileMeta::ForAppend(fmt::format("data-{}.avro", id), 100, 10, old_stats, 0, 9,
                                    historical ? 0 : schema_id_, std::nullopt, std::nullopt,
                                    std::nullopt, std::nullopt, std::nullopt));
        entries.emplace_back(FileKind::Add(), BinaryRow::EmptyRow(), historical ? id : bucket,
                             kNumBuckets, file);
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> format, FileFormatFactory::Get("avro", {}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> paths,
        FileStorePathFactory::Create(dir->Str(), initial->schema_, {}, "", "avro", "data-", true,
                                     {}, std::nullopt, false, pool_));
    ASSERT_OK(fs->Mkdirs(FileStorePathFactory::ManifestPath(dir->Str())));
    for (bool cache_enabled : {false, true}) {
        SCOPED_TRACE(cache_enabled);
        ASSERT_OK_AND_ASSIGN(CoreOptions manifest_options, CoreOptions::FromMap({}));
        if (cache_enabled) {
            manifest_options.WithCache(std::make_shared<LruCache>(16 * 1024 * 1024));
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ManifestFile> manifest,
                             ManifestFile::Create(fs, format, "null", paths, 1024 * 1024, pool_,
                                                  manifest_options, arrow::schema({})));
        ASSERT_OK_AND_ASSIGN(std::vector<ManifestFileMeta> metas, manifest->Write(entries));
        ASSERT_EQ(metas.size(), 1);
        ASSERT_EQ(metas[0].SchemaId(), schema_id_);
        auto serializer = std::make_unique<CountingManifestEntrySerializer>(pool_);
        auto* counter = serializer.get();
        manifest->serializer_ = std::move(serializer);
        for (bool lazy_decode : {false, true}) {
            SCOPED_TRACE(lazy_decode);
            options_[Options::SCAN_MANIFEST_ENTRY_LAZY_DECODE_ENABLED] =
                lazy_decode ? "true" : "false";
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> scan,
                                 CreateScan(predicate));
            scan->manifest_file_ = manifest;
            std::vector<ManifestEntry> baseline;
            ASSERT_OK(scan->ReadAndMergeFileEntries(metas, &baseline));
            ASSERT_EQ(baseline.size(), 2);
            // Keep schema reads used by stats evolution out of the bucket-read measurement.
            scan->schema_manager_ = std::make_shared<SchemaManager>(fs, dir->Str());
            counter->decoded_entries = 0;
            fs->read_count = 0;
            std::vector<ManifestEntry> candidates;
            ASSERT_OK(scan->ReadAndMergeBucketFileEntries(metas, bucket, &candidates));
            ASSERT_EQ(fs->read_count, 0);
            ASSERT_EQ(counter->decoded_entries.load(),
                      entries.size() - (lazy_decode ? kNumBuckets - 1 : 0));
            std::vector<ManifestEntry> filtered;
            for (const auto& entry : candidates) {
                ASSERT_OK_AND_ASSIGN(bool keep, scan->FilterManifestEntry(entry));
                if (keep) {
                    filtered.push_back(entry);
                }
            }
            ASSERT_EQ(filtered, baseline);
        }
    }
}

TEST_F(AppendBucketPruningTest, PreservesExplicitBucketFilter) {
    BinaryRow key = BinaryRowGenerator::GenerateRow({std::string("key")}, pool_.get());
    int32_t other_bucket = (DefaultBucketFunction().Bucket(key, kNumBuckets) + 1) % kNumBuckets;
    CheckBuckets(KeyEquals(), other_bucket, other_bucket);
}

TEST_F(AppendBucketPruningTest, DoesNotPruneWithoutCompleteEqualKeys) {
    CheckBuckets(nullptr, std::nullopt);
    CheckBuckets(PredicateBuilder::GreaterThan(0, "rowkey", FieldType::STRING,
                                               Literal(FieldType::STRING, "key", 3)),
                 std::nullopt);
    options_[Options::BUCKET_KEY] = "rowkey,value";
    CheckBuckets(KeyEquals(), std::nullopt);
}

TEST_F(AppendBucketPruningTest, DoesNotPruneBucketUnawareTable) {
    options_[Options::BUCKET] = "-1";
    CheckBuckets(KeyEquals(), std::nullopt);
}

TEST_F(AppendBucketPruningTest, UsesEachEntriesBucketCount) {
    BinaryRow key = BinaryRowGenerator::GenerateRow({std::string("key")}, pool_.get());
    for (int32_t total_buckets : {2, 4, 8, 17}) {
        CheckBuckets(KeyEquals(), DefaultBucketFunction().Bucket(key, total_buckets), std::nullopt,
                     total_buckets);
    }
}

TEST_F(AppendBucketPruningTest, PrunesCompatibleHistoricalSchema) {
    auto test_dir = UniqueTestDirectory::Create("local");
    schema_manager_ =
        std::make_shared<SchemaManager>(std::make_shared<LocalFileSystem>(), test_dir->Str());
    ASSERT_OK_AND_ASSIGN(auto scan, CreateScan(nullptr));
    ASSERT_OK(schema_manager_->CreateTable(scan->schema_, {}, {}, options_));
    schema_id_ = 1;
    extra_field_ = true;
    BinaryRow key = BinaryRowGenerator::GenerateRow({std::string("key")}, pool_.get());
    CheckBuckets(KeyEquals(), DefaultBucketFunction().Bucket(key, kNumBuckets));
}

TEST_F(AppendBucketPruningTest, PrunesHistoricalBucketsWithoutReadingSchemas) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<CountingSchemaFileSystem>();
    schema_manager_ = std::make_shared<SchemaManager>(fs, dir->Str());
    schema_id_ = 1000;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendOnlyFileStoreScan> scan, CreateScan(KeyEquals()));
    const int32_t other_bucket = (scan->bucket_selector_->Bucket(kNumBuckets) + 1) % kNumBuckets;
    for (int64_t data_schema_id : {0, 500, 999, 1000}) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<DataFileMeta> file,
            DataFileMeta::ForAppend("data.parquet", 100, 10, SimpleStats::EmptyStats(), 0, 9,
                                    data_schema_id, std::nullopt, std::nullopt, std::nullopt,
                                    std::nullopt, std::nullopt));
        ManifestEntry entry(FileKind::Add(), BinaryRow::EmptyRow(), other_bucket, kNumBuckets,
                            file);
        ASSERT_OK_AND_ASSIGN(bool keep, scan->FilterManifestEntry(entry));
        ASSERT_FALSE(keep);
    }
    ASSERT_EQ(fs->read_count, 0);
}

TEST_F(AppendBucketPruningTest, PreservesCrossScaleDecimalMatch) {
    rowkey_type_ = arrow::decimal128(10, 2);
    CheckMatchingValue(FieldType::DECIMAL, Decimal::FromUnscaledLong(12, 10, 1),
                       Decimal::FromUnscaledLong(120, 10, 2));
}

TEST_F(AppendBucketPruningTest, PreservesDifferentNaNPayloadMatch) {
    rowkey_type_ = arrow::float64();
    auto query_value = FloatingPointFromBits<double>(uint64_t{0x7ff8000000000000ULL});
    auto stored_value = FloatingPointFromBits<double>(uint64_t{0x7ff8000000000001ULL});
    CheckMatchingValue(FieldType::DOUBLE, query_value, stored_value);
}

TEST(AppendOnlyFileStoreScanTest, TestReconstructPredicateWithNonCastedFields) {
    std::string table_root =
        paimon::test::GetDataDir() +
        "/orc/append_table_alter_table_with_cast.db/append_table_alter_table_with_cast";
    auto fs = std::make_shared<LocalFileSystem>();
    auto pool = GetDefaultPool();
    SchemaManager manager(fs, table_root);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema, manager.ReadSchema(/*schema_id=*/0));
    ASSERT_TRUE(schema);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> schema1, manager.ReadSchema(/*schema_id=*/1));
    ASSERT_TRUE(schema1);
    auto evo = std::make_shared<SimpleStatsEvolution>(schema->Fields(), schema1->Fields(),
                                                      /*need_mapping=*/true, pool);
    ASSERT_OK_AND_ASSIGN(
        auto child1,
        PredicateBuilder::Or(
            {PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f4", FieldType::TIMESTAMP),
             PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"key0", FieldType::INT),
             PredicateBuilder::IsNull(/*field_index=*/2, /*field_name=*/"key1", FieldType::INT)}));

    auto sub_child1 =
        PredicateBuilder::IsNull(/*field_index=*/3, /*field_name=*/"f3", FieldType::INT);
    auto sub_child2 =
        PredicateBuilder::IsNull(/*field_index=*/4, /*field_name=*/"f1", FieldType::STRING);
    auto sub_child3 =
        PredicateBuilder::IsNull(/*field_index=*/5, /*field_name=*/"f2", FieldType::DECIMAL);
    ASSERT_OK_AND_ASSIGN(auto child2, PredicateBuilder::And({sub_child1, sub_child2, sub_child3}));

    auto child3 =
        PredicateBuilder::IsNull(/*field_index=*/6, /*field_name=*/"f0", FieldType::BOOLEAN);
    auto child4 = PredicateBuilder::IsNull(/*field_index=*/7, /*field_name=*/"f6", FieldType::INT);

    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({child1, child2, child3, child4}));

    ASSERT_OK_AND_ASSIGN(
        auto result,
        AppendOnlyFileStoreScan::ReconstructPredicateWithNonCastedFields(predicate, evo));
    ASSERT_EQ(*result, *child4);

    auto key_predicate =
        PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"key0", FieldType::INT);
    ASSERT_OK_AND_ASSIGN(result, AppendOnlyFileStoreScan::ReconstructPredicateWithNonCastedFields(
                                     key_predicate, evo));
    ASSERT_EQ(*result, *key_predicate);
}

TEST(AppendOnlyFileStoreScanTest, TestReadPartitionEntries) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());

    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    auto typed_table_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    ASSERT_TRUE(typed_table_scan);

    auto file_store_scan = typed_table_scan->snapshot_reader_->scan_;
    ASSERT_TRUE(file_store_scan);
    ASSERT_OK_AND_ASSIGN(std::vector<PartitionEntry> result_partition_entries,
                         file_store_scan->ReadPartitionEntries());

    auto GenerateRow = [&](int32_t value) {
        BinaryRow row(1);
        BinaryRowWriter writer(&row, 0, pool.get());
        writer.WriteInt(0, value);
        writer.Complete();
        return row;
    };

    std::vector<PartitionEntry> expected_partition_entries = {
        PartitionEntry(GenerateRow(10), /*record_count=*/9, /*file_size_in_bytes=*/1183,
                       /*file_count=*/2, /*last_file_creation_time=*/1721643834472l - 28800000l,
                       /*total_buckets=*/2),
        PartitionEntry(GenerateRow(20), /*record_count=*/2, /*file_size_in_bytes=*/1047,
                       /*file_count=*/2, /*last_file_creation_time=*/1721643267404l - 28800000l,
                       /*total_buckets=*/2)};
    auto ComparePartitionEntryByPartition = [](const PartitionEntry& lhs,
                                               const PartitionEntry& rhs) -> bool {
        return lhs.Partition().GetInt(0) < rhs.Partition().GetInt(0);
    };

    std::stable_sort(result_partition_entries.begin(), result_partition_entries.end(),
                     ComparePartitionEntryByPartition);
    std::stable_sort(expected_partition_entries.begin(), expected_partition_entries.end(),
                     ComparePartitionEntryByPartition);

    ASSERT_EQ(result_partition_entries, expected_partition_entries);
}

TEST(AppendOnlyFileStoreScanTest, TestScanDurationMetric) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::SCAN_SNAPSHOT_ID, "5");
    ASSERT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());

    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    auto typed_table_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    ASSERT_TRUE(typed_table_scan);

    auto file_store_scan = typed_table_scan->snapshot_reader_->scan_;
    ASSERT_TRUE(file_store_scan);

    constexpr uint64_t kPlanCount = 5;
    for (uint64_t i = 0; i < kPlanCount; ++i) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> raw_plan,
                             file_store_scan->CreatePlan());
        (void)raw_plan;
    }

    std::shared_ptr<Metrics> metrics = file_store_scan->GetScanMetrics();
    ASSERT_TRUE(metrics);

    ASSERT_OK_AND_ASSIGN(uint64_t last_scan_duration,
                         metrics->GetCounter(ScanMetrics::LAST_SCAN_DURATION));
    ASSERT_OK_AND_ASSIGN(HistogramStats stats,
                         metrics->GetHistogramStats(ScanMetrics::SCAN_DURATION));
    ASSERT_OK_AND_ASSIGN(uint64_t manifest_read_duration,
                         metrics->GetCounter(ScanMetrics::LAST_MANIFEST_READ_DURATION));
    ASSERT_OK_AND_ASSIGN(HistogramStats manifest_stats,
                         metrics->GetHistogramStats(ScanMetrics::MANIFEST_READ_DURATION));
    ASSERT_OK_AND_ASSIGN(uint64_t scanned_rows,
                         metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_SCANNED_ROWS));
    ASSERT_OK_AND_ASSIGN(uint64_t materialized_rows,
                         metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_MATERIALIZED_ROWS));
    ASSERT_EQ(stats.count, kPlanCount);
    ASSERT_EQ(manifest_stats.count, kPlanCount);
    ASSERT_LE(manifest_stats.min, static_cast<double>(manifest_read_duration));
    ASSERT_LE(static_cast<double>(manifest_read_duration), manifest_stats.max);
    ASSERT_GT(scanned_rows, 0);
    ASSERT_GE(scanned_rows, materialized_rows);
    ASSERT_LE(stats.min, stats.max);
    ASSERT_LE(stats.min, static_cast<double>(last_scan_duration));
    ASSERT_LE(static_cast<double>(last_scan_duration), stats.max);
    ASSERT_LE(stats.min, stats.p99);
    ASSERT_LE(stats.p50, stats.p99);
    ASSERT_LE(stats.p99, stats.max);
}

namespace {

class CountingPartitionPredicate : public PredicateFilter {
 public:
    explicit CountingPartitionPredicate(const std::shared_ptr<PredicateFilter>& predicate,
                                        const Status& first_row_error = Status::OK())
        : predicate_(predicate), first_row_error_(first_row_error) {}

    bool operator==(const Predicate& other) const override {
        return *predicate_ == other;
    }
    const Function& GetFunction() const override {
        return predicate_->GetFunction();
    }
    std::shared_ptr<Predicate> Negate() const override {
        return predicate_->Negate();
    }
    std::string ToString() const override {
        return predicate_->ToString();
    }
    Result<std::vector<char>> Test(const arrow::Array& array,
                                   arrow::MemoryPool* pool) const override {
        return predicate_->Test(array, pool);
    }
    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema,
                      const InternalRow& row) const override {
        if (++row_calls_ == 1 && !first_row_error_.ok()) {
            return first_row_error_;
        }
        return predicate_->Test(schema, row);
    }
    Result<bool> Test(const std::shared_ptr<arrow::Schema>& schema, int64_t row_count,
                      const InternalRow& min_values, const InternalRow& max_values,
                      const InternalArray& null_counts) const override {
        ++stats_calls_;
        // Retain every manifest so even an empty result exercises entry filtering.
        return true;
    }

    mutable std::atomic<int32_t> row_calls_{0};
    mutable std::atomic<int32_t> stats_calls_{0};

 private:
    std::shared_ptr<PredicateFilter> predicate_;
    Status first_row_error_;
};

std::shared_ptr<FileStoreScan> BuildScan(const std::string& table_path,
                                         const std::shared_ptr<Cache>& cache,
                                         const std::optional<int32_t>& bucket = std::nullopt,
                                         const std::shared_ptr<Predicate>& predicate = nullptr,
                                         bool manifest_entry_lazy_decode_enabled = true,
                                         int32_t max_snapshots = 8) {
    ScanContextBuilder context_builder(table_path);
    context_builder.AddOption(Options::FILE_FORMAT, "orc")
        .AddOption(Options::SCAN_MANIFEST_ENTRY_CACHE_MAX_SNAPSHOTS,
                   fmt::format("{}", max_snapshots))
        .AddOption(Options::SCAN_MANIFEST_ENTRY_LAZY_DECODE_ENABLED,
                   manifest_entry_lazy_decode_enabled ? "true" : "false")
        .WithCache(cache);
    if (bucket) {
        context_builder.SetBucketFilter(bucket.value());
    }
    if (predicate) {
        context_builder.SetPredicate(predicate);
    }
    EXPECT_OK_AND_ASSIGN(auto scan_context, context_builder.Finish());
    EXPECT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
    auto typed_table_scan = dynamic_cast<AbstractTableScan*>(table_scan.get());
    EXPECT_TRUE(typed_table_scan);
    return typed_table_scan->snapshot_reader_->scan_;
}

std::vector<std::string> SortedFileNames(std::vector<ManifestEntry>&& entries) {
    std::vector<std::string> file_names;
    file_names.reserve(entries.size());
    for (const auto& entry : entries) {
        file_names.push_back(entry.FileName());
    }
    std::sort(file_names.begin(), file_names.end());
    return file_names;
}

}  // namespace

TEST(AppendOnlyFileStoreScanTest, TestDropStatsAfterFiltering) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
        Literal(FieldType::STRING, "David", 5));

    std::shared_ptr<FileStoreScan> scan_with_stats =
        BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/std::nullopt, predicate);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot,
                         scan_with_stats->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/3));
    scan_with_stats->WithSnapshot(snapshot);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> plan_with_stats,
                         scan_with_stats->CreatePlan());
    std::vector<ManifestEntry> entries_with_stats = plan_with_stats->Files();
    ASSERT_FALSE(entries_with_stats.empty());

    std::shared_ptr<FileStoreScan> scan_without_stats =
        BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/std::nullopt, predicate);
    scan_without_stats->WithSnapshot(snapshot)->EnableDropStats();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStoreScan::RawPlan> plan_without_stats,
                         scan_without_stats->CreatePlan());
    std::vector<ManifestEntry> entries_without_stats = plan_without_stats->Files();

    ASSERT_EQ(entries_with_stats.size(), entries_without_stats.size());
    for (size_t i = 0; i < entries_with_stats.size(); ++i) {
        ASSERT_EQ(entries_with_stats[i].CreateIdentifier(),
                  entries_without_stats[i].CreateIdentifier());
        ASSERT_FALSE(entries_with_stats[i].File()->value_stats == SimpleStats::EmptyStats());
        ASSERT_EQ(SimpleStats::EmptyStats(), entries_without_stats[i].File()->value_stats);
        ASSERT_TRUE(entries_without_stats[i].File()->value_stats_cols.has_value());
        ASSERT_TRUE(entries_without_stats[i].File()->value_stats_cols->empty());
    }
}

TEST(AppendOnlyFileStoreScanTest, TestSnapshotLiveManifestCachePath) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto cache = std::make_shared<LruCache>(/*max_weight=*/16 * 1024 * 1024);

    // First scan on snapshot 5: cache miss, entries rebuilt from all manifests.
    auto scan_first = BuildScan(table_path, cache, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_5,
                         scan_first->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/5));
    scan_first->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_first, scan_first->CreatePlan());
    std::vector<std::string> first_file_names = SortedFileNames(plan_first->Files());
    std::shared_ptr<Metrics> first_metrics = scan_first->GetScanMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t first_cache_enabled,
                         first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_ENABLED));
    ASSERT_OK_AND_ASSIGN(uint64_t first_cache_hit,
                         first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
    ASSERT_OK_AND_ASSIGN(uint64_t first_cache_misses,
                         first_metrics->GetCounter(ScanMetrics::SNAPSHOT_CACHE_MISSES));
    ASSERT_OK(first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_LOAD_DURATION));
    ASSERT_OK(first_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_STORE_DURATION));
    ASSERT_OK(first_metrics->GetHistogramStats(ScanMetrics::SNAPSHOT_CACHE_LOAD_DURATION));
    ASSERT_OK(first_metrics->GetHistogramStats(ScanMetrics::SNAPSHOT_CACHE_STORE_DURATION));
    ASSERT_EQ(first_cache_enabled, 1);
    ASSERT_EQ(first_cache_hit, 0);
    ASSERT_EQ(first_cache_misses, 1);

    // Second scan on the same snapshot should read the same bucket live entries from cache.
    auto scan_second = BuildScan(table_path, cache, /*bucket=*/0);
    scan_second->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_second, scan_second->CreatePlan());
    ASSERT_EQ(first_file_names, SortedFileNames(plan_second->Files()));
    std::shared_ptr<Metrics> second_metrics = scan_second->GetScanMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t second_cache_hit,
                         second_metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
    ASSERT_OK_AND_ASSIGN(uint64_t second_cache_hits,
                         second_metrics->GetCounter(ScanMetrics::SNAPSHOT_CACHE_HITS));
    ASSERT_OK_AND_ASSIGN(uint64_t scanned_rows,
                         second_metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_SCANNED_ROWS));
    ASSERT_OK_AND_ASSIGN(
        uint64_t materialized_rows,
        second_metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_MATERIALIZED_ROWS));
    ASSERT_EQ(second_cache_hit, 1);
    ASSERT_EQ(second_cache_hits, 1);
    ASSERT_GE(scanned_rows, materialized_rows);
    ASSERT_OK(second_metrics->GetHistogramStats(ScanMetrics::SNAPSHOT_CACHE_LOAD_DURATION));
}

TEST(AppendOnlyFileStoreScanTest, TestSnapshotLiveManifestCacheRebuildOnMiss) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto cache =
        std::make_shared<CountingRoutingCache>(CacheKind::SNAPSHOT_LIVE_MANIFEST, 16 * 1024 * 1024);

    // Seed the cache with an earlier snapshot.
    auto scan_base = BuildScan(table_path, cache, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_3,
                         scan_base->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/3));
    scan_base->WithSnapshot(snapshot_3);
    ASSERT_OK_AND_ASSIGN(auto plan_base, scan_base->CreatePlan());
    ASSERT_EQ(cache->GetCount(CacheKind::SNAPSHOT_LIVE_MANIFEST), 1);

    // Advance to a newer snapshot: cache miss rebuilds the target snapshot bucket.
    auto scan_next = BuildScan(table_path, cache, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_5,
                         scan_next->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/5));
    scan_next->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_next, scan_next->CreatePlan());
    ASSERT_EQ(cache->GetCount(CacheKind::SNAPSHOT_LIVE_MANIFEST), 2);

    auto scan_expected = BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/0);
    scan_expected->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_expected, scan_expected->CreatePlan());
    ASSERT_EQ(SortedFileNames(plan_expected->Files()), SortedFileNames(plan_next->Files()));

    auto scan_retained = BuildScan(table_path, cache, /*bucket=*/0);
    scan_retained->WithSnapshot(snapshot_3);
    ASSERT_OK_AND_ASSIGN(auto plan_retained, scan_retained->CreatePlan());
    ASSERT_EQ(SortedFileNames(plan_base->Files()), SortedFileNames(plan_retained->Files()));
    ASSERT_EQ(cache->GetCount(CacheKind::SNAPSHOT_LIVE_MANIFEST), 3);
    ASSERT_OK_AND_ASSIGN(uint64_t hit, scan_retained->GetScanMetrics()->GetCounter(
                                           ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
    ASSERT_EQ(hit, 1);
    auto scan_evicted = BuildScan(table_path, cache, /*bucket=*/0, /*predicate=*/nullptr,
                                  /*manifest_entry_lazy_decode_enabled=*/true, /*max_snapshots=*/1);
    scan_evicted->WithSnapshot(snapshot_3);
    ASSERT_OK_AND_ASSIGN(auto plan_evicted, scan_evicted->CreatePlan());
    ASSERT_EQ(SortedFileNames(plan_base->Files()), SortedFileNames(plan_evicted->Files()));
    ASSERT_EQ(cache->GetCount(CacheKind::SNAPSHOT_LIVE_MANIFEST), 4);
    ASSERT_OK_AND_ASSIGN(
        hit, scan_evicted->GetScanMetrics()->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
    ASSERT_EQ(hit, 0);
}

TEST(AppendOnlyFileStoreScanTest, TestSnapshotLiveManifestCacheFallbackWithoutLazyDecode) {
    TimezoneGuard guard("Asia/Shanghai");
    std::string table_path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09/";
    auto cache = std::make_shared<LruCache>(/*max_weight=*/16 * 1024 * 1024);

    auto scan_fallback = BuildScan(table_path, cache, /*bucket=*/0, /*predicate=*/nullptr,
                                   /*manifest_entry_lazy_decode_enabled=*/false);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot_5,
                         scan_fallback->GetSnapshotManager()->LoadSnapshot(/*snapshot_id=*/5));
    scan_fallback->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_fallback, scan_fallback->CreatePlan());

    auto scan_expected = BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/0);
    scan_expected->WithSnapshot(snapshot_5);
    ASSERT_OK_AND_ASSIGN(auto plan_expected, scan_expected->CreatePlan());
    ASSERT_EQ(SortedFileNames(plan_expected->Files()), SortedFileNames(plan_fallback->Files()));
}

TEST(AppendOnlyFileStoreScanTest, SnapshotCachePropagatesPartitionPredicateError) {
    TimezoneGuard guard("Asia/Shanghai");
    const std::string table_path = GetDataDir() + "/orc/append_09.db/append_09/";
    auto cache = std::make_shared<LruCache>(16 * 1024 * 1024);
    auto warm = BuildScan(table_path, cache, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, warm->GetSnapshotManager()->LoadSnapshot(5));
    warm->WithSnapshot(snapshot);
    ASSERT_OK_AND_ASSIGN(auto warm_plan, warm->CreatePlan());
    auto files = warm_plan->Files();
    ASSERT_FALSE(files.empty());
    const int32_t partition = files.front().Partition().GetInt(0);
    auto predicate = PredicateBuilder::Equal(1, "f1", FieldType::INT, Literal(partition));
    for (bool lazy_decode : {false, true}) {
        SCOPED_TRACE(lazy_decode);
        for (const Status& error : {Status::IOError("injected partition predicate error"),
                                    Status::Invalid("injected partition predicate error")}) {
            SCOPED_TRACE(error.ToString());
            auto scan = BuildScan(table_path, cache, /*bucket=*/0, predicate, lazy_decode);
            scan->WithSnapshot(snapshot);
            auto failing =
                std::make_shared<CountingPartitionPredicate>(scan->GetPartitionPredicate(), error);
            scan->partition_filter_ = failing;
            // A transient predicate failure must reach the caller, without an internal retry.
            auto result = scan->CreatePlan();
            ASSERT_FALSE(result.ok());
            ASSERT_EQ(result.status().code(), error.code());
            ASSERT_NE(result.status().message().find(error.message()), std::string::npos);
            ASSERT_EQ(failing->row_calls_.load(), 1);
            // The failure must leave the complete cached snapshot available for the next request.
            ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
            ASSERT_FALSE(plan->Files().empty());
            for (const auto& entry : plan->Files()) {
                ASSERT_EQ(entry.Partition().GetInt(0), partition);
            }
            ASSERT_OK_AND_ASSIGN(uint64_t hit, scan->GetScanMetrics()->GetCounter(
                                                   ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
            ASSERT_EQ(hit, 1);
        }
    }
}

TEST(AppendOnlyFileStoreScanTest, SnapshotCacheRebuildsCorruptValues) {
    TimezoneGuard guard("Asia/Shanghai");
    const std::string table_path = GetDataDir() + "/orc/append_09.db/append_09/";
    auto source = BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/0);
    ASSERT_OK_AND_ASSIGN(Snapshot snapshot, source->GetSnapshotManager()->LoadSnapshot(5));
    source->WithSnapshot(snapshot);
    ASSERT_OK_AND_ASSIGN(auto source_plan, source->CreatePlan());
    auto files = source_plan->Files();
    ASSERT_FALSE(files.empty());
    const ManifestEntry first = files.front();
    const auto all_names = SortedFileNames(std::move(files));
    auto predicate =
        PredicateBuilder::Equal(1, "f1", FieldType::INT, Literal(first.Partition().GetInt(0)));
    auto expected = BuildScan(table_path, /*cache=*/nullptr, /*bucket=*/0, predicate);
    expected->WithSnapshot(snapshot);
    ASSERT_OK_AND_ASSIGN(auto expected_plan, expected->CreatePlan());
    const auto expected_names = SortedFileNames(expected_plan->Files());
    auto pool = GetDefaultPool();
    ManifestEntrySerializer serializer(pool);
    BinaryRowSerializer row_serializer(serializer.NumFields(), pool);
    for (const std::string corruption : {"magic", "version", "truncated", "partition"}) {
        SCOPED_TRACE(corruption);
        ASSERT_OK_AND_ASSIGN(BinaryRow row, serializer.ToRow(first));
        if (corruption == "version") {
            row.SetInt(0, 999);
        } else if (corruption == "partition") {
            // A negative partition arity fails before predicate evaluation, regardless of mode.
            auto segment = row.GetSegment();
            const int32_t offset =
                static_cast<int32_t>(row.GetStringView(2).data() - segment.Data());
            segment.PutValue<int32_t>(offset, -1);
        }
        MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
        out.WriteValue<int32_t>(corruption == "magic" ? 0 : 0x534d4543);
        out.WriteValue<int32_t>(1);
        out.WriteValue<int64_t>(snapshot.Id());
        out.WriteValue<int32_t>(1);
        ASSERT_OK(row_serializer.Serialize(row, &out));
        ASSERT_EQ(out.Segments().size(), 1);
        const int64_t size = out.CurrentSize() - (corruption == "truncated" ? 1 : 0);
        std::shared_ptr<Bytes> bytes =
            Bytes::AllocateBytes(std::string(out.Segments().front().Data(), size), pool.get());
        const auto segment = MemorySegment::Wrap(bytes);
        ASSERT_NOK(SnapshotLiveManifestEntries::ReadSnapshot(segment, snapshot.Id(), 8,
                                                             /*row_filter=*/nullptr, pool));
        for (bool lazy_decode : {false, true}) {
            SCOPED_TRACE(lazy_decode);
            auto cache = std::make_shared<CountingRoutingCache>(CacheKind::SNAPSHOT_LIVE_MANIFEST,
                                                                16 * 1024 * 1024);
            auto scan = BuildScan(table_path, cache, /*bucket=*/0, predicate, lazy_decode);
            scan->WithSnapshot(snapshot);
            ASSERT_OK(cache->Put(scan->CreateSnapshotLiveManifestEntriesCacheKey(0),
                                 std::make_shared<CacheValue>(segment, CacheCallback())));
            for (int32_t attempt = 0; attempt < 2; ++attempt) {
                ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
                ASSERT_EQ(SortedFileNames(plan->Files()), expected_names);
                ASSERT_OK_AND_ASSIGN(uint64_t hit, scan->GetScanMetrics()->GetCounter(
                                                       ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
                ASSERT_EQ(hit, attempt);
                ASSERT_EQ(cache->GetCount(CacheKind::SNAPSHOT_LIVE_MANIFEST), attempt + 1);
            }
            auto unfiltered = BuildScan(table_path, cache, /*bucket=*/0, nullptr, lazy_decode);
            unfiltered->WithSnapshot(snapshot);
            ASSERT_OK_AND_ASSIGN(auto all, unfiltered->CreatePlan());
            ASSERT_EQ(SortedFileNames(all->Files()), all_names);
        }
    }
}

TEST(AppendOnlyFileStoreScanTest, PartitionPruningAcrossCacheModes) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fields =
        arrow::FieldVector{arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32())};
    std::map<std::string, std::string> options = {
        {Options::BUCKET, "1"}, {Options::BUCKET_KEY, "f0"}, {Options::FILE_FORMAT, "orc"}};
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(dir->Str(), arrow::schema(fields), {"f1"}, {}, options,
                                            /*is_streaming_mode=*/false));
    std::vector<std::unique_ptr<RecordBatch>> batches;
    for (int32_t partition : {10, 20}) {
        ASSERT_OK_AND_ASSIGN(
            auto batch,
            TestHelper::MakeRecordBatch(arrow::struct_(fields), fmt::format("[[1, {}]]", partition),
                                        {{"f1", fmt::format("{}", partition)}}, /*bucket=*/0,
                                        {RecordBatch::RowKind::INSERT}));
        batches.push_back(std::move(batch));
    }
    ASSERT_OK(helper->WriteAndCommit(std::move(batches), 0, std::nullopt));
    const std::string table_path = dir->Str() + "/foo.db/bar";
    for (bool lazy_decode : {false, true}) {
        SCOPED_TRACE(lazy_decode);
        // Exercise uncached reads, manifest Arrow caching, and bucket snapshot caching.
        for (int32_t mode : {0, 1, 2}) {
            SCOPED_TRACE(mode);
            auto cache = std::make_shared<CountingRoutingCache>(std::map<CacheKind, int64_t>{
                {CacheKind::MANIFEST, 16 * 1024 * 1024},
                {CacheKind::SNAPSHOT_LIVE_MANIFEST, 16 * 1024 * 1024}});
            std::shared_ptr<Cache> scan_cache = mode == 0 ? nullptr : cache;
            std::optional<int32_t> bucket = mode == 2 ? std::optional<int32_t>(0) : std::nullopt;
            int32_t scans = 0;
            int64_t manifest_loads = 0;
            // A filtered cold read must retain other partitions for subsequent cached reads.
            for (int32_t partition : {10, 20, 99, 10}) {
                SCOPED_TRACE(partition);
                auto predicate =
                    PredicateBuilder::Equal(1, "f1", FieldType::INT, Literal(partition));
                auto scan = BuildScan(table_path, scan_cache, bucket, predicate, lazy_decode);
                auto counting =
                    std::make_shared<CountingPartitionPredicate>(scan->GetPartitionPredicate());
                scan->partition_filter_ = counting;
                ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
                ASSERT_EQ(counting->row_calls_.load(), 2);
                ASSERT_GT(counting->stats_calls_.load(), 0);
                ASSERT_EQ(plan->Files().size(), partition == 99 ? 0 : 1);
                if (partition != 99) {
                    ASSERT_EQ(plan->Files()[0].Partition().GetInt(0), partition);
                }
                auto metrics = scan->GetScanMetrics();
                ASSERT_OK_AND_ASSIGN(uint64_t hit,
                                     metrics->GetCounter(ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
                ASSERT_EQ(hit, mode == 2 && scans > 0 ? 1 : 0);
                if (mode == 2) {
                    ASSERT_EQ(cache->GetCount(CacheKind::SNAPSHOT_LIVE_MANIFEST), scans + 1);
                    ASSERT_OK_AND_ASSIGN(
                        uint64_t scanned,
                        metrics->GetCounter(ScanMetrics::LAST_LAZY_DECODE_SCANNED_ROWS));
                    ASSERT_EQ(scanned, 2);
                }
                if (mode == 1) {
                    if (scans == 0) {
                        manifest_loads = cache->SupplierCallCount(CacheKind::MANIFEST);
                        ASSERT_GT(manifest_loads, 0);
                    }
                    ASSERT_EQ(cache->SupplierCallCount(CacheKind::MANIFEST), manifest_loads);
                    ASSERT_EQ(cache->GetCount(CacheKind::MANIFEST), (scans + 1) * manifest_loads);
                }
                ++scans;
            }
            auto unfiltered = BuildScan(table_path, scan_cache, bucket, nullptr, lazy_decode);
            ASSERT_OK_AND_ASSIGN(auto all, unfiltered->CreatePlan());
            ASSERT_EQ(all->Files().size(), 2);
        }
    }
}

TEST(AppendOnlyFileStoreScanTest, PartitionPruningSkipsExcludedMetadataInCreatePlan) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fields =
        arrow::FieldVector{arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32())};
    ASSERT_OK_AND_ASSIGN(
        auto helper,
        TestHelper::Create(
            dir->Str(), arrow::schema(fields), {"f1"}, {},
            {{Options::BUCKET, "1"}, {Options::BUCKET_KEY, "f0"}, {Options::FILE_FORMAT, "orc"}},
            /*is_streaming_mode=*/false));
    const std::string table_path = dir->Str() + "/foo.db/bar";
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(
        auto file,
        DataFileMeta::ForAppend("file.orc", 10, 1, SimpleStats::EmptyStats(), 0, 0, 0, std::nullopt,
                                std::nullopt, std::nullopt, std::nullopt, std::nullopt));
    ManifestEntry kept(FileKind::Add(), BinaryRowGenerator::GenerateRow({10}, pool.get()), 0, 1,
                       file);
    ManifestEntry excluded(FileKind(static_cast<int8_t>(2)),
                           BinaryRowGenerator::GenerateRow({20}, pool.get()), 0, 1, file);
    auto writer = BuildScan(table_path, nullptr);
    using WrittenFile = std::pair<std::string, int64_t>;
    ASSERT_OK_AND_ASSIGN(WrittenFile written,
                         writer->manifest_file_->WriteWithoutRolling({kept, excluded}));
    // Both partitions share one manifest, whose statistics cannot prune the requested partition.
    ManifestFileMeta meta(written.first, written.second, 2, 0,
                          BinaryRowGenerator::GenerateStats({10}, {20}, {0}, pool.get()), 0, 0, 0,
                          0, 0, std::nullopt, std::nullopt);
    ASSERT_OK_AND_ASSIGN(WrittenFile base, writer->manifest_list_->Write({}));
    ASSERT_OK_AND_ASSIGN(WrittenFile delta, writer->manifest_list_->Write({meta}));
    Snapshot snapshot(1, 0, base.first, base.second, delta.first, delta.second, std::nullopt,
                      std::nullopt, std::nullopt, "test", 0, Snapshot::CommitKind::Append(), 0, 2,
                      2, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    auto predicate = PredicateBuilder::Equal(1, "f1", FieldType::INT, Literal(10));
    for (int32_t mode : {0, 1, 2}) {
        SCOPED_TRACE(mode);
        std::shared_ptr<Cache> cache =
            mode == 0 ? nullptr : std::make_shared<LruCache>(16 * 1024 * 1024);
        std::optional<int32_t> bucket = mode == 2 ? std::optional<int32_t>(0) : std::nullopt;
        auto scan = BuildScan(table_path, cache, bucket, predicate);
        scan->WithSnapshot(snapshot);
        if (mode == 2) {
            // Seed the complete snapshot directly: rebuilding must decode every partition.
            SnapshotLiveManifestEntries entries(8);
            entries.Put(1, {kept, excluded});
            ASSERT_OK(scan->StoreSnapshotLiveManifestEntries(0, entries));
        }
        for (int32_t attempt = 0; attempt < 2; ++attempt) {
            ASSERT_OK_AND_ASSIGN(auto plan, scan->CreatePlan());
            ASSERT_EQ(plan->Files(), std::vector<ManifestEntry>{kept});
            ASSERT_OK_AND_ASSIGN(uint64_t hit, scan->GetScanMetrics()->GetCounter(
                                                   ScanMetrics::LAST_SNAPSHOT_CACHE_HIT));
            ASSERT_EQ(hit, mode == 2 ? 1 : 0);
        }
        auto full_decode = BuildScan(table_path, cache, bucket, predicate,
                                     /*manifest_entry_lazy_decode_enabled=*/false);
        full_decode->WithSnapshot(snapshot);
        ASSERT_NOK_WITH_MSG(full_decode->CreatePlan(), "Unsupported byte value 2 for file kind.");
    }
}

}  // namespace paimon::test
