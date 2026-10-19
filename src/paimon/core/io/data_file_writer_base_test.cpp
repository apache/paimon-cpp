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

#include "paimon/core/io/data_file_writer_base.h"

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/long_counter.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/append_data_file_writer_factory.h"
#include "paimon/core/io/data_file_index_writer.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/core/io/key_value_data_file_writer_factory.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class OpenCountingFileSystem : public LocalFileSystem {
 public:
    using LocalFileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        ++open_count;
        return LocalFileSystem::Open(path);
    }

    mutable int32_t open_count = 0;
};

class FailingOffsetFileSystem : public LocalFileSystem {
 public:
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        if (path.size() >= 7 && path.compare(path.size() - 7, 7, ".offset") == 0 &&
            ++offset_files == 2) {
            return Status::IOError("injected offset index create failure");
        }
        return LocalFileSystem::Create(path, overwrite);
    }

    mutable int32_t offset_files = 0;
};

}  // namespace

class IndexedDataFileWriter : public DataFileWriterBase<::ArrowArray*> {
 public:
    IndexedDataFileWriter() : DataFileWriterBase(/*compression=*/"zstd", /*converter=*/nullptr) {}

    Status Write(::ArrowArray* batch) override {
        return WriteRecordWithFileIndex(batch);
    }

    Result<std::shared_ptr<DataFileMeta>> GetResult() override {
        return std::shared_ptr<DataFileMeta>();
    }
};

TEST(DataFileWriterBaseTest, WriteAfterCloseDoesNotConsumeIndexBatch) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("col", arrow::int32())});
    std::shared_ptr<arrow::DataType> data_type = arrow::struct_(schema->fields());
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::FILE_FORMAT, "orc"},
                              {"file-index.bitmap.columns", "col"},
                              {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}}));
    std::shared_ptr<FileSystem> file_system = options.GetFileSystem();
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", "data-", nullptr));
    ASSERT_OK_AND_ASSIGN(FileIndexOptions file_index_options,
                         FileIndexOptions::FromCoreOptions(options));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DataFileIndexWriter> file_index_writer,
        DataFileIndexWriter::Create(schema, file_index_options, file_system, path_factory, pool));

    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<WriterBuilder> writer_builder,
                         options.GetFileFormat()->CreateWriterBuilder(&c_schema,
                                                                      /*batch_size=*/100));
    IndexedDataFileWriter writer;
    writer.SetFileIndexWriter(std::move(file_index_writer), schema);
    ASSERT_OK(writer.Init(file_system, path_factory->NewPath(), writer_builder));

    std::shared_ptr<arrow::Array> first_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([[1]])").ValueOrDie();
    ::ArrowArray first_batch;
    ASSERT_TRUE(arrow::ExportArray(*first_array, &first_batch).ok());
    ASSERT_OK(writer.Write(&first_batch));
    ASSERT_OK(writer.Close());

    std::shared_ptr<arrow::Array> second_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([[2]])").ValueOrDie();
    ::ArrowArray second_batch;
    ASSERT_TRUE(arrow::ExportArray(*second_array, &second_batch).ok());
    ASSERT_NOK_WITH_MSG(writer.Write(&second_batch), "Writer has already closed");
    ASSERT_NE(nullptr, second_batch.release);
    ArrowArrayRelease(&second_batch);
}

TEST(DataFileWriterBaseTest, OffsetIndexesFollowRollingWithAndWithoutFileIndexes) {
    for (const std::string index_storage : {"none", "embedded", "external"}) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const std::shared_ptr<MemoryPool> pool = GetDefaultPool();
        const std::shared_ptr<arrow::Schema> schema =
            arrow::schema({arrow::field(SpecialFields::RealtimeOffset().Name(), arrow::int64()),
                           arrow::field("id", arrow::utf8())});
        std::map<std::string, std::string> raw_options = {
            {Options::FILE_FORMAT, "parquet"}, {Options::REALTIME_DEDUPLICATE_KEY_FIELDS, "id"}};
        if (index_storage != "none") {
            raw_options["file-index.bitmap.columns"] = "id";
            raw_options[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] =
                index_storage == "embedded" ? "1MB" : "0";
        }
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));
        auto path_factory = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory->Init(dir->Str(), "parquet", "data-", nullptr));
        auto factory = std::make_shared<AppendDataFileWriterFactory>(
            options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
            std::make_shared<LongCounter>(0), FileSource::Append(), path_factory, pool);
        RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>> writer(
            std::numeric_limits<int64_t>::max(), /*target_file_row_num=*/2, factory);
        for (const std::string json :
             {R"([[100, "a"], [102, "b"]])", R"([[200, "c"], [203, "d"]])"}) {
            const std::shared_ptr<arrow::Array> array =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
                    .ValueOrDie();
            ::ArrowArray batch;
            ASSERT_TRUE(arrow::ExportArray(*array, &batch).ok());
            ASSERT_OK(writer.Write(&batch));
        }
        ASSERT_OK(writer.Close());
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataFileMeta>> files, writer.GetResult());
        ASSERT_EQ(2, files.size());
        auto cache_manager = std::make_shared<CacheManager>(1024, 0.0);
        const std::shared_ptr<arrow::StructArray> keys = checked_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({schema->field(1)}),
                                                      R"([["a"], ["b"], ["c"], ["d"]])")
                .ValueOrDie());
        const std::vector<std::vector<int64_t>> expected_offsets = {{100, 102}, {200, 203}};
        for (size_t index = 0; index < files.size(); ++index) {
            const std::shared_ptr<DataFileMeta>& file = files[index];
            ASSERT_EQ(2, file->row_count);
            ASSERT_EQ(index_storage == "external" ? 2 : 1, file->extra_files.size());
            ASSERT_EQ(index_storage == "embedded", file->embedded_index != nullptr);
            ASSERT_EQ(file->file_name + ".offset", file->extra_files.back().value());
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
                DataFileKeyOffsetIndexReader::Create(32, cache_manager, schema->field(1), file,
                                                     path_factory, options.GetFileSystem(), pool));
            ASSERT_OK_AND_ASSIGN(RoaringBitmap64 offsets, reader->LookupOffsets(keys));
            ASSERT_EQ(2, offsets.Cardinality());
            for (int64_t offset : expected_offsets[index]) {
                ASSERT_TRUE(offsets.Contains(offset));
            }
            for (const std::string& path : path_factory->CollectFiles(file)) {
                ASSERT_OK_AND_ASSIGN(bool exists, options.GetFileSystem()->Exists(path));
                ASSERT_TRUE(exists);
            }
        }
        // A rolling writer must retain sidecar paths in the closed writers' abort executors.
        writer.Abort();
        for (const std::shared_ptr<DataFileMeta>& file : files) {
            for (const std::string& path : path_factory->CollectFiles(file)) {
                ASSERT_OK_AND_ASSIGN(bool exists, options.GetFileSystem()->Exists(path));
                ASSERT_FALSE(exists);
            }
        }
    }
}

TEST(DataFileWriterBaseTest, OffsetIndexFailureCleansCurrentAndPreviouslyRolledFiles) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto fs = std::make_shared<FailingOffsetFileSystem>();
    const std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field(SpecialFields::RealtimeOffset().Name(), arrow::int64()),
                       arrow::field("id", arrow::int64())});
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::FILE_FORMAT, "parquet"},
                                               {Options::REALTIME_DEDUPLICATE_KEY_FIELDS, "id"},
                                               {"file-index.bitmap.columns", "id"},
                                               {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "0"}},
                                              fs));
    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "parquet", "data-", nullptr));
    auto factory = std::make_shared<AppendDataFileWriterFactory>(
        options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
        std::make_shared<LongCounter>(0), FileSource::Append(), path_factory, GetDefaultPool());
    RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>> writer(
        std::numeric_limits<int64_t>::max(), /*target_file_row_num=*/1, factory);
    for (int32_t index = 0; index < 2; ++index) {
        const std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                      R"([[7, 1]])")
                .ValueOrDie();
        ::ArrowArray batch;
        ASSERT_TRUE(arrow::ExportArray(*array, &batch).ok());
        if (index == 0) {
            ASSERT_OK(writer.Write(&batch));
        } else {
            ASSERT_NOK_WITH_MSG(writer.Write(&batch), "injected offset index create failure");
        }
    }
    ASSERT_EQ(2, fs->offset_files);
    std::vector<BasicFileStatus> files;
    ASSERT_OK(fs->ListDir(dir->Str(), &files));
    ASSERT_TRUE(files.empty());
}

class DataFileWriterStatsTest : public ::testing::TestWithParam<std::string> {
 public:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        pool_ = GetDefaultPool();
        file_system_ = std::make_shared<OpenCountingFileSystem>();
        ASSERT_OK_AND_ASSIGN(
            options_, CoreOptions::FromMap({{Options::FILE_FORMAT, GetParam()}}, file_system_));
        path_factory_ = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory_->Init(dir_->Str(), GetParam(), "data-", nullptr));
    }

    int32_t ExpectedOpenCount() const {
        return GetParam() == "parquet" ? 0 : 1;
    }

    Result<ColumnStatsVector> ReadBackStats(const std::shared_ptr<arrow::Schema>& file_schema,
                                            const std::string& path) const {
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema, &c_schema));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatStatsExtractor> stats_extractor,
                               options_.GetFileFormat()->CreateStatsExtractor(&c_schema));
        return stats_extractor->Extract(file_system_, path, pool_);
    }

 protected:
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<OpenCountingFileSystem> file_system_;
    CoreOptions options_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
};

TEST_P(DataFileWriterStatsTest, AppendWriter) {
    using AppendFileWriter = SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>;
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::utf8()),
                       arrow::field("f2", arrow::float64())});
    AppendDataFileWriterFactory writer_factory(options_, /*schema_id=*/0, schema,
                                               /*write_cols=*/std::nullopt,
                                               std::make_shared<LongCounter>(0),
                                               FileSource::Append(), path_factory_, pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<AppendFileWriter> writer, writer_factory.CreateWriter());

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), R"([
            [1, "b", 1.5],
            [null, "a", -0.5],
            [3, null, 2.5]
        ])")
            .ValueOrDie();
    ::ArrowArray batch;
    ASSERT_TRUE(arrow::ExportArray(*array, &batch).ok());
    ASSERT_OK(writer->Write(&batch));
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> file_meta, writer->GetResult());
    ASSERT_EQ(ExpectedOpenCount(), file_system_->open_count);

    ASSERT_OK_AND_ASSIGN(ColumnStatsVector read_back_stats,
                         ReadBackStats(schema, writer->GetPath()));
    ASSERT_OK_AND_ASSIGN(SimpleStats expected_value_stats,
                         SimpleStatsConverter::ToBinary(read_back_stats, pool_.get()));
    ASSERT_EQ(expected_value_stats, file_meta->value_stats);
}

TEST_P(DataFileWriterStatsTest, KeyValueWriter) {
    using KeyValueFileWriter = SingleFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>;
    std::shared_ptr<arrow::Schema> schema = SpecialFields::CompleteSequenceAndValueKindField(
        arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::utf8())}));
    KeyValueDataFileWriterFactory writer_factory(options_, /*schema_id=*/0, schema, /*level=*/0,
                                                 FileSource::Append(), /*primary_keys=*/{"k"},
                                                 path_factory_, /*create_stats_extractor=*/true,
                                                 /*is_changelog=*/false, pool_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<KeyValueFileWriter> writer, writer_factory.CreateWriter());

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), R"([
            [0, 0, 1, "b"],
            [1, 0, 2, null],
            [2, 0, 3, "a"]
        ])")
            .ValueOrDie();
    KeyValueBatch batch;
    batch.min_sequence_number = 0;
    batch.max_sequence_number = 2;
    batch.min_key = BinaryRowGenerator::GenerateRowPtr({1}, pool_.get());
    batch.max_key = BinaryRowGenerator::GenerateRowPtr({3}, pool_.get());
    batch.batch = std::make_unique<::ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*array, batch.batch.get()).ok());
    ASSERT_OK(writer->Write(std::move(batch)));
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> file_meta, writer->GetResult());
    ASSERT_EQ(ExpectedOpenCount(), file_system_->open_count);

    ASSERT_OK_AND_ASSIGN(ColumnStatsVector read_back_stats,
                         ReadBackStats(schema, writer->GetPath()));
    ColumnStatsVector key_column_stats = {read_back_stats[schema->GetFieldIndex("k")]};
    ColumnStatsVector value_column_stats(
        read_back_stats.begin() + SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT,
        read_back_stats.end());
    ASSERT_OK_AND_ASSIGN(SimpleStats expected_key_stats,
                         SimpleStatsConverter::ToBinary(key_column_stats, pool_.get()));
    ASSERT_EQ(expected_key_stats, file_meta->key_stats);
    ASSERT_OK_AND_ASSIGN(SimpleStats expected_value_stats,
                         SimpleStatsConverter::ToBinary(value_column_stats, pool_.get()));
    ASSERT_EQ(expected_value_stats, file_meta->value_stats);
}

INSTANTIATE_TEST_SUITE_P(FileFormat, DataFileWriterStatsTest, ::testing::Values("parquet", "orc"));

}  // namespace paimon::test
