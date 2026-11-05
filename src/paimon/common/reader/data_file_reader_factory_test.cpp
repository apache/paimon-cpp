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
#include "paimon/common/reader/data_file_reader_factory.h"

#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/io/cache_input_stream.h"
#include "paimon/common/reader/delegating_prefetch_reader.h"
#include "paimon/common/reader/warmup_file_batch_reader.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/mock/mock_format_reader_builder.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/prefetch_cache_config.h"

namespace paimon::test {

// Deliberately expose only FileBatchReader, even though the test data source can report ranges.
class PlainFileBatchReader : public FileBatchReader {
 public:
    explicit PlainFileBatchReader(std::unique_ptr<FileBatchReader> reader)
        : reader_(std::move(reader)) {}

    Result<ReadBatch> NextBatch() override {
        return reader_->NextBatch();
    }
    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }
    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        return reader_->GetFileSchema();
    }
    Status SetReadSchema(::ArrowSchema* schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& bitmap) override {
        return reader_->SetReadSchema(schema, predicate, bitmap);
    }
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t row) const override {
        return reader_->GetPreviousBatchFileRowId(row);
    }
    Result<uint64_t> GetNumberOfRows() const override {
        return reader_->GetNumberOfRows();
    }
    bool SupportPreciseBitmapSelection() const override {
        return reader_->SupportPreciseBitmapSelection();
    }
    void Close() override {
        reader_->Close();
    }

 private:
    std::unique_ptr<FileBatchReader> reader_;
};

class CapabilityReaderBuilder : public MockFormatReaderBuilder {
 public:
    CapabilityReaderBuilder(const std::shared_ptr<arrow::Array>& data,
                            const std::shared_ptr<arrow::DataType>& type, bool capable)
        : MockFormatReaderBuilder(data, type, /*read_batch_size=*/2), capable_(capable) {
        SetPreBufferRangesPerSchema({{{0, 4}}});
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& input) const override {
        ++build_count;
        stream = std::dynamic_pointer_cast<CacheInputStream>(input);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                               MockFormatReaderBuilder::Build(input));
        checked_cast<MockFileBatchReader*>(reader.get())->EnableRandomizeBatchSize(false);
        if (!capable_) {
            reader = std::make_unique<PlainFileBatchReader>(std::move(reader));
        }
        original_reader = reader.get();
        return reader;
    }

    mutable uint32_t build_count = 0;
    mutable FileBatchReader* original_reader = nullptr;
    mutable std::shared_ptr<CacheInputStream> stream;

 private:
    bool capable_;
};

class CountingOpenFileSystem : public MockFileSystem {
 public:
    using MockFileSystem::Open;
    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        if (++open_count == 2 && fail_cache_open) {
            return Status::IOError("cache stream open failed");
        }
        return MockFileSystem::Open(path);
    }
    mutable uint32_t open_count = 0;
    bool fail_cache_open = false;
};

TEST(DataFileReaderFactoryTest, PlainReaderCapabilityOverridesFormatName) {
    auto type = arrow::struct_({arrow::field("v", arrow::int32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[1],[2]]").ValueOrDie();
    auto pool = GetDefaultPool();
    DataFileReadOptions options;
    for (const char* format :
         {"custom", "parquet", "orc", "avro", "blob", "lance", "mosaic", "vortex"}) {
        SCOPED_TRACE(format);
        CapabilityReaderBuilder builder(data, type, /*capable=*/false);
        auto fs = std::make_shared<CountingOpenFileSystem>();
        fs->fail_cache_open = true;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                             DataFileReaderFactory::Open(format, "dummy", 0, &builder, options, fs,
                                                         nullptr, pool, GetArrowPool(pool)));
        ASSERT_EQ(reader.get(), builder.original_reader);
        ASSERT_EQ(builder.build_count, 1);
        ASSERT_EQ(fs->open_count, 1);
        ASSERT_NE(builder.stream, nullptr);
        ASSERT_OK_AND_ASSIGN(bool cache_active, builder.stream->EnableCache());
        ASSERT_FALSE(cache_active);
        reader->Warmup();
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        ASSERT_EQ(batch.first->length, 2);
        batch.first->release(batch.first.get());
        batch.second->release(batch.second.get());
        ASSERT_EQ(fs->open_count, 1);
    }
}

TEST(DataFileReaderFactoryTest, CapableReaderSupportsReadAheadRegardlessOfFormatName) {
    auto type = arrow::struct_({arrow::field("v", arrow::int32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[1],[2]]").ValueOrDie();
    auto pool = GetDefaultPool();
    for (const char* format : {"custom", "avro", "parquet", "orc"}) {
        for (bool cache_enabled : {false, true}) {
            SCOPED_TRACE(format);
            SCOPED_TRACE(cache_enabled);
            CapabilityReaderBuilder builder(data, type, /*capable=*/true);
            auto fs = std::make_shared<CountingOpenFileSystem>();
            DataFileReadOptions options;
            options.read_ahead_cache_enabled = cache_enabled;
            options.prefetch_max_parallel_num = 0;
            options.prefetch_batch_count = 0;
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<FileBatchReader> reader,
                DataFileReaderFactory::Open(format, "dummy", 0, &builder, options, fs, nullptr,
                                            pool, GetArrowPool(pool)));
            ASSERT_NE(dynamic_cast<WarmupFileBatchReader*>(reader.get()), nullptr);
            ASSERT_EQ(builder.build_count, 1);
            ASSERT_EQ(fs->open_count, cache_enabled ? 2 : 1);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowSchema> schema, reader->GetFileSchema());
            ASSERT_OK(reader->SetReadSchema(schema.get(), nullptr, std::nullopt));
            reader->Warmup();
            if (cache_enabled) {
                ASSERT_OK_AND_ASSIGN(uint64_t io_count, reader->GetReaderMetrics()->GetCounter(
                                                            ReadAheadCacheMetrics::IO_COUNT));
                ASSERT_EQ(io_count, 1);
            }
            ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
            ASSERT_EQ(batch.first->length, 2);
            batch.first->release(batch.first.get());
            batch.second->release(batch.second.get());
            reader->Close();
        }
    }
}

TEST(DataFileReaderFactoryTest, CapableReaderPropagatesCacheOpenFailureWithoutRebuilding) {
    auto type = arrow::struct_({arrow::field("v", arrow::int32())});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(type, "[[1],[2]]").ValueOrDie();
    CapabilityReaderBuilder builder(data, type, /*capable=*/true);
    auto fs = std::make_shared<CountingOpenFileSystem>();
    fs->fail_cache_open = true;
    auto pool = GetDefaultPool();
    ASSERT_NOK_WITH_MSG(DataFileReaderFactory::Open("custom", "dummy", 0, &builder, {}, fs, nullptr,
                                                    pool, GetArrowPool(pool)),
                        "cache stream open failed");
    ASSERT_EQ(builder.build_count, 1);
    ASSERT_EQ(fs->open_count, 2);
}

/// `Open()` is the one place a production read builds the prefetching reader, and a level that is
/// not forwarded here fails nothing: the reader warms up however its own default says, and the only
/// symptom is latency and memory a query never asked for.
TEST(DataFileReaderFactoryTest, OpenForwardsWarmupLevelToPrefetchReader) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    auto mock_fs = std::make_shared<MockFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor,
                         CreateDefaultExecutor(/*thread_count=*/1));

    std::shared_ptr<arrow::DataType> data_type =
        arrow::struct_({arrow::field("f1", arrow::int32())});
    std::shared_ptr<arrow::Array> data =
        arrow::StructArray::Make(
            {arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 3]").ValueOrDie()},
            data_type->fields())
            .ValueOrDie();

    for (WarmupLevel level : {WarmupLevel::NONE, WarmupLevel::RAW}) {
        MockFormatReaderBuilder reader_builder(data, data_type, /*read_batch_size=*/10);
        DataFileReadOptions read_options;
        read_options.read_batch_size = 10;
        read_options.prefetch_enabled = true;
        read_options.prefetch_max_parallel_num = 1;
        read_options.prefetch_batch_count = 2;
        // No file backs the mock file system, so keep the read-ahead cache out of the read.
        read_options.read_ahead_cache_enabled = false;
        read_options.warmup_level = level;

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                             DataFileReaderFactory::Open(
                                 "parquet", /*file_path=*/"DUMMY", /*file_size=*/0, &reader_builder,
                                 read_options, mock_fs, executor, pool, GetArrowPool(pool)));

        // `Open()` hands back the delegating wrapper. The level has to have arrived on the
        // prefetching reader underneath it, because that reader's Warmup() is what it governs.
        auto* delegating_reader = dynamic_cast<DelegatingPrefetchReader*>(reader.get());
        ASSERT_NE(delegating_reader, nullptr);
        EXPECT_EQ(level, delegating_reader->prefetch_reader_->warmup_level_);
    }
}

}  // namespace paimon::test
