/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may obtain a copy of the License at
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
#include "paimon/common/reader/warmup_file_batch_reader.h"

#include <functional>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/io/cache_input_stream.h"
#include "paimon/common/reader/data_file_reader_factory.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/executor.h"
#include "paimon/format/reader_builder.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/gated_async_input_stream.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class ObservedReader : public MockFileBatchReader {
 public:
    ObservedReader(const std::shared_ptr<arrow::Array>& data,
                   const std::shared_ptr<InputStream>& stream)
        : MockFileBatchReader(data, data->type(), /*read_batch_size=*/2), stream_(stream) {
        EnableRandomizeBatchSize(false);
        if (auto cache_stream = std::dynamic_pointer_cast<CacheInputStream>(stream)) {
            read_ahead_ = cache_stream->GetReadAhead();
        }
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        ++read_count;
        last_read_thread = std::this_thread::get_id();
        return Read();
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> PreBufferRange() override {
        PAIMON_RETURN_NOT_OK(range_status);
        return std::vector<std::pair<uint64_t, uint64_t>>{{range_offset_, range_length}};
    }

    Status PrepareLateRanges(uint64_t offset) {
        uint64_t previous_offset = range_offset_;
        range_offset_ = offset;
        Status status = read_ahead_ ? read_ahead_->PrepareRanges(this) : Status::OK();
        range_offset_ = previous_offset;
        return status;
    }

    uint32_t read_count = 0;
    std::thread::id last_read_thread;
    bool read_bytes = false;
    uint64_t range_length = 4;
    uint64_t read_stride = 0;
    Status range_status;

 private:
    Result<ReadBatchWithBitmap> Read() {
        if (read_bytes) {
            char bytes[4];
            PAIMON_ASSIGN_OR_RAISE(int64_t size,
                                   stream_->Read(bytes, 4, (read_count - 1) * read_stride));
            if (size != 4) {
                return Status::IOError("short observed read");
            }
        }
        return MockFileBatchReader::NextBatchWithBitmap();
    }

    std::shared_ptr<InputStream> stream_;
    std::shared_ptr<FileReadAhead> read_ahead_;
    uint64_t range_offset_ = 0;
};

class ObservedBuilder : public ReaderBuilder {
 public:
    explicit ObservedBuilder(std::shared_ptr<arrow::Array> data) : data_(std::move(data)) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& stream) const override {
        auto reader = std::make_unique<ObservedReader>(data_, stream);
        last_reader = reader.get();
        return reader;
    }

    mutable ObservedReader* last_reader = nullptr;

 private:
    std::shared_ptr<arrow::Array> data_;
};

class GatedFileSystem : public MockFileSystem {
 public:
    class Stream : public MockInputStream {
     public:
        explicit Stream(std::shared_ptr<GatedAsyncInputStream> gated) : gated_(std::move(gated)) {}
        void ReadAsync(char* buffer, int64_t size, int64_t offset,
                       std::function<void(Status)>&& callback) override {
            gated_->ReadAsync(buffer, size, offset, std::move(callback));
        }

     private:
        std::shared_ptr<GatedAsyncInputStream> gated_;
    };

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return std::make_unique<Stream>(gated);
    }

    std::shared_ptr<GatedAsyncInputStream> gated =
        std::make_shared<GatedAsyncInputStream>(std::make_shared<MockInputStream>());
};

}  // namespace

class WarmupFileBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        type_ = arrow::struct_({arrow::field("v", arrow::int32())});
        data_ = arrow::ipc::internal::json::ArrayFromJSON(type_, "[[0],[1],[2],[3],[4],[5]]")
                    .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(executor_, CreateDefaultExecutor(1));
    }

    Result<std::unique_ptr<WarmupFileBatchReader>> Open(
        WarmupLevel level, bool cache_enabled,
        const std::shared_ptr<FileSystem>& fs = std::make_shared<MockFileSystem>()) {
        ObservedBuilder builder(data_);
        PAIMON_ASSIGN_OR_RAISE(FileReadAhead::OpenedReaders opened,
                               FileReadAhead::Open("dummy", 0, &builder, fs, 1, executor_,
                                                   cache_enabled, CacheConfig(), true, pool_));
        observed_ = checked_pointer_cast<ObservedReader>(opened.readers[0]);
        return std::make_unique<WarmupFileBatchReader>(opened.readers[0], opened.read_ahead, level);
    }

    Status Configure(FileBatchReader* reader) {
        ArrowSchema schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportSchema(*arrow::schema(type_->fields()), &schema));
        return reader->SetReadSchema(&schema, nullptr, std::nullopt);
    }

    uint64_t Counter(FileBatchReader* reader, const char* name) {
        EXPECT_OK_AND_ASSIGN(uint64_t value, reader->GetReaderMetrics()->GetCounter(name));
        return value;
    }

    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<arrow::Array> data_;
    std::shared_ptr<ObservedReader> observed_;
};

class SparseMappingReader : public MockFileBatchReader {
 public:
    explicit SparseMappingReader(const std::shared_ptr<arrow::Array>& data)
        : MockFileBatchReader(data, data->type(), 4) {
        EnableRandomizeBatchSize(false);
    }
    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        compact_ = false;
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch,
                               MockFileBatchReader::NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch)) {
            return batch;
        }
        batch.second = RoaringBitmap32();
        batch.second.Add(1);
        batch.second.Add(3);
        return batch;
    }
    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch, NextBatchWithBitmap());
        PAIMON_ASSIGN_OR_RAISE(
            ReadBatch result,
            ReaderUtils::ApplyBitmapToReadBatch(std::move(batch), GetArrowPool(GetDefaultPool())));
        compact_ = true;
        return result;
    }
    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t index) const override {
        if (index >= (compact_ ? 2u : 4u)) {
            return Status::Invalid("out of range");
        }
        return compact_ ? 1 + 2 * index : index;
    }

 private:
    bool compact_ = false;
};

TEST_F(WarmupFileBatchReaderTest, PreservesSparseBitmapAndFileRowMapping) {
    auto data = data_->Slice(0, 4);
    for (WarmupLevel level : {WarmupLevel::NONE, WarmupLevel::RAW}) {
        for (bool cache_enabled : {false, true}) {
            for (bool with_bitmap : {false, true}) {
                auto inner = std::make_shared<SparseMappingReader>(data);
                std::shared_ptr<ReadAheadCache> cache;
                if (cache_enabled) {
                    cache = std::make_shared<ReadAheadCache>(std::make_shared<MockInputStream>(),
                                                             CacheConfig(), 0, pool_);
                }
                WarmupFileBatchReader reader(inner, FileReadAhead::Create(cache, "sparse"), level);
                ASSERT_OK(Configure(&reader));
                ASSERT_NOK(reader.GetPreviousBatchFileRowId(0));
                reader.Warmup();
                if (with_bitmap) {
                    ASSERT_OK_AND_ASSIGN(auto batch, reader.NextBatchWithBitmap());
                    ASSERT_EQ(batch.first.first->length, 4);
                    ASSERT_TRUE(batch.second.Contains(1));
                    ASSERT_TRUE(batch.second.Contains(3));
                    ASSERT_EQ(batch.second.Cardinality(), 2);
                    ReaderUtils::ReleaseReadBatch(std::move(batch.first));
                } else {
                    ASSERT_OK_AND_ASSIGN(auto batch, reader.NextBatch());
                    ASSERT_EQ(batch.first->length, 2);
                    auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
                    ASSERT_TRUE(imported.ok());
                    ASSERT_TRUE(imported.ValueOrDie()->Equals(
                        arrow::ipc::internal::json::ArrayFromJSON(type_, "[[1],[3]]")
                            .ValueOrDie()));
                }
                ASSERT_OK_AND_ASSIGN(uint64_t first, reader.GetPreviousBatchFileRowId(0));
                ASSERT_OK_AND_ASSIGN(uint64_t second, reader.GetPreviousBatchFileRowId(1));
                ASSERT_EQ(first, with_bitmap ? 0 : 1);
                ASSERT_EQ(second, with_bitmap ? 1 : 3);
                ASSERT_NOK(reader.GetPreviousBatchFileRowId(with_bitmap ? 4 : 2));
                ASSERT_OK_AND_ASSIGN(auto eof, reader.NextBatch());
                ASSERT_TRUE(BatchReader::IsEofBatch(eof));
                ASSERT_NOK(reader.GetPreviousBatchFileRowId(0));
                ASSERT_OK(Configure(&reader));
                ASSERT_NOK(reader.GetPreviousBatchFileRowId(0));
                reader.Close();
                ASSERT_NOK(reader.GetPreviousBatchFileRowId(0));
            }
        }
    }
}

TEST_F(WarmupFileBatchReaderTest, RawWarmsBytesWithoutDecodingAndIsIdempotent) {
    auto fs = std::make_shared<GatedFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, true, fs));
    ScopeGuard release([&fs]() { fs->gated->ReleaseAll(); });
    reader->Warmup();
    ASSERT_EQ(fs->gated->AsyncReadCount(), 0);
    ASSERT_OK(Configure(reader.get()));
    reader->Warmup();
    reader->Warmup();
    ASSERT_EQ(fs->gated->AsyncReadCount(), 1);
    ASSERT_EQ(observed_->read_count, 0);
    fs->gated->ReleaseAll();
    observed_->read_bytes = true;
    ASSERT_OK_AND_ASSIGN(auto batch, reader->NextBatchWithBitmap());
    ReaderUtils::ReleaseReadBatch(std::move(batch.first));
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::READ_HITS), 1);
    reader->Close();
    reader->Warmup();
    ASSERT_EQ(fs->gated->AsyncReadCount(), 1);
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::READ_HITS), 1);
}

TEST_F(WarmupFileBatchReaderTest, NoneAndRawWithoutCacheDoNotStartWork) {
    for (const auto& config :
         {std::make_pair(WarmupLevel::NONE, true), std::make_pair(WarmupLevel::RAW, false)}) {
        ASSERT_OK_AND_ASSIGN(auto reader, Open(config.first, config.second));
        ASSERT_OK(Configure(reader.get()));
        reader->Warmup();
        ASSERT_EQ(observed_->read_count, 0);
        if (config.second) {
            ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 0);
        }
        ASSERT_OK_AND_ASSIGN(auto result, ReadResultCollector::CollectResult(reader.get()));
        ASSERT_TRUE(result->Equals(std::make_shared<arrow::ChunkedArray>(data_)));
        if (config.second) {
            ASSERT_GT(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 0);
        }
    }
}

TEST_F(WarmupFileBatchReaderTest, WarmupNeverDecodesAndReadsStaySynchronous) {
    for (WarmupLevel level : {WarmupLevel::NONE, WarmupLevel::RAW}) {
        for (bool cache_enabled : {false, true}) {
            ASSERT_OK_AND_ASSIGN(auto reader, Open(level, cache_enabled));
            ASSERT_OK(Configure(reader.get()));
            reader->Warmup();
            reader->Warmup();
            ASSERT_EQ(observed_->read_count, 0);
            ASSERT_NOK(reader->GetPreviousBatchFileRowId(0));
            ASSERT_OK_AND_ASSIGN(auto first, reader->NextBatchWithBitmap());
            ASSERT_EQ(observed_->last_read_thread, std::this_thread::get_id());
            ASSERT_EQ(observed_->read_count, 1);
            ASSERT_OK_AND_ASSIGN(uint64_t row_id, reader->GetPreviousBatchFileRowId(1));
            ASSERT_EQ(row_id, 1);
            ReaderUtils::ReleaseReadBatch(std::move(first.first));
            reader->Warmup();
            ASSERT_EQ(observed_->read_count, 1);
            ASSERT_OK_AND_ASSIGN(auto second, reader->NextBatchWithBitmap());
            ASSERT_EQ(observed_->last_read_thread, std::this_thread::get_id());
            ASSERT_EQ(observed_->read_count, 2);
            ASSERT_OK_AND_ASSIGN(row_id, reader->GetPreviousBatchFileRowId(0));
            ASSERT_EQ(row_id, 2);
            ReaderUtils::ReleaseReadBatch(std::move(second.first));
            ASSERT_OK_AND_ASSIGN(double enabled,
                                 reader->GetReaderMetrics()->GetGauge(PrefetchMetrics::ENABLED));
            ASSERT_EQ(enabled, 0.0);
        }
    }
}

TEST_F(WarmupFileBatchReaderTest, SchemaResetRearmsRawWarmup) {
    ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, true));
    ASSERT_OK(Configure(reader.get()));
    reader->Warmup();
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 1);
    ASSERT_EQ(observed_->read_count, 0);
    ASSERT_OK(Configure(reader.get()));
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 0);
    reader->Warmup();
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 1);
    ASSERT_EQ(observed_->read_count, 0);
    ASSERT_OK_AND_ASSIGN(auto result, ReadResultCollector::CollectResult(reader.get()));
    ASSERT_TRUE(result->Equals(std::make_shared<arrow::ChunkedArray>(data_)));
}

TEST_F(WarmupFileBatchReaderTest, ReadFailureIsTerminalUntilSchemaReset) {
    ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, false));
    ASSERT_OK(Configure(reader.get()));
    observed_->SetNextBatchStatus(Status::IOError("first batch failed"));
    reader->Warmup();
    ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(), "first batch failed");
    observed_->SetNextBatchStatus(Status::OK());
    ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(), "first batch failed");
    ASSERT_OK(Configure(reader.get()));
    ASSERT_OK_AND_ASSIGN(auto result, ReadResultCollector::CollectResult(reader.get()));
    ASSERT_TRUE(result->Equals(std::make_shared<arrow::ChunkedArray>(data_)));
}

TEST_F(WarmupFileBatchReaderTest, EmptyFileAndRawWarmupCloseSafely) {
    for (bool empty : {false, true}) {
        if (empty) {
            data_ = data_->Slice(0, 0);
        }
        ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, true));
        ASSERT_OK(Configure(reader.get()));
        reader->Warmup();
        if (empty) {
            ASSERT_OK_AND_ASSIGN(auto eof, reader->NextBatchWithBitmap());
            ASSERT_TRUE(BatchReader::IsEofBatch(eof));
        }
        reader->Close();
        reader->Close();
        reader->Warmup();
        ASSERT_NOK(reader->NextBatchWithBitmap());
    }
    ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, true));
    ASSERT_OK(Configure(reader.get()));
    reader->Warmup();
    reader.reset();
}

TEST_F(WarmupFileBatchReaderTest, RawRangeAndAsyncFailuresAreDeferredUntilRead) {
    {
        ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, true));
        ASSERT_OK(Configure(reader.get()));
        observed_->range_status = Status::IOError("range discovery failed");
        reader->Warmup();
        ASSERT_EQ(observed_->read_count, 0);
        ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(), "range discovery failed");
    }
    auto fs = std::make_shared<GatedFileSystem>();
    ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, true, fs));
    ScopeGuard release([&fs]() { fs->gated->ReleaseAll(); });
    ASSERT_OK(Configure(reader.get()));
    observed_->read_bytes = true;
    reader->Warmup();
    ASSERT_EQ(fs->gated->AsyncReadCount(), 1);
    fs->gated->FailAll(Status::IOError("async warmup failed"));
    ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(), "async warmup failed");
}

TEST_F(WarmupFileBatchReaderTest, SynchronousAndAdaptiveFallbackBoundRetainedBytes) {
    constexpr uint64_t kMiB = 1024 * 1024;
    for (bool adaptive_fallback : {false, true}) {
        for (WarmupLevel level : {WarmupLevel::NONE, WarmupLevel::RAW}) {
            auto pool = std::shared_ptr<MemoryPool>(GetMemoryPool());
            ObservedBuilder builder(data_);
            DataFileReadOptions options;
            options.warmup_level = level;
            options.prefetch_enabled = adaptive_fallback;
            options.adaptive_prefetch_strategy = true;
            options.prefetch_max_parallel_num = 1;
            options.prefetch_batch_count = 1;
            options.read_batch_size = 1;
            options.cache_config.SetRangeSizeLimit(kMiB);
            options.cache_config.SetHoleSizeLimit(0);
            options.cache_config.SetPreBufferLimit(2 * kMiB);
            options.cache_config.SetBlockSize(0);
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<FileBatchReader> reader,
                DataFileReaderFactory::Open("parquet", "bounded-sync", 32 * kMiB, &builder, options,
                                            std::make_shared<MockFileSystem>(), executor_, pool,
                                            GetArrowPool(pool)));
            ASSERT_NE(builder.last_reader, nullptr);
            builder.last_reader->range_length = 32 * kMiB;
            builder.last_reader->read_stride = 8 * kMiB;
            builder.last_reader->read_bytes = true;
            ASSERT_OK(Configure(reader.get()));
            reader->Warmup();
            uint64_t row_count = 0;
            while (true) {
                ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch,
                                     reader->NextBatchWithBitmap());
                if (BatchReader::IsEofBatch(batch)) {
                    break;
                }
                row_count += batch.first.first->length;
                ReaderUtils::ReleaseReadBatch(std::move(batch.first));
            }
            ASSERT_EQ(row_count, 6);
            ASSERT_LE(pool->MaxMemoryUsage(), 2 * kMiB);
            reader->Close();
            ASSERT_EQ(pool->CurrentUsage(), 0);
        }
    }
}

TEST_F(WarmupFileBatchReaderTest, InvalidInitialAndLateRangesReportFileOnRead) {
    for (bool late : {false, true}) {
        ObservedBuilder builder(data_);
        ASSERT_OK_AND_ASSIGN(FileReadAhead::OpenedReaders opened,
                             FileReadAhead::Open("range-bounds-file", late ? 4 : 2, &builder,
                                                 std::make_shared<MockFileSystem>(), 1, nullptr,
                                                 true, CacheConfig(), false, pool_));
        auto reader = std::make_unique<WarmupFileBatchReader>(opened.readers[0], opened.read_ahead,
                                                              WarmupLevel::RAW);
        ASSERT_OK(Configure(reader.get()));
        reader->Warmup();
        if (late) {
            ASSERT_NOK_WITH_MSG(builder.last_reader->PrepareLateRanges(2), "exceeds file size");
        }
        ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(), "range-bounds-file");
        ASSERT_NOK_WITH_MSG(reader->NextBatchWithBitmap(), "exceeds file size");
        ASSERT_EQ(builder.last_reader->read_count, 0);
        reader->Close();
    }
}

TEST_F(WarmupFileBatchReaderTest, LateRangesAreRegisteredOnlyInActiveGeneration) {
    ASSERT_OK_AND_ASSIGN(auto reader, Open(WarmupLevel::RAW, true));
    ASSERT_OK(Configure(reader.get()));
    ASSERT_OK(observed_->PrepareLateRanges(16));
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 0);
    reader->Warmup();
    ASSERT_OK(observed_->PrepareLateRanges(16));
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 2);
    ASSERT_OK(observed_->PrepareLateRanges(16));
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 2);
    ASSERT_OK(Configure(reader.get()));
    ASSERT_OK(observed_->PrepareLateRanges(32));
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 0);
    reader->Warmup();
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 1);
    reader->Close();
    ASSERT_OK(observed_->PrepareLateRanges(32));
    ASSERT_EQ(Counter(reader.get(), ReadAheadCacheMetrics::IO_COUNT), 1);
}

}  // namespace paimon::test
