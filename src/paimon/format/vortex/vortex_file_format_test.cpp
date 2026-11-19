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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Round-trip unit tests for the Vortex file format: write, then read back via vortex-ffi.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/key_value_metadata.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/vortex/vortex_defs.h"
#include "paimon/format/vortex/vortex_file_batch_reader.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::vortex::test {

class VortexFileFormatTest : public ::testing::Test {
 public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(format_,
                             FileFormatFactory::Get("vortex", {{"file.format", "vortex"}}));
        file_system_ = std::make_shared<LocalFileSystem>();
        directory_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_NE(directory_, nullptr);
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
    }

    // Writes `array` (a struct array matching `schema`) to a Vortex file at `path`, pushing it in
    // `batch_size` slices exactly like the production write path does.
    Status WriteFile(const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
                     const std::shared_ptr<arrow::Array>& array, int32_t batch_size) const {
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> writer_builder,
                               format_->CreateWriterBuilder(&ffi_schema, batch_size));
        writer_builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                               file_system_->Create(path, /*overwrite=*/false));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                               writer_builder->Build(output, "zstd"));
        for (int64_t offset = 0; offset < array->length(); offset += batch_size) {
            std::shared_ptr<arrow::Array> slice = array->Slice(offset, batch_size);
            ::ArrowArray ffi_array = {};
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*slice, &ffi_array));
            PAIMON_RETURN_NOT_OK(writer->AddBatch(&ffi_array));
        }
        PAIMON_RETURN_NOT_OK(writer->Finish());
        return output->Close();
    }

    // Reads the whole Vortex file at `path` back as a single concatenated struct array, checking
    // the reported row count and that no batch exceeds the configured batch size.
    Result<std::shared_ptr<arrow::Array>> ReadFile(const std::string& path,
                                                   const std::shared_ptr<arrow::Schema>& schema,
                                                   int32_t batch_size,
                                                   uint64_t expected_row_count) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                               format_->CreateReaderBuilder(batch_size));
        reader_builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system_->Open(path));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                               reader_builder->Build(input));
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                                   /*selection_bitmap=*/std::nullopt));
        PAIMON_ASSIGN_OR_RAISE(uint64_t total_rows, reader->GetNumberOfRows());
        if (total_rows != expected_row_count) {
            return Status::Invalid("unexpected Vortex row count");
        }

        std::vector<std::shared_ptr<arrow::Array>> batches;
        uint64_t rows_seen = 0;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            PAIMON_RETURN_NOT_OK(paimon::test::ReadResultCollector::CheckBatchOffset(batch));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> arrow_batch,
                arrow::ImportArray(batch.first.get(), batch.second.get()));
            if (arrow_batch->length() > batch_size) {
                return Status::Invalid("Vortex read batch exceeds configured batch size");
            }
            rows_seen += static_cast<uint64_t>(arrow_batch->length());
            batches.push_back(std::move(arrow_batch));
        }
        if (rows_seen != expected_row_count) {
            return Status::Invalid("Vortex batches do not contain all rows");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> result,
                                          arrow::Concatenate(batches, arrow_pool_.get()));
        return result;
    }

    void AssertReadWithBatchSizes(const std::string& path,
                                  const std::shared_ptr<arrow::Schema>& schema,
                                  const std::shared_ptr<arrow::Array>& expected,
                                  std::initializer_list<int32_t> batch_sizes) const {
        for (int32_t batch_size : batch_sizes) {
            SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<arrow::Array> actual,
                ReadFile(path, schema, batch_size,
                         /*expected_row_count=*/static_cast<uint64_t>(expected->length())));
            ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n"
                                                  << expected->ToString();
        }
    }

    Result<std::unique_ptr<FileBatchReader>> CreatePredicateReader(
        const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<Predicate>& predicate, const std::string& max_values) const {
        std::map<std::string, std::string> options = {{"file.format", "vortex"}};
        if (!max_values.empty()) {
            options.emplace(kVortexReadInPushdownMaxValues, max_values);
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileFormat> format,
                               FileFormatFactory::Get("vortex", options));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> builder,
                               format->CreateReaderBuilder(/*batch_size=*/2));
        builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system_->Open(path));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader, builder->Build(input));
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        PAIMON_RETURN_NOT_OK(
            reader->SetReadSchema(&ffi_schema, predicate, /*selection_bitmap=*/std::nullopt));
        return reader;
    }

    // This is the native file reader, before the SDK residual predicate. A declined membership
    // predicate intentionally returns a superset, including null rows, for the caller to filter.
    Result<std::vector<uint64_t>> CollectNativeRowIds(
        std::unique_ptr<FileBatchReader> reader) const {
        std::vector<uint64_t> row_ids;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            PAIMON_RETURN_NOT_OK(paimon::test::ReadResultCollector::CheckBatchOffset(batch));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> array,
                arrow::ImportArray(batch.first.get(), batch.second.get()));
            for (int64_t i = 0; i < array->length(); ++i) {
                PAIMON_ASSIGN_OR_RAISE(uint64_t row_id,
                                       reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
                row_ids.push_back(row_id);
            }
        }
        return row_ids;
    }

    // Verify both native candidates and the real SDK residual predicate against an independent
    // expected result. Every returned field is also checked against the source physical row.
    void AssertNativeAndResidualRows(std::unique_ptr<FileBatchReader> reader,
                                     const std::shared_ptr<Predicate>& predicate,
                                     const std::shared_ptr<arrow::Array>& source,
                                     const std::vector<uint64_t>& expected_candidates,
                                     const std::vector<uint64_t>& expected_matches) const {
        const auto residual = std::dynamic_pointer_cast<PredicateFilter>(predicate);
        ASSERT_NE(residual, nullptr);
        const auto& source_rows = checked_cast<const arrow::StructArray&>(*source);
        std::vector<uint64_t> candidates;
        std::vector<uint64_t> matches;
        while (true) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
            if (array == nullptr) {
                break;
            }
            ASSERT_TRUE(array->ValidateFull().ok());
            const auto& rows = checked_cast<const arrow::StructArray&>(*array);
            ASSERT_EQ(rows.num_fields(), source_rows.num_fields());
            ASSERT_OK_AND_ASSIGN(std::vector<char> mask, residual->Test(*array, arrow_pool_.get()));
            ASSERT_EQ(mask.size(), static_cast<size_t>(array->length()));
            for (int64_t i = 0; i < array->length(); ++i) {
                ASSERT_OK_AND_ASSIGN(uint64_t row_id,
                                     reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
                ASSERT_LT(row_id, static_cast<uint64_t>(source->length()));
                candidates.push_back(row_id);
                for (int32_t field = 0; field < rows.num_fields(); ++field) {
                    const auto actual = rows.field(field)->GetScalar(i);
                    const auto expected =
                        source_rows.field(field)->GetScalar(static_cast<int64_t>(row_id));
                    ASSERT_TRUE(actual.ok());
                    ASSERT_TRUE(expected.ok());
                    ASSERT_TRUE(actual.ValueOrDie()->Equals(*expected.ValueOrDie()));
                }
                if (mask[i] != 0) {
                    matches.push_back(row_id);
                }
            }
        }
        ASSERT_EQ(candidates, expected_candidates);
        ASSERT_EQ(matches, expected_matches);
    }

    static std::shared_ptr<arrow::Schema> ReaderTestSchema() {
        return arrow::schema(
            {arrow::field("id", arrow::int32(), false), arrow::field("name", arrow::utf8())});
    }

    Result<std::string> WriteReaderTestFile() const {
        const std::shared_ptr<arrow::Schema> schema = ReaderTestSchema();
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> data,
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_(schema->fields()),
                R"([[0,"zero"],[1,null],[2,"two"],[3,"three"],[4,null],[5,"five"]])"));
        const std::string path = PathUtil::JoinPath(directory_->Str(), "reader.vortex");
        PAIMON_RETURN_NOT_OK(WriteFile(path, schema, data, /*batch_size=*/2));
        return path;
    }

    Result<std::unique_ptr<FileBatchReader>> CreateReader(const std::string& path,
                                                          int32_t batch_size = 2) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> builder,
                               format_->CreateReaderBuilder(batch_size));
        builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system_->Open(path));
        return builder->Build(input);
    }

    Status ConfigureReader(FileBatchReader* reader, const std::shared_ptr<arrow::Schema>& schema,
                           const std::shared_ptr<Predicate>& predicate = nullptr,
                           const std::optional<RoaringBitmap32>& selection = std::nullopt) const {
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        ScopeGuard release_schema([&ffi_schema]() {
            if (ffi_schema.release != nullptr) {
                ffi_schema.release(&ffi_schema);
            }
        });
        return reader->SetReadSchema(&ffi_schema, predicate, selection);
    }

    void AssertSelectionState(const FileBatchReader* reader, uint64_t expected_rows,
                              uint64_t expected_indices) const {
        const auto& selection_state =
            checked_cast<const VortexFileBatchReader*>(reader)->selection_;
        ASSERT_EQ(selection_state.SelectedRowCount(), expected_rows);
        ASSERT_EQ(selection_state.IndexCount(), expected_indices);
    }

    Result<std::shared_ptr<arrow::Array>> ReadNextBatch(FileBatchReader* reader) const {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            return std::shared_ptr<arrow::Array>();
        }
        PAIMON_RETURN_NOT_OK(paimon::test::ReadResultCollector::CheckBatchOffset(batch));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(batch.first.get(), batch.second.get()));
        return array;
    }

 protected:
    std::shared_ptr<FileFormat> format_;
    std::shared_ptr<LocalFileSystem> file_system_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> directory_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

namespace {

// Counts Close() calls so tests can observe when the input context releases the stream.
class RecordingInputStream : public InputStream {
 public:
    Status Close() override {
        ++close_count_;
        return Status::OK();
    }
    Status Seek(int64_t offset, SeekOrigin origin) override {
        return Status::OK();
    }
    Result<int64_t> GetPos() const override {
        return 0;
    }
    Result<int64_t> Read(char* buffer, int64_t size) override {
        return 0;
    }
    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        return 0;
    }
    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        callback(Status::OK());
    }
    Result<std::string> GetUri() const override {
        return std::string("recording://test");
    }
    Result<int64_t> Length() const override {
        return 0;
    }
    int close_count() const {
        return close_count_;
    }

 private:
    int close_count_ = 0;
};

// Record callback I/O without serializing the underlying reads.
class ReadAtTrackingInputStream : public InputStream {
 public:
    explicit ReadAtTrackingInputStream(std::shared_ptr<InputStream> input)
        : input_(std::move(input)) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return input_->Seek(offset, origin);
    }
    Result<int64_t> GetPos() const override {
        return input_->GetPos();
    }
    Result<int64_t> Read(char* buffer, int64_t size) override {
        return input_->Read(buffer, size);
    }
    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        RecordRead(offset, size);
        return input_->Read(buffer, size, offset);
    }
    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        RecordRead(offset, size);
        input_->ReadAsync(buffer, size, offset, std::move(callback));
    }
    Result<std::string> GetUri() const override {
        return input_->GetUri();
    }
    Result<int64_t> Length() const override {
        return input_->Length();
    }
    Status Close() override {
        return input_->Close();
    }
    void ClearReadAtRanges() {
        std::lock_guard<std::mutex> lock(mutex_);
        read_at_ranges_.clear();
    }
    std::vector<std::pair<int64_t, int64_t>> GetReadAtRanges() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return read_at_ranges_;
    }

 private:
    void RecordRead(int64_t offset, int64_t size) {
        std::lock_guard<std::mutex> lock(mutex_);
        read_at_ranges_.emplace_back(offset, size);
    }

    std::shared_ptr<InputStream> input_;
    mutable std::mutex mutex_;
    std::vector<std::pair<int64_t, int64_t>> read_at_ranges_;
};

}  // namespace

// The stream must stay open while a read callback still owns the context, and close exactly once
// when the last owner releases it.
namespace {

// Blocks inside Write() until the test releases it, so a callback can be held in flight.
class BlockingOutputStream : public OutputStream {
 public:
    Status Close() override {
        return Status::OK();
    }
    Result<int64_t> Write(const char* buffer, int64_t size) override {
        ++writes_;
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return released_; });
        return size;
    }
    Status Flush() override {
        return Status::OK();
    }
    Result<int64_t> GetPos() const override {
        return 0;
    }
    Result<std::string> GetUri() const override {
        return std::string("blocking://test");
    }

    void Release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }
    int32_t writes() const {
        return writes_.load();
    }

 private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool released_ = false;
    std::atomic<int32_t> writes_{0};
};

}  // namespace

// An aborting writer must not close the stream under a running write callback, and a callback
// arriving after the abort must not reach the stream at all.
TEST(VortexOutputContextTest, AbortWaitsForTheInFlightWriteCallback) {
    auto stream = std::make_shared<BlockingOutputStream>();
    auto context = std::make_shared<VortexOutputContext>(stream);
    auto holder = std::make_unique<std::shared_ptr<VortexOutputContext>>(context);
    const std::string payload = "data";
    const auto* bytes = reinterpret_cast<const uint8_t*>(payload.data());
    int32_t in_flight_result = -1;
    std::thread in_flight([&] {
        in_flight_result = VortexOutputContext::Write(holder.get(), bytes, payload.size());
    });
    while (stream->writes() == 0) {
        std::this_thread::yield();
    }
    context->MarkAborted();
    std::atomic<bool> wait_returned{false};
    std::thread waiting([&] {
        context->WaitForIdleCallbacks();
        wait_returned = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(wait_returned.load());
    stream->Release();
    in_flight.join();
    waiting.join();
    EXPECT_TRUE(wait_returned.load());
    EXPECT_EQ(in_flight_result, 0);
    EXPECT_EQ(VortexOutputContext::Write(holder.get(), bytes, payload.size()), -1);
    EXPECT_EQ(stream->writes(), 1);
}

TEST(VortexInputContextTest, CloseDefersUntilLastOwnerReleases) {
    auto stream = std::make_shared<RecordingInputStream>();
    auto context = std::make_shared<VortexInputContext>(stream);
    std::shared_ptr<VortexInputContext> in_flight = context;
    context.reset();
    ASSERT_EQ(stream->close_count(), 0);
    in_flight.reset();
    ASSERT_EQ(stream->close_count(), 1);
}

TEST(VortexInputContextTest, CloseRunsWhenReaderIsLastOwner) {
    auto stream = std::make_shared<RecordingInputStream>();
    auto context = std::make_shared<VortexInputContext>(stream);
    context.reset();
    ASSERT_EQ(stream->close_count(), 1);
}

// MAP hits `unimplemented!` in Vortex, whose panic would abort across the FFI boundary.
TEST_F(VortexFileFormatTest, WriterBuilderRejectsUnsupportedTypes) {
    const std::vector<std::shared_ptr<arrow::Field>> unsupported = {
        arrow::field("m", arrow::map(arrow::utf8(), arrow::int32())),
        arrow::field(
            "r", arrow::struct_({arrow::field("m", arrow::map(arrow::utf8(), arrow::int32()))}))};
    for (const std::shared_ptr<arrow::Field>& field : unsupported) {
        std::string path = PathUtil::JoinPath(directory_->Str(), "unsupported.vortex");
        std::shared_ptr<arrow::Schema> schema = arrow::schema({field});
        ::ArrowSchema ffi_schema = {};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriterBuilder> writer_builder,
                             format_->CreateWriterBuilder(&ffi_schema, /*batch_size=*/2));
        writer_builder->WithMemoryPool(pool_);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output,
                             file_system_->Create(path, /*overwrite=*/true));
        ASSERT_NOK_WITH_MSG(writer_builder->Build(output, "zstd"),
                            "Vortex file format does not support type MAP");
    }
}

// Vortex keeps the timezone the writer stored while the read schema asks for another one; the
// instants must survive at the top level and inside nested columns.
TEST_F(VortexFileFormatTest, ReadRetypesStoredTimestampTimezone) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "timezone.vortex");
    auto fields = [](const std::string& tz) {
        auto timestamp = arrow::timestamp(arrow::TimeUnit::MILLI, tz);
        return arrow::FieldVector{
            arrow::field("ts", timestamp),
            arrow::field("r", arrow::struct_({arrow::field("ts", timestamp)})),
            arrow::field("l", arrow::list(arrow::field("element", timestamp)))};
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields("UTC"));
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields("UTC")),
            R"([[1700000000000,{"ts":1700000000000},[1700000000000,null]]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/1));

    const std::string wanted_tz = "Asia/Shanghai";
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(fields(wanted_tz));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, read_schema, /*batch_size=*/1, /*expected_row_count=*/1));
    ASSERT_TRUE(actual->type()->Equals(arrow::struct_(fields(wanted_tz))))
        << actual->type()->ToString();

    const auto& row = checked_cast<const arrow::StructArray&>(*actual);
    const auto& top = checked_cast<const arrow::TimestampArray&>(*row.field(0));
    ASSERT_FALSE(top.IsNull(0));
    ASSERT_EQ(top.Value(0), 1700000000000);
    const auto& nested = checked_cast<const arrow::StructArray&>(*row.field(1));
    ASSERT_EQ(checked_cast<const arrow::TimestampArray&>(*nested.field(0)).Value(0), 1700000000000);
    const auto& list = checked_cast<const arrow::ListArray&>(*row.field(2));
    const auto& values = checked_cast<const arrow::TimestampArray&>(*list.values());
    ASSERT_EQ(values.Value(0), 1700000000000);
    ASSERT_TRUE(values.IsNull(1));
}

// Java's LTZ schema and predicate literals use UTC. Normalize only the timezone metadata;
// changing the integer timestamps here would change the instants being stored.
TEST_F(VortexFileFormatTest, WriterNormalizesTimestampTimezonesAcrossPrecisions) {
    const std::vector<arrow::TimeUnit::type> units = {
        arrow::TimeUnit::SECOND, arrow::TimeUnit::MILLI, arrow::TimeUnit::MICRO,
        arrow::TimeUnit::NANO};
    const std::string json =
        R"([[-1000001,-1000001,-1000001],[-1,-1,-1],[null,null,0],[0,0,0],[1,1,1],[123456789,123456789,123456789]])";
    const std::vector<std::string> zones = {"Asia/Shanghai", "America/New_York"};
    for (const std::string& zone : zones) {
        for (arrow::TimeUnit::type unit : units) {
            SCOPED_TRACE(fmt::format("zone={}, unit={}", zone, static_cast<int32_t>(unit)));
            const auto make_schema = [unit](const std::string& timezone) {
                const auto metadata = arrow::key_value_metadata({"timestamp-tag"}, {"retained"});
                return arrow::schema(
                    {arrow::field("plain", arrow::timestamp(unit), true, metadata),
                     arrow::field("ltz", arrow::timestamp(unit, timezone), true, metadata),
                     arrow::field("utc", arrow::timestamp(unit, "UTC"), false, metadata)},
                    metadata);
            };
            const auto schema = make_schema(zone);
            const auto original_schema = make_schema(zone);
            const auto utc_schema = make_schema("UTC");
            std::shared_ptr<arrow::Array> written =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
                    .ValueOrDie();
            std::shared_ptr<arrow::Array> expected_utc =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(utc_schema->fields()),
                                                          json)
                    .ValueOrDie();
            const std::string path =
                PathUtil::JoinPath(directory_->Str(), fmt::format("timezone-{}-{}.vortex",
                                                                  zone.substr(zone.find('/') + 1),
                                                                  static_cast<int32_t>(unit)));
            ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));
            ASSERT_TRUE(schema->Equals(*original_schema, /*check_metadata=*/true));
            ASSERT_TRUE(written->type()->Equals(arrow::struct_(original_schema->fields()),
                                                /*check_metadata=*/true));

            ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<::ArrowSchema> file_schema,
                                 reader->GetFileSchema());
            std::shared_ptr<arrow::Schema> stored_schema =
                arrow::ImportSchema(file_schema.get()).ValueOrDie();
            ASSERT_TRUE(stored_schema->Equals(*utc_schema)) << stored_schema->ToString();
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual_utc,
                                 ReadFile(path, utc_schema, /*batch_size=*/2,
                                          /*expected_row_count=*/6));
            ASSERT_TRUE(actual_utc->Equals(expected_utc));
            ASSERT_TRUE(actual_utc->type()->Equals(expected_utc->type(), /*check_metadata=*/true));
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual_local,
                                 ReadFile(path, schema, /*batch_size=*/3,
                                          /*expected_row_count=*/6));
            ASSERT_TRUE(actual_local->Equals(written));
            ASSERT_TRUE(actual_local->type()->Equals(written->type(), /*check_metadata=*/true));
        }
    }
}

TEST_F(VortexFileFormatTest, WriterNormalizesNestedTimestampTimezones) {
    const auto make_schema = [](bool utc) {
        const auto metadata = arrow::key_value_metadata({"nested-tag"}, {"retained"});
        const auto local_millis =
            arrow::timestamp(arrow::TimeUnit::MILLI, utc ? "UTC" : "Asia/Shanghai");
        const auto local_nanos =
            arrow::timestamp(arrow::TimeUnit::NANO, utc ? "UTC" : "America/New_York");
        const auto local_seconds =
            arrow::timestamp(arrow::TimeUnit::SECOND, utc ? "UTC" : "America/New_York");
        return arrow::schema(
            {arrow::field(
                 "r",
                 arrow::struct_(
                     {arrow::field("ts", local_millis, false, metadata),
                      arrow::field("plain", arrow::timestamp(arrow::TimeUnit::MICRO), true,
                                   metadata),
                      arrow::field("l",
                                   arrow::list(arrow::field("item", local_nanos, true, metadata)),
                                   true, metadata)}),
                 false, metadata),
             arrow::field(
                 "f",
                 arrow::fixed_size_list(arrow::field("item", local_seconds, true, metadata), 2),
                 true, metadata)},
            metadata);
    };
    const auto schema = make_schema(/*utc=*/false);
    const auto original_schema = make_schema(/*utc=*/false);
    const auto utc_schema = make_schema(/*utc=*/true);
    const std::string json = R"([
        [{"ts":-1,"plain":-1000001,"l":[-1,null,123456789]},[-1,0]],
        [{"ts":-2,"plain":null,"l":null},null],
        [{"ts":0,"plain":null,"l":[]},[1,null]],
        [{"ts":1,"plain":1,"l":[0]},[null,-1000001]]
    ])";
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json)
            .ValueOrDie();
    std::shared_ptr<arrow::Array> expected_utc =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(utc_schema->fields()), json)
            .ValueOrDie();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "nested-timezones.vortex");
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));
    ASSERT_TRUE(schema->Equals(*original_schema, /*check_metadata=*/true));
    ASSERT_TRUE(written->type()->Equals(arrow::struct_(original_schema->fields()),
                                        /*check_metadata=*/true));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<::ArrowSchema> file_schema, reader->GetFileSchema());
    std::shared_ptr<arrow::Schema> stored_schema =
        arrow::ImportSchema(file_schema.get()).ValueOrDie();
    ASSERT_TRUE(stored_schema->Equals(*utc_schema)) << stored_schema->ToString();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual_utc,
                         ReadFile(path, utc_schema, /*batch_size=*/3, /*expected_row_count=*/4));
    ASSERT_TRUE(actual_utc->Equals(expected_utc));
    ASSERT_TRUE(actual_utc->type()->Equals(expected_utc->type(), /*check_metadata=*/true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual_local,
                         ReadFile(path, schema, /*batch_size=*/1, /*expected_row_count=*/4));
    ASSERT_TRUE(actual_local->Equals(written));
    ASSERT_TRUE(actual_local->type()->Equals(written->type(), /*check_metadata=*/true));
}

// Use the native file reader so an SDK residual filter cannot hide declined LTZ pushdown.
TEST_F(VortexFileFormatTest, NormalizedTimestampTimezoneSupportsNativePredicates) {
    const auto schema = arrow::schema(
        {arrow::field("ltz", arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Shanghai"))});
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                  R"([[-1001],[-1],[null],[0],[1],[1000]])")
            .ValueOrDie();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "timezone-predicates.vortex");
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));
    const auto predicate = PredicateBuilder::Equal(0, "ltz", FieldType::TIMESTAMP,
                                                   Literal(Timestamp::FromEpochMillis(-1)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                         CreatePredicateReader(path, schema, predicate, /*max_values=*/""));
    AssertNativeAndResidualRows(std::move(reader), predicate, written, {1}, {1});
}

TEST_F(VortexFileFormatTest, WriteThenRead) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "data.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields), R"([[1,"one"],[2,null],[3,"three"],[4,"four"],[5,"five"]])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5, 8});
}

TEST_F(VortexFileFormatTest, WriteThenReadSupportedTypes) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "supported-types.vortex");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::date32()),
        arrow::field("f10", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f11", arrow::decimal128(2, 2)),
        arrow::field("f12", arrow::decimal128(30, 2)),
        arrow::field("f13", arrow::list(arrow::float32())),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [true,-128,-32768,-2147483648,-4294967298,0.5,1.141592659,"vortex","binary",
           -1,"1970-01-01 00:00:00.000000001","-0.99","-123456789987654321.45",[1.5,null]],
          [false,127,32767,2147483647,4294967296,2.0,3.141592657,"","",
           12345,"2030-12-31 23:59:59.999999999","0.78","123456789987654321.45",[]],
          [null,null,null,null,null,null,null,null,null,null,null,null,null,null]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5});
}

// Regression: a list<utf8> column round-trips through Vortex's Utf8View export. The leaf strings
// include a null element, an empty list, a null list, an empty string and a string long enough to
// exceed the 12-byte StringView inline threshold (exercising the non-inline view buffer path).
TEST_F(VortexFileFormatTest, WriteThenReadListOfString) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "list-of-string.vortex");
    arrow::FieldVector fields = {arrow::field("tags", arrow::list(arrow::utf8()))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [["one","two",null]],
          [[]],
          [null],
          [["","a-longer-string-that-exceeds-the-inline-view-threshold","x"]],
          [[null,null]]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5});
}

// NULL parent lists must export correctly even when a bounded batch has no child ranges.
TEST_F(VortexFileFormatTest, NestedListNullAndEmptyBatchesRoundTrip) {
    struct NestedListCase {
        std::string name;
        std::shared_ptr<arrow::DataType> type;
        std::vector<std::pair<std::string, std::string>> values;
    };
    const std::vector<NestedListCase> cases = {
        {"list-list-int",
         arrow::list(arrow::list(arrow::int32())),
         {{"null-only", R"([[null]])"},
          {"all-null", R"([[null],[null]])"},
          {"empty-only", R"([[[]]])"},
          {"empty-inner", R"([[[[]]]])"},
          {"nullable-inner", R"([[[null,[],[null],[1,null,2]]]])"},
          {"mixed", R"([[null],[[]],[[[]]],[[null]],[[[1,2],[],null,[3]]],[null]])"}}},
        {"list-list-string",
         arrow::list(arrow::list(arrow::utf8())),
         {{"null-only", R"([[null]])"},
          {"all-null", R"([[null],[null]])"},
          {"empty-only", R"([[[]]])"},
          {"empty-inner", R"([[[[]]]])"},
          {"nullable-inner", R"([[[null,[],[null],["",null,"one"]]]])"},
          {"mixed",
           R"([[null],[[]],[[[]]],[[null]],[[["one","a-long-string-beyond-the-inline-view-limit"],[],null,["three",null]]],[null]])"}}},
        {"list-struct-list-string",
         arrow::list(arrow::struct_({arrow::field("tags", arrow::list(arrow::utf8()))})),
         {{"null-only", R"([[null]])"},
          {"all-null", R"([[null],[null]])"},
          {"empty-only", R"([[[]]])"},
          {"empty-inner", R"([[[{"tags":[]}]]])"},
          {"nullable-inner", R"([[[null,{"tags":null},{"tags":[]},{"tags":[null,""]}]]])"},
          {"mixed",
           R"([[null],[[]],[[null]],[[{"tags":[]}]],[[{"tags":["one","a-long-string-beyond-the-inline-view-limit",null]},{"tags":null},null]],[null]])"}}}};
    for (const NestedListCase& test_case : cases) {
        const auto schema = arrow::schema({arrow::field("nested", test_case.type)});
        for (const auto& [name, json] : test_case.values) {
            SCOPED_TRACE(fmt::format("type={}, case={}", test_case.name, name));
            const auto source_result =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json);
            ASSERT_TRUE(source_result.ok()) << source_result.status().ToString();
            const std::shared_ptr<arrow::Array> source = source_result.ValueOrDie();
            ASSERT_TRUE(source->ValidateFull().ok());
            const std::string path = PathUtil::JoinPath(
                directory_->Str(), fmt::format("{}-{}.vortex", test_case.name, name));
            // Keep mixed rows in one input batch so the reader slices shared child elements.
            ASSERT_OK(WriteFile(path, schema, source, /*batch_size=*/16));
            for (int32_t batch_size : {1, 2}) {
                SCOPED_TRACE(fmt::format("batch_size={}", batch_size));
                ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                                     CreateReader(path, batch_size));
                ASSERT_OK(ConfigureReader(reader.get(), schema));
                ASSERT_OK_AND_ASSIGN(uint64_t total_rows, reader->GetNumberOfRows());
                ASSERT_EQ(total_rows, static_cast<uint64_t>(source->length()));
                int64_t rows_seen = 0;
                while (true) {
                    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch,
                                         ReadNextBatch(reader.get()));
                    if (batch == nullptr) {
                        break;
                    }
                    ASSERT_TRUE(batch->ValidateFull().ok());
                    ASSERT_GT(batch->length(), 0);
                    ASSERT_LE(batch->length(), batch_size);
                    ASSERT_LE(rows_seen + batch->length(), source->length());
                    ASSERT_TRUE(batch->type()->Equals(arrow::struct_(schema->fields())));
                    ASSERT_TRUE(batch->Equals(source->Slice(rows_seen, batch->length())))
                        << batch->ToString();
                    for (int64_t i = 0; i < batch->length(); ++i) {
                        ASSERT_OK_AND_ASSIGN(
                            uint64_t physical_row,
                            reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
                        ASSERT_EQ(physical_row, static_cast<uint64_t>(rows_seen + i));
                    }
                    rows_seen += batch->length();
                }
                ASSERT_EQ(rows_seen, source->length());
                ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0),
                                    "last Vortex batch was EOF");
            }
        }
    }
}

// Regression for the FIXED_SIZE_LIST branch of NormalizeViewArray: without recursion into the
// elements, a fixed_size_list<utf8> column comes back as fixed_size_list<utf8view> (Vortex
// hardcodes Utf8 -> Utf8View), mismatching the normalized read schema and silently corrupting the
// data.
TEST_F(VortexFileFormatTest, WriteThenReadFixedSizeListOfString) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "fsl-of-string.vortex");
    arrow::FieldVector fields = {arrow::field("triples", arrow::fixed_size_list(arrow::utf8(), 3))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [["a","bb","ccc"]],
          [null],
          [["","a-longer-string-that-exceeds-the-inline-view-threshold","z"]],
          [[null,"q",null]]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 4});
}

TEST_F(VortexFileFormatTest, WriteThenReadVectors) {
    const std::vector<std::shared_ptr<arrow::DataType>> element_types = {
        arrow::boolean(), arrow::int8(),    arrow::int16(),  arrow::int32(),
        arrow::int64(),   arrow::float32(), arrow::float64()};
    for (const auto& element_type : element_types) {
        SCOPED_TRACE(element_type->ToString());
        std::string path =
            PathUtil::JoinPath(directory_->Str(), "vector-" + element_type->ToString() + ".vortex");
        auto vector_type =
            arrow::fixed_size_list(arrow::field("item", element_type, /*nullable=*/false), 3);
        arrow::FieldVector fields = {arrow::field("embedding", vector_type)};
        auto schema = arrow::schema(fields);
        std::string json = R"([
            [[1, 2, 3]], [null], [[4, 5, 6]], [[-1, 0, 1]], [[7, 8, 9]]
        ])";
        if (element_type->id() == arrow::Type::BOOL) {
            json = R"([
                [[true, false, true]], [null], [[false, true, false]],
                [[true, true, false]], [[false, false, true]]
            ])";
        } else if (element_type->id() == arrow::Type::FLOAT ||
                   element_type->id() == arrow::Type::DOUBLE) {
            json = R"([
                [[1.25, 2.5, 3.75]], [null], [[4.5, 5.25, 6.75]],
                [[-1.5, 0, 1.5]], [[7.25, 8.5, 9.75]]
            ])";
        }
        std::shared_ptr<arrow::Array> expected =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), json).ValueOrDie();

        ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
        AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 8});
    }
}

TEST_F(VortexFileFormatTest, RejectNullVectorElements) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto schema = arrow::schema({arrow::field("embedding", vector_type)});
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                           R"([[[1, null, 3]]])")
                     .ValueOrDie();
    ASSERT_NOK_WITH_MSG(WriteFile(PathUtil::JoinPath(directory_->Str(), "invalid-vector.vortex"),
                                  schema, array, /*batch_size=*/1),
                        "VECTOR cannot contain null elements, found one at row 0 position 1");
}

TEST_F(VortexFileFormatTest, WriteThenReadVectorsWithNullAncestors) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto vectors = arrow::ipc::internal::json::ArrayFromJSON(
                       vector_type, R"([[1, null, 3], [4, 5, 6], [7, 8, 9], [10, null, 12]])")
                       .ValueOrDie();
    auto validity = arrow::Buffer::FromString(std::string(1, '\x06'));
    for (bool list_parent : {false, true}) {
        SCOPED_TRACE(list_parent);
        std::shared_ptr<arrow::Array> parent;
        if (list_parent) {
            auto offsets =
                arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 2, 3, 4]")
                    .ValueOrDie();
            parent = std::make_shared<arrow::ListArray>(arrow::list(vector_type), 4,
                                                        offsets->data()->buffers[1], vectors,
                                                        validity, /*null_count=*/2);
        } else {
            parent = arrow::StructArray::Make({vectors}, {arrow::field("embedding", vector_type)},
                                              validity, /*null_count=*/2)
                         .ValueOrDie();
        }
        auto schema = arrow::schema({arrow::field("payload", parent->type())});
        auto input = arrow::StructArray::Make({parent}, schema->fields()).ValueOrDie();
        ASSERT_TRUE(input->ValidateFull().ok());
        auto expected = arrow::ipc::internal::json::ArrayFromJSON(
                            arrow::struct_(schema->fields()),
                            R"([[null], [[[4, 5, 6]]], [[[7, 8, 9]]], [null]])")
                            .ValueOrDie();
        std::string path =
            PathUtil::JoinPath(directory_->Str(), list_parent ? "null-list-vector.vortex"
                                                              : "null-struct-vector.vortex");
        ASSERT_OK(WriteFile(path, schema, input, /*batch_size=*/2));
        AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5});

        // The same elements are invalid when their ancestors are visible.
        auto visible_parent = parent->data()->Copy();
        visible_parent->buffers[0].reset();
        visible_parent->SetNullCount(0);
        input = arrow::StructArray::Make({arrow::MakeArray(visible_parent)}, schema->fields())
                    .ValueOrDie();
        ASSERT_NOK_WITH_MSG(WriteFile(path + ".invalid", schema, input, /*batch_size=*/2),
                            "VECTOR cannot contain null elements, found one at row 0 position 1");
    }
}

// Regression for a struct nested inside a list: the LIST branch recurses into the struct, whose
// utf8 leaf is a view; the combination must normalize back to list<struct<utf8, int32>>.
TEST_F(VortexFileFormatTest, WriteThenReadListOfStruct) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "list-of-struct.vortex");
    std::shared_ptr<arrow::DataType> item =
        arrow::struct_({arrow::field("name", arrow::utf8()), arrow::field("id", arrow::int32())});
    arrow::FieldVector fields = {arrow::field("items", arrow::list(item))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [[{"name":"a","id":1},{"name":null,"id":2}]],
          [[]],
          [null],
          [[{"name":"a-longer-string-that-exceeds-the-inline-view-threshold","id":null}]]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3});
}

TEST_F(VortexFileFormatTest, ExtractStatisticsReportsRowCount) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "statistics.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields), R"([[1,"one"],[2,"two"],[3,null],[4,"four"],[5,"five"]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatStatsExtractor> extractor,
                         format_->CreateStatsExtractor(&ffi_schema));
    ASSERT_OK_AND_ASSIGN(auto result, extractor->ExtractWithFileInfo(file_system_, path, pool_));
    ASSERT_EQ(result.second.GetRowCount(), 5);
    // Vortex exposes no per-column statistics: one unknown entry per field, all values nullopt.
    ASSERT_EQ(result.first.size(), fields.size());
    for (const std::shared_ptr<ColumnStats>& column_stats : result.first) {
        ASSERT_EQ(column_stats->NullCount(), std::nullopt);
    }
}

// Regression: consumers index the stats vector by field position and primary-key finalization
// requires one entry per write-schema field, so every supported type must yield an unknown entry.
TEST_F(VortexFileFormatTest, ExtractStatisticsReportsUnknownStatsPerField) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "statistics-types.vortex");
    arrow::FieldVector fields = {
        arrow::field("flag", arrow::boolean()),
        arrow::field("id", arrow::int32(), false),
        arrow::field("name", arrow::utf8()),
        arrow::field("payload", arrow::binary()),
        arrow::field("score", arrow::float64()),
        arrow::field("day", arrow::date32()),
        arrow::field("tod", arrow::time32(arrow::TimeUnit::MILLI)),
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("amount", arrow::decimal128(10, 2)),
        arrow::field("tags", arrow::list(arrow::field("element", arrow::int32()))),
        arrow::field("vec", arrow::fixed_size_list(arrow::field("element", arrow::float64()), 2)),
        arrow::field("r", arrow::struct_({arrow::field("a", arrow::int32())}))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields),
            R"([[true,1,"one","AA",1.5,17000,3723123,1700000000000,"1.25",[1],[1.0,2.0],{"a":1}]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/1));

    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatStatsExtractor> extractor,
                         format_->CreateStatsExtractor(&ffi_schema));
    ASSERT_OK_AND_ASSIGN(auto result, extractor->ExtractWithFileInfo(file_system_, path, pool_));
    ASSERT_EQ(result.second.GetRowCount(), 1);
    ASSERT_EQ(result.first.size(), fields.size());
    for (const std::shared_ptr<ColumnStats>& column_stats : result.first) {
        ASSERT_EQ(column_stats->NullCount(), std::nullopt);
    }
}

TEST_F(VortexFileFormatTest, ProjectedReadReturnsOnlyRequestedColumns) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "projection.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8()),
                                 arrow::field("score", arrow::float64())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,"one",1.5],[2,null,2.5],[3,"three",3.5]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    // Read a reordered subset {name, id}; the reader must return exactly those columns, proving it
    // honors SetReadSchema's projection rather than always emitting the full file schema.
    std::shared_ptr<arrow::Schema> projected = arrow::schema({fields[1], fields[0]});
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[1], fields[0]}),
                                                  R"([["one",1],[null,2],["three",3]])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, projected, /*batch_size=*/2, /*expected_row_count=*/3));
    ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n" << expected->ToString();
}

// Regression: nested projection must prune sub-fields, not merely relabel the parent type. Reading
// only r.b from a file whose r is ROW<a, b> must yield r as ROW<b> with matching data; before the
// recursive fix the full ROW<a, b> child was kept under a ROW<b> type, so the exported ArrowSchema
// disagreed with the array layout and silently corrupted the output. The b column also exercises
// view normalization inside a nested struct.
TEST_F(VortexFileFormatTest, ProjectedReadPrunesNestedStruct) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "nested-projection.vortex");
    std::shared_ptr<arrow::DataType> r_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("r", r_type)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields),
            R"([[1,{"a":10,"b":"x"}],[2,{"a":20,"b":null}],[3,{"a":30,"b":"z"}]])")
            .ValueOrDie();
    // batch_size=2 also slices the top-level struct column on write (offset > 0), exercising the
    // writer's offset rebasing together with the nested-projection read fix.
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    // Read only r.b: drop the top-level id column and prune r.a.
    std::shared_ptr<arrow::DataType> r_pruned = arrow::struct_({arrow::field("b", arrow::utf8())});
    arrow::FieldVector projected_fields = {arrow::field("r", r_pruned)};
    std::shared_ptr<arrow::Schema> projected = arrow::schema(projected_fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(projected_fields),
                                                  R"([[{"b":"x"}],[{"b":null}],[{"b":"z"}]])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, projected, /*batch_size=*/2, /*expected_row_count=*/3));
    ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n" << expected->ToString();
}

// Regression: an incompatible leaf type must be rejected, not silently reinterpreted. Reading an
// int32 column through an int64 read schema used to keep the 32-bit buffers under a 64-bit type, so
// the exported schema disagreed with the data and [1, 2, 3] came back as garbage.
TEST_F(VortexFileFormatTest, ProjectedReadRejectsIncompatibleLeafType) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "leaf-mismatch.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([[1],[2],[3]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    std::shared_ptr<arrow::Schema> widened = arrow::schema({arrow::field("id", arrow::int64())});
    ASSERT_NOK_WITH_MSG(
        ReadFile(path, widened, /*batch_size=*/2, /*expected_row_count=*/3),
        "AlignArrayToReadType unsupported leaf type change: data int32 vs read int64");
}

// Regression guard for ordered scans: 200 sequential rows span many Vortex chunks, and a small read
// batch size forces many batches. With ScanOptions.ordered(true) the rows must come back in storage
// order, so the concatenated result equals the written 0..199 sequence exactly (physical row
// positions, which the upper layer assigns by batch order, stay aligned for deletion vectors and
// primary-key merge).
TEST_F(VortexFileFormatTest, ReadPreservesRowOrderAcrossBatches) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "row-order.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::string json = "[";
    for (int32_t i = 0; i < 200; ++i) {
        json += (i > 0 ? "," : "") + std::string("[") + std::to_string(i) + "]";
    }
    json += "]";
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), json).ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/16));
    AssertReadWithBatchSizes(path, schema, expected, {1, 7, 64, 200});
}

// Regression: writing a top-level struct column in slices must not abort. arrow-rs cannot import a
// sliced (offset > 0) top-level struct (the parent offset is re-applied to already-offset children,
// tripping an arrow-data slice assertion), so VortexFormatWriter::AddBatch rebases such a batch to
// offset 0 before handing it to Vortex. batch_size=2 over 4 rows produces a slice at offset 2.
// A sliced nested column hides inside an offset-0 batch, which arrow-rs rejects with an aborting
// assertion unless every level is rebased first.
TEST_F(VortexFileFormatTest, WriteThenReadSlicedNestedStructColumn) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "sliced-nested.vortex");
    arrow::FieldVector fields = {
        arrow::field("r", arrow::struct_({arrow::field("a", arrow::int32())}))};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> full =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[{"a":1}],[{"a":2}],[{"a":3}],[{"a":4}]])")
            .ValueOrDie();
    const auto& full_struct = checked_cast<const arrow::StructArray&>(*full);
    std::shared_ptr<arrow::Array> sliced_inner = full_struct.field(0)->Slice(1, 3);
    arrow::Result<std::shared_ptr<arrow::Array>> wrapped =
        arrow::StructArray::Make({sliced_inner}, {"r"});
    ASSERT_TRUE(wrapped.ok()) << wrapped.status().ToString();
    ASSERT_EQ(wrapped.ValueOrDie()->offset(), 0);
    ASSERT_OK(WriteFile(path, schema, wrapped.ValueOrDie(), /*batch_size=*/2));

    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[{"a":2}],[{"a":3}],[{"a":4}]])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, schema, /*batch_size=*/2, /*expected_row_count=*/3));
    ASSERT_TRUE(actual->Equals(expected))
        << "actual: " << actual->ToString() << "\nexpected: " << expected->ToString();
}

// The exported rebased batch must keep whatever lifetime it was given until it is released, which
// is what keeps the writer's Arrow pool alive under Rust-held buffers.
TEST_F(VortexFileFormatTest, ExportedRebasedBatchRetainsItsLifetime) {
    arrow::FieldVector fields = {
        arrow::field("r", arrow::struct_({arrow::field("a", arrow::int32())}))};
    std::shared_ptr<arrow::Array> full =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[{"a":1}],[{"a":2}],[{"a":3}],[{"a":4}]])")
            .ValueOrDie();
    const auto& full_struct = checked_cast<const arrow::StructArray&>(*full);
    arrow::Result<std::shared_ptr<arrow::Array>> wrapped =
        arrow::StructArray::Make({full_struct.field(0)->Slice(1, 3)}, {"r"});
    ASSERT_TRUE(wrapped.ok()) << wrapped.status().ToString();
    ::ArrowArray exported{};
    ASSERT_TRUE(arrow::ExportArray(*wrapped.ValueOrDie(), &exported).ok());

    auto probe = std::make_shared<int>(0);
    ASSERT_OK(AddArrowArrayLifetime(&exported, nullptr, probe));
    ASSERT_GT(probe.use_count(), 1);
    ArrowArrayRelease(&exported);
    ASSERT_EQ(probe.use_count(), 1);
}

TEST_F(VortexFileFormatTest, WriteThenReadSlicedStructColumn) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "sliced-struct.vortex");
    std::shared_ptr<arrow::DataType> r_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("r", r_type)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields),
            R"([[1,{"a":10,"b":"x"}],[2,{"a":20,"b":null}],[3,{"a":30,"b":"z"}],[4,{"a":40,"b":"w"}]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 4});
}

TEST_F(VortexFileFormatTest, RollingSizeIncludesBufferedBatches) {
    const std::string path = PathUtil::JoinPath(directory_->Str(), "rolling-size.vortex");
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                 arrow::field("embedding", vector_type, /*nullable=*/false)});
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(schema->fields()),
            R"([[1,[1,2,3]],[2,[2,3,4]],[3,[3,4,5]],[4,[4,5,6]]])")
            .ValueOrDie();
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriterBuilder> writer_builder,
                         format_->CreateWriterBuilder(&ffi_schema, /*batch_size=*/2));
    writer_builder->WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output,
                         file_system_->Create(path, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatWriter> writer,
                         writer_builder->Build(output, "zstd"));
    ASSERT_OK_AND_ASSIGN(bool full, writer->ReachTargetSize(/*suggested_check=*/true, 1));
    ASSERT_FALSE(full);
    for (int64_t offset : {0, 2}) {
        ::ArrowArray ffi_array = {};
        ASSERT_TRUE(arrow::ExportArray(*written->Slice(offset, 2), &ffi_array).ok());
        ASSERT_OK(writer->AddBatch(&ffi_array));
        ASSERT_OK_AND_ASSIGN(full, writer->ReachTargetSize(/*suggested_check=*/false, 1));
        ASSERT_FALSE(full);
        // Each two-row slice contributes 32 input bytes; Java estimates a compression ratio of 1/4.
        const int64_t target = (offset + 2) * 4;
        ASSERT_OK_AND_ASSIGN(full, writer->ReachTargetSize(/*suggested_check=*/true, target));
        ASSERT_TRUE(full);
        ASSERT_OK_AND_ASSIGN(full, writer->ReachTargetSize(/*suggested_check=*/true, target + 1));
        ASSERT_FALSE(full);
    }
    ASSERT_OK(writer->Finish());
    ASSERT_OK(output->Close());
    AssertReadWithBatchSizes(path, schema, written, {1, 3});
}

// Regression: Flush() between batches must be safe (it is serialized with the background writer
// task's writes on the host OutputStream) and must not lose or reorder any data.
TEST_F(VortexFileFormatTest, FlushBetweenBatchesThenRead) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "flush-between.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,"one"],[2,null],[3,"three"],[4,"four"]])")
            .ValueOrDie();

    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriterBuilder> writer_builder,
                         format_->CreateWriterBuilder(&ffi_schema, /*batch_size=*/2));
    writer_builder->WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output,
                         file_system_->Create(path, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatWriter> writer,
                         writer_builder->Build(output, "zstd"));
    for (int64_t offset = 0; offset < written->length(); offset += 2) {
        std::shared_ptr<arrow::Array> slice = written->Slice(offset, 2);
        ::ArrowArray ffi_array = {};
        ASSERT_TRUE(arrow::ExportArray(*slice, &ffi_array).ok());
        ASSERT_OK(writer->AddBatch(&ffi_array));
        ASSERT_OK(writer->Flush());
    }
    ASSERT_OK(writer->Finish());
    ASSERT_OK(output->Close());

    AssertReadWithBatchSizes(path, schema, written, {1, 2, 4});
}

// The IO callback bridge must surface a paimon IO failure as that paimon error, not Vortex's
// opaque "stream error": the callback can only hand a status code back across the FFI boundary, so
// the reader/writer stashes the real error and prefers it. `IOHook` injects a failure at the Nth
// LocalFile IO; the loop walks the injection point across every IO of the whole read (or write)
// workflow, so each failure position is checked, and stops once the point falls past the last IO
// and the workflow runs clean. `CHECK_HOOK_STATUS` continues the loop when the surfaced error is
// the injected one (proving it propagated), and fails the test on any other error.
TEST_F(VortexFileFormatTest, ReadSurfacesInjectedIOError) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "read-io-error.vortex");
    // Nullable columns: ReadResultCollector imports batches through RecordBatch, whose
    // struct conversion drops NOT NULL; NOT NULL columns are covered by the ReadFile-based tests.
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,"one"],[2,null],[3,"three"]])")
            .ValueOrDie();
    // Write the fixture with the hook disarmed so only the read path is exercised below.
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    IOHook* io_hook = IOHook::GetInstance();
    bool run_complete = false;
    for (size_t i = 0; i < 200; i++) {
        ScopeGuard guard([io_hook]() { io_hook->Clear(); });
        io_hook->Reset(static_cast<int64_t>(i), IOHook::Mode::RETURN_ERROR);

        Result<std::unique_ptr<InputStream>> input = file_system_->Open(path);
        CHECK_HOOK_STATUS(input.status(), i);
        Result<std::unique_ptr<ReaderBuilder>> reader_builder =
            format_->CreateReaderBuilder(/*batch_size=*/2);
        CHECK_HOOK_STATUS(reader_builder.status(), i);
        reader_builder.value()->WithMemoryPool(pool_);

        std::shared_ptr<InputStream> input_stream = std::move(input).value();
        Result<std::unique_ptr<FileBatchReader>> reader =
            reader_builder.value()->Build(input_stream);
        CHECK_HOOK_STATUS(reader.status(), i);

        ::ArrowSchema ffi_schema = {};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
        CHECK_HOOK_STATUS(reader.value()->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                                        /*selection_bitmap=*/std::nullopt),
                          i);

        std::unique_ptr<FileBatchReader> owned_reader = std::move(reader).value();
        Result<std::shared_ptr<arrow::ChunkedArray>> collected =
            paimon::test::ReadResultCollector::CollectResult(std::move(owned_reader));
        CHECK_HOOK_STATUS(collected.status(), i);
        arrow::Result<std::shared_ptr<arrow::Array>> concat_result =
            arrow::Concatenate(collected.value()->chunks(), arrow_pool_.get());
        ASSERT_TRUE(concat_result.ok()) << concat_result.status().ToString();
        std::shared_ptr<arrow::Array> actual = concat_result.ValueOrDie();
        ASSERT_TRUE(actual->Equals(written))
            << "actual: " << actual->ToString() << "\nexpected: " << written->ToString();
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(VortexFileFormatTest, WriteSurfacesInjectedIOError) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "write-io-error.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([[1],[2],[3]])")
            .ValueOrDie();

    IOHook* io_hook = IOHook::GetInstance();
    bool run_complete = false;
    for (size_t i = 0; i < 200; i++) {
        ScopeGuard guard([io_hook]() { io_hook->Clear(); });
        io_hook->Reset(static_cast<int64_t>(i), IOHook::Mode::RETURN_ERROR);

        ::ArrowSchema ffi_schema = {};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
        Result<std::unique_ptr<WriterBuilder>> writer_builder =
            format_->CreateWriterBuilder(&ffi_schema, /*batch_size=*/2);
        CHECK_HOOK_STATUS(writer_builder.status(), i);
        writer_builder.value()->WithMemoryPool(pool_);

        Result<std::unique_ptr<OutputStream>> output =
            file_system_->Create(path, /*overwrite=*/true);
        CHECK_HOOK_STATUS(output.status(), i);
        std::shared_ptr<OutputStream> output_stream = std::move(output).value();

        Result<std::unique_ptr<FormatWriter>> writer =
            writer_builder.value()->Build(output_stream, "zstd");
        CHECK_HOOK_STATUS(writer.status(), i);

        ::ArrowArray ffi_array = {};
        ASSERT_TRUE(arrow::ExportArray(*data, &ffi_array).ok());
        CHECK_HOOK_STATUS(writer.value()->AddBatch(&ffi_array), i);
        CHECK_HOOK_STATUS(writer.value()->Finish(), i);
        CHECK_HOOK_STATUS(output_stream->Close(), i);
        // The write survived every injected position; stop injecting and verify the bytes.
        io_hook->Clear();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                             ReadFile(path, schema, /*batch_size=*/2, /*expected_row_count=*/3));
        ASSERT_TRUE(actual->Equals(data)) << actual->ToString() << "\nvs\n" << data->ToString();
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

// Selection pushdown must preserve physical row ids across output batches.
TEST_F(VortexFileFormatTest, SelectionPushdownReturnsSelectedRowsAndPhysicalRowIds) {
    constexpr int64_t kRows = 10;
    arrow::Int32Builder id_builder;
    arrow::StringBuilder name_builder;
    for (int64_t i = 0; i < kRows; ++i) {
        ASSERT_TRUE(id_builder.Append(static_cast<int32_t>(i * 10)).ok());
        ASSERT_TRUE(name_builder.Append(fmt::format("row_{}", i)).ok());
    }
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> name_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(name_builder.Finish(&name_array).ok());
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::StructArray::Make({id_array, name_array}, fields).ValueOrDie();

    std::string path = PathUtil::JoinPath(directory_->Str(), "selection.vortex");
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/4));

    // Select physical rows {1, 3, 5, 7}; their id values are {10, 30, 50, 70}.
    RoaringBitmap32 selection = RoaringBitmap32::From({1, 3, 5, 7});

    // batch_size=2 spreads the 4 selected rows over two batches, exercising the cross-batch row id.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> reader_builder,
                         format_->CreateReaderBuilder(/*batch_size=*/2));
    reader_builder->WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, reader_builder->Build(input));
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr, selection));

    std::vector<int32_t> got_ids;
    std::vector<uint64_t> got_row_ids;
    while (true) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        std::shared_ptr<arrow::Array> arr =
            arrow::ImportArray(batch.first.get(), batch.second.get()).ValueOrDie();
        std::shared_ptr<arrow::Int32Array> id_col = checked_pointer_cast<arrow::Int32Array>(
            checked_pointer_cast<arrow::StructArray>(arr)->GetFieldByName("id"));
        for (int64_t i = 0; i < arr->length(); ++i) {
            got_ids.push_back(id_col->Value(i));
            ASSERT_OK_AND_ASSIGN(uint64_t row_id,
                                 reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
            got_row_ids.push_back(row_id);
        }
    }
    ASSERT_EQ(got_ids, (std::vector<int32_t>{10, 30, 50, 70}));
    ASSERT_EQ(got_row_ids, (std::vector<uint64_t>{1, 3, 5, 7}));
}

TEST(VortexSelectionStateTest, LargeDenseSelectionsPreserveBitmapAndPhysicalRanks) {
    constexpr int32_t kRows = 100000000;
    for (const int32_t deleted_row : {-1, 0, kRows / 2, kRows - 1}) {
        SCOPED_TRACE(deleted_row);
        RoaringBitmap32 bitmap;
        bitmap.AddRange(0, kRows);
        if (deleted_row >= 0) {
            bitmap.RemoveRange(deleted_row, deleted_row + 1);
        }
        const RoaringBitmap32 original = bitmap;
        ASSERT_OK_AND_ASSIGN(VortexFileBatchReader::SelectionState state,
                             VortexFileBatchReader::SelectionState::Create(bitmap, kRows));
        ASSERT_TRUE(bitmap == original);
        const uint64_t expected_count = kRows - (deleted_row >= 0 ? 1 : 0);
        ASSERT_EQ(state.SelectedRowCount(), expected_count);
        ASSERT_EQ(state.IndexCount(), deleted_row >= 0 ? 1 : 0);
        for (const uint64_t rank : {uint64_t{0}, expected_count / 2, expected_count - 1}) {
            ASSERT_OK_AND_ASSIGN(uint64_t physical_row, state.GetPhysicalRowId(rank));
            ASSERT_EQ(physical_row,
                      rank + (deleted_row >= 0 && rank >= static_cast<uint64_t>(deleted_row)));
        }
    }
}

TEST(VortexSelectionStateTest, DenseSelectionSupportsRowsBeyondSignedBitmapRange) {
    constexpr int32_t kMaxRow = std::numeric_limits<int32_t>::max();
    for (const bool selects_max_row : {false, true}) {
        SCOPED_TRACE(selects_max_row);
        RoaringBitmap32 bitmap;
        bitmap.AddRange(0, kMaxRow);
        if (selects_max_row) {
            bitmap.RemoveRange(kMaxRow - 1, kMaxRow);
            bitmap.Add(kMaxRow);
            ASSERT_NOK_WITH_MSG(
                VortexFileBatchReader::SelectionState::Create(bitmap, kMaxRow),
                fmt::format("Vortex selected row {} is out of range {}", kMaxRow, kMaxRow));
        }
        const RoaringBitmap32 original = bitmap;
        const uint64_t total_rows = static_cast<uint64_t>(kMaxRow) + 2;
        ASSERT_OK_AND_ASSIGN(VortexFileBatchReader::SelectionState state,
                             VortexFileBatchReader::SelectionState::Create(bitmap, total_rows));
        ASSERT_TRUE(bitmap == original);
        ASSERT_EQ(state.SelectedRowCount(), static_cast<uint64_t>(kMaxRow));
        const auto excluded = static_cast<uint64_t>(selects_max_row ? kMaxRow - 1 : kMaxRow);
        ASSERT_EQ(state.indices_, (std::vector<uint64_t>{excluded, total_rows - 1}));
        ASSERT_OK_AND_ASSIGN(uint64_t physical_row, state.GetPhysicalRowId(kMaxRow - 1));
        ASSERT_EQ(physical_row, static_cast<uint64_t>(selects_max_row ? kMaxRow : kMaxRow - 1));
        ASSERT_NOK_WITH_MSG(
            state.GetPhysicalRowId(kMaxRow),
            fmt::format("emitted row offset {} exceeds selection size {}", kMaxRow, kMaxRow));
    }
}

TEST(VortexSelectionStateTest, SelectionBoundsAreValidatedBeforeFastPaths) {
    constexpr uint64_t kInvalidRow = std::numeric_limits<uint64_t>::max();
    ASSERT_NOK_WITH_MSG(
        VortexFileBatchReader::SelectionState::Create(RoaringBitmap32::From({0, 1, 2, 4}), 4),
        "Vortex selected row 4 is out of range 4");
    ASSERT_NOK_WITH_MSG(
        VortexFileBatchReader::SelectionState::Create(RoaringBitmap32::From({-1, 0, 1, 2}), 4),
        fmt::format("Vortex selected row {} is out of range 4", kInvalidRow));
    constexpr int32_t kMaxRow = std::numeric_limits<int32_t>::max();
    const RoaringBitmap32 bitmap = RoaringBitmap32::From({0, kMaxRow});
    ASSERT_NOK_WITH_MSG(VortexFileBatchReader::SelectionState::Create(bitmap, kMaxRow),
                        fmt::format("Vortex selected row {} is out of range {}", kMaxRow, kMaxRow));
    ASSERT_OK_AND_ASSIGN(
        VortexFileBatchReader::SelectionState state,
        VortexFileBatchReader::SelectionState::Create(bitmap, static_cast<uint64_t>(kMaxRow) + 1));
    ASSERT_OK_AND_ASSIGN(uint64_t physical_row, state.GetPhysicalRowId(1));
    ASSERT_EQ(physical_row, static_cast<uint64_t>(kMaxRow));
    const uint64_t total_rows = static_cast<uint64_t>(kMaxRow) + 2;
    ASSERT_NOK_WITH_MSG(
        VortexFileBatchReader::SelectionState::Create(RoaringBitmap32::From({0, -1}), total_rows),
        fmt::format("Vortex selected row {} is out of range {}", kInvalidRow, total_rows));
}

TEST_F(VortexFileFormatTest, DenseSelectionKeepsOnlyExcludedIndicesAndPreservesNullValues) {
    constexpr int32_t kRows = 4096;
    constexpr int32_t kDeletedRow = 2048;
    arrow::Int32Builder id_builder;
    arrow::Int32Builder value_builder;
    for (int32_t row = 0; row < kRows; ++row) {
        ASSERT_TRUE(id_builder.Append(row).ok());
        if (row % 17 == 0) {
            ASSERT_TRUE(value_builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(value_builder.Append(row * 10).ok());
        }
    }
    std::shared_ptr<arrow::Array> ids;
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(id_builder.Finish(&ids).ok());
    ASSERT_TRUE(value_builder.Finish(&values).ok());
    const auto schema = arrow::schema(
        {arrow::field("id", arrow::int32(), false), arrow::field("value", arrow::int32())});
    const auto data = arrow::StructArray::Make({ids, values}, schema->fields()).ValueOrDie();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "dense-selection.vortex");
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/1024));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                         CreateReader(path, /*batch_size=*/127));
    RoaringBitmap32 selection;
    selection.AddRange(0, kRows);
    selection.RemoveRange(kDeletedRow, kDeletedRow + 1);
    ASSERT_OK(ConfigureReader(reader.get(), schema, nullptr, selection));
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), kRows - 1, 1));
    uint64_t emitted = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
        if (array == nullptr) {
            break;
        }
        ASSERT_TRUE(array->ValidateFull().ok());
        const auto& batch = checked_cast<const arrow::StructArray&>(*array);
        const auto& batch_ids = checked_cast<const arrow::Int32Array&>(*batch.field(0));
        const auto& batch_values = checked_cast<const arrow::Int32Array&>(*batch.field(1));
        // Reverse requests also check that physical-id lookup does not depend on call order.
        for (int64_t i = array->length(); i > 0; --i) {
            const uint64_t output_row = emitted + static_cast<uint64_t>(i - 1);
            const uint64_t expected_row = output_row + (output_row >= kDeletedRow ? 1 : 0);
            ASSERT_OK_AND_ASSIGN(uint64_t physical_row,
                                 reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i - 1)));
            ASSERT_EQ(physical_row, expected_row);
            ASSERT_EQ(batch_ids.Value(i - 1), static_cast<int32_t>(expected_row));
            ASSERT_EQ(batch_values.IsNull(i - 1), expected_row % 17 == 0);
            if (!batch_values.IsNull(i - 1)) {
                ASSERT_EQ(batch_values.Value(i - 1), static_cast<int32_t>(expected_row * 10));
            }
        }
        emitted += static_cast<uint64_t>(array->length());
    }
    ASSERT_EQ(emitted, uint64_t{kRows - 1});
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "last Vortex batch was EOF");
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), kRows - 1, 1));
}

TEST_F(VortexFileFormatTest, DenseSelectionMapsConsecutiveExcludedRowsAndSparseSelections) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    const std::vector<std::vector<int32_t>> selections = {
        {1, 2, 3, 4, 5}, {0, 1, 2, 3, 4}, {0, 1, 4, 5}, {2, 3, 4, 5},
        {0, 1, 2, 3},    {0, 2, 3, 5},    {0, 5}};
    for (const auto& selected_rows : selections) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
        ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), nullptr,
                                  RoaringBitmap32::From(selected_rows)));
        ASSERT_NO_FATAL_FAILURE(
            AssertSelectionState(reader.get(), selected_rows.size(),
                                 std::min(selected_rows.size(), size_t{6} - selected_rows.size())));
        std::vector<uint64_t> actual;
        while (true) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
            if (array == nullptr) {
                break;
            }
            const size_t offset = actual.size();
            actual.resize(offset + static_cast<size_t>(array->length()));
            for (int64_t i = array->length(); i > 0; --i) {
                ASSERT_OK_AND_ASSIGN(
                    uint64_t row, reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i - 1)));
                actual[offset + static_cast<size_t>(i - 1)] = row;
            }
        }
        ASSERT_EQ(actual, (std::vector<uint64_t>(selected_rows.begin(), selected_rows.end())));
    }
}

TEST_F(VortexFileFormatTest, FullEmptyAndDenseSelectionsCanBeReconfiguredWithoutExpandedMapping) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    RoaringBitmap32 full;
    full.AddRange(0, 6);
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), nullptr, full));
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 6, 0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> first, ReadNextBatch(reader.get()));
    ASSERT_NE(first, nullptr);
    ASSERT_EQ(first->length(), 2);
    ASSERT_OK_AND_ASSIGN(uint64_t row, reader->GetPreviousBatchFileRowId(1));
    ASSERT_EQ(row, uint64_t{1});

    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), nullptr, RoaringBitmap32()));
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 0, 0));
    ASSERT_OK_AND_ASSIGN(first, ReadNextBatch(reader.get()));
    ASSERT_EQ(first, nullptr);
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "no Vortex batch has been read yet");
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), nullptr,
                              RoaringBitmap32::From({1, 2, 3, 4, 5})));
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 5, 1));
    ASSERT_OK_AND_ASSIGN(first, ReadNextBatch(reader.get()));
    ASSERT_OK_AND_ASSIGN(row, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(row, uint64_t{1});
    ASSERT_OK_AND_ASSIGN(row, reader->GetPreviousBatchFileRowId(1));
    ASSERT_EQ(row, uint64_t{2});
    ASSERT_OK_AND_ASSIGN(first, ReadNextBatch(reader.get()));
    ASSERT_OK_AND_ASSIGN(row, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(row, uint64_t{3});
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema()));
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 6, 0));
    ASSERT_OK_AND_ASSIGN(first, ReadNextBatch(reader.get()));
    ASSERT_OK_AND_ASSIGN(row, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(row, uint64_t{0});
}

TEST_F(VortexFileFormatTest, NativePredicateErrorsPropagateFromLazyScanCreation) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    const Literal invalid_utf8(FieldType::STRING, "\xff", 1);
    const std::shared_ptr<Predicate> invalid =
        PredicateBuilder::Equal(1, "name", FieldType::STRING, invalid_utf8);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), invalid));
    ASSERT_NOK_WITH_MSG(reader->NextBatch(),
                        "create Vortex predicate scalar: Other error: invalid utf-8: "
                        "invalid utf-8 sequence of 1 bytes from index 0");

    const std::shared_ptr<Predicate> valid =
        PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(int32_t{1}));
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), valid));
    ASSERT_OK_AND_ASSIGN(std::vector<uint64_t> row_ids, CollectNativeRowIds(std::move(reader)));
    ASSERT_EQ(row_ids, (std::vector<uint64_t>{1}));
}

TEST_F(VortexFileFormatTest, NativeTemporalLimitsFallBackAndPreserveResidualResults) {
    const auto schema =
        arrow::schema({arrow::field("date", arrow::date32()),
                       arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))});
    const auto data = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::struct_(schema->fields()), "[[0,0],[1,1000],[null,null],[-1,-1000]]");
    ASSERT_TRUE(data.ok()) << data.status().ToString();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "temporal-fallback.vortex");
    ASSERT_OK(WriteFile(path, schema, data.ValueOrDie(), /*batch_size=*/2));

    const Literal max_date(FieldType::DATE, std::numeric_limits<int32_t>::max());
    // The timestamp fits in Paimon's calendar range but exceeds Vortex's native range.
    const Literal end_of_year_9999(Timestamp::FromEpochMillis(253402300799999, 999000));
    const std::vector<std::pair<std::shared_ptr<Predicate>, std::vector<uint64_t>>> cases = {
        {PredicateBuilder::LessThan(0, "date", FieldType::DATE, max_date), {0, 1, 3}},
        {PredicateBuilder::NotIn(1, "ts", FieldType::TIMESTAMP,
                                 {Literal(Timestamp::FromEpochMillis(0)), end_of_year_9999}),
         {1, 3}}};
    for (const auto& [predicate, expected] : cases) {
        SCOPED_TRACE(predicate->GetFunction().ToString());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
        ASSERT_OK(ConfigureReader(reader.get(), schema, predicate));
        ASSERT_NO_FATAL_FAILURE(AssertNativeAndResidualRows(
            std::move(reader), predicate, data.ValueOrDie(), {0, 1, 2, 3}, expected));
    }
}

TEST_F(VortexFileFormatTest, IoCoalesceOptionsValidateLimitsAtReaderBuild) {
    const std::string path = PathUtil::JoinPath(directory_->Str(), "io-coalesce-limits.vortex");
    const auto schema = arrow::schema({arrow::field("id", arrow::int32())});
    const auto data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), "[[1]]")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    struct LimitCase {
        std::map<std::string, std::string> options;
        const char* error;
    };
    const std::string max_limit = std::to_string(std::numeric_limits<int64_t>::max());
    const std::string above_limit =
        std::to_string(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1);
    // Single overrides are checked against the other option's default.
    const LimitCase cases[] = {
        {{{kVortexReadIoCoalesceMaxGap, "16777216"}}, nullptr},
        {{{kVortexReadIoCoalesceMaxSize, "1048576"}}, nullptr},
        {{{kVortexReadIoCoalesceMaxGap, max_limit}, {kVortexReadIoCoalesceMaxSize, max_limit}},
         nullptr},
        {{{kVortexReadIoCoalesceMaxSize, "0"}}, "coalesced max_size must be positive"},
        {{{kVortexReadIoCoalesceMaxGap, "17"}, {kVortexReadIoCoalesceMaxSize, "16"}},
         "coalesced max_gap must not exceed max_size"},
        {{{kVortexReadIoCoalesceMaxGap, "16777217"}}, "coalesced max_gap must not exceed max_size"},
        {{{kVortexReadIoCoalesceMaxSize, "1048575"}}, "coalesced max_gap must not exceed max_size"},
        {{{kVortexReadIoCoalesceMaxGap, above_limit}, {kVortexReadIoCoalesceMaxSize, above_limit}},
         "coalesced max_size must not exceed INT64_MAX"},
    };
    for (const auto& test_case : cases) {
        SCOPED_TRACE(::testing::PrintToString(test_case.options));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> format,
                             FileFormatFactory::Get("vortex", test_case.options));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> builder,
                             format->CreateReaderBuilder(2));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
        Result<std::unique_ptr<FileBatchReader>> result = builder->Build(input);
        if (test_case.error == nullptr) {
            ASSERT_OK(result.status());
            ASSERT_NE(result.value(), nullptr);
        } else {
            ASSERT_NOK_WITH_MSG(
                result,
                fmt::format("open Vortex callback data source: Other error: {}", test_case.error));
        }
    }
}

TEST_F(VortexFileFormatTest, IoCoalesceOptionsChangePhysicalReadRanges) {
    // Both payload start offsets must fit within max-gap regardless of which read is polled first.
    constexpr int32_t kRows = 1536;
    constexpr int32_t kPayloadBytes = 512;
    constexpr int64_t kMaxGap = 2LL << 20;
    constexpr int64_t kSmallMaxSize = 2LL << 20;
    constexpr int64_t kLargeMaxSize = 4LL << 20;
    constexpr int64_t kLargeRead = 128LL << 10;
    arrow::Int32Builder id_builder;
    arrow::BinaryBuilder left_builder;
    arrow::BinaryBuilder omitted_builder;
    arrow::BinaryBuilder right_builder;
    uint64_t state = 0x9e3779b97f4a7c15ULL;
    for (int32_t row = 0; row < kRows; ++row) {
        ASSERT_TRUE(id_builder.Append(row).ok());
        for (arrow::BinaryBuilder* builder : {&left_builder, &omitted_builder, &right_builder}) {
            std::string bytes(kPayloadBytes, '\0');
            for (char& byte : bytes) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                byte = static_cast<char>(state & 0xff);
            }
            ASSERT_TRUE(builder->Append(bytes).ok());
        }
    }
    std::shared_ptr<arrow::Array> ids;
    std::shared_ptr<arrow::Array> left;
    std::shared_ptr<arrow::Array> omitted;
    std::shared_ptr<arrow::Array> right;
    ASSERT_TRUE(id_builder.Finish(&ids).ok());
    ASSERT_TRUE(left_builder.Finish(&left).ok());
    ASSERT_TRUE(omitted_builder.Finish(&omitted).ok());
    ASSERT_TRUE(right_builder.Finish(&right).ok());
    const auto schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                       arrow::field("left", arrow::binary(), false),
                                       arrow::field("omitted", arrow::binary(), false),
                                       arrow::field("right", arrow::binary(), false)});
    const auto data =
        arrow::StructArray::Make({ids, left, omitted, right}, schema->fields()).ValueOrDie();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "io-coalesce-ranges.vortex");
    // Distinct high-entropy columns leave a real unprojected payload between the two read columns.
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/kRows));
    const auto projection = arrow::schema({schema->field(1), schema->field(3)});
    const auto predicate = PredicateBuilder::GreaterOrEqual(
        0, "id", FieldType::INT, Literal(static_cast<int32_t>(kRows / 2)));
    const auto& expected_left = checked_cast<const arrow::BinaryArray&>(*left);
    const auto& expected_right = checked_cast<const arrow::BinaryArray&>(*right);
    const std::vector<std::map<std::string, std::string>> option_sets = {
        {{kVortexReadIoCoalesceMaxGap, "0"},
         {kVortexReadIoCoalesceMaxSize, std::to_string(kLargeMaxSize)}},
        {{kVortexReadIoCoalesceMaxGap, std::to_string(kMaxGap)},
         {kVortexReadIoCoalesceMaxSize, std::to_string(kLargeMaxSize)}},
        {{kVortexReadIoCoalesceMaxGap, std::to_string(kMaxGap)},
         {kVortexReadIoCoalesceMaxSize, std::to_string(kSmallMaxSize)}},
    };
    std::vector<std::vector<std::pair<int64_t, int64_t>>> physical_reads;
    for (size_t i = 0; i < option_sets.size(); ++i) {
        SCOPED_TRACE(i);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> format,
                             FileFormatFactory::Get("vortex", option_sets[i]));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> builder,
                             format->CreateReaderBuilder(/*batch_size=*/64));
        builder->WithMemoryPool(pool_);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
        auto tracking = std::make_shared<ReadAtTrackingInputStream>(std::move(input));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, builder->Build(tracking));
        ASSERT_OK(ConfigureReader(reader.get(), projection, predicate));
        // Opening the file reads its footer outside the segment coalescer under test.
        tracking->ClearReadAtRanges();
        int64_t expected_row = kRows / 2;
        while (true) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
            if (array == nullptr) {
                break;
            }
            ASSERT_TRUE(array->ValidateFull().ok());
            ASSERT_TRUE(array->type()->Equals(arrow::struct_(projection->fields())));
            const auto& batch = checked_cast<const arrow::StructArray&>(*array);
            const auto& actual_left = checked_cast<const arrow::BinaryArray&>(*batch.field(0));
            const auto& actual_right = checked_cast<const arrow::BinaryArray&>(*batch.field(1));
            for (int64_t row = 0; row < array->length(); ++row) {
                ASSERT_LT(expected_row, kRows);
                ASSERT_OK_AND_ASSIGN(uint64_t physical_row,
                                     reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(row)));
                ASSERT_EQ(physical_row, static_cast<uint64_t>(expected_row));
                ASSERT_EQ(actual_left.GetString(row), expected_left.GetString(expected_row));
                ASSERT_EQ(actual_right.GetString(row), expected_right.GetString(expected_row));
                ++expected_row;
            }
        }
        ASSERT_EQ(expected_row, kRows);
        reader.reset();
        physical_reads.push_back(tracking->GetReadAtRanges());
    }

    auto baseline = physical_reads[0];
    std::sort(baseline.begin(), baseline.end());
    const auto covers = [](const std::pair<int64_t, int64_t>& merged,
                           const std::pair<int64_t, int64_t>& first,
                           const std::pair<int64_t, int64_t>& second) {
        return merged.first <= first.first &&
               merged.first + merged.second >= second.first + second.second;
    };
    bool observed_limited_merge = false;
    for (size_t i = 0; i < baseline.size(); ++i) {
        if (baseline[i].second <= kLargeRead) {
            continue;
        }
        for (size_t j = i + 1; j < baseline.size(); ++j) {
            const int64_t hole = baseline[j].first - (baseline[i].first + baseline[i].second);
            const int64_t span = baseline[j].first + baseline[j].second - baseline[i].first;
            if (baseline[j].second <= kLargeRead || hole <= kLargeRead || hole > kMaxGap ||
                span <= kSmallMaxSize + (64LL << 10)) {
                continue;
            }
            const bool merged = std::any_of(
                physical_reads[1].begin(), physical_reads[1].end(),
                [&](const auto& range) { return covers(range, baseline[i], baseline[j]); });
            const bool limited = std::any_of(
                physical_reads[2].begin(), physical_reads[2].end(),
                [&](const auto& range) { return covers(range, baseline[i], baseline[j]); });
            observed_limited_merge |= merged && !limited;
        }
    }
    ASSERT_TRUE(observed_limited_merge)
        << "Expected a large read to bridge two baseline payload reads only with the larger span: "
        << ::testing::PrintToString(physical_reads);
}

TEST_F(VortexFileFormatTest, MembershipMaximumIsReaderLocalAndAppliesWithProjection) {
    const std::string path = PathUtil::JoinPath(directory_->Str(), "membership-limits.vortex");
    const arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                       arrow::field("payload", arrow::utf8())};
    const auto schema = arrow::schema(fields);
    const auto data = arrow::ipc::internal::json::ArrayFromJSON(
        arrow::struct_(fields), R"([[null,"null-id"],[0,"zero"],[100,"hundred"],[1,"one"]])");
    ASSERT_TRUE(data.ok()) << data.status().ToString();
    ASSERT_OK(WriteFile(path, schema, data.ValueOrDie(), /*batch_size=*/4));

    struct MembershipCase {
        int32_t size;
        const char* maximum;
        bool pushed;
    };
    const std::vector<MembershipCase> cases = {
        {32, "", true}, {33, "", false}, {33, "33", true}, {33, "0", false}};
    const auto projection = arrow::schema({fields[1]});
    const std::vector<uint64_t> all_rows = {0, 1, 2, 3};
    std::vector<std::unique_ptr<FileBatchReader>> readers;
    // Create all readers before scanning to verify that later overrides do not affect earlier
    // readers. The filter column is not part of the projected schema.
    for (const MembershipCase& test_case : cases) {
        std::vector<Literal> literals;
        for (int32_t value = 0; value < test_case.size; ++value) {
            literals.emplace_back(value);
        }
        const auto predicate = PredicateBuilder::In(0, "id", FieldType::INT, literals);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                             CreatePredicateReader(path, projection, predicate, test_case.maximum));
        readers.push_back(std::move(reader));
    }
    const std::vector<uint64_t> matches = {1, 3};
    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(fmt::format("size={}, maximum={}", cases[i].size, cases[i].maximum));
        ASSERT_OK_AND_ASSIGN(std::vector<uint64_t> rows,
                             CollectNativeRowIds(std::move(readers[i])));
        ASSERT_EQ(rows, cases[i].pushed ? matches : all_rows);
    }

    const auto equality = PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(int32_t{0}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                         CreatePredicateReader(path, projection, equality, /*max_values=*/"0"));
    ASSERT_OK_AND_ASSIGN(std::vector<uint64_t> rows, CollectNativeRowIds(std::move(reader)));
    ASSERT_EQ(rows, (std::vector<uint64_t>{1}));
}

TEST_F(VortexFileFormatTest, FilterPushdownOnDateColumn) {
    constexpr int64_t kRows = 10;
    arrow::Date32Builder date_builder;
    for (int64_t i = 0; i < kRows; ++i) {
        ASSERT_TRUE(date_builder.Append(static_cast<int32_t>(18000 + i)).ok());
    }
    std::shared_ptr<arrow::Array> date_array;
    ASSERT_TRUE(date_builder.Finish(&date_array).ok());
    arrow::FieldVector fields = {arrow::field("d", arrow::date32(), false)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::StructArray::Make({date_array}, fields).ValueOrDie();

    std::string path = PathUtil::JoinPath(directory_->Str(), "filter_date.vortex");
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/4));

    // DATE literals use epoch days; d > 18005 keeps physical rows 6..9.
    std::shared_ptr<Predicate> predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/0, "d", FieldType::DATE, Literal(FieldType::DATE, 18005));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> reader_builder,
                         format_->CreateReaderBuilder(/*batch_size=*/3));
    reader_builder->WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, reader_builder->Build(input));
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, predicate, /*selection_bitmap=*/std::nullopt));

    std::vector<int32_t> got_days;
    std::vector<uint64_t> got_row_ids;
    while (true) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        std::shared_ptr<arrow::Array> arr =
            arrow::ImportArray(batch.first.get(), batch.second.get()).ValueOrDie();
        std::shared_ptr<arrow::Date32Array> date_column = checked_pointer_cast<arrow::Date32Array>(
            checked_pointer_cast<arrow::StructArray>(arr)->GetFieldByName("d"));
        for (int64_t i = 0; i < arr->length(); ++i) {
            got_days.push_back(date_column->Value(i));
            ASSERT_OK_AND_ASSIGN(uint64_t row_id,
                                 reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
            got_row_ids.push_back(row_id);
        }
    }
    ASSERT_EQ(got_days, (std::vector<int32_t>{18006, 18007, 18008, 18009}));
    ASSERT_EQ(got_row_ids, (std::vector<uint64_t>{6, 7, 8, 9}));
}

TEST_F(VortexFileFormatTest, ReaderWithoutReadSchemaReturnsFullDataAndChecksRowIdBounds) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "no Vortex batch has been read yet");
    ASSERT_OK_AND_ASSIGN(uint64_t total_rows, reader->GetNumberOfRows());
    ASSERT_EQ(total_rows, uint64_t{6});
    uint64_t rows = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
        if (array == nullptr) {
            break;
        }
        ASSERT_TRUE(array->ValidateFull().ok());
        ASSERT_TRUE(array->type()->Equals(arrow::struct_(ReaderTestSchema()->fields())));
        ASSERT_OK_AND_ASSIGN(uint64_t first_row, reader->GetPreviousBatchFileRowId(0));
        ASSERT_EQ(first_row, rows);
        const uint64_t max_row_id = std::numeric_limits<uint64_t>::max();
        for (uint64_t row_id : {static_cast<uint64_t>(array->length()), max_row_id}) {
            ASSERT_NOK_WITH_MSG(
                reader->GetPreviousBatchFileRowId(row_id),
                fmt::format("batch row id {} is out of range {}", row_id, array->length()));
        }
        rows += static_cast<uint64_t>(array->length());
    }
    ASSERT_EQ(rows, total_rows);
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "last Vortex batch was EOF");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> repeated_eof, ReadNextBatch(reader.get()));
    ASSERT_EQ(repeated_eof, nullptr);
}

TEST_F(VortexFileFormatTest, CombinedSelectionAndFilterPreservePhysicalRowsAfterProjection) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    const auto metadata = arrow::key_value_metadata({"projection-tag"}, {"retained"});
    const auto projection =
        arrow::schema({arrow::field("name", arrow::utf8(), /*nullable=*/false, metadata)});
    const auto predicate =
        PredicateBuilder::GreaterOrEqual(0, "id", FieldType::INT, Literal(static_cast<int32_t>(2)));
    ASSERT_OK(
        ConfigureReader(reader.get(), projection, predicate, RoaringBitmap32::From({0, 2, 3, 5})));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowSchema> file_schema, reader->GetFileSchema());
    const auto imported_file_schema = arrow::ImportSchema(file_schema.get()).ValueOrDie();
    ASSERT_TRUE(imported_file_schema->Equals(*ReaderTestSchema()));
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 4, 2));
    std::vector<uint64_t> physical_rows;
    std::vector<std::shared_ptr<arrow::Array>> batches;
    while (true) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
        if (array == nullptr) {
            break;
        }
        ASSERT_TRUE(array->ValidateFull().ok());
        ASSERT_TRUE(array->type()->Equals(arrow::struct_(projection->fields()),
                                          /*check_metadata=*/true));
        for (int64_t i = 0; i < array->length(); ++i) {
            ASSERT_OK_AND_ASSIGN(uint64_t physical_row,
                                 reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
            physical_rows.push_back(physical_row);
        }
        batches.push_back(std::move(array));
    }
    ASSERT_EQ(physical_rows, (std::vector<uint64_t>{2, 3, 5}));
    ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 4, 2));
    auto actual = arrow::Concatenate(batches, arrow_pool_.get()).ValueOrDie();
    auto expected = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(projection->fields()),
                                                              R"([["two"],["three"],["five"]])")
                        .ValueOrDie();
    ASSERT_TRUE(actual->Equals(expected));
}

TEST_F(VortexFileFormatTest, BoundedStringBatchesPreserveProjectionRowIdsAndBufferOwnership) {
    constexpr int32_t kRows = 40;
    constexpr int32_t kBatchSize = 4;
    const std::string repeated_text(4096, 't');
    std::string repeated_binary(4096, '\0');
    repeated_binary.front() = '\xff';
    repeated_binary.back() = '\x7f';
    arrow::Int32Builder id_builder;
    arrow::StringBuilder text_builder;
    arrow::BinaryBuilder binary_builder;
    RoaringBitmap32 selection;
    std::vector<uint64_t> expected_rows;
    for (int32_t row = 0; row < kRows; ++row) {
        ASSERT_TRUE(id_builder.Append(row).ok());
        if (row % 4 == 2) {
            ASSERT_TRUE(text_builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(text_builder.Append(row % 4 == 3 ? "" : repeated_text).ok());
        }
        if (row % 5 == 2) {
            ASSERT_TRUE(binary_builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(binary_builder.Append(row % 5 == 3 ? "" : repeated_binary).ok());
        }
        if (row % 3 != 1) {
            selection.Add(row);
            if (row >= 5) {
                expected_rows.push_back(row);
            }
        }
    }
    std::shared_ptr<arrow::Array> ids;
    std::shared_ptr<arrow::Array> texts;
    std::shared_ptr<arrow::Array> binaries;
    ASSERT_TRUE(id_builder.Finish(&ids).ok());
    ASSERT_TRUE(text_builder.Finish(&texts).ok());
    ASSERT_TRUE(binary_builder.Finish(&binaries).ok());
    const auto schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                       arrow::field("text", arrow::utf8()),
                                       arrow::field("payload", arrow::binary())});
    const auto data =
        arrow::StructArray::Make({ids, texts, binaries}, schema->fields()).ValueOrDie();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "bounded-strings.vortex");
    // A single write batch contains repeated values much larger than an inline string view.
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/kRows));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path, kBatchSize));
    const auto projection = arrow::schema({schema->field(2), schema->field(1)});
    const auto predicate =
        PredicateBuilder::GreaterOrEqual(0, "id", FieldType::INT, Literal(static_cast<int32_t>(5)));
    ASSERT_OK(ConfigureReader(reader.get(), projection, predicate, selection));
    std::vector<BatchReader::ReadBatch> retained;
    size_t rows_seen = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        ASSERT_OK(paimon::test::ReadResultCollector::CheckBatchOffset(batch));
        ASSERT_GT(batch.first->length, 0);
        ASSERT_LE(batch.first->length, kBatchSize);
        for (int64_t i = 0; i < batch.first->length; ++i) {
            ASSERT_LT(rows_seen, expected_rows.size());
            ASSERT_OK_AND_ASSIGN(uint64_t physical_row,
                                 reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
            ASSERT_EQ(physical_row, expected_rows[rows_seen++]);
        }
        retained.push_back(std::move(batch));
    }
    ASSERT_EQ(rows_seen, expected_rows.size());
    ASSERT_GT(retained.size(), 1);
    // Keep the raw Arrow exports across a new scan, closing, and destroying the reader.
    ASSERT_OK(ConfigureReader(reader.get(), schema));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> restarted, ReadNextBatch(reader.get()));
    ASSERT_NE(restarted, nullptr);
    reader->Close();
    reader.reset();
    ASSERT_TRUE(restarted->ValidateFull().ok());
    ASSERT_TRUE(restarted->Equals(data->Slice(0, restarted->length())));

    const auto& expected_texts = checked_cast<const arrow::StringArray&>(*texts);
    const auto& expected_binaries = checked_cast<const arrow::BinaryArray&>(*binaries);
    rows_seen = 0;
    for (auto& batch : retained) {
        auto imported = arrow::ImportArray(batch.first.get(), batch.second.get()).ValueOrDie();
        ASSERT_TRUE(imported->ValidateFull().ok());
        ASSERT_TRUE(imported->type()->Equals(arrow::struct_(projection->fields())));
        const auto& values = checked_cast<const arrow::StructArray&>(*imported);
        const auto& actual_binaries = checked_cast<const arrow::BinaryArray&>(*values.field(0));
        const auto& actual_texts = checked_cast<const arrow::StringArray&>(*values.field(1));
        for (int64_t i = 0; i < imported->length(); ++i) {
            const auto physical_row = static_cast<int64_t>(expected_rows[rows_seen++]);
            ASSERT_EQ(actual_texts.IsNull(i), expected_texts.IsNull(physical_row));
            ASSERT_EQ(actual_binaries.IsNull(i), expected_binaries.IsNull(physical_row));
            if (!actual_texts.IsNull(i)) {
                ASSERT_EQ(actual_texts.GetString(i), expected_texts.GetString(physical_row));
            }
            if (!actual_binaries.IsNull(i)) {
                ASSERT_EQ(actual_binaries.GetString(i), expected_binaries.GetString(physical_row));
            }
        }
    }
    ASSERT_EQ(rows_seen, expected_rows.size());
}

TEST_F(VortexFileFormatTest, ResetReadSchemaRestartsAndClearsSelectionAndFilter) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    const auto predicate =
        PredicateBuilder::GreaterThan(0, "id", FieldType::INT, Literal(static_cast<int32_t>(2)));
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), predicate,
                              RoaringBitmap32::From({1, 3, 5})));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> first, ReadNextBatch(reader.get()));
    ASSERT_EQ(first->length(), 2);
    ASSERT_OK_AND_ASSIGN(uint64_t first_row, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(first_row, uint64_t{3});
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema()));
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "no Vortex batch has been read yet");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> restarted, ReadNextBatch(reader.get()));
    ASSERT_EQ(restarted->length(), 2);
    ASSERT_OK_AND_ASSIGN(first_row, reader->GetPreviousBatchFileRowId(0));
    ASSERT_EQ(first_row, uint64_t{0});
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), nullptr, RoaringBitmap32()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> empty, ReadNextBatch(reader.get()));
    ASSERT_EQ(empty, nullptr);
    ASSERT_OK(
        ConfigureReader(reader.get(), ReaderTestSchema(), nullptr, RoaringBitmap32::From({0, 5})));
    ASSERT_OK_AND_ASSIGN(std::vector<uint64_t> rows, CollectNativeRowIds(std::move(reader)));
    ASSERT_EQ(rows, (std::vector<uint64_t>{0, 5}));
}

TEST_F(VortexFileFormatTest, EmptyProjectionPreservesRowCountsAndPhysicalIds) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    const auto projection = arrow::schema({});
    for (const bool filtered : {false, true}) {
        SCOPED_TRACE(filtered);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
        const std::shared_ptr<Predicate> predicate =
            filtered ? PredicateBuilder::GreaterThan(0, "id", FieldType::INT,
                                                     Literal(static_cast<int32_t>(2)))
                     : nullptr;
        const std::optional<RoaringBitmap32> selection =
            filtered ? std::optional<RoaringBitmap32>(RoaringBitmap32::From({0, 1, 3, 5}))
                     : std::nullopt;
        ASSERT_OK(ConfigureReader(reader.get(), projection, predicate, selection));
        std::vector<uint64_t> rows;
        while (true) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
            if (array == nullptr) {
                break;
            }
            ASSERT_TRUE(array->ValidateFull().ok());
            ASSERT_TRUE(array->type()->Equals(arrow::struct_(projection->fields())));
            const auto* native_reader = checked_cast<const VortexFileBatchReader*>(reader.get());
            // Inspect the native export before C++ alignment can discard payload columns.
            ASSERT_EQ(native_reader->current_stream_schema_->num_fields(), filtered ? 1 : 0);
            ASSERT_EQ(native_reader->current_batch_->type()->num_fields(), filtered ? 1 : 0);
            if (filtered) {
                ASSERT_EQ(native_reader->current_stream_schema_->field(0)->name(), "_row_idx");
            }
            for (int64_t i = 0; i < array->length(); ++i) {
                ASSERT_OK_AND_ASSIGN(uint64_t row,
                                     reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
                // File readers may return candidates; retain the residual predicate.
                if (!filtered || row > 2) {
                    rows.push_back(row);
                }
            }
        }
        ASSERT_EQ(rows, filtered ? (std::vector<uint64_t>{3, 5})
                                 : (std::vector<uint64_t>{0, 1, 2, 3, 4, 5}));
        ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "last Vortex batch was EOF");
    }
}

TEST_F(VortexFileFormatTest, EmptyProjectionAvoidsPayloadReads) {
    constexpr int32_t kRows = 64;
    arrow::Int32Builder id_builder;
    arrow::StringBuilder payload_builder;
    uint32_t seed = 13;
    std::string payload(16384, '\0');
    for (int32_t row = 0; row < kRows; ++row) {
        ASSERT_TRUE(id_builder.Append(row).ok());
        for (char& byte : payload) {
            seed = seed * 1664525U + 1013904223U;
            byte = static_cast<char>('!' + ((seed >> 24) % 90));
        }
        ASSERT_TRUE(payload_builder.Append(payload).ok());
    }
    std::shared_ptr<arrow::Array> ids;
    std::shared_ptr<arrow::Array> payloads;
    ASSERT_TRUE(id_builder.Finish(&ids).ok());
    ASSERT_TRUE(payload_builder.Finish(&payloads).ok());
    const auto schema = arrow::schema(
        {arrow::field("id", arrow::int32(), false), arrow::field("payload", arrow::utf8(), false)});
    const auto data = arrow::StructArray::Make({ids, payloads}, schema->fields()).ValueOrDie();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "empty-projection-io.vortex");
    ASSERT_OK(WriteFile(path, schema, data, kRows));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> format,
                         FileFormatFactory::Get("vortex", {{"file.format", "vortex"},
                                                           {kVortexReadIoCoalesceMaxGap, "0"},
                                                           {kVortexReadIoCoalesceMaxSize, "1"}}));
    for (const bool filtered : {false, true}) {
        SCOPED_TRACE(filtered);
        std::vector<uint64_t> bytes_read;
        for (const bool empty_projection : {false, true}) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
            ASSERT_OK_AND_ASSIGN(int64_t file_bytes, input->Length());
            // Keep payload outside the native footer read-ahead cache for the I/O comparison.
            ASSERT_GT(file_bytes, int64_t{256 * 1024});
            auto tracked_input = std::make_shared<ReadAtTrackingInputStream>(input);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> builder,
                                 format->CreateReaderBuilder(/*batch_size=*/16));
            builder->WithMemoryPool(pool_);
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader,
                                 builder->Build(tracked_input));
            const std::shared_ptr<Predicate> predicate =
                filtered ? PredicateBuilder::GreaterThan(0, "id", FieldType::INT,
                                                         Literal(static_cast<int32_t>(31)))
                         : nullptr;
            ASSERT_OK(ConfigureReader(reader.get(), empty_projection ? arrow::schema({}) : schema,
                                      predicate));
            tracked_input->ClearReadAtRanges();
            uint64_t rows = 0;
            while (true) {
                ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch,
                                     ReadNextBatch(reader.get()));
                if (batch == nullptr) {
                    break;
                }
                ASSERT_TRUE(batch->ValidateFull().ok());
                ASSERT_EQ(batch->type()->num_fields(), empty_projection ? 0 : 2);
                rows += static_cast<uint64_t>(batch->length());
            }
            ASSERT_EQ(rows, filtered ? kRows / 2 : kRows);
            reader->Close();
            uint64_t bytes = 0;
            for (const auto& range : tracked_input->GetReadAtRanges()) {
                bytes += static_cast<uint64_t>(range.second);
            }
            bytes_read.push_back(bytes);
        }
        ASSERT_GT(bytes_read[0], uint64_t{0});
        // Metadata and predicate columns may still be read; the large payload must be omitted.
        ASSERT_LT(bytes_read[1], bytes_read[0] / 4);
    }
}

TEST_F(VortexFileFormatTest, NestedProjectionValidatesEveryRequestedField) {
    const std::string path = PathUtil::JoinPath(directory_->Str(), "nested-schema-contract.vortex");
    const auto item_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    const auto schema =
        arrow::schema({arrow::field("r", item_type), arrow::field("l", arrow::list(item_type))});
    const std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(schema->fields()),
            R"([[{"a":10,"b":"x"},[{"a":1,"b":"one"},{"a":2,"b":null}]],
                [{"a":20,"b":"y"},[{"a":4,"b":"four"}]]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/1));
    const auto missing = arrow::struct_({arrow::field("missing", arrow::int32())});
    const auto pruned = arrow::struct_({arrow::field("b", arrow::utf8())});
    const auto invalid =
        arrow::schema({arrow::field("r", pruned), arrow::field("l", arrow::list(missing))});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    ASSERT_NOK_WITH_MSG(ConfigureReader(reader.get(), invalid),
                        "Field 'missing' not found or duplicate in src schema");

    // The list wrapper name is not a physical STRUCT field and may differ from the file.
    const auto projection =
        arrow::schema({arrow::field("l", arrow::list(arrow::field("element", pruned))),
                       arrow::field("r", pruned)});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, projection, /*batch_size=*/1, /*expected_row_count=*/2));
    const std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(projection->fields()),
            R"([[[{"b":"one"},{"b":null}],{"b":"x"}],[[{"b":"four"}],{"b":"y"}]])")
            .ValueOrDie();
    ASSERT_TRUE(actual->Equals(expected)) << actual->ToString();
}

TEST_F(VortexFileFormatTest, InvalidResetPreservesFilterSelectionAndPhysicalRows) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    const auto schema = ReaderTestSchema();
    const auto& id = schema->field(0);
    struct ResetCase {
        std::shared_ptr<arrow::Schema> projection;
        RoaringBitmap32 selection;
        const char* error;
    };
    const ResetCase cases[] = {
        {arrow::schema({id, arrow::field("extra", arrow::utf8())}), RoaringBitmap32::From({0}),
         "Field 'extra' not found or duplicate in src schema"},
        {schema, RoaringBitmap32::From({1, 6}), "Vortex selected row 6 is out of range 6"},
    };
    const auto predicate =
        PredicateBuilder::GreaterThan(0, "id", FieldType::INT, Literal(int32_t{0}));
    const RoaringBitmap32 selection = RoaringBitmap32::From({0, 1, 3, 4, 5});
    for (const ResetCase& test_case : cases) {
        SCOPED_TRACE(test_case.error);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
        ASSERT_OK(ConfigureReader(reader.get(), schema, predicate, selection));
        ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 5, 1));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> first, ReadNextBatch(reader.get()));
        ASSERT_NE(first, nullptr);
        ASSERT_EQ(first->length(), 2);

        ASSERT_NOK_WITH_MSG(
            ConfigureReader(reader.get(), test_case.projection, nullptr, test_case.selection),
            test_case.error);
        ASSERT_NO_FATAL_FAILURE(AssertSelectionState(reader.get(), 5, 1));
        ASSERT_OK_AND_ASSIGN(uint64_t row, reader->GetPreviousBatchFileRowId(0));
        ASSERT_EQ(row, uint64_t{1});
        ASSERT_OK_AND_ASSIGN(row, reader->GetPreviousBatchFileRowId(1));
        ASSERT_EQ(row, uint64_t{3});
        ASSERT_OK_AND_ASSIGN(std::vector<uint64_t> remaining,
                             CollectNativeRowIds(std::move(reader)));
        ASSERT_EQ(remaining, (std::vector<uint64_t>{4, 5}));
    }
}

TEST_F(VortexFileFormatTest, FilterDoesNotOverwriteAFileColumnNamedRowIdx) {
    const auto schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                       arrow::field("_row_idx", arrow::int64(), false)});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                          R"([[0,100],[1,101],[2,102],[3,103]])")
                    .ValueOrDie();
    const std::string path = PathUtil::JoinPath(directory_->Str(), "row-idx-column.vortex");
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));
    const auto predicate =
        PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(static_cast<int32_t>(3)));
    const auto filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    ASSERT_NE(filter, nullptr);
    for (const auto& read_schema : {schema, arrow::schema({schema->field(0)})}) {
        SCOPED_TRACE(read_schema->ToString());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
        ASSERT_OK(
            ConfigureReader(reader.get(), read_schema, predicate, RoaringBitmap32::From({1, 3})));
        std::vector<uint64_t> scanned_rows;
        std::vector<uint64_t> matched_rows;
        while (true) {
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> array, ReadNextBatch(reader.get()));
            if (array == nullptr) {
                break;
            }
            ASSERT_TRUE(array->ValidateFull().ok());
            ASSERT_TRUE(array->type()->Equals(arrow::struct_(read_schema->fields())));
            const auto& values = checked_cast<const arrow::StructArray&>(*array);
            const auto& ids = checked_cast<const arrow::Int32Array&>(*values.field(0));
            ASSERT_OK_AND_ASSIGN(std::vector<char> mask, filter->Test(*array, arrow_pool_.get()));
            for (int64_t i = 0; i < array->length(); ++i) {
                ASSERT_OK_AND_ASSIGN(uint64_t physical_row,
                                     reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(i)));
                scanned_rows.push_back(physical_row);
                ASSERT_EQ(ids.Value(i), static_cast<int32_t>(physical_row));
                if (read_schema->num_fields() == 2) {
                    const auto& original_column =
                        checked_cast<const arrow::Int64Array&>(*values.field(1));
                    ASSERT_EQ(original_column.Value(i), 100 + static_cast<int64_t>(physical_row));
                }
                if (mask[i] != 0) {
                    matched_rows.push_back(physical_row);
                }
            }
        }
        // The stored _row_idx prevents native filtering even when it is not projected.
        ASSERT_EQ(scanned_rows, (std::vector<uint64_t>{1, 3}));
        ASSERT_EQ(matched_rows, (std::vector<uint64_t>{3}));
    }
}

TEST_F(VortexFileFormatTest, CloseReleasesSelectionAndRowPositionWhileReaderRemainsAlive) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    const auto predicate =
        PredicateBuilder::GreaterThan(0, "id", FieldType::INT, Literal(static_cast<int32_t>(0)));
    ASSERT_OK(ConfigureReader(reader.get(), ReaderTestSchema(), predicate,
                              RoaringBitmap32::From({1, 3, 5})));
    auto* native_reader = checked_cast<VortexFileBatchReader*>(reader.get());
    ASSERT_GT(native_reader->selection_.indices_.capacity(), size_t{0});
    std::weak_ptr<arrow::Array> row_ids;
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, ReadNextBatch(reader.get()));
        ASSERT_NE(batch, nullptr);
        ASSERT_NE(native_reader->row_position_.previous_batch_row_idx_, nullptr);
        row_ids = native_reader->row_position_.previous_batch_row_idx_;
    }
    ASSERT_FALSE(row_ids.expired());
    reader->Close();
    ASSERT_NE(reader, nullptr);
    // The index vector uses the standard allocator, so Arrow pool counters cannot check it.
    ASSERT_EQ(native_reader->selection_.indices_.capacity(), size_t{0});
    ASSERT_TRUE(row_ids.expired());
    reader->Close();
    ASSERT_NOK_WITH_MSG(ConfigureReader(reader.get(), ReaderTestSchema()),
                        "Vortex reader is closed");
    ASSERT_NOK_WITH_MSG(reader->NextBatch(), "Vortex reader is closed");
}

TEST_F(VortexFileFormatTest, SchemaStreamErrorsPreserveDetailsAndReleaseStream) {
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    for (const bool has_native_message : {false, true}) {
        for (const bool has_callback_error : {false, true}) {
            SCOPED_TRACE(has_native_message);
            SCOPED_TRACE(has_callback_error);
            struct SchemaFailure {
                std::string message = "native schema export sentinel";
                bool has_message = false;
                bool error_read_before_release = false;
                int32_t releases = 0;
            } failure;
            failure.has_message = has_native_message;
            const std::string native_message = failure.message;
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
            auto* native_reader = checked_cast<VortexFileBatchReader*>(reader.get());
            ::ArrowArrayStream stream{};
            stream.private_data = &failure;
            stream.get_schema = [](::ArrowArrayStream*, ::ArrowSchema*) -> int32_t { return EIO; };
            stream.get_last_error = [](::ArrowArrayStream* current) -> const char* {
                auto* state = static_cast<SchemaFailure*>(current->private_data);
                state->error_read_before_release = state->releases == 0;
                return state->has_message ? state->message.c_str() : nullptr;
            };
            stream.release = [](::ArrowArrayStream* current) {
                auto* state = static_cast<SchemaFailure*>(current->private_data);
                ++state->releases;
                // Invalidate the stream-owned message without introducing a dangling pointer.
                std::fill(state->message.begin(), state->message.end(), '#');
                current->release = nullptr;
            };
            native_reader->current_stream_ = stream;
            native_reader->stream_active_ = true;
            if (has_callback_error) {
                native_reader->input_context_->SetCallbackStatus(
                    Status::IOError("filesystem schema callback sentinel"));
            }
            const Status status = native_reader->ImportCurrentStreamSchema();
            ASSERT_EQ(failure.releases, 1);
            ASSERT_FALSE(native_reader->stream_active_);
            ASSERT_EQ(native_reader->current_stream_schema_, nullptr);
            if (has_callback_error) {
                ASSERT_NOK_WITH_MSG(
                    status, "read Vortex stream schema: filesystem schema callback sentinel");
                ASSERT_EQ(status.message().find(native_message), std::string::npos);
            } else if (has_native_message) {
                ASSERT_TRUE(failure.error_read_before_release);
                ASSERT_NOK_WITH_MSG(status,
                                    "Vortex Arrow stream get_schema failed: " + native_message);
            } else {
                ASSERT_NOK_WITH_MSG(status, "Vortex Arrow stream get_schema failed: unknown");
            }
            reader->Close();
            ASSERT_EQ(failure.releases, 1);
        }
    }
}

TEST_F(VortexFileFormatTest, EmptyFileAndZeroMatchFilterReturnEof) {
    const auto schema = ReaderTestSchema();
    auto empty = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), "[]")
                     .ValueOrDie();
    const std::string empty_path = PathUtil::JoinPath(directory_->Str(), "empty.vortex");
    ASSERT_OK(WriteFile(empty_path, schema, empty, /*batch_size=*/2));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> empty_reader, CreateReader(empty_path));
    ASSERT_OK_AND_ASSIGN(uint64_t count, empty_reader->GetNumberOfRows());
    ASSERT_EQ(count, uint64_t{0});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, ReadNextBatch(empty_reader.get()));
    ASSERT_EQ(batch, nullptr);
    ASSERT_NOK_WITH_MSG(empty_reader->GetPreviousBatchFileRowId(0),
                        "no Vortex batch has been read yet");
    ASSERT_OK_AND_ASSIGN(std::string path, WriteReaderTestFile());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, CreateReader(path));
    const auto predicate =
        PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(static_cast<int32_t>(100)));
    ASSERT_OK(ConfigureReader(reader.get(), schema, predicate));
    ASSERT_OK_AND_ASSIGN(batch, ReadNextBatch(reader.get()));
    ASSERT_EQ(batch, nullptr);
    ASSERT_NOK_WITH_MSG(reader->GetPreviousBatchFileRowId(0), "no Vortex batch has been read yet");
}

}  // namespace paimon::vortex::test
