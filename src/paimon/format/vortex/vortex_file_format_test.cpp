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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

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
        Result<std::unique_ptr<FormatWriter>> writer = writer_builder->Build(output, "zstd");
        ASSERT_FALSE(writer.ok());
        ASSERT_TRUE(writer.status().IsInvalid()) << writer.status().ToString();
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
                        "VECTOR cannot contain null elements");
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
                            "VECTOR cannot contain null elements");
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
    Result<std::shared_ptr<arrow::Array>> rejected =
        ReadFile(path, widened, /*batch_size=*/2, /*expected_row_count=*/3);
    ASSERT_FALSE(rejected.ok());
    ASSERT_TRUE(rejected.status().IsInvalid()) << rejected.status().ToString();
}

// A read field the file does not carry is null-filled instead of rejected: that is the
// schema-evolution answer the rest of the read path gives, so every file yields the read schema.
TEST_F(VortexFileFormatTest, ProjectedReadNullFillsFieldMissingFromFile) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "missing-field.vortex");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> written =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([[1],[2],[3]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, written, /*batch_size=*/2));

    arrow::FieldVector projected_fields = {fields[0], arrow::field("extra", arrow::utf8())};
    std::shared_ptr<arrow::Schema> projected = arrow::schema(projected_fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(projected_fields),
                                                  R"([[1,null],[2,null],[3,null]])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> actual,
                         ReadFile(path, projected, /*batch_size=*/2, /*expected_row_count=*/3));
    ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n" << expected->ToString();
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

}  // namespace paimon::vortex::test
