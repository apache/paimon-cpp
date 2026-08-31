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

#include "paimon/core/operation/key_value_file_store_write.h"

#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/map_shredding_defs.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/realtime/realtime_primary_key_reader.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {
namespace {

class TestingMemoryPool final : public MemoryPool {
 public:
    void* Malloc(uint64_t size, uint64_t alignment) override {
        ++allocation_count;
        if (reject_allocations) {
            throw std::bad_alloc();
        }
        return delegate_->Malloc(size, alignment);
    }

    void* Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment) override {
        ++allocation_count;
        if (reject_allocations) {
            throw std::bad_alloc();
        }
        return delegate_->Realloc(pointer, old_size, new_size, alignment);
    }

    void Free(void* pointer, uint64_t size) override {
        delegate_->Free(pointer, size);
    }

    void Free(void* pointer, uint64_t size, uint64_t alignment) override {
        delegate_->Free(pointer, size, alignment);
    }

    uint64_t CurrentUsage() const override {
        return delegate_->CurrentUsage();
    }

    uint64_t MaxMemoryUsage() const override {
        return delegate_->MaxMemoryUsage();
    }

    bool reject_allocations = false;
    int64_t allocation_count = 0;

 private:
    std::unique_ptr<MemoryPool> delegate_ = GetMemoryPool();
};

}  // namespace

class KeyValueFileStoreWriteTest : public ::testing::Test {
 protected:
    Result<std::unique_ptr<FileStoreWrite>> CreateSingleStringFileStoreWrite(
        const std::map<std::string, std::string>& table_options, bool with_temp_directory) {
        auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(typed_schema, &schema));

        auto dir = UniqueTestDirectory::Create();
        if (!dir) {
            return Status::Invalid("failed to create test directory");
        }
        PAIMON_ASSIGN_OR_RAISE(auto catalog, Catalog::Create(dir->Str(), {}));
        PAIMON_RETURN_NOT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        PAIMON_RETURN_NOT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                                  /*partition_keys=*/{},
                                                  /*primary_keys=*/{"f0"}, table_options,
                                                  /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
        if (with_temp_directory) {
            context_builder.WithTempDirectory(dir->Str());
        }

        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context,
                               context_builder.Finish());
        return FileStoreWrite::Create(std::move(write_context));
    }

    Status WriteSingleStringRow(FileStoreWrite* file_store_write, int32_t bucket,
                                const std::string& value) {
        auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
        auto struct_type = arrow::struct_(fields);
        arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::StringBuilder>()});
        auto string_builder = checked_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(string_builder->Append(value));

        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        ::ArrowArray arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));

        RecordBatchBuilder batch_builder(&arrow_array);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                               batch_builder.SetBucket(bucket).Finish());
        Status write_status = file_store_write->Write(std::move(batch));
        if (!ArrowArrayIsReleased(&arrow_array)) {
            ArrowArrayRelease(&arrow_array);
        }
        return write_status;
    }

    void CreateTable(const std::string& warehouse, const std::shared_ptr<arrow::Schema>& schema,
                     const std::map<std::string, std::string>& options) const {
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(warehouse, options));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                       /*partition_keys=*/{}, /*primary_keys=*/{"id"}, options,
                                       /*ignore_if_exists=*/false));
    }

    std::unique_ptr<RecordBatch> MakeBatch(
        const std::shared_ptr<arrow::Schema>& schema, const std::string& json,
        const std::vector<RecordBatch::RowKind>& row_kinds = {}) const {
        auto struct_type = arrow::struct_(schema->fields());
        auto array = arrow::ipc::internal::json::ArrayFromJSON(struct_type, json).ValueOrDie();
        ::ArrowArray arrow_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.SetRowKinds(row_kinds).SetBucket(0).Finish().value();
    }

    std::vector<std::shared_ptr<CommitMessage>> WriteAndPrepare(
        const std::string& table_path, const std::shared_ptr<arrow::Schema>& schema,
        const std::map<std::string, std::string>& options, const std::string& json,
        int64_t commit_identifier) const {
        WriteContextBuilder builder(table_path, "test");
        builder.SetOptions(options);
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        EXPECT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        EXPECT_OK(file_store_write->Write(MakeBatch(schema, json)));
        EXPECT_OK_AND_ASSIGN(auto commit_msgs, file_store_write->PrepareCommit(
                                                   /*wait_compaction=*/false, commit_identifier));
        EXPECT_OK(file_store_write->Close());
        return commit_msgs;
    }

    std::shared_ptr<DataFileMeta> OnlyNewFile(
        const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        EXPECT_EQ(1, commit_msgs.size());
        auto msg = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msgs[0]);
        EXPECT_NE(nullptr, msg);
        EXPECT_EQ(1, msg->GetNewFilesIncrement().NewFiles().size());
        return msg->GetNewFilesIncrement().NewFiles()[0];
    }

    void Commit(const std::string& table_path, const std::map<std::string, std::string>& options,
                const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        CommitContextBuilder builder(table_path, "test");
        builder.SetOptions(options);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_commit,
                             FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK(file_store_commit->Commit(commit_msgs));
    }

    std::shared_ptr<arrow::Schema> ReadDataFileSchema(
        const std::string& table_path, const std::shared_ptr<DataFileMeta>& file,
        const std::map<std::string, std::string>& options) const {
        std::string file_path =
            PathUtil::JoinPath(PathUtil::JoinPath(table_path, "bucket-0"), file->file_name);
        auto fs = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, fs->Open(file_path));
        EXPECT_OK_AND_ASSIGN(auto format_str, file->FileFormat());
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get(format_str, options));
        EXPECT_OK_AND_ASSIGN(auto reader_builder, file_format->CreateReaderBuilder(10));
        EXPECT_OK_AND_ASSIGN(auto reader, reader_builder->Build(input_stream));
        EXPECT_OK_AND_ASSIGN(auto c_file_schema, reader->GetFileSchema());
        return arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
    }

    MapSharedShreddingFieldMeta ShreddingMeta(const std::shared_ptr<arrow::Schema>& file_schema,
                                              int32_t field_index) const {
        auto metadata = file_schema->field(field_index)->metadata();
        EXPECT_NE(nullptr, metadata);
        return MapSharedShreddingUtils::DeserializeMetadata(metadata->Copy()).value();
    }

    Result<std::vector<std::tuple<int8_t, int64_t, std::string, int64_t, int64_t>>>
    ReadRealtimePrimaryKeyTransportRows(
        const std::shared_ptr<RealtimeContext>& realtime_context) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> context,
                               RealtimeContextImpl::Cast(realtime_context));
        PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimePartitionBucketView> views,
                               context->AcquireReadViews());
        if (views.size() != 1) {
            return Status::Invalid("expected exactly one real-time store");
        }
        arrow::FieldVector value_fields = {DataField::ConvertDataFieldToArrowField(DataField(
                                               0, arrow::field("id", arrow::int64(), false))),
                                           DataField::ConvertDataFieldToArrowField(
                                               DataField(1, arrow::field("value", arrow::utf8())))};
        std::shared_ptr<arrow::Schema> transport_schema =
            RealtimePrimaryKeyLayout::CreateSchema(value_fields);
        auto c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*transport_schema, c_schema.get()));
        RealtimeQueryContext query_context{c_schema.get(), /*predicate=*/nullptr};
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::unique_ptr<BatchReader>> readers,
            views[0].store->CreateQueryReaders(views[0].read_view, query_context));
        std::vector<std::tuple<int8_t, int64_t, std::string, int64_t, int64_t>> rows;
        for (const std::unique_ptr<BatchReader>& reader : readers) {
            while (true) {
                PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
                if (BatchReader::IsEofBatch(batch)) {
                    break;
                }
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> array,
                    arrow::ImportArray(batch.first.get(), batch.second.get()));
                std::shared_ptr<arrow::StructArray> values =
                    std::dynamic_pointer_cast<arrow::StructArray>(array);
                if (!values || values->num_fields() != 5) {
                    return Status::Invalid("unexpected realtime primary-key transport batch");
                }
                std::shared_ptr<arrow::Int8Array> row_kinds =
                    std::dynamic_pointer_cast<arrow::Int8Array>(values->field(0));
                std::shared_ptr<arrow::Int64Array> sequences =
                    std::dynamic_pointer_cast<arrow::Int64Array>(values->field(1));
                std::shared_ptr<arrow::Int64Array> offsets =
                    std::dynamic_pointer_cast<arrow::Int64Array>(values->field(2));
                std::shared_ptr<arrow::Int64Array> ids =
                    std::dynamic_pointer_cast<arrow::Int64Array>(values->field(3));
                std::shared_ptr<arrow::StringArray> payloads =
                    std::dynamic_pointer_cast<arrow::StringArray>(values->field(4));
                if (!row_kinds || !sequences || !offsets || !ids || !payloads) {
                    return Status::Invalid("unexpected realtime primary-key transport column type");
                }
                for (int64_t row = 0; row < values->length(); ++row) {
                    rows.emplace_back(row_kinds->Value(row), ids->Value(row),
                                      payloads->GetString(row), sequences->Value(row),
                                      offsets->Value(row));
                }
            }
            reader->Close();
        }
        return rows;
    }
};

TEST_F(KeyValueFileStoreWriteTest, TestWriteWithInvalidBatch) {
    auto fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int8()),     arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int16()),    arrow::field("f5", arrow::int32()),
        arrow::field("f6", arrow::int32()),    arrow::field("f7", arrow::int64()),
        arrow::field("f8", arrow::int64()),    arrow::field("f9", arrow::float32()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};
    std::string commit_user = "test";
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "1"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), commit_user);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        ASSERT_NOK_WITH_MSG(file_store_write->Write(nullptr), "batch is null pointer");
    }
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "-2"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        auto array = std::make_shared<arrow::Array>();
        arrow::StringBuilder builder;
        for (size_t j = 0; j < 100; j++) {
            ASSERT_TRUE(builder.Append(std::to_string(j)).ok());
        }
        ASSERT_TRUE(builder.Finish(&array).ok());
        ::ArrowArray arrow_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetBucket(1).Finish());
        ASSERT_NOK_WITH_MSG(file_store_write->Write(std::move(batch)),
                            "batch bucket is 1 while options bucket is -2");
        ArrowArrayRelease(&arrow_array);
    }
    {
        arrow::Schema typed_schema(fields);
        ::ArrowSchema schema;
        ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema, /*partition_keys=*/{},
                                       /*primary_keys=*/{"f1"}, /*options=*/{{"bucket", "2"}},
                                       /*ignore_if_exists=*/false));

        WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"),
                                            commit_user);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        auto array = std::make_shared<arrow::Array>();
        arrow::StringBuilder builder;
        for (size_t j = 0; j < 100; j++) {
            ASSERT_TRUE(builder.Append(std::to_string(j)).ok());
        }
        ASSERT_TRUE(builder.Finish(&array).ok());
        ::ArrowArray arrow_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
        RecordBatchBuilder batch_builder(&arrow_array);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetBucket(3).Finish());
        ASSERT_NOK_WITH_MSG(
            file_store_write->Write(std::move(batch)),
            "fixed bucketed mode must specify a bucket which in [0, 2) in RecordBatch");
        ArrowArrayRelease(&arrow_array);
    }
}

TEST_F(KeyValueFileStoreWriteTest, TestPrepareCommitShouldSucceedWhenLookupEnabledWithIOManager) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}, {Options::FORCE_LOOKUP, "true"}},
                                         /*with_temp_directory=*/true));

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"));
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);
}

TEST_F(KeyValueFileStoreWriteTest, TestRealtimeWrite) {
    const std::map<std::string, std::string> options = {
        {Options::BUCKET, "1"},
        {Options::WRITE_BUFFER_SIZE, "1"},
        {Options::REALTIME_ENABLED, "true"},
    };
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("id", arrow::int64(), false),
        arrow::field("value", arrow::utf8()),
    });
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), schema, options);
    const std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    WriteContextBuilder builder(table_path, "test");
    builder.SetOptions(options)
        .WithStreamingMode(true)
        .WithRealtimeContext(realtime_context)
        .WithTempDirectory(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         FileStoreWrite::Create(std::move(write_context)));

    std::unique_ptr<RecordBatch> batch =
        MakeBatch(schema, R"([
        [1, "old"],
        [2, "two"],
        [1, "new"]
    ])",
                  {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::DELETE,
                   RecordBatch::RowKind::UPDATE_AFTER});
    ASSERT_OK(writer->Write(std::move(batch)));
    using RealtimePrimaryKeyTransportRow =
        std::tuple<int8_t, int64_t, std::string, int64_t, int64_t>;
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePrimaryKeyTransportRow> transport_rows,
                         ReadRealtimePrimaryKeyTransportRows(realtime_context));
    ASSERT_EQ((std::vector<RealtimePrimaryKeyTransportRow>{
                  {0, 1, "old", 0, 0}, {2, 1, "new", 2, 2}, {3, 2, "two", 1, 1}}),
              transport_rows);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> progresses,
                         writer->PrepareCommitWithProgress(0));
    ASSERT_EQ(1, progresses.size());
    ASSERT_EQ(OffsetRange(0, 3), progresses[0].offset_range);
    std::shared_ptr<CommitMessageImpl> commit_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(progresses[0].commit_message);
    ASSERT_NE(nullptr, commit_message);
    int64_t row_count = 0;
    for (const std::shared_ptr<DataFileMeta>& file :
         commit_message->GetNewFilesIncrement().NewFiles()) {
        row_count += file->row_count;
    }
    ASSERT_EQ(2, row_count);
    ASSERT_EQ(0, TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()));
    ASSERT_OK(writer->Close());
}

TEST_F(KeyValueFileStoreWriteTest, TestRealtimePool) {
    const std::map<std::string, std::string> options = {{Options::BUCKET, "1"},
                                                        {Options::REALTIME_ENABLED, "true"}};
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("id", arrow::int64(), false),
        arrow::field("value", arrow::utf8()),
    });
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), schema, options);
    const std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    std::shared_ptr<TestingMemoryPool> pool = std::make_shared<TestingMemoryPool>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    WriteContextBuilder builder(table_path, "test");
    builder.SetOptions(options)
        .WithStreamingMode(true)
        .WithRealtimeContext(realtime_context)
        .WithMemoryPool(pool);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         FileStoreWrite::Create(std::move(write_context)));

    const int64_t allocations_before_write = pool->allocation_count;
    ASSERT_OK(writer->Write(MakeBatch(schema, R"([[1, "one"]])")));
    ASSERT_GT(pool->allocation_count, allocations_before_write);
    ASSERT_OK(writer->Close());
    writer.reset();
    using RealtimePrimaryKeyTransportRow =
        std::tuple<int8_t, int64_t, std::string, int64_t, int64_t>;
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePrimaryKeyTransportRow> retained_rows,
                         ReadRealtimePrimaryKeyTransportRows(realtime_context));
    ASSERT_EQ((std::vector<RealtimePrimaryKeyTransportRow>{{0, 1, "one", 0, 0}}), retained_rows);

    std::shared_ptr<TestingMemoryPool> rejecting_pool = std::make_shared<TestingMemoryPool>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> rejecting_context,
                         RealtimeContext::Create());
    WriteContextBuilder rejecting_builder(table_path, "rejecting");
    rejecting_builder.SetOptions(options)
        .WithStreamingMode(true)
        .WithRealtimeContext(rejecting_context)
        .WithMemoryPool(rejecting_pool);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> rejecting_write_context,
                         rejecting_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> rejecting_writer,
                         FileStoreWrite::Create(std::move(rejecting_write_context)));
    ASSERT_OK(rejecting_writer->Write(MakeBatch(schema, "[]")));
    const int64_t rejecting_allocations_before_write = rejecting_pool->allocation_count;
    rejecting_pool->reject_allocations = true;
    ASSERT_NOK_WITH_MSG(rejecting_writer->Write(MakeBatch(schema, R"([[2, "two"]])")),
                        "Out of memory");
    ASSERT_GT(rejecting_pool->allocation_count, rejecting_allocations_before_write);
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePrimaryKeyTransportRow> rejected_rows,
                         ReadRealtimePrimaryKeyTransportRows(rejecting_context));
    ASSERT_TRUE(rejected_rows.empty());
    ASSERT_OK(rejecting_writer->Close());
}

TEST_F(KeyValueFileStoreWriteTest, TestRealtimeLimits) {
    const int64_t max = std::numeric_limits<int64_t>::max();
    const std::map<std::string, std::string> options = {{Options::BUCKET, "1"},
                                                        {Options::REALTIME_ENABLED, "true"}};
    const std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("id", arrow::int64(), false),
        arrow::field("value", arrow::utf8()),
    });
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), schema, options);
    const std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> initial_context,
                         RealtimeContext::Create());
    WriteContextBuilder initial_builder(table_path, "initial");
    initial_builder.SetOptions(options).WithStreamingMode(true).WithRealtimeContext(
        initial_context);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> initial_write_context,
                         initial_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> initial_writer,
                         FileStoreWrite::Create(std::move(initial_write_context)));
    ASSERT_OK(initial_writer->Write(MakeBatch(schema, R"([[0, "initial"]])")));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimeCommitProgress> initial_progress,
                         initial_writer->PrepareCommitWithProgress(0));
    ASSERT_EQ(1, initial_progress.size());
    std::shared_ptr<CommitMessageImpl> initial_message =
        std::dynamic_pointer_cast<CommitMessageImpl>(initial_progress[0].commit_message);
    ASSERT_NE(nullptr, initial_message);
    ASSERT_EQ(1, initial_message->GetNewFilesIncrement().NewFiles().size());
    initial_message->GetNewFilesIncrement().NewFiles()[0]->AssignSequenceNumber(max - 2, max - 2);
    initial_progress[0].offset_range = OffsetRange(0, max - 1);

    CommitContextBuilder commit_builder(table_path, "initial");
    commit_builder.SetOptions(options);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> committer,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK_AND_ASSIGN(int64_t snapshot_id,
                         committer->CommitWithProgress(initial_progress, 0, std::nullopt));
    ASSERT_OK(initial_writer->Close());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContext> realtime_context,
                         RealtimeContext::Create());
    WriteContextBuilder builder(table_path, "boundary");
    builder.SetOptions(options).WithStreamingMode(true).WithRealtimeContext(realtime_context);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         FileStoreWrite::Create(std::move(write_context)));
    ASSERT_OK(writer->Write(MakeBatch(schema, R"([[1, "legal"]])")));
    using RealtimePrimaryKeyTransportRow =
        std::tuple<int8_t, int64_t, std::string, int64_t, int64_t>;
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePrimaryKeyTransportRow> transport_rows,
                         ReadRealtimePrimaryKeyTransportRows(realtime_context));
    ASSERT_EQ((std::vector<RealtimePrimaryKeyTransportRow>{{0, 1, "legal", max - 1, max - 1}}),
              transport_rows);

    ASSERT_NOK_WITH_MSG(writer->Write(MakeBatch(schema, R"([[2, "overflow"]])")),
                        "real-time offset range exceeds INT64_MAX");
    ASSERT_OK_AND_ASSIGN(transport_rows, ReadRealtimePrimaryKeyTransportRows(realtime_context));
    ASSERT_EQ((std::vector<RealtimePrimaryKeyTransportRow>{{0, 1, "legal", max - 1, max - 1}}),
              transport_rows);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeContextImpl> context_impl,
                         RealtimeContextImpl::Cast(realtime_context));
    ASSERT_OK_AND_ASSIGN(std::vector<RealtimePartitionBucketView> views,
                         context_impl->AcquireReadViews());
    ASSERT_EQ(1, views.size());
    ASSERT_EQ(std::optional<OffsetRange>(OffsetRange(max - 1, max)),
              views[0].read_view->GetOffsetRange());
    ASSERT_OK(writer->Close());
    ASSERT_GE(snapshot_id, 1);
}

TEST_F(KeyValueFileStoreWriteTest,
       TestPrepareCommitShouldSucceedWhenDefaultCompactRewriterPathEnabled) {
    ASSERT_OK_AND_ASSIGN(
        auto file_store_write,
        CreateSingleStringFileStoreWrite({{"bucket", "1"}}, /*with_temp_directory=*/false));

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "k1"));
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);
}

TEST_F(KeyValueFileStoreWriteTest, TestSharedShreddingMapAdaptsAcrossRollingFiles) {
    std::map<std::string, std::string> options = {
        {"file.format", "parquet"},
        {"target-file-row-num", "2"},
        {"write.batch-size", "2"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "10"},
        {"fields.tags.map.shared-shredding.column-placement-policy", "plain"},
        {"write-only", "true"},
        {"bucket", "1"},
    };
    auto logical_schema = arrow::schema({
        arrow::field("id", arrow::int32(), /*nullable=*/false),
        arrow::field("tags", arrow::map(arrow::utf8(), arrow::int64())),
    });
    auto write_schema = SpecialFields::CompleteSequenceAndValueKindField(logical_schema);

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    CreateTable(dir->Str(), logical_schema, options);
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    auto commit_msgs = WriteAndPrepare(table_path, logical_schema, options, R"([
        [1, [["a", 1], ["b", 2]]],
        [2, [["c", 3], ["d", 4], ["e", 5]]],
        [3, [["f", 6]]],
        [4, [["g", 7], ["h", 8]]]
    ])",
                                       /*commit_identifier=*/0);

    ASSERT_EQ(1, commit_msgs.size());
    auto commit_msg = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msgs[0]);
    ASSERT_NE(nullptr, commit_msg);
    const auto& files = commit_msg->GetNewFilesIncrement().NewFiles();
    ASSERT_EQ(2, files.size());

    auto first_file_schema = ReadDataFileSchema(table_path, files[0], options);
    auto first_meta = ShreddingMeta(first_file_schema, /*field_index=*/3);
    ASSERT_OK_AND_ASSIGN(
        auto expected_first_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(write_schema, {{"tags", 10}}));
    ASSERT_TRUE(first_file_schema->Equals(*expected_first_schema, /*check_metadata=*/false));
    ASSERT_EQ(10, first_meta.num_columns);
    ASSERT_EQ(3, first_meta.max_row_width);

    auto second_file_schema = ReadDataFileSchema(table_path, files[1], options);
    auto second_meta = ShreddingMeta(second_file_schema, /*field_index=*/3);
    ASSERT_OK_AND_ASSIGN(
        auto expected_second_schema,
        MapSharedShreddingUtils::LogicalToPhysicalSchema(write_schema, {{"tags", 3}}));
    ASSERT_TRUE(second_file_schema->Equals(*expected_second_schema, /*check_metadata=*/false));
    ASSERT_EQ(3, second_meta.num_columns);
    ASSERT_EQ(2, second_meta.max_row_width);
}

TEST_F(KeyValueFileStoreWriteTest, TestSpillSimple) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"f0"},
                                   {{Options::BUCKET, "2"},
                                    {Options::WRITE_BUFFER_SIZE, "64"},
                                    {Options::WRITE_BUFFER_SPILLABLE, "true"}},
                                   /*ignore_if_exists=*/false));

    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    context_builder.WithTempDirectory(dir->Str());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));
    auto key_value_file_store_write = dynamic_cast<KeyValueFileStoreWrite*>(file_store_write.get());
    auto get_writer = [&](int32_t bucket) -> std::shared_ptr<paimon::BatchWriter> {
        auto partition_iter = key_value_file_store_write->writers_.find(BinaryRow::EmptyRow());
        if (partition_iter != key_value_file_store_write->writers_.end()) {
            auto& buckets = partition_iter->second;
            auto bucket_iter = buckets.find(bucket);
            if (PAIMON_LIKELY(bucket_iter != buckets.end())) {
                return bucket_iter->second.writer;
            }
        }
        assert(false);
        return nullptr;
    };

    // write bucket 0, not trigger spill
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, std::string(48, 'a')));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);
    ASSERT_GT(get_writer(0)->GetMemoryUsage(), 0);

    // write bucket 1, spill bucket 0 (pick largest writer)
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/1, std::string(32, 'b')));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 1);
    ASSERT_EQ(get_writer(0)->GetMemoryUsage(), 0);
    ASSERT_GT(get_writer(1)->GetMemoryUsage(), 0);

    // prepare commit, clean all spill files and memory buffers
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 2);
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);
    ASSERT_EQ(get_writer(0)->GetMemoryUsage(), 0);
    ASSERT_EQ(get_writer(1)->GetMemoryUsage(), 0);
}

TEST_F(KeyValueFileStoreWriteTest, TestWriterRestoreKeepsValueStats) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::map<std::string, std::string> options = {
        {Options::BUCKET, "1"},
        {Options::FILE_FORMAT, "orc"},
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::MANIFEST_DELETE_FILE_DROP_STATS, "true"}};
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"f0"}, options,
                                   /*ignore_if_exists=*/false));

    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    WriteContextBuilder first_context_builder(table_path, "first-writer");
    first_context_builder.SetOptions(options);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> first_context,
                         first_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto first_write, FileStoreWrite::Create(std::move(first_context)));
    ASSERT_OK(WriteSingleStringRow(first_write.get(), /*bucket=*/0, "alice"));
    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         first_write->PrepareCommit(/*wait_compaction=*/false, 1));
    ASSERT_OK(first_write->Close());
    ASSERT_EQ(1, commit_messages.size());
    auto commit_message = std::dynamic_pointer_cast<CommitMessageImpl>(commit_messages[0]);
    ASSERT_NE(nullptr, commit_message);
    ASSERT_EQ(1, commit_message->GetNewFilesIncrement().NewFiles().size());
    ASSERT_FALSE(commit_message->GetNewFilesIncrement().NewFiles()[0]->value_stats ==
                 SimpleStats::EmptyStats());
    Commit(table_path, options, commit_messages);

    WriteContextBuilder restored_context_builder(table_path, "restored-writer");
    restored_context_builder.SetOptions(options);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> restored_context,
                         restored_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto restored_write, FileStoreWrite::Create(std::move(restored_context)));
    auto key_value_write = dynamic_cast<KeyValueFileStoreWrite*>(restored_write.get());
    ASSERT_NE(nullptr, key_value_write);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RestoreFiles> restore_files,
                         key_value_write->ScanExistingFileMetas(BinaryRow::EmptyRow(),
                                                                /*bucket=*/0));
    ASSERT_EQ(1, restore_files->DataFiles().size());
    ASSERT_FALSE(restore_files->DataFiles()[0]->value_stats == SimpleStats::EmptyStats());
    ASSERT_OK(restored_write->Close());
}

TEST_F(KeyValueFileStoreWriteTest, TestSpillDiskQuotaExhaustedFallsBackToFlushDataFile) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"f0"},
                                   {{Options::BUCKET, "1"},
                                    {Options::WRITE_BUFFER_SIZE, "1"},
                                    {Options::WRITE_BUFFER_SPILLABLE, "true"},
                                    {Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE, "1b"}},
                                   /*ignore_if_exists=*/false));
    ArrowSchemaRelease(&schema);
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    context_builder.WithTempDirectory(dir->Str());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    // Disk quota is 1 byte, so spill will exhaust quota immediately and fall back to
    // FlushWriteBuffer (writing data files directly instead of spill temp files).
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "alice"));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);

    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "bob"));
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);

    ASSERT_OK_AND_ASSIGN(auto commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true));
    ASSERT_EQ(commit_messages.size(), 1);

    // Verify all rows are committed correctly despite disk quota exhaustion.
    auto* commit_impl = dynamic_cast<CommitMessageImpl*>(commit_messages[0].get());
    ASSERT_NE(commit_impl, nullptr);
    const auto& new_files = commit_impl->GetNewFilesIncrement().NewFiles();
    ASSERT_FALSE(new_files.empty());

    int64_t total_row_count = 0;
    for (const auto& file : new_files) {
        total_row_count += file->row_count;
    }
    ASSERT_EQ(total_row_count, 2);
}

TEST_F(KeyValueFileStoreWriteTest, TestMultiRoundSpillWithSameKeyDeduplication) {
    auto fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/false)};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &schema,
                                   /*partition_keys=*/{}, /*primary_keys=*/{"f0"},
                                   {{Options::BUCKET, "1"},
                                    {Options::WRITE_BUFFER_SIZE, "1"},
                                    {Options::WRITE_BUFFER_SPILLABLE, "true"}},
                                   /*ignore_if_exists=*/false));
    ArrowSchemaRelease(&schema);
    WriteContextBuilder context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"), "test");
    context_builder.WithTempDirectory(dir->Str()).WithStreamingMode(true);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto file_store_write, FileStoreWrite::Create(std::move(write_context)));

    // Round 1: alice, bob, alice (duplicate key) → after dedup: alice + bob = 2 rows
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "alice"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "bob"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "alice"));

    ASSERT_OK_AND_ASSIGN(auto commit_messages_1,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true, 0));
    ASSERT_EQ(commit_messages_1.size(), 1);
    {
        auto* commit_impl = dynamic_cast<CommitMessageImpl*>(commit_messages_1[0].get());
        ASSERT_NE(commit_impl, nullptr);
        int64_t total_row_count = 0;
        for (const auto& file : commit_impl->GetNewFilesIncrement().NewFiles()) {
            total_row_count += file->row_count;
        }
        ASSERT_EQ(total_row_count, 2);
    }
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);

    // Round 2: bob, charlie, charlie (duplicate key) → after dedup: bob + charlie = 2 rows
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "bob"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "charlie"));
    ASSERT_OK(WriteSingleStringRow(file_store_write.get(), /*bucket=*/0, "charlie"));

    ASSERT_OK_AND_ASSIGN(auto commit_messages_2,
                         file_store_write->PrepareCommit(/*wait_compaction=*/true, 1));
    ASSERT_EQ(commit_messages_2.size(), 1);
    {
        auto* commit_impl = dynamic_cast<CommitMessageImpl*>(commit_messages_2[0].get());
        ASSERT_NE(commit_impl, nullptr);
        int64_t total_row_count = 0;
        for (const auto& file : commit_impl->GetNewFilesIncrement().NewFiles()) {
            total_row_count += file->row_count;
        }
        ASSERT_EQ(total_row_count, 2);
    }
    ASSERT_EQ(TestHelper::CountChannelFiles(dir->GetFileSystem(), dir->Str()), 0);
}

}  // namespace paimon::test
