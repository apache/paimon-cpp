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

#include "paimon/file_index/file_index_write.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/common/file_index/file_index_format_utils.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory_cache.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/deletion_vector_test_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {
namespace {

using Messages = std::vector<std::shared_ptr<CommitMessage>>;
// Outer key: original data-file field name; value: indexes for that field.
// Inner key: index type (e.g. bitmap or bsi); value: raw index payload bytes.
// std::nullopt represents an explicit empty index.
using PayloadBytes = std::map<std::string, std::map<std::string, std::optional<std::string>>>;

class PayloadInput : public MockInputStream {
 public:
    explicit PayloadInput(std::string bytes) : bytes_(std::move(bytes)) {}

    using MockInputStream::Read;

    Status Seek(int64_t offset, SeekOrigin origin) override {
        if (fail_seek) {
            return Status::IOError("injected payload seek failure");
        }
        int64_t start =
            origin == FS_SEEK_SET
                ? 0
                : (origin == FS_SEEK_CUR ? position_ : static_cast<int64_t>(bytes_.size()));
        if (offset < -start || offset > static_cast<int64_t>(bytes_.size()) - start) {
            return Status::Invalid("payload seek out of bounds");
        }
        position_ = start + offset;
        return Status::OK();
    }

    Result<int64_t> GetPos() const override {
        return position_;
    }

    Result<int64_t> Length() const override {
        if (fail_length) {
            return Status::IOError("injected payload length failure");
        }
        return bytes_.size();
    }

    Result<int64_t> Read(char* buffer, int64_t size) override {
        if (fail_read) {
            return Status::IOError("injected payload read failure");
        }
        if (early_eof) {
            return 0;
        }
        int64_t count = std::min({size, max_read, static_cast<int64_t>(bytes_.size()) - position_});
        std::memcpy(buffer, bytes_.data() + position_, count);
        position_ += count;
        return count;
    }

    Status Close() override {
        ++close_count;
        return Status::OK();
    }

    const std::string& BytesValue() const {
        return bytes_;
    }

    bool fail_length = false;
    bool fail_seek = false;
    bool fail_read = false;
    bool early_eof = false;
    int64_t max_read = std::numeric_limits<int64_t>::max();
    int32_t close_count = 0;

 private:
    std::string bytes_;
    int64_t position_ = 0;
};

enum class OutputFailure { NONE, CREATE, WRITE, FLUSH, CLOSE };

class IndexOutput : public OutputStream {
 public:
    IndexOutput(std::unique_ptr<OutputStream> output, OutputFailure failure)
        : output_(std::move(output)), failure_(failure) {}

    Result<int64_t> Write(const char* buffer, int64_t size) override {
        if (failure_ == OutputFailure::WRITE) {
            return Status::IOError("injected index write failure");
        }
        return output_->Write(buffer, size);
    }

    Result<int64_t> GetPos() const override {
        return output_->GetPos();
    }

    Result<std::string> GetUri() const override {
        return output_->GetUri();
    }

    Status Flush() override {
        if (failure_ == OutputFailure::FLUSH) {
            return Status::IOError("injected index flush failure");
        }
        return output_->Flush();
    }

    Status Close() override {
        PAIMON_RETURN_NOT_OK(output_->Close());
        if (failure_ == OutputFailure::CLOSE) {
            return Status::IOError("injected index close failure");
        }
        return Status::OK();
    }

 private:
    std::unique_ptr<OutputStream> output_;
    OutputFailure failure_;
};

class IndexFileSystem : public LocalFileSystem {
 public:
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        if (!StringUtils::EndsWith(path, ".index")) {
            return LocalFileSystem::Create(path, overwrite);
        }
        ++create_attempts;
        if (failure == OutputFailure::CREATE) {
            return Status::IOError("injected index create failure");
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> output,
                               LocalFileSystem::Create(path, overwrite));
        created.push_back(path);
        return std::unique_ptr<OutputStream>(
            std::make_unique<IndexOutput>(std::move(output), failure));
    }

    Status Delete(const std::string& path, bool recursive = true) const override {
        delete_attempts.push_back(path);
        if (fail_delete) {
            return Status::IOError("injected index delete failure");
        }
        return LocalFileSystem::Delete(path, recursive);
    }

    OutputFailure failure = OutputFailure::NONE;
    bool fail_delete = false;
    mutable int32_t create_attempts = 0;
    mutable std::vector<std::string> created;
    mutable std::vector<std::string> delete_attempts;
};

}  // namespace

class FileIndexWriteTest : public ::testing::Test {
 protected:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(directory_);
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        executor_ = CreateDefaultExecutor();
        schema_ = arrow::schema({arrow::field("part", arrow::int32(), false),
                                 arrow::field("value", arrow::int32(), false),
                                 arrow::field("other", arrow::int32())});
    }

    Status CreateTable(const std::map<std::string, std::string>& overrides = {},
                       const std::vector<std::string>& primary_keys = {},
                       const std::string& branch = "main") {
        table_path_ = PathUtil::JoinPath(directory_->Str(), fmt::format("table-{}", next_table_++));
        branch_ = branch;
        options_ = {{Options::FILE_SYSTEM, "local"},
                    {Options::FILE_FORMAT, "parquet"},
                    {Options::BUCKET, "2"},
                    {Options::WRITE_ONLY, "true"},
                    {Options::FILE_INDEX_READ_ENABLED, "true"}};
        for (const auto& [key, value] : overrides) {
            options_[key] = value;
        }
        if (options_[Options::BUCKET] != "-1") {
            options_.try_emplace(Options::BUCKET_KEY, "value");
        }
        options_[Options::BRANCH] = branch;
        SchemaManager manager(fs_, table_path_, branch);
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] std::unique_ptr<TableSchema> created,
                               manager.CreateTable(schema_, {"part"}, primary_keys, options_));
        return Status::OK();
    }

    Status Commit(const Messages& messages) {
        CommitContextBuilder builder(table_path_, "file-index-test");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->Commit(messages, next_commit_++);
    }

    Status Abort(const Messages& messages) const {
        CommitContextBuilder builder(table_path_, "file-index-test");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->Abort(messages);
    }

    Result<Messages> WriteData(const std::string& json = "[[10,1,10],[10,2,20],[10,1,30]]",
                               int32_t bucket = 0, int32_t partition = 10) {
        WriteContextBuilder builder(table_path_, "file-index-test");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> context,
                               builder.SetOptions(options_).Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> writer,
                               FileStoreWrite::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<RecordBatch> batch,
            TestHelper::MakeRecordBatch(arrow::struct_(schema_->fields()), json,
                                        {{"part", std::to_string(partition)}}, bucket, {}));
        PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
        PAIMON_ASSIGN_OR_RAISE(Messages messages,
                               writer->PrepareCommit(/*wait_compaction=*/true, next_commit_));
        PAIMON_RETURN_NOT_OK(Commit(messages));
        PAIMON_RETURN_NOT_OK(writer->Close());
        return messages;
    }

    Result<std::shared_ptr<DataFileMeta>> NewFile(const Messages& messages) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CommitMessageImpl> message, OnlyMessage(messages));
        const auto& files = message->GetNewFilesIncrement().NewFiles();
        if (files.size() != 1) {
            return Status::Invalid("expected one new data file");
        }
        return files[0];
    }

    Result<std::shared_ptr<DataFileMeta>> UpdatedFile(const Messages& messages) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CommitMessageImpl> message, OnlyMessage(messages));
        const auto& files = message->GetCompactIncrement().CompactAfter();
        if (files.size() != 1) {
            return Status::Invalid("expected one updated data file");
        }
        return files[0];
    }

    Result<std::shared_ptr<CommitMessageImpl>> OnlyMessage(const Messages& messages) const {
        if (messages.size() != 1) {
            return Status::Invalid("expected one commit message");
        }
        auto message = std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
        if (!message) {
            return Status::Invalid("expected CommitMessageImpl");
        }
        return message;
    }

    Result<Snapshot> LatestSnapshot() const {
        SnapshotManager manager(fs_, table_path_, branch_);
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, manager.LatestSnapshot());
        if (!snapshot) {
            return Status::Invalid("expected a snapshot");
        }
        return *snapshot;
    }

    Result<Messages> Prepare(const std::vector<DataFileIndexInput>& inputs,
                             const std::map<std::string, std::string>& overrides = {},
                             const std::optional<int64_t>& snapshot = std::nullopt) const {
        return FileIndexWrite::PrepareCommit(table_path_, branch_, overrides, snapshot, inputs, fs_,
                                             pool_, executor_);
    }

    Result<std::shared_ptr<PayloadInput>> MakePayload(
        const std::string& field = "value", const std::string& type = "bitmap",
        const std::string& json = "[[1],[2],[1]]") const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexer> indexer,
                               FileIndexerFactory::Get(type, {{"version", "2"}}));
        if (!indexer) {
            return Status::Invalid("missing test indexer");
        }
        auto schema = arrow::schema({arrow::field(field, arrow::int32())});
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        ScopeGuard close_schema([&c_schema]() { ArrowSchemaRelease(&c_schema); });
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexWriter> writer,
                               indexer->CreateWriter(&c_schema, pool_));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()), json));
        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        ScopeGuard close_array([&c_array]() { ArrowArrayRelease(&c_array); });
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, writer->SerializedBytes());
        return std::make_shared<PayloadInput>(std::string(bytes->data(), bytes->size()));
    }

    Result<std::shared_ptr<DataFilePathFactory>> Paths(int32_t partition = 10,
                                                       int32_t bucket = 0) const {
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, LatestSnapshot());
        SchemaManager manager(fs_, table_path_, branch_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                               manager.ReadSchema(snapshot.SchemaId()));
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(schema->Options(), fs_));
        FileStorePathFactoryCache cache(table_path_, schema, options, pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> paths,
                               cache.GetOrCreatePathFactory(options.GetFileFormat()->Identifier()));
        PAIMON_ASSIGN_OR_RAISE(BinaryRow partition_row,
                               paths->ToBinaryRow({{"part", std::to_string(partition)}}));
        return paths->CreateDataFilePathFactory(partition_row, bucket);
    }

    Result<std::shared_ptr<InputStream>> OpenIndex(const std::shared_ptr<DataFileMeta>& file,
                                                   int32_t partition = 10,
                                                   int32_t bucket = 0) const {
        if (file->embedded_index) {
            return std::shared_ptr<InputStream>(std::make_shared<ByteArrayInputStream>(
                file->embedded_index->data(), file->embedded_index->size()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> paths,
                               Paths(partition, bucket));
        for (const auto& extra : file->extra_files) {
            if (extra && StringUtils::EndsWith(*extra, ".index")) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input,
                                       fs_->Open(paths->ToAlignedPath(*extra, file)));
                return input;
            }
        }
        return Status::Invalid("expected an index container");
    }

    Result<PayloadBytes> ReadPayloads(const std::shared_ptr<DataFileMeta>& file,
                                      int32_t partition = 10, int32_t bucket = 0) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input,
                               OpenIndex(file, partition, bucket));
        ScopeGuard close([&input]() { [[maybe_unused]] Status ignored = input->Close(); });
        PAIMON_ASSIGN_OR_RAISE(FileIndexFormatUtils::FileIndexEntries entries,
                               FileIndexFormatUtils::ReadIndexEntries(input, pool_));
        PayloadBytes bytes;
        DataInputStream in(input);
        for (const auto& [field, indexes] : entries) {
            for (const auto& [type, range] : indexes) {
                if (range.first == FileIndexFormat::EMPTY_INDEX_FLAG) {
                    bytes[field][type] = std::nullopt;
                } else {
                    std::string payload(range.second, '\0');
                    PAIMON_RETURN_NOT_OK(in.Seek(range.first));
                    PAIMON_RETURN_NOT_OK(in.Read(payload.data(), range.second));
                    bytes[field][type] = std::move(payload);
                }
            }
        }
        return bytes;
    }

    Result<std::vector<std::shared_ptr<Split>>> Scan(
        const std::optional<int64_t>& snapshot = std::nullopt) const {
        ScanContextBuilder builder(table_path_);
        builder.SetOptions(options_).WithStreamingMode(false);
        if (snapshot) {
            builder.AddOption(Options::SCAN_SNAPSHOT_ID, std::to_string(*snapshot));
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                               TableScan::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());
        return plan->Splits();
    }

    Result<std::vector<int32_t>> ReadValues(
        const std::optional<int64_t>& snapshot = std::nullopt, const std::string& field = "value",
        const std::shared_ptr<Predicate>& predicate = nullptr) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Split>> splits, Scan(snapshot));
        ReadContextBuilder builder(table_path_);
        builder.SetOptions(options_).SetReadFieldNames({field});
        if (predicate) {
            builder.SetPredicate(predicate);
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> read,
                               TableRead::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, read->CreateReader(splits));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> rows,
                               ReadResultCollector::CollectResult(std::move(reader)));
        std::vector<int32_t> values;
        for (const auto& chunk : rows->chunks()) {
            const auto* batch = dynamic_cast<const arrow::StructArray*>(chunk.get());
            if (!batch) {
                return Status::Invalid("expected struct rows");
            }
            auto column = batch->GetFieldByName(field);
            const auto* ints = dynamic_cast<const arrow::Int32Array*>(column.get());
            if (!ints) {
                return Status::Invalid("expected int values");
            }
            for (int64_t row = 0; row < ints->length(); ++row) {
                values.push_back(ints->Value(row));
            }
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    Status RewriteMetadata(const std::shared_ptr<DataFileMeta>& before,
                           const std::shared_ptr<DataFileMeta>& after) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Split>> splits, Scan());
        if (splits.size() != 1) {
            return Status::Invalid("expected one split for metadata rewrite");
        }
        auto split = std::dynamic_pointer_cast<DataSplitImpl>(splits[0]);
        if (!split) {
            return Status::Invalid("expected a data split");
        }
        return Commit({std::make_shared<CommitMessageImpl>(
            split->Partition(), split->Bucket(), split->TotalBuckets(), DataIncrement({}, {}, {}),
            CompactIncrement({before}, {after}, {}))});
    }

    Status WriteContainer(const std::shared_ptr<DataFileMeta>& file, const std::string& name,
                          const PayloadBytes& payloads, int32_t version = 1) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> paths, Paths());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                               fs_->Create(paths->ToAlignedPath(name, file), false));
        ScopeGuard close([&output]() { [[maybe_unused]] Status ignored = output->Close(); });
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Writer> writer,
                               FileIndexFormat::CreateWriter(output, version, pool_));
        for (const auto& [field, indexes] : payloads) {
            for (const auto& [type, payload] : indexes) {
                FileIndexFormat::Writer::PayloadWriter copy;
                if (payload) {
                    auto bytes = std::make_shared<Bytes>(*payload, pool_.get());
                    copy = [bytes](OutputStream* destination) -> Result<bool> {
                        PAIMON_RETURN_NOT_OK(DataOutputStream(destination).WriteBytes(bytes));
                        return true;
                    };
                }
                PAIMON_RETURN_NOT_OK(writer->WriteIndex(field, type, copy));
            }
        }
        PAIMON_RETURN_NOT_OK(writer->Finish());
        PAIMON_RETURN_NOT_OK(writer->Close());
        close.Release();
        return Status::OK();
    }

    void AssertRemoved(const std::vector<std::string>& paths) const {
        for (const auto& path : paths) {
            ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(path));
            ASSERT_FALSE(exists) << path;
        }
    }

    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<LocalFileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    std::string table_path_;
    std::string branch_ = "main";
    int32_t next_table_ = 0;
    int64_t next_commit_ = 0;
};

// Parameters: old/new container format versions; old index location (0=none, 1=embedded,
// 2=external).
class VersionedFileIndexWriteTest
    : public FileIndexWriteTest,
      public ::testing::WithParamInterface<std::tuple<int32_t, int32_t, int32_t>> {};

TEST_P(VersionedFileIndexWriteTest, MergeAndCommit) {
    const auto& [old_version, new_version, old_location] = GetParam();
    std::map<std::string, std::string> table_options = {
        {Options::FILE_INDEX_FORMAT_VERSION, std::to_string(old_version)}};
    if (old_location != 0) {
        table_options["file-index.bitmap.columns"] = "value";
        table_options["file-index.bitmap.value.version"] = "1";
        table_options["file-index.bsi.columns"] = "other";
        table_options[Options::FILE_INDEX_IN_MANIFEST_THRESHOLD] = old_location == 1 ? "1MB" : "0B";
    }
    ASSERT_OK(CreateTable(table_options));
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> before, NewFile(written));
    ASSERT_OK_AND_ASSIGN(Snapshot base, LatestSnapshot());
    PayloadBytes old_payloads;
    if (old_location != 0) {
        ASSERT_EQ(old_location == 1, before->embedded_index != nullptr);
        ASSERT_OK_AND_ASSIGN(old_payloads, ReadPayloads(before));
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PayloadInput> bitmap, MakePayload());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PayloadInput> bsi, MakePayload("value", "bsi"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<PayloadInput> other,
                         MakePayload("other", "bitmap", "[[10],[20],[30]]"));
    ASSERT_OK_AND_ASSIGN(
        Messages updates,
        Prepare(
            {{before->file_name,
              {{"value", "bitmap", bitmap}, {"value", "bsi", bsi}, {"other", "bitmap", other}}}},
            {{Options::FILE_INDEX_FORMAT_VERSION, std::to_string(new_version)},
             {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1GB"}},
            base.Id()));
    ASSERT_OK_AND_ASSIGN(Snapshot uncommitted, LatestSnapshot());
    ASSERT_EQ(base.Id(), uncommitted.Id());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitMessageImpl> update, OnlyMessage(updates));
    ASSERT_TRUE(update->GetNewFilesIncrement().IsEmpty());
    ASSERT_EQ(2, update->TotalBuckets());
    const auto& increment = update->GetCompactIncrement();
    ASSERT_EQ(1u, increment.CompactBefore().size());
    ASSERT_EQ(1u, increment.CompactAfter().size());
    ASSERT_TRUE(increment.ChangelogFiles().empty());
    ASSERT_TRUE(increment.NewIndexFiles().empty());
    ASSERT_TRUE(increment.DeletedIndexFiles().empty());
    auto after = increment.CompactAfter()[0];
    ASSERT_FALSE(after->embedded_index);
    ASSERT_EQ(1u, after->extra_files.size());
    ASSERT_EQ(before->file_name + (old_location == 2 ? "-1.index" : ".index"),
              after->extra_files[0].value());
    auto restored = after->CopyWithExtraFiles(before->extra_files);
    restored->embedded_index = before->embedded_index;
    ASSERT_EQ(*before, *restored);
    ASSERT_OK_AND_ASSIGN(PayloadBytes payloads, ReadPayloads(after));
    ASSERT_EQ(bitmap->BytesValue(), payloads.at("value").at("bitmap"));
    ASSERT_EQ(bsi->BytesValue(), payloads.at("value").at("bsi"));
    ASSERT_EQ(other->BytesValue(), payloads.at("other").at("bitmap"));
    if (old_location != 0) {
        ASSERT_NE(old_payloads.at("value").at("bitmap"), payloads.at("value").at("bitmap"));
        ASSERT_EQ(old_payloads.at("other").at("bsi"), payloads.at("other").at("bsi"));
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> index, OpenIndex(after));
    DataInputStream in(index);
    ASSERT_OK_AND_ASSIGN(int64_t magic, in.ReadValue<int64_t>());
    ASSERT_EQ(FileIndexFormat::MAGIC, magic);
    ASSERT_OK_AND_ASSIGN(int32_t version, in.ReadValue<int32_t>());
    ASSERT_EQ(new_version, version);
    ASSERT_OK(index->Close());
    ASSERT_OK(Commit(updates));
    ASSERT_OK_AND_ASSIGN(Snapshot committed, LatestSnapshot());
    ASSERT_EQ(base.Id() + 1, committed.Id());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), committed.GetCommitKind());
    ASSERT_OK_AND_ASSIGN(auto splits, Scan());
    ASSERT_EQ(1u, splits.size());
    auto split = std::dynamic_pointer_cast<DataSplitImpl>(splits[0]);
    ASSERT_TRUE(split);
    ASSERT_EQ(1u, split->DataFiles().size());
    ASSERT_EQ(*after, *split->DataFiles()[0]);
    ASSERT_OK_AND_ASSIGN(
        auto filtered, ReadValues(std::nullopt, "value",
                                  PredicateBuilder::Equal(0, "value", FieldType::INT, Literal(1))));
    ASSERT_EQ((std::vector<int32_t>{1, 1}), filtered);
    if (old_location != 0) {
        ASSERT_OK_AND_ASSIGN(auto historical_payloads, ReadPayloads(before));
        ASSERT_EQ(old_payloads, historical_payloads);
    }
}

INSTANTIATE_TEST_SUITE_P(AllContainers, VersionedFileIndexWriteTest,
                         ::testing::Combine(::testing::Values(1, 2), ::testing::Values(1, 2),
                                            ::testing::Values(0, 1, 2)));

TEST_F(FileIndexWriteTest, GroupsFilesByPartitionAndBucket) {
    ASSERT_OK(CreateTable());
    std::vector<DataFileIndexInput> requests;
    std::unordered_map<std::pair<BinaryRow, int32_t>, std::set<std::string>> expected;
    for (const auto& [partition, bucket] :
         std::vector<std::pair<int32_t, int32_t>>{{10, 0}, {10, 0}, {10, 1}, {20, 0}}) {
        ASSERT_OK_AND_ASSIGN(Messages written,
                             WriteData(fmt::format("[[{},1,10],[{},2,20],[{},1,30]]", partition,
                                                   partition, partition),
                                       bucket, partition));
        ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
        ASSERT_OK_AND_ASSIGN(auto message, OnlyMessage(written));
        expected[{message->Partition(), message->Bucket()}].insert(file->file_name);
        requests.push_back(
            {file->file_name, {{"value", "opaque", std::make_shared<PayloadInput>("payload")}}});
    }
    ASSERT_OK_AND_ASSIGN(Messages updates, Prepare(requests));
    ASSERT_EQ(expected.size(), updates.size());
    for (const auto& raw : updates) {
        auto message = std::dynamic_pointer_cast<CommitMessageImpl>(raw);
        ASSERT_TRUE(message);
        std::set<std::string> names;
        ASSERT_EQ(2, message->TotalBuckets());
        for (const auto& file : message->GetCompactIncrement().CompactAfter()) {
            names.insert(file->file_name);
            ASSERT_EQ(1u, file->extra_files.size());
        }
        auto key = std::make_pair(message->Partition(), message->Bucket());
        ASSERT_EQ(expected.at(key), names);
        expected.erase(key);
    }
    ASSERT_TRUE(expected.empty());
    ASSERT_OK(Commit(updates));
    ASSERT_OK_AND_ASSIGN(auto values, ReadValues());
    ASSERT_EQ(12u, values.size());
    ASSERT_EQ(8, std::count(values.begin(), values.end(), 1));
    ASSERT_EQ(4, std::count(values.begin(), values.end(), 2));
}

TEST_F(FileIndexWriteTest, PreservesOpaqueIndexesAndExtraFiles) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    const PayloadBytes existing = {
        {"value", {{"unregistered", "opaque bytes"}, {"empty-unregistered", std::nullopt}}}};
    ASSERT_OK(WriteContainer(file, "old.index", existing));
    ASSERT_OK_AND_ASSIGN(auto paths, Paths());
    const std::string aux_path = paths->ToAlignedPath("shared.aux", file);
    ASSERT_OK(fs_->WriteFile(aux_path, "auxiliary data", false));
    auto before = file->CopyWithExtraFiles({"old.index", "shared.aux"});
    ASSERT_OK(RewriteMetadata(file, before));
    ASSERT_OK_AND_ASSIGN(auto bitmap, MakePayload());
    ASSERT_OK_AND_ASSIGN(Messages updates,
                         Prepare({{before->file_name,
                                   {{"value", "bitmap", bitmap}, {"other", "opaque", nullptr}}}}));
    ASSERT_OK_AND_ASSIGN(auto after, UpdatedFile(updates));
    ASSERT_EQ(2u, after->extra_files.size());
    ASSERT_EQ("shared.aux", after->extra_files[0]);
    ASSERT_OK_AND_ASSIGN(PayloadBytes actual, ReadPayloads(after));
    ASSERT_EQ(bitmap->BytesValue(), actual.at("value").at("bitmap"));
    ASSERT_EQ(existing.at("value").at("unregistered"), actual.at("value").at("unregistered"));
    ASSERT_EQ(std::nullopt, actual.at("value").at("empty-unregistered"));
    ASSERT_EQ(std::nullopt, actual.at("other").at("opaque"));
    ASSERT_OK(Commit(updates));
    ASSERT_OK_AND_ASSIGN(bool old_exists, fs_->Exists(paths->ToAlignedPath("old.index", before)));
    ASSERT_TRUE(old_exists);
    std::string content;
    ASSERT_OK(fs_->ReadFile(aux_path, &content));
    ASSERT_EQ("auxiliary data", content);
}

TEST_F(FileIndexWriteTest, WritesIndexBesideExternalDataFile) {
    ASSERT_OK(CreateTable({{Options::DATA_FILE_EXTERNAL_PATHS,
                            "file://" + PathUtil::JoinPath(directory_->Str(), "external")},
                           {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}}));
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    ASSERT_TRUE(file->external_path);
    ASSERT_OK_AND_ASSIGN(Snapshot base, LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(auto payload, MakePayload());
    ASSERT_OK(payload->Seek(1, FS_SEEK_SET));
    for (int32_t generation = 0; generation < 3; ++generation) {
        ASSERT_OK_AND_ASSIGN(Messages updates,
                             Prepare({{file->file_name, {{"value", "bitmap", payload}}}}));
        ASSERT_OK_AND_ASSIGN(auto after, UpdatedFile(updates));
        ASSERT_EQ(file->external_path, after->external_path);
        ASSERT_EQ(1u, after->extra_files.size());
        ASSERT_TRUE(after->extra_files[0]);
        std::string expected_name = generation == 0
                                        ? file->file_name + ".index"
                                        : fmt::format("{}-{}.index", file->file_name, generation);
        ASSERT_EQ(expected_name, *after->extra_files[0]);
        const std::string index_path = PathUtil::JoinPath(
            PathUtil::GetParentDirPath(*file->external_path), *after->extra_files[0]);
        ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(index_path));
        ASSERT_TRUE(exists);
        ASSERT_OK(Commit(updates));
        if (generation > 0) {
            ASSERT_OK_AND_ASSIGN(PayloadBytes old_payloads, ReadPayloads(file));
            ASSERT_EQ(payload->BytesValue(), old_payloads.at("value").at("bitmap"));
        }
        file = after;
    }
    ASSERT_OK_AND_ASSIGN(auto values, ReadValues());
    ASSERT_EQ((std::vector<int32_t>{1, 1, 2}), values);
    ASSERT_OK_AND_ASSIGN(auto historical, ReadValues(base.Id()));
    ASSERT_EQ(values, historical);
    ASSERT_OK_AND_ASSIGN(
        auto filtered, ReadValues(std::nullopt, "value",
                                  PredicateBuilder::Equal(0, "value", FieldType::INT, Literal(1))));
    ASSERT_EQ((std::vector<int32_t>{1, 1}), filtered);
    ASSERT_EQ(0, payload->close_count);
    ASSERT_EQ(1, payload.use_count());
}

TEST_F(FileIndexWriteTest, HonorsHistoricalSnapshotAndLatestSelection) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages first_write, WriteData());
    ASSERT_OK_AND_ASSIGN(auto first, NewFile(first_write));
    ASSERT_OK_AND_ASSIGN(Snapshot base, LatestSnapshot());
    ASSERT_OK_AND_ASSIGN(Messages second_write, WriteData());
    ASSERT_OK_AND_ASSIGN(auto second, NewFile(second_write));
    ASSERT_OK_AND_ASSIGN(auto payload, MakePayload());
    const DataFileIndexInput first_input{first->file_name, {{"value", "bitmap", payload}}};
    const DataFileIndexInput second_input{second->file_name, {{"value", "bitmap", payload}}};
    ASSERT_NOK_WITH_MSG(Prepare({second_input}, {}, base.Id()), "not live in the base snapshot");
    ASSERT_OK_AND_ASSIGN(Messages explicit_updates, Prepare({first_input}, {}, base.Id()));
    ASSERT_OK_AND_ASSIGN(auto explicit_message, OnlyMessage(explicit_updates));
    ASSERT_EQ(1u, explicit_message->GetCompactIncrement().CompactBefore().size());
    ASSERT_EQ(*first, *explicit_message->GetCompactIncrement().CompactBefore()[0]);
    ASSERT_OK_AND_ASSIGN(Messages latest_updates, Prepare({second_input}));
    explicit_updates.insert(explicit_updates.end(), latest_updates.begin(), latest_updates.end());
    ASSERT_OK(Commit(explicit_updates));
    ASSERT_OK_AND_ASSIGN(auto old_values, ReadValues(base.Id()));
    ASSERT_EQ((std::vector<int32_t>{1, 1, 2}), old_values);
    ASSERT_OK_AND_ASSIGN(auto current_values, ReadValues());
    ASSERT_EQ((std::vector<int32_t>{1, 1, 1, 1, 2, 2}), current_values);
}

TEST_F(FileIndexWriteTest, IsolatesBranchCommits) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    ASSERT_OK_AND_ASSIGN(Snapshot base, LatestSnapshot());
    SchemaManager main_schema(fs_, table_path_);
    ASSERT_OK_AND_ASSIGN(auto schema, main_schema.ReadSchema(base.SchemaId()));
    ASSERT_OK_AND_ASSIGN(std::string schema_json, schema->ToJsonString());
    SchemaManager branch_schema(fs_, table_path_, "dev");
    ASSERT_OK(fs_->AtomicStore(PathUtil::JoinPath(branch_schema.SchemaDirectory(), "schema-0"),
                               schema_json));
    SnapshotManager branch_snapshots(fs_, table_path_, "dev");
    ASSERT_OK_AND_ASSIGN(std::string snapshot_json, base.ToJsonString());
    ASSERT_OK(fs_->AtomicStore(branch_snapshots.SnapshotPath(base.Id()), snapshot_json));
    branch_ = "dev";
    options_[Options::BRANCH] = branch_;
    ASSERT_OK_AND_ASSIGN(auto payload, MakePayload());
    ASSERT_OK_AND_ASSIGN(
        Messages updates,
        Prepare({{file->file_name, {{"value", "bitmap", payload}}}}, {}, base.Id()));
    ASSERT_OK(Commit(updates));
    ASSERT_OK_AND_ASSIGN(Snapshot dev, LatestSnapshot());
    ASSERT_EQ(base.Id() + 1, dev.Id());
    ASSERT_OK_AND_ASSIGN(auto dev_splits, Scan());
    ASSERT_EQ(1u, dev_splits.size());
    auto dev_split = std::dynamic_pointer_cast<DataSplitImpl>(dev_splits[0]);
    ASSERT_TRUE(dev_split);
    ASSERT_EQ(1u, dev_split->DataFiles().size());
    ASSERT_EQ(1u, dev_split->DataFiles()[0]->extra_files.size());
    auto predicate = PredicateBuilder::Equal(0, "value", FieldType::INT, Literal(1));
    ASSERT_OK_AND_ASSIGN(auto dev_values, ReadValues(std::nullopt, "value", predicate));
    ASSERT_EQ((std::vector<int32_t>{1, 1}), dev_values);
    branch_ = "main";
    options_[Options::BRANCH] = branch_;
    ASSERT_OK_AND_ASSIGN(Snapshot main, LatestSnapshot());
    ASSERT_EQ(base.Id(), main.Id());
    ASSERT_OK_AND_ASSIGN(auto main_splits, Scan());
    ASSERT_EQ(1u, main_splits.size());
    auto main_split = std::dynamic_pointer_cast<DataSplitImpl>(main_splits[0]);
    ASSERT_TRUE(main_split);
    ASSERT_EQ(1u, main_split->DataFiles().size());
    ASSERT_EQ(*file, *main_split->DataFiles()[0]);
    ASSERT_OK_AND_ASSIGN(auto main_values, ReadValues(std::nullopt, "value", predicate));
    ASSERT_EQ((std::vector<int32_t>{1, 1, 2}), main_values);
}

TEST_F(FileIndexWriteTest, UsesOriginalFieldNameAfterRename) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    SchemaManager manager(fs_, table_path_);
    ASSERT_OK_AND_ASSIGN(auto schema, manager.ReadSchema(file->schema_id));
    auto fields = schema->Fields();
    fields[1] = DataField(fields[1].Id(), arrow::field("renamed_value", arrow::int32(), false));
    auto renamed_options = schema->Options();
    renamed_options[Options::BUCKET_KEY] = "renamed_value";
    ASSERT_OK(TestHelper::WriteNextSchema(fs_, table_path_, fields, schema->HighestFieldId(),
                                          renamed_options));
    options_[Options::BUCKET_KEY] = "renamed_value";
    ASSERT_OK_AND_ASSIGN(auto payload, MakePayload());
    ASSERT_OK_AND_ASSIGN(Messages updates,
                         Prepare({{file->file_name, {{"value", "bitmap", payload}}}}));
    ASSERT_OK_AND_ASSIGN(auto after, UpdatedFile(updates));
    ASSERT_EQ(file->schema_id, after->schema_id);
    ASSERT_OK_AND_ASSIGN(PayloadBytes payloads, ReadPayloads(after));
    ASSERT_EQ(1u, payloads.count("value"));
    ASSERT_EQ(0u, payloads.count("renamed_value"));
    ASSERT_OK(Commit(updates));
    ASSERT_OK_AND_ASSIGN(
        auto values,
        ReadValues(std::nullopt, "renamed_value",
                   PredicateBuilder::Equal(0, "renamed_value", FieldType::INT, Literal(1))));
    ASSERT_EQ((std::vector<int32_t>{1, 1}), values);
}

TEST_F(FileIndexWriteTest, PreservesDeletionVectorsAndPhysicalPositions) {
    ASSERT_OK(CreateTable({{Options::DELETION_VECTORS_ENABLED, "true"}}));
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<CommitMessage> deletion,
        DeletionVectorTestHelper::CreateDeletionVectorCommitMessage(
            fs_, table_path_, "parquet", written[0], {{file->file_name, {0}}}, pool_));
    ASSERT_OK(Commit({deletion}));
    ASSERT_OK_AND_ASSIGN(auto payload, MakePayload());
    ASSERT_OK_AND_ASSIGN(Messages updates,
                         Prepare({{file->file_name, {{"value", "bitmap", payload}}}}));
    ASSERT_OK(Commit(updates));
    ASSERT_OK_AND_ASSIGN(auto values, ReadValues());
    ASSERT_EQ((std::vector<int32_t>{1, 2}), values);
    ASSERT_OK_AND_ASSIGN(
        auto filtered, ReadValues(std::nullopt, "value",
                                  PredicateBuilder::Equal(0, "value", FieldType::INT, Literal(1))));
    ASSERT_EQ((std::vector<int32_t>{1}), filtered);
}

TEST_F(FileIndexWriteTest, OptionalDependenciesAndCallerOverrides) {
    ASSERT_OK(CreateTable({{Options::FILE_INDEX_FORMAT_VERSION, "1"}}));
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    auto fs = std::make_shared<IndexFileSystem>();
    auto payload = std::make_shared<PayloadInput>("opaque payload");
    // Bits 1/2/4 supply filesystem/pool/executor respectively.
    // 0: all null; 7: all supplied; 6: filesystem null; 5: pool null; 3: executor null.
    for (int32_t mask : {0, 7, 6, 5, 3}) {
        SCOPED_TRACE(mask);
        size_t creates = fs->created.size();
        ASSERT_OK_AND_ASSIGN(
            Messages updates,
            FileIndexWrite::PrepareCommit(table_path_, "", {}, std::nullopt,
                                          {{file->file_name, {{"value", "opaque", payload}}}},
                                          mask & 1 ? fs : nullptr, mask & 2 ? pool_ : nullptr,
                                          mask & 4 ? executor_ : nullptr));
        ASSERT_EQ(creates + ((mask & 1) != 0), fs->created.size());
        ASSERT_OK(Abort(updates));
    }
    ASSERT_OK_AND_ASSIGN(
        Messages updates,
        FileIndexWrite::PrepareCommit(
            table_path_, "",
            {{Options::FILE_INDEX_FORMAT_VERSION, "2"}, {Options::FILE_SYSTEM, "unregistered-fs"}},
            std::nullopt, {{file->file_name, {{"value", "opaque", payload}}}}, fs, pool_,
            executor_));
    ASSERT_OK_AND_ASSIGN(auto after, UpdatedFile(updates));
    ASSERT_OK_AND_ASSIGN(auto input, OpenIndex(after));
    DataInputStream in(input);
    ASSERT_OK(in.Seek(8));
    ASSERT_OK_AND_ASSIGN(int32_t version, in.ReadValue<int32_t>());
    ASSERT_EQ(2, version);
    ASSERT_OK(input->Close());
    ASSERT_OK(Abort(updates));
    ASSERT_EQ(0, payload->close_count);
    ASSERT_OK_AND_ASSIGN(auto values, ReadValues());
    ASSERT_EQ((std::vector<int32_t>{1, 1, 2}), values);
}

TEST_F(FileIndexWriteTest, EmptyAndInvalidInputs) {
    ASSERT_OK(CreateTable());
    ASSERT_NOK_WITH_MSG(Prepare({}), "requires an existing snapshot");
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    ASSERT_OK_AND_ASSIGN(Messages empty, Prepare({}));
    ASSERT_TRUE(empty.empty());
    ASSERT_OK_AND_ASSIGN(Messages skipped, Prepare({{file->file_name, {}}}));
    ASSERT_TRUE(skipped.empty());
    auto payload = std::make_shared<PayloadInput>("payload");
    DataFileIndexInput valid{file->file_name, {{"value", "opaque", payload}}};
    const std::vector<std::pair<std::vector<DataFileIndexInput>, std::string>> invalid = {
        {{valid, valid}, "Duplicate data file name"},
        {{{file->file_name, {{"value", "opaque", payload}, {"value", "opaque", payload}}}},
         "Duplicate field"},
        {{{file->file_name, {{"", "opaque", payload}}}}, "must not be empty"},
        {{{file->file_name, {{"value", "", payload}}}}, "must not be empty"},
        {{{"", valid.indexes}}, "data file name"},
        {{{"../outside.orc", valid.indexes}}, "data file name"},
        {{{"missing.orc", valid.indexes}}, "not live in the base snapshot"},
        {{{file->file_name, {{"value", "opaque", std::make_shared<PayloadInput>("")}}}},
         "positive length"}};
    for (const auto& [inputs, error] : invalid) {
        SCOPED_TRACE(error);
        ASSERT_NOK_WITH_MSG(Prepare(inputs), error);
    }
    ASSERT_NOK_WITH_MSG(Prepare({valid}, {}, 0), "Invalid base snapshot id");
    ASSERT_NOK(Prepare({valid}, {}, 999999));
    ASSERT_NOK_WITH_MSG(Prepare({valid}, {{Options::BUCKET, "3"}}), "cannot override");
    ASSERT_NOK(Prepare({valid}, {{Options::FILE_INDEX_FORMAT_VERSION, "3"}}));
    ASSERT_NOK(Prepare({valid}, {{Options::BRANCH, "dev"}}));
    ASSERT_NOK(FileIndexWrite::PrepareCommit(table_path_, "../dev", {}, std::nullopt, {valid}, fs_,
                                             pool_, executor_));
    ASSERT_NOK(FileIndexWrite::PrepareCommit("", "main", {}, std::nullopt, {valid}, nullptr,
                                             nullptr, nullptr));
    ASSERT_EQ(0, payload->close_count);
}

TEST_F(FileIndexWriteTest, RejectsUnsupportedTableModes) {
    for (bool primary_key : {false, true}) {
        ASSERT_OK(CreateTable(
            {{Options::BUCKET, primary_key ? "2" : "-1"}},
            primary_key ? std::vector<std::string>{"part", "value"} : std::vector<std::string>{}));
        ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
        ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
        ASSERT_NOK_WITH_MSG(Prepare({{file->file_name, {{"value", "opaque", nullptr}}}}),
                            "only supports append tables with fixed buckets");
    }
}

TEST_F(FileIndexWriteTest, RejectsAmbiguousFileNames) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages first_write, WriteData());
    ASSERT_OK_AND_ASSIGN(Messages second_write, WriteData("[[20,1,10]]", 0, 20));
    ASSERT_OK_AND_ASSIGN(auto first, NewFile(first_write));
    ASSERT_OK_AND_ASSIGN(auto second, NewFile(second_write));
    ASSERT_OK_AND_ASSIGN(auto paths, Paths(20, 0));
    auto duplicate = std::make_shared<DataFileMeta>(*second);
    duplicate->file_name = first->file_name;
    std::string data;
    ASSERT_OK(fs_->ReadFile(paths->ToPath(second), &data));
    ASSERT_OK(fs_->WriteFile(paths->ToPath(duplicate), data, false));
    ASSERT_OK_AND_ASSIGN(auto message, OnlyMessage(second_write));
    ASSERT_OK(Commit(
        {std::make_shared<CommitMessageImpl>(message->Partition(), 0, 2, DataIncrement({}, {}, {}),
                                             CompactIncrement({second}, {duplicate}, {}))}));
    ASSERT_NOK_WITH_MSG(Prepare({{first->file_name, {{"value", "opaque", nullptr}}}}),
                        "Ambiguous data file name");
}

TEST_F(FileIndexWriteTest, ShortReadsPreservePayload) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    auto payload = std::make_shared<PayloadInput>(std::string(1024 * 1024 + 17, 'x'));
    payload->max_read = 65521;
    ASSERT_OK_AND_ASSIGN(Messages updates,
                         Prepare({{file->file_name, {{"value", "opaque", payload}}}},
                                 {{Options::FILE_INDEX_FORMAT_VERSION, "2"}}));
    ASSERT_OK_AND_ASSIGN(auto after, UpdatedFile(updates));
    ASSERT_OK_AND_ASSIGN(PayloadBytes actual, ReadPayloads(after));
    ASSERT_EQ(payload->BytesValue(), actual.at("value").at("opaque"));
    ASSERT_EQ(0, payload->close_count);
    ASSERT_OK(Commit(updates));
}

TEST_F(FileIndexWriteTest, PayloadFailuresRemoveAllGeneratedIndexes) {
    ASSERT_OK(CreateTable({{"file-index.bitmap.columns", "value"},
                           {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "0B"}}));
    ASSERT_OK_AND_ASSIGN(Messages first, WriteData());
    ASSERT_OK_AND_ASSIGN(Messages second, WriteData());
    ASSERT_OK_AND_ASSIGN(auto first_file, NewFile(first));
    ASSERT_OK_AND_ASSIGN(auto second_file, NewFile(second));
    std::vector<std::shared_ptr<DataFileMeta>> files{first_file, second_file};
    std::sort(files.begin(), files.end(),
              [](const auto& lhs, const auto& rhs) { return lhs->file_name < rhs->file_name; });
    ASSERT_OK_AND_ASSIGN(auto paths, Paths());
    ASSERT_OK_AND_ASSIGN(Snapshot base, LatestSnapshot());
    for (int32_t version : {1, 2}) {
        for (int32_t failure = 0; failure < 4; ++failure) {
            SCOPED_TRACE(fmt::format("version={}, failure={}", version, failure));
            auto good = std::make_shared<PayloadInput>("good payload");
            auto bad = std::make_shared<PayloadInput>("bad payload");
            bad->fail_length = failure == 0;
            bad->fail_seek = failure == 1;
            bad->fail_read = failure == 2;
            bad->early_eof = failure == 3;
            auto fs = std::make_shared<IndexFileSystem>();
            ASSERT_NOK(FileIndexWrite::PrepareCommit(
                table_path_, branch_,
                {{Options::FILE_INDEX_FORMAT_VERSION, std::to_string(version)}}, base.Id(),
                {{files[0]->file_name, {{"value", "opaque", good}}},
                 {files[1]->file_name, {{"value", "opaque", bad}}}},
                fs, pool_, executor_));
            ASSERT_FALSE(fs->created.empty());
            AssertRemoved(fs->created);
            ASSERT_EQ(0, good->close_count);
            ASSERT_EQ(0, bad->close_count);
            ASSERT_OK_AND_ASSIGN(Snapshot unchanged, LatestSnapshot());
            ASSERT_EQ(base.Id(), unchanged.Id());
            for (const auto& file : files) {
                ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(paths->ToPath(file)));
                ASSERT_TRUE(exists);
                ASSERT_OK_AND_ASSIGN(PayloadBytes original, ReadPayloads(file));
                ASSERT_EQ(1u, original.at("value").count("bitmap"));
            }
        }
    }
}

TEST_F(FileIndexWriteTest, OutputFailuresRemovePartialIndexes) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
    ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
    auto payload = std::make_shared<PayloadInput>("payload");
    for (int32_t version : {1, 2}) {
        for (auto failure : {OutputFailure::CREATE, OutputFailure::WRITE, OutputFailure::FLUSH,
                             OutputFailure::CLOSE}) {
            auto fs = std::make_shared<IndexFileSystem>();
            fs->failure = failure;
            ASSERT_NOK(FileIndexWrite::PrepareCommit(
                table_path_, branch_,
                {{Options::FILE_INDEX_FORMAT_VERSION, std::to_string(version)}}, std::nullopt,
                {{file->file_name, {{"value", "opaque", payload}}}}, fs, pool_, executor_));
            ASSERT_EQ(1, fs->create_attempts);
            if (failure != OutputFailure::CREATE) {
                ASSERT_EQ(1u, fs->created.size());
            }
            AssertRemoved(fs->created);
        }
    }
    ASSERT_EQ(0, payload->close_count);
    ASSERT_OK_AND_ASSIGN(auto values, ReadValues());
    ASSERT_EQ((std::vector<int32_t>{1, 1, 2}), values);
}

TEST_F(FileIndexWriteTest, CleanupFailuresPreserveOriginalError) {
    ASSERT_OK(CreateTable());
    ASSERT_OK_AND_ASSIGN(Messages first_write, WriteData());
    ASSERT_OK_AND_ASSIGN(Messages second_write, WriteData());
    ASSERT_OK_AND_ASSIGN(auto first, NewFile(first_write));
    ASSERT_OK_AND_ASSIGN(auto second, NewFile(second_write));
    if (second->file_name < first->file_name) {
        std::swap(first, second);
    }
    auto good = std::make_shared<PayloadInput>("good payload");
    auto bad = std::make_shared<PayloadInput>("bad payload");
    bad->fail_read = true;
    auto fs = std::make_shared<IndexFileSystem>();
    fs->fail_delete = true;
    ASSERT_NOK_WITH_MSG(
        FileIndexWrite::PrepareCommit(table_path_, branch_, {}, std::nullopt,
                                      {{first->file_name, {{"value", "opaque", good}}},
                                       {second->file_name, {{"value", "opaque", bad}}}},
                                      fs, pool_, executor_),
        "injected payload read failure");
    ASSERT_EQ(2u, fs->created.size());
    ASSERT_EQ(fs->created, fs->delete_attempts);
    fs->fail_delete = false;
    for (const auto& path : fs->created) {
        ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(path));
        ASSERT_TRUE(exists);
        ASSERT_OK(fs->Delete(path, false));
    }
    ASSERT_OK_AND_ASSIGN(auto values, ReadValues());
    ASSERT_EQ((std::vector<int32_t>{1, 1, 1, 1, 2, 2}), values);
}

TEST_F(FileIndexWriteTest, RejectsMalformedExistingIndexes) {
    for (int32_t malformed = 0; malformed < 3; ++malformed) {
        SCOPED_TRACE(malformed);
        ASSERT_OK(CreateTable());
        ASSERT_OK_AND_ASSIGN(Messages written, WriteData());
        ASSERT_OK_AND_ASSIGN(auto file, NewFile(written));
        ASSERT_OK_AND_ASSIGN(auto paths, Paths());
        const std::string index_path = paths->ToAlignedPath("old.index", file);
        if (malformed == 0) {
            ASSERT_OK(fs_->WriteFile(index_path, "invalid container", false));
        } else {
            ASSERT_OK(WriteContainer(file, "old.index", {{"value", {{"opaque", "payload"}}}}));
            if (malformed == 1) {
                std::string bytes;
                ASSERT_OK(fs_->ReadFile(index_path, &bytes));
                auto input = std::make_shared<ByteArrayInputStream>(bytes.data(), bytes.size());
                DataInputStream in(input);
                ASSERT_OK(in.Seek(12));
                ASSERT_OK_AND_ASSIGN(int32_t header_length, in.ReadValue<int32_t>());
                ASSERT_GE(header_length, 12);
                ASSERT_LE(static_cast<size_t>(header_length), bytes.size());
                bytes[header_length - 12] = 0x7f;
                ASSERT_OK(fs_->WriteFile(index_path, bytes, true));
            }
        }
        auto before = file->CopyWithExtraFiles(
            malformed == 2 ? std::vector<std::optional<std::string>>{"old.index", "duplicate.index"}
                           : std::vector<std::optional<std::string>>{"old.index"});
        if (malformed == 2) {
            ASSERT_OK(
                fs_->WriteFile(paths->ToAlignedPath("duplicate.index", file), "duplicate", false));
        }
        ASSERT_OK(RewriteMetadata(file, before));
        auto fs = std::make_shared<IndexFileSystem>();
        ASSERT_NOK(FileIndexWrite::PrepareCommit(
            table_path_, branch_, {}, std::nullopt,
            {{file->file_name, {{"other", "opaque", nullptr}}}}, fs, pool_, executor_));
        ASSERT_TRUE(fs->created.empty());
        ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(paths->ToPath(file)));
        ASSERT_TRUE(exists);
    }
}

}  // namespace paimon::test
