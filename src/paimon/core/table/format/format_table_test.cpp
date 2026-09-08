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

#include "paimon/table/format/format_table.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/cache/cache.h"
#include "paimon/commit_context.h"
#include "paimon/commit_message.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/format/format_commit_message.h"
#include "paimon/core/table/format/format_data_split.h"
#include "paimon/core/table/format/format_path_validation.h"
#include "paimon/core/table/format/format_table_commit.h"
#include "paimon/core/table/format/format_table_read.h"
#include "paimon/core/table/format/format_table_scan.h"
#include "paimon/core/table/format/format_table_write.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

namespace {

/// What `ListPartitions()` returns. Aliased because a macro argument cannot hold the comma in
/// `std::map<std::string, std::string>`: the preprocessor would read it as two arguments.
using PartitionList = std::vector<std::map<std::string, std::string>>;

std::shared_ptr<arrow::Schema> MakeSchema() {
    return arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8()),
                          arrow::field("dt", arrow::utf8())});
}

/// Wraps a file system and writes down what a write did to each path, in order. Only the order
/// gives away a temp file deleted while its stream is still open, which on a store that flushes
/// from the destructor lands the write after the delete.
class CallOrderFileSystem : public FileSystem {
 public:
    explicit CallOrderFileSystem(const std::shared_ptr<FileSystem>& delegate)
        : delegate_(delegate), calls_(std::make_shared<std::vector<std::string>>()) {}

    /// What happened, as "<verb> <path>" in the order it happened.
    const std::vector<std::string>& Calls() const {
        return *calls_;
    }

    using FileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return delegate_->Open(path);
    }
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               delegate_->Create(path, overwrite));
        calls_->push_back("create " + path);
        return std::unique_ptr<OutputStream>(
            new RecordingOutputStream(std::move(out), path, calls_));
    }
    Status Mkdirs(const std::string& path) const override {
        return delegate_->Mkdirs(path);
    }
    Status Rename(const std::string& src, const std::string& dst) const override {
        return delegate_->Rename(src, dst);
    }
    Status Delete(const std::string& path, bool recursive = true) const override {
        calls_->push_back("delete " + path);
        return delegate_->Delete(path, recursive);
    }
    Result<FileStatus> GetFileStatus(const std::string& path) const override {
        return delegate_->GetFileStatus(path);
    }
    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* status_list) const override {
        return delegate_->ListDir(directory, status_list);
    }
    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* status_list) const override {
        return delegate_->ListFileStatus(path, status_list);
    }
    Result<bool> Exists(const std::string& path) const override {
        return delegate_->Exists(path);
    }

 private:
    /// Records its own close, so a stream still open when its file was deleted can be told apart
    /// from one that was closed first.
    class RecordingOutputStream : public OutputStream {
     public:
        RecordingOutputStream(std::unique_ptr<OutputStream> delegate, const std::string& path,
                              const std::shared_ptr<std::vector<std::string>>& calls)
            : delegate_(std::move(delegate)), path_(path), calls_(calls) {}

        Result<int64_t> Write(const char* buffer, int64_t size) override {
            return delegate_->Write(buffer, size);
        }
        Status Flush() override {
            return delegate_->Flush();
        }
        Result<int64_t> GetPos() const override {
            return delegate_->GetPos();
        }
        Result<std::string> GetUri() const override {
            return delegate_->GetUri();
        }
        Status Close() override {
            calls_->push_back("close " + path_);
            return delegate_->Close();
        }

     private:
        std::unique_ptr<OutputStream> delegate_;
        std::string path_;
        std::shared_ptr<std::vector<std::string>> calls_;
    };

    std::shared_ptr<FileSystem> delegate_;
    /// Shared with every stream this hands out, so one list holds the whole sequence.
    std::shared_ptr<std::vector<std::string>> calls_;
};

/// When a `FailingRenameFileSystem` gives up on the rename it is told to fail.
enum class RenameFailure {
    /// Nothing moves, so the target is not there when the commit gives up.
    kWithoutPublishing,
    /// The rename takes effect and reports a failure all the same - what a store that completed
    /// the request and lost its response looks like from here.
    kAfterPublishing,
};

/// Wraps a file system and refuses the nth `Rename()`, so that a commit can be stopped part way
/// through publishing - after some files are visible and, for an overwrite, after the data they
/// replace is already gone.
class FailingRenameFileSystem : public FileSystem {
 public:
    FailingRenameFileSystem(const std::shared_ptr<FileSystem>& delegate, int32_t fail_nth_rename,
                            RenameFailure failure = RenameFailure::kWithoutPublishing)
        : delegate_(delegate), fail_nth_rename_(fail_nth_rename), failure_(failure) {}

    using FileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return delegate_->Open(path);
    }
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        return delegate_->Create(path, overwrite);
    }
    Status Mkdirs(const std::string& path) const override {
        return delegate_->Mkdirs(path);
    }
    Status Rename(const std::string& src, const std::string& dst) const override {
        if (++renames_ != fail_nth_rename_) {
            return delegate_->Rename(src, dst);
        }
        if (failure_ == RenameFailure::kAfterPublishing) {
            PAIMON_RETURN_NOT_OK(delegate_->Rename(src, dst));
        }
        return Status::IOError("injected rename failure");
    }
    Status Delete(const std::string& path, bool recursive = true) const override {
        return delegate_->Delete(path, recursive);
    }
    Result<FileStatus> GetFileStatus(const std::string& path) const override {
        return delegate_->GetFileStatus(path);
    }
    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* status_list) const override {
        return delegate_->ListDir(directory, status_list);
    }
    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* status_list) const override {
        return delegate_->ListFileStatus(path, status_list);
    }
    Result<bool> Exists(const std::string& path) const override {
        return delegate_->Exists(path);
    }

 private:
    std::shared_ptr<FileSystem> delegate_;
    int32_t fail_nth_rename_;
    RenameFailure failure_;
    mutable int32_t renames_ = 0;
};

/// The one call a `FailingWriteFileSystem`'s streams refuse.
enum class FailingStreamCall { kGetPos, kFlush, kClose };

/// Wraps a file system and hands out streams that fail one call, so that a write can be stopped
/// where a real store would stop it: after the writer is finished and while the file it produced
/// is still hidden.
class FailingWriteFileSystem : public FileSystem {
 public:
    /// `armed` says whether the streams refuse from the start. A test that has to reach past the
    /// writer's own construction - a parquet writer asks the stream where it is as it opens the
    /// file - hands out `false` and calls `Arm()` once a file is open and written to.
    FailingWriteFileSystem(const std::shared_ptr<FileSystem>& delegate, FailingStreamCall failing,
                           bool armed = true)
        : delegate_(delegate), failing_(failing), armed_(std::make_shared<bool>(armed)) {}

    /// Refuses from here on, through the stream already open as well as any opened later.
    void Arm() const {
        *armed_ = true;
    }

    using FileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return delegate_->Open(path);
    }
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               delegate_->Create(path, overwrite));
        return std::unique_ptr<OutputStream>(
            new FailingOutputStream(std::move(out), failing_, armed_));
    }
    Status Mkdirs(const std::string& path) const override {
        return delegate_->Mkdirs(path);
    }
    Status Rename(const std::string& src, const std::string& dst) const override {
        return delegate_->Rename(src, dst);
    }
    Status Delete(const std::string& path, bool recursive = true) const override {
        return delegate_->Delete(path, recursive);
    }
    Result<FileStatus> GetFileStatus(const std::string& path) const override {
        return delegate_->GetFileStatus(path);
    }
    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* status_list) const override {
        return delegate_->ListDir(directory, status_list);
    }
    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* status_list) const override {
        return delegate_->ListFileStatus(path, status_list);
    }
    Result<bool> Exists(const std::string& path) const override {
        return delegate_->Exists(path);
    }

 private:
    class FailingOutputStream : public OutputStream {
     public:
        FailingOutputStream(std::unique_ptr<OutputStream> delegate, FailingStreamCall failing,
                            const std::shared_ptr<bool>& armed)
            : delegate_(std::move(delegate)), failing_(failing), armed_(armed) {}

        Result<int64_t> Write(const char* buffer, int64_t size) override {
            return delegate_->Write(buffer, size);
        }
        Status Flush() override {
            if (*armed_ && failing_ == FailingStreamCall::kFlush) {
                return Status::IOError("injected flush failure");
            }
            return delegate_->Flush();
        }
        Result<int64_t> GetPos() const override {
            if (*armed_ && failing_ == FailingStreamCall::kGetPos) {
                return Status::IOError("injected get position failure");
            }
            return delegate_->GetPos();
        }
        Result<std::string> GetUri() const override {
            return delegate_->GetUri();
        }
        Status Close() override {
            if (*armed_ && failing_ == FailingStreamCall::kClose) {
                // Closed all the same, so the file it wrote can still be removed.
                [[maybe_unused]] Status closed = delegate_->Close();
                return Status::IOError("injected close failure");
            }
            return delegate_->Close();
        }

     private:
        std::unique_ptr<OutputStream> delegate_;
        FailingStreamCall failing_;
        std::shared_ptr<bool> armed_;
    };

    std::shared_ptr<FileSystem> delegate_;
    FailingStreamCall failing_;
    /// Shared with every stream this hands out, so arming it reaches the one already open.
    std::shared_ptr<bool> armed_;
};

/// Wraps a file system and hands out streams that refuse `Write()` from the moment a test arms it.
///
/// A format's writer buffers what it is given and writes the file's footer when it is finished, so
/// arming the refusal between the two is what pins a failure to the finish. Counting writes
/// instead would pin it to however many a format happens to make first.
class ArmedWriteFailureFileSystem : public FileSystem {
 public:
    explicit ArmedWriteFailureFileSystem(const std::shared_ptr<FileSystem>& delegate)
        : delegate_(delegate), armed_(std::make_shared<bool>(false)) {}

    /// Every write through this file system fails from here on, including through a stream that
    /// is already open.
    void FailFurtherWrites() {
        *armed_ = true;
    }

    using FileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return delegate_->Open(path);
    }
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               delegate_->Create(path, overwrite));
        return std::unique_ptr<OutputStream>(new ArmedOutputStream(std::move(out), armed_));
    }
    Status Mkdirs(const std::string& path) const override {
        return delegate_->Mkdirs(path);
    }
    Status Rename(const std::string& src, const std::string& dst) const override {
        return delegate_->Rename(src, dst);
    }
    Status Delete(const std::string& path, bool recursive = true) const override {
        return delegate_->Delete(path, recursive);
    }
    Result<FileStatus> GetFileStatus(const std::string& path) const override {
        return delegate_->GetFileStatus(path);
    }
    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* status_list) const override {
        return delegate_->ListDir(directory, status_list);
    }
    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* status_list) const override {
        return delegate_->ListFileStatus(path, status_list);
    }
    Result<bool> Exists(const std::string& path) const override {
        return delegate_->Exists(path);
    }

 private:
    class ArmedOutputStream : public OutputStream {
     public:
        ArmedOutputStream(std::unique_ptr<OutputStream> delegate,
                          const std::shared_ptr<bool>& armed)
            : delegate_(std::move(delegate)), armed_(armed) {}

        Result<int64_t> Write(const char* buffer, int64_t size) override {
            if (*armed_) {
                return Status::IOError("injected write failure");
            }
            return delegate_->Write(buffer, size);
        }
        Status Flush() override {
            return delegate_->Flush();
        }
        Result<int64_t> GetPos() const override {
            return delegate_->GetPos();
        }
        Result<std::string> GetUri() const override {
            return delegate_->GetUri();
        }
        Status Close() override {
            return delegate_->Close();
        }

     private:
        std::unique_ptr<OutputStream> delegate_;
        std::shared_ptr<bool> armed_;
    };

    std::shared_ptr<FileSystem> delegate_;
    /// Shared with every stream this hands out, so arming it reaches the one already open.
    std::shared_ptr<bool> armed_;
};

/// Creates a format table's schema on disk and loads the table.
Result<std::shared_ptr<FormatTable>> CreateTable(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    const std::vector<std::string>& partition_keys,
    const std::map<std::string, std::string>& extra_options = {}) {
    std::map<std::string, std::string> options = {{Options::TYPE, "format-table"},
                                                  {Options::FILE_FORMAT, "parquet"}};
    for (const auto& [key, value] : extra_options) {
        options[key] = value;
    }
    SchemaManager schema_manager(file_system, path);
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableSchema> table_schema,
        schema_manager.CreateTable(MakeSchema(), partition_keys, /*primary_keys=*/{}, options));
    return FormatTable::Create(file_system, path, Identifier("db", "tbl"), /*dynamic_options=*/{});
}

/// Builds one batch of the table's columns, inserts unless other row kinds are given.
Result<std::unique_ptr<RecordBatch>> MakeBatch(
    const std::vector<int32_t>& ids, const std::vector<std::string>& names, const std::string& dt,
    const std::map<std::string, std::string>& partition,
    const std::vector<RecordBatch::RowKind>& row_kinds = {}) {
    arrow::Int32Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::StringBuilder dt_builder;
    for (size_t i = 0; i < ids.size(); i++) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(ids[i]));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Append(names[i]));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.Append(dt));
    }
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> name_array;
    std::shared_ptr<arrow::Array> dt_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Finish(&name_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.Finish(&dt_array));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> struct_array,
        arrow::StructArray::Make({id_array, name_array, dt_array}, MakeSchema()->fields()));

    auto c_array = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
    RecordBatchBuilder builder(c_array.get());
    builder.SetPartition(partition);
    if (!row_kinds.empty()) {
        builder.SetRowKinds(row_kinds);
    }
    return builder.Finish();
}

/// Builds a one-row batch whose partition column is null, declared as `default_partition_name`.
Result<std::unique_ptr<RecordBatch>> MakeBatchWithNullPartition(
    const std::string& default_partition_name) {
    arrow::Int32Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::StringBuilder dt_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Append("alice"));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.AppendNull());
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> name_array;
    std::shared_ptr<arrow::Array> dt_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Finish(&name_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.Finish(&dt_array));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> struct_array,
        arrow::StructArray::Make({id_array, name_array, dt_array}, MakeSchema()->fields()));

    auto c_array = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
    RecordBatchBuilder builder(c_array.get());
    builder.SetPartition({{"dt", default_partition_name}});
    return builder.Finish();
}

/// Builds one batch of `count` rows, large enough that a small split target has to cut it up.
Result<std::unique_ptr<RecordBatch>> MakeManyRowBatch(int32_t count) {
    std::vector<int32_t> ids;
    std::vector<std::string> names;
    ids.reserve(count);
    names.reserve(count);
    for (int32_t i = 0; i < count; i++) {
        ids.push_back(i);
        names.push_back("name-" + std::to_string(i));
    }
    return MakeBatch(ids, names, "20240101", {});
}

/// Builds one batch of `count` wide, all-different rows starting at `start_id`. A file is
/// measured by the bytes its writer has finished with, and small repeated values sit in a
/// dictionary it has not written yet.
Result<std::unique_ptr<RecordBatch>> MakeWideRowBatch(int32_t count, int32_t start_id) {
    constexpr size_t kNameLength = 1024;
    std::vector<int32_t> ids;
    std::vector<std::string> names;
    ids.reserve(count);
    names.reserve(count);
    for (int32_t i = 0; i < count; i++) {
        const int32_t id = start_id + i;
        ids.push_back(id);
        names.push_back(std::to_string(id) +
                        std::string(kNameLength, static_cast<char>('a' + (id % 26))));
    }
    return MakeBatch(ids, names, "20240101", {});
}

/// The path a write stages `file_path` under: a `_temporary` directory beside where the file will
/// be published, holding a hidden name of its own. Java Paimon's `RenamingTwoPhaseOutputStream`
/// uses the same layout.
std::string StagedPath(const std::string& file_path) {
    return PathUtil::JoinPath(PathUtil::GetParentDirPath(file_path),
                              "_temporary/.tmp.d9b7f0a2-0c11-4a35-9f6e-2f2f0f9e6c41");
}

/// Writes one batch and commits it, so the files become part of the table.
Status WriteAndCommit(const std::shared_ptr<FormatTable>& table,
                      std::unique_ptr<RecordBatch>&& batch, bool overwrite = false,
                      const std::map<std::string, std::string>& static_partition = {}) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableWrite> write,
                           FormatTableWrite::Create(table, /*pool=*/nullptr));
    PAIMON_RETURN_NOT_OK(write->Write(std::move(batch)));
    PAIMON_ASSIGN_OR_RAISE(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableCommit> commit,
                           FormatTableCommit::Create(table, overwrite, static_partition));
    return commit->Commit(messages);
}

/// Imports a batch the reader handed out. `ASSERT_OK_AND_ASSIGN` only understands paimon's
/// `Result`, so arrow's has to be converted before a test body can use it.
Result<std::shared_ptr<arrow::RecordBatch>> ImportBatch(const BatchReader::ReadBatch& batch) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::RecordBatch> record_batch,
        arrow::ImportRecordBatch(batch.first.get(), batch.second.get()));
    return record_batch;
}

/// Reads every row of a plan, returning the rows as `id|name|dt` strings.
Result<std::vector<std::string>> ReadAll(const std::shared_ptr<FormatTable>& table,
                                         const std::vector<std::shared_ptr<Split>>& splits,
                                         const std::shared_ptr<Predicate>& predicate = nullptr,
                                         bool enable_predicate_filter = false) {
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     predicate, enable_predicate_filter));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, read->CreateReader(splits));
    std::vector<std::string> rows;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::RecordBatch> record_batch,
            arrow::ImportRecordBatch(batch.first.get(), batch.second.get()));
        // The leading field is `_VALUE_KIND`, which every `BatchReader` puts first; a format
        // table has no row kinds of its own, so every row of it is an insert.
        if (record_batch->schema()->field(0)->name() != SpecialFields::ValueKind().Name()) {
            return Status::Invalid("a format table read must still carry the _VALUE_KIND field");
        }
        auto row_kinds = checked_pointer_cast<arrow::Int8Array>(record_batch->column(0));
        for (int64_t i = 0; i < record_batch->num_rows(); i++) {
            if (row_kinds->Value(i) != RowKind::Insert()->ToByteValue()) {
                return Status::Invalid("a format table read must return inserts only");
            }
        }
        auto ids = checked_pointer_cast<arrow::Int32Array>(record_batch->column(1));
        auto names = checked_pointer_cast<arrow::StringArray>(record_batch->column(2));
        auto dts = checked_pointer_cast<arrow::StringArray>(record_batch->column(3));
        for (int64_t i = 0; i < record_batch->num_rows(); i++) {
            rows.push_back(std::to_string(ids->Value(i)) + "|" + names->GetString(i) + "|" +
                           dts->GetString(i));
        }
    }
    reader->Close();
    return rows;
}

}  // namespace

TEST(FormatTableTest, TestParseFormat) {
    ASSERT_OK_AND_ASSIGN(FormatTable::Format parquet, FormatTable::ParseFormat("PARQUET"));
    ASSERT_EQ(parquet, FormatTable::Format::PARQUET);
    ASSERT_OK_AND_ASSIGN(FormatTable::Format orc, FormatTable::ParseFormat("orc"));
    ASSERT_EQ(orc, FormatTable::Format::ORC);
    ASSERT_EQ(FormatTable::FormatToString(FormatTable::Format::ORC), "orc");

    // Format table formats with no reader here yet answer `NotImplemented`, which is a different
    // answer from a name that is no format at all.
    for (const char* format : {"csv", "text", "json", "mosaic"}) {
        Result<FormatTable::Format> unimplemented = FormatTable::ParseFormat(format);
        ASSERT_FALSE(unimplemented.ok()) << format;
        ASSERT_TRUE(unimplemented.status().IsNotImplemented()) << format;
    }

    Result<FormatTable::Format> unknown = FormatTable::ParseFormat("nonesuch");
    ASSERT_FALSE(unknown.ok());
    ASSERT_TRUE(unknown.status().IsInvalid());
}

TEST(FormatTableTest, TestCreateReadsOptions) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_EQ(table->Location(), dir->Str());
    ASSERT_EQ(table->GetFormat(), FormatTable::Format::PARQUET);
    ASSERT_EQ(table->PartitionKeys(), std::vector<std::string>({"dt"}));
    ASSERT_EQ(table->FileCompression(), "snappy");
    ASSERT_EQ(table->PartitionDefaultName(), "__DEFAULT_PARTITION__");
    ASSERT_EQ(table->FullName(), "db.tbl");
}

TEST(FormatTableTest, TestFileCompressionComesFromCoreOptions) {
    // The resolution order itself is `CoreOptionsTest.TestFormatTableFileCompression`'s business;
    // what matters here is that the table asks for it rather than resolving compression again.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        CreateTable(dir->GetFileSystem(), dir->Str(), {},
                    {{Options::FORMAT_TABLE_FILE_COMPRESSION, "lz4"}, {"compression", "zstd"}}));
    ASSERT_EQ(table->FileCompression(), "lz4");

    std::unique_ptr<UniqueTestDirectory> default_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(default_dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> default_table,
                         CreateTable(default_dir->GetFileSystem(), default_dir->Str(), {}));
    ASSERT_EQ(default_table->FileCompression(), "snappy");
}

TEST(FormatTableTest, TestFileFormatDefaultsToParquet) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
        schema_manager.CreateTable(MakeSchema(), /*partition_keys=*/{},
                                   /*primary_keys=*/{}, {{Options::TYPE, "format-table"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         FormatTable::Create(dir->GetFileSystem(), dir->Str(),
                                             Identifier("db", "tbl"), /*dynamic_options=*/{}));
    ASSERT_EQ(table->GetFormat(), FormatTable::Format::PARQUET);
    ASSERT_EQ(table->FileCompression(), "snappy");
}

TEST(FormatTableTest, TestUnknownTableTypeIsRejected) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    // Refused at creation: read as a managed table it would look for snapshots it never had.
    Result<std::unique_ptr<TableSchema>> unknown_type = schema_manager.CreateTable(
        MakeSchema(), /*partition_keys=*/{}, /*primary_keys=*/{}, {{Options::TYPE, "nonesuch"}});
    ASSERT_FALSE(unknown_type.ok());
    ASSERT_TRUE(unknown_type.status().IsInvalid());
}

TEST(FormatTableTest, TestATableTypeThisLibraryCannotOpenIsRejectedAtCreation) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    // A table type paimon names but this library cannot open: refused up front, with its own
    // status code and the type quoted.
    Result<std::unique_ptr<TableSchema>> object_table =
        schema_manager.CreateTable(MakeSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                                   {{Options::TYPE, "object-table"}});
    ASSERT_FALSE(object_table.ok());
    ASSERT_TRUE(object_table.status().IsNotImplemented()) << object_table.status().ToString();
    ASSERT_NE(std::string::npos, object_table.status().ToString().find("object-table"));
}

TEST(FormatTableTest, TestAPartitionColumnOfAnUnsupportedTypeIsRefusedUpFront) {
    // A partition value makes the round trip through its column type on the way to a directory
    // name and back. `BINARY` cannot, so a table partitioned by one is refused where the table is
    // decided rather than at the first read or write of a table that already looked created.
    std::shared_ptr<arrow::Schema> binary_schema =
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8()),
                       arrow::field("bin", arrow::binary())});
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    Result<std::unique_ptr<TableSchema>> created = schema_manager.CreateTable(
        binary_schema, /*partition_keys=*/{"bin"}, /*primary_keys=*/{},
        {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}});
    ASSERT_NOK_WITH_MSG(created, "cannot be partitioned");

    // And opening one another engine wrote fails the same way, not at its first read or write.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> unchecked_schema,
        TableSchema::Create(/*schema_id=*/0, binary_schema, /*partition_keys=*/{"bin"},
                            /*primary_keys=*/{},
                            {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    std::shared_ptr<DataSchema> data_schema =
        checked_pointer_cast<DataSchema>(std::shared_ptr<TableSchema>(std::move(unchecked_schema)));
    Result<std::shared_ptr<FormatTable>> opened =
        FormatTable::Create(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl"), data_schema,
                            /*location_carries_paimon_metadata=*/true, /*dynamic_options=*/{});
    ASSERT_NOK_WITH_MSG(opened, "cannot be partitioned");
}

TEST(FormatTableTest, TestCatalogManagedPartitionsAreRejected) {
    // The option moves partition visibility into the catalog: a directory nobody registered stops
    // being part of the table. A scan here reads the directories instead, so honouring the option
    // by ignoring it would return partitions the catalog never registered.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    Result<std::shared_ptr<FormatTable>> table = CreateTable(
        dir->GetFileSystem(), dir->Str(), {"dt"}, {{Options::METASTORE_PARTITIONED_TABLE, "true"}});
    ASSERT_FALSE(table.ok());
    ASSERT_TRUE(table.status().IsNotImplemented());

    // Explicitly turning it off is the behaviour that is implemented, so it is accepted.
    std::unique_ptr<UniqueTestDirectory> off_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(off_dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> off_table,
                         CreateTable(off_dir->GetFileSystem(), off_dir->Str(), {"dt"},
                                     {{Options::METASTORE_PARTITIONED_TABLE, "false"}}));

    // And the same refusal when it arrives at the call rather than in the schema. Validating the
    // schema's own options alone would let this one through and then drop it, leaving a caller
    // who asked for catalog-managed partitions with a scan that read the directories anyway.
    Result<std::shared_ptr<FormatTable>> dynamic_table =
        FormatTable::Create(off_dir->GetFileSystem(), off_dir->Str(), Identifier("db", "tbl"),
                            off_table->LatestSchema(),
                            /*location_carries_paimon_metadata=*/true,
                            {{Options::METASTORE_PARTITIONED_TABLE, "true"}});
    ASSERT_FALSE(dynamic_table.ok());
    ASSERT_TRUE(dynamic_table.status().IsNotImplemented()) << dynamic_table.status().ToString();
}

TEST(FormatTableTest, TestTheGenericEntryPointsValidateOptionsGivenAtTheCall) {
    // An option a format table refuses has to be refused wherever it comes from. Every generic
    // entry point merges what the call gave it over what the schema stored, so each runs the same
    // checks over the merged result; otherwise a caller could set through a context what the
    // schema would have rejected and have it silently dropped.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    const std::map<std::string, std::string> refused = {
        {Options::METASTORE_PARTITIONED_TABLE, "true"}};

    ScanContextBuilder scan_builder(dir->Str());
    scan_builder.SetOptions(refused);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), "metastore.partitioned-table");

    ReadContextBuilder read_builder(dir->Str());
    read_builder.SetOptions(refused);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)), "metastore.partitioned-table");

    WriteContextBuilder write_builder(dir->Str(), "test-user");
    write_builder.SetOptions(refused);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(std::move(write_context)),
                        "metastore.partitioned-table");

    CommitContextBuilder commit_builder(dir->Str(), "test-user");
    commit_builder.SetOptions(refused);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreCommit::Create(std::move(commit_context)),
                        "metastore.partitioned-table");
}

TEST(FormatTableTest, TestValueOnlyPartitionCannotBeNamedAfterThisTablesMetadata) {
    // Under the value-only layout a value becomes a directory name unchanged, so `dt` of
    // `schema` would be written over this table's own schema, and an overwrite would delete it.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"},
                                     {{Options::FORMAT_TABLE_PARTITION_PATH_ONLY_VALUE, "true"}}));
    ASSERT_TRUE(table->LocationCarriesPaimonMetadata());

    for (const std::string& reserved : {std::string("schema"), std::string("branch")}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                             FormatTableWrite::Create(table, /*pool=*/nullptr));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({1}, {"alice"}, reserved, {{"dt", reserved}}));
        SCOPED_TRACE(reserved);
        ASSERT_NOK_WITH_MSG(write->Write(std::move(batch)), "own metadata rather than data");
        ASSERT_OK(write->Abort());
    }

    // An overwrite names its partition itself, so it is refused on its own account, before it
    // deletes anything.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableCommit> commit,
                         FormatTableCommit::Create(table, /*overwrite=*/true, {{"dt", "schema"}}));
    ASSERT_NOK_WITH_MSG(commit->Commit({}), "own metadata rather than data");
    // The schema is still where the table keeps it.
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest,
                         schema_manager.Latest());
    ASSERT_TRUE(latest.has_value());

    // Any other value is ordinary data and still works.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> ordinary,
                         MakeBatch({2}, {"bob"}, "schematic", {{"dt", "schematic"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(ordinary)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"2|bob|schematic"}));
}

TEST(FormatTableTest, TestValueOnlyPartitionCannotBeNamedByAHiddenValue) {
    // Under the value-only layout the partition value is the whole directory name, so a value
    // starting with `_` or `.` names a directory every scan skips: the rows would be written and
    // never read back. The write is refused instead of losing them quietly.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"},
                                     {{Options::FORMAT_TABLE_PARTITION_PATH_ONLY_VALUE, "true"}}));
    for (const std::string& hidden : {std::string("_2025"), std::string(".2025")}) {
        SCOPED_TRACE(hidden);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                             FormatTableWrite::Create(table, /*pool=*/nullptr));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({1}, {"alice"}, hidden, {{"dt", hidden}}));
        ASSERT_NOK_WITH_MSG(write->Write(std::move(batch)), "a scan of this table would skip");
        ASSERT_OK(write->Abort());
    }

    // The one hidden name that is table content: the directory standing for a null partition
    // value, which this layout writes and reads like any other. What the null itself reads back
    // as is `TestNullPartitionValue`'s business.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> null_partition,
                         MakeBatchWithNullPartition(table->PartitionDefaultName()));
    ASSERT_OK(WriteAndCommit(table, std::move(null_partition)));
    ASSERT_OK_AND_ASSIGN(bool default_dir_exists, dir->GetFileSystem()->Exists(PathUtil::JoinPath(
                                                      dir->Str(), table->PartitionDefaultName())));
    ASSERT_TRUE(default_dir_exists);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 1u);

    // The same value under the `key=value` layout is ordinary data: the key in front of it makes
    // the directory `dt=_2025`, which is not hidden at all.
    std::unique_ptr<UniqueTestDirectory> key_value_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(key_value_dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> key_value_table,
                         CreateTable(key_value_dir->GetFileSystem(), key_value_dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> key_value_batch,
                         MakeBatch({1}, {"alice"}, "_2025", {{"dt", "_2025"}}));
    ASSERT_OK(WriteAndCommit(key_value_table, std::move(key_value_batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> key_value_scan,
        FormatTableScan::Create(key_value_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> key_value_plan, key_value_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> key_value_rows,
                         ReadAll(key_value_table, key_value_plan->Splits()));
    ASSERT_EQ(key_value_rows, (std::vector<std::string>{"1|alice|_2025"}));
}

TEST(FormatTableTest, TestCommitMessageCannotPublishIntoThisTablesMetadata) {
    // The same rule applies to a message that no writer here produced, since a commit checks the
    // message it is given rather than where it came from.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));

    FormatCommitMessage into_metadata(StagedPath(dir->Str() + "/schema/data-a-0.parquet"),
                                      dir->Str() + "/schema/data-a-0.parquet",
                                      std::map<std::string, std::string>{},
                                      /*record_count=*/1, /*file_size=*/1);
    ASSERT_NOK_WITH_MSG(commit->Commit({into_metadata}), "own metadata rather than data");
}

TEST(FormatTableTest, TestLocationBoundsAreCheckedFromTheRightComponent) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> table_schema,
                         schema_manager.CreateTable(
                             MakeSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    std::shared_ptr<DataSchema> data_schema =
        checked_pointer_cast<DataSchema>(std::shared_ptr<TableSchema>(std::move(table_schema)));

    // Every path is checked against the location, and an empty one is a prefix of nothing: it
    // would either pass every path, including those outside the table, or fail them all.
    ASSERT_NOK_WITH_MSG(
        FormatTable::Create(dir->GetFileSystem(), "", Identifier("db", "tbl"), data_schema,
                            /*location_carries_paimon_metadata=*/false, /*dynamic_options=*/{}),
        "requires a location");

    // The file system root is its own separator, so what is below it starts one character in.
    // Reading it two characters in would take `/data.parquet` for `ata.parquet`, and
    // `/.hidden.parquet` for a name that is not hidden at all.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> root_table,
        FormatTable::Create(dir->GetFileSystem(), "/", Identifier("db", "tbl"), data_schema,
                            /*location_carries_paimon_metadata=*/false, /*dynamic_options=*/{}));
    ASSERT_EQ(root_table->Location(), "/");
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(root_table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));

    auto visible = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{"/data.parquet", 0}},
        std::map<std::string, std::string>{});
    ASSERT_OK(read->CreateReader(std::static_pointer_cast<Split>(visible)));

    auto hidden = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{"/.data.parquet", 0}},
        std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(hidden)), "would skip");

    // A location written with a trailing separator names the same directory as one without, and
    // bounds the same paths.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> trailing,
        FormatTable::Create(dir->GetFileSystem(), dir->Str() + "/", Identifier("db", "tbl"),
                            data_schema,
                            /*location_carries_paimon_metadata=*/false, /*dynamic_options=*/{}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> trailing_read,
        FormatTableRead::TEST_Create(trailing, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));
    auto inside = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/data.parquet", 0}},
        std::map<std::string, std::string>{});
    ASSERT_OK(trailing_read->CreateReader(std::static_pointer_cast<Split>(inside)));
    auto outside = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "-sibling/data.parquet", 0}},
        std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(trailing_read->CreateReader(std::static_pointer_cast<Split>(outside)),
                        "not under the table location");
}

TEST(FormatTableTest, TestATrailingSeparatorStillNamesTheTableLocation) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> created,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest,
                         schema_manager.Latest());
    ASSERT_TRUE(latest.has_value());
    std::shared_ptr<DataSchema> data_schema = checked_pointer_cast<DataSchema>(latest.value());

    // The same table with a trailing separator. Whether `schema` below the location is metadata
    // is decided by comparing the two, so both spellings must compare equal; otherwise an
    // overwrite at the root would list the schema as data and delete it.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        FormatTable::Create(dir->GetFileSystem(), dir->Str() + "/", Identifier("db", "tbl"),
                            data_schema,
                            /*location_carries_paimon_metadata=*/true, /*dynamic_options=*/{}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch), /*overwrite=*/true));

    // The schema is still where the table keeps it, and the rows are readable.
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> after,
                         schema_manager.Latest());
    ASSERT_TRUE(after.has_value());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101"}));
}

TEST(FormatTableTest, TestExternalLocationHasNoReservedDirectories) {
    // A format table served by a catalog that keeps schemas elsewhere has nothing but data below
    // its location. A value-only partition whose value happens to be `schema` is such data, and
    // skipping it would drop rows without a word.
    std::unique_ptr<UniqueTestDirectory> schema_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(schema_dir);
    std::map<std::string, std::string> options = {
        {Options::TYPE, "format-table"},
        {Options::FILE_FORMAT, "parquet"},
        {Options::FORMAT_TABLE_PARTITION_PATH_ONLY_VALUE, "true"}};
    SchemaManager schema_manager(schema_dir->GetFileSystem(), schema_dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> table_schema,
                         schema_manager.CreateTable(MakeSchema(), /*partition_keys=*/{"dt"},
                                                    /*primary_keys=*/{}, options));

    // The data lives somewhere else entirely, the way an external table's does.
    std::unique_ptr<UniqueTestDirectory> data_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(data_dir);
    std::shared_ptr<DataSchema> data_schema =
        checked_pointer_cast<DataSchema>(std::shared_ptr<TableSchema>(std::move(table_schema)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> external,
        FormatTable::Create(data_dir->GetFileSystem(), data_dir->Str(), Identifier("db", "tbl"),
                            data_schema,
                            /*location_carries_paimon_metadata=*/false, /*dynamic_options=*/{}));
    ASSERT_FALSE(external->LocationCarriesPaimonMetadata());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "schema", {{"dt", "schema"}}));
    ASSERT_OK(WriteAndCommit(external, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(external, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(external, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|schema"}));
}

TEST(FormatTableTest, TestFileSuffixIncludesCompressionWhenAsked) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // A format that records its own compression keeps a plain name unless the option asks for it,
    // and then the compression goes in front of the format: `data-<uuid>-0.snappy.parquet`.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                     {{Options::FILE_FORMAT, "parquet"},
                                      {Options::FILE_SUFFIX_INCLUDE_COMPRESSION, "true"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_TRUE(StringUtils::EndsWith(messages[0].file_path, ".snappy.parquet"))
        << messages[0].file_path;
    ASSERT_OK(write->Abort());

    // A compression hadoop does not name goes into the file name as the option spelled it.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> uncompressed,
                         CreateTable(dir->GetFileSystem(), dir->Str() + "/other", {},
                                     {{Options::FILE_FORMAT, "parquet"},
                                      {Options::FILE_COMPRESSION, "uncompressed"},
                                      {Options::FILE_SUFFIX_INCLUDE_COMPRESSION, "true"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> other_write,
                         FormatTableWrite::Create(uncompressed, /*pool=*/nullptr));
    ASSERT_OK_AND_ASSIGN(batch, MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK(other_write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(messages, other_write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_TRUE(StringUtils::EndsWith(messages[0].file_path, ".uncompressed.parquet"))
        << messages[0].file_path;
    ASSERT_OK(other_write->Abort());
}

TEST(FormatTableTest, TestValueOnlyLayoutNeedsPartitionKeys) {
    // The layout names a directory by its partition value alone, so a table with no partition
    // keys asks for a layout that has nothing to lay out.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_NOK_WITH_MSG(CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                    {{Options::FORMAT_TABLE_PARTITION_PATH_ONLY_VALUE, "true"}}),
                        "on a table with no partition keys");
}

TEST(FormatTableTest, TestCreateRejectsManagedTable) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN(
        [[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
        schema_manager.CreateTable(MakeSchema(), /*partition_keys=*/{}, /*primary_keys=*/{},
                                   {{Options::FILE_FORMAT, "parquet"}}));
    Result<std::shared_ptr<FormatTable>> table = FormatTable::Create(
        dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl"), /*dynamic_options=*/{});
    ASSERT_FALSE(table.ok());
    ASSERT_TRUE(table.status().IsInvalid());
}

TEST(FormatTableTest, TestValueOnlyPartitionLayout) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"},
                                     {{Options::FORMAT_TABLE_PARTITION_PATH_ONLY_VALUE, "true"}}));
    ASSERT_TRUE(table->PartitionOnlyValueInPath());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    // The directory is the bare value, with no field name in it.
    ASSERT_OK_AND_ASSIGN(bool value_only_dir_exists,
                         dir->GetFileSystem()->Exists(PathUtil::JoinPath(dir->Str(), "20240101")));
    ASSERT_TRUE(value_only_dir_exists);
    ASSERT_OK_AND_ASSIGN(bool key_value_dir_exists, dir->GetFileSystem()->Exists(PathUtil::JoinPath(
                                                        dir->Str(), "dt=20240101")));
    ASSERT_FALSE(key_value_dir_exists);

    // And the scan reads back the layout the write produced.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions.size(), 1);
    ASSERT_EQ(partitions[0].at("dt"), "20240101");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 2);
    ASSERT_EQ(rows[0], "1|alice|20240101");
}

TEST(FormatTableTest, TestScanFindsDataFilesInSubdirectories) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));

    // A real data file, written by this table and then moved: `data-file.path-directory` puts
    // data files a level down, and an engine writing the directory may do the same. The partition
    // columns are not in the file, so nesting changes nothing about what one holds.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> written,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> written_plan, written->CreatePlan());
    ASSERT_EQ(written_plan->Splits().size(), 1u);
    auto written_split = std::dynamic_pointer_cast<FormatDataSplit>(written_plan->Splits()[0]);
    ASSERT_NE(written_split, nullptr);
    ASSERT_EQ(written_split->files.size(), 1u);
    const std::string written_path = written_split->files[0].file_path;

    std::string nested = PathUtil::JoinPath(dir->Str(), "bucket-0");
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(nested));
    const std::string nested_path = PathUtil::JoinPath(nested, PathUtil::GetName(written_path));
    ASSERT_OK(dir->GetFileSystem()->Rename(written_path, nested_path));

    // A staging tree at the same level stays invisible, whatever its files are called, even when
    // the file in it is a perfectly readable copy of the one above.
    std::string staging = PathUtil::JoinPath(dir->Str(), "_temporary");
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(staging));
    std::string content;
    ASSERT_OK(dir->GetFileSystem()->ReadFile(nested_path, &content));
    ASSERT_OK(dir->GetFileSystem()->WriteFile(
        PathUtil::JoinPath(staging, PathUtil::GetName(written_path)), content,
        /*overwrite=*/true));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 2u);
    ASSERT_EQ(rows[0], "1|alice|20240101");
    ASSERT_EQ(rows[1], "2|bob|20240101");
}

TEST(FormatTableTest, TestTargetFileRowNumRollsFiles) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        CreateTable(dir->GetFileSystem(), dir->Str(), {}, {{Options::TARGET_FILE_ROW_NUM, "1"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    for (int32_t i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({i}, {"name"}, "20240101", {}));
        ASSERT_OK(write->Write(std::move(batch)));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    // Rolling is checked between batches, so each one-row batch closes its own file.
    ASSERT_EQ(messages.size(), 3);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Commit(messages));
}

TEST(FormatTableTest, TestTargetFileSizeRollsFiles) {
    constexpr int32_t kBatches = 2;
    constexpr int32_t kRowsPerBatch = 2000;
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // Rolling is checked between batches, so each batch is weighed against the target and a file
    // closes once it is past it. Without this a write would put a whole partition in one file
    // however large it grew.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        CreateTable(dir->GetFileSystem(), dir->Str(), {}, {{Options::TARGET_FILE_SIZE, "1 kb"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    // Megabytes of values, each of them different: a file is weighed by what its writer has
    // finished with, and a writer holds on to what it can still encode more cheaply later.
    for (int32_t batch = 0; batch < kBatches; batch++) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> rows,
                             MakeWideRowBatch(kRowsPerBatch, batch * kRowsPerBatch));
        ASSERT_OK(write->Write(std::move(rows)));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_GT(messages.size(), 1u);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Commit(messages));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> read_rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(read_rows.size(), static_cast<size_t>(kBatches) * static_cast<size_t>(kRowsPerBatch));
}

TEST(FormatTableTest, TestANonPositiveLimitPlansNothing) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    // A limit of zero asks for no rows, so there is nothing to read and no split to hand out. A
    // positive limit cannot drop anything, since a format table records no row counts.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableScan> none,
                         FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/0));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> empty_plan, none->CreatePlan());
    ASSERT_TRUE(empty_plan->Splits().empty());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableScan> some,
                         FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, some->CreatePlan());
    ASSERT_FALSE(plan->Splits().empty());
}

TEST(FormatTableTest, TestSplitsAreBoundedByTheSplitTargetSize) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // One file per split: every file costs at least the open-file cost, which fills a split on its
    // own. Without packing the whole partition would be a single split however many files it has.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                     {{Options::TARGET_FILE_ROW_NUM, "1"},
                                      {Options::SOURCE_SPLIT_TARGET_SIZE, "1 kb"},
                                      {Options::SOURCE_SPLIT_OPEN_FILE_COST, "1 kb"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    for (int32_t i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({i}, {"name"}, "20240101", {}));
        ASSERT_OK(write->Write(std::move(batch)));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Commit(messages));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 3);
    // Every row is still read exactly once, whichever split it landed in.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 3);
}

TEST(FormatTableTest, TestAFileLargerThanTheSplitTargetIsStillOneSplit) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // One file, far larger than a split.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                     {{Options::SOURCE_SPLIT_TARGET_SIZE, "1 kb"},
                                      {Options::SOURCE_SPLIT_OPEN_FILE_COST, "1 kb"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, MakeManyRowBatch(500));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    // A split holds whole files, because parquet and orc record their own row group and stripe
    // boundaries, so a file past the target size is one split rather than several.
    ASSERT_EQ(plan->Splits().size(), 1u);

    // And every row of it is read exactly once.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 500u);
    std::vector<std::string> expected;
    expected.reserve(500);
    for (int32_t i = 0; i < 500; i++) {
        expected.push_back(std::to_string(i) + "|name-" + std::to_string(i) + "|20240101");
    }
    std::sort(rows.begin(), rows.end());
    std::sort(expected.begin(), expected.end());
    ASSERT_EQ(rows, expected);
}

TEST(FormatTableTest, TestBlankPartitionValueLandsInTheDefaultPartition) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    // A null, an empty string and a whitespace-only string alike stand for the default partition
    // name. Treating only null that way would put the other two in directories of their own, and
    // an empty one has no legal directory name at all.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> blank,
        MakeBatch({1, 2}, {"alice", "bob"}, "   ", {{"dt", table->PartitionDefaultName()}}));
    ASSERT_OK(WriteAndCommit(table, std::move(blank)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions.size(), 1u);
    ASSERT_EQ(partitions[0].at("dt"), table->PartitionDefaultName());
    // The value reads back as null, as it does for a null partition.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 2u);
}

TEST(FormatTableTest, TestAFileThatCannotBeClosedEndsTheWrite) {
    // A store can refuse `Flush()`, `GetPos()` or `Close()` on the stream a file is written
    // through, and each lands at a different point: opening the file, adding to it, or closing it.
    // Wherever it lands, the write answers with the failure and hands out nothing to publish,
    // rather than reaching through a writer that is no longer there.
    for (const FailingStreamCall failing :
         {FailingStreamCall::kGetPos, FailingStreamCall::kFlush, FailingStreamCall::kClose}) {
        SCOPED_TRACE(static_cast<int32_t>(failing));
        // Rolling on every row, so a refusal in the closing path lands inside `Write()`.
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> created,
                             CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                         {{Options::TARGET_FILE_ROW_NUM, "1"}}));
        auto failing_fs = std::make_shared<FailingWriteFileSystem>(dir->GetFileSystem(), failing);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl"),
                                                 /*dynamic_options=*/{}));

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                             FormatTableWrite::Create(table, /*pool=*/nullptr));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                             MakeBatch({1}, {"alice"}, "20240101", {}));
        ASSERT_NOK(write->Write(std::move(first)));

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second,
                             MakeBatch({2}, {"bob"}, "20240101", {}));
        ASSERT_NOK(write->Write(std::move(second)));
        // A refusal while closing ends the write, since the rows of a file that cannot be closed
        // can never be published. One while the file was being opened staged nothing at all, so
        // there is nothing to refuse and nothing to hand out either.
        Result<std::vector<FormatCommitMessage>> prepared = write->PrepareCommit();
        if (prepared.ok()) {
            ASSERT_TRUE(prepared.value().empty());
        }
        ASSERT_OK(write->Abort());

        // Whatever it gave up on is gone, so a later scan does not see a partial file.
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> plain_table,
                             FormatTable::Create(dir->GetFileSystem(), dir->Str(),
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(plain_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_TRUE(plan->Splits().empty());
    }

    // `Close()` is reached only once the writer has been finished and dropped, so a refusal there
    // is the case worth pinning down: the write is over, and both entry points say so rather than
    // publishing what is left or reaching through the writer that is gone.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> created,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    auto failing_fs =
        std::make_shared<FailingWriteFileSystem>(dir->GetFileSystem(), FailingStreamCall::kClose);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl"),
                                             /*dynamic_options=*/{}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_NOK(write->PrepareCommit());
    ASSERT_NOK(write->PrepareCommit());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> after,
                         MakeBatch({2}, {"bob"}, "20240101", {}));
    ASSERT_NOK(write->Write(std::move(after)));
    ASSERT_OK(write->Abort());
}

TEST(FormatTableTest, TestAFileWhoseFooterCannotBeWrittenIsGivenUpOnOnce) {
    // A writer releases what it writes through before it writes the file's footer, so a store that
    // refuses that last write leaves nothing to finish a second time. The cleanup after the failure
    // must not ask it to - that would reach through what the failed finish already freed - and the
    // caller must still be told what went wrong. orc frees its batch there, so both are walked.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> created,
                             CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                         {{Options::FILE_FORMAT, file_format}}));
        auto failing_fs = std::make_shared<ArmedWriteFailureFileSystem>(dir->GetFileSystem());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl"),
                                                 /*dynamic_options=*/{}));

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                             FormatTableWrite::Create(table, /*pool=*/nullptr));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({1}, {"alice"}, "20240101", {}));
        ASSERT_OK(write->Write(std::move(batch)));

        // Everything the writer buffered went through; only what finishing it writes is refused.
        failing_fs->FailFurtherWrites();
        ASSERT_NOK_WITH_MSG(write->PrepareCommit(), "injected write failure");

        // The write is over: it takes no more rows and has nothing whole to publish.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> after,
                             MakeBatch({2}, {"bob"}, "20240101", {}));
        ASSERT_NOK(write->Write(std::move(after)));
        ASSERT_NOK(write->PrepareCommit());
        ASSERT_OK(write->Abort());

        // What it gave up on is gone, so a later scan does not find a file with no footer.
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> plain_table,
                             FormatTable::Create(dir->GetFileSystem(), dir->Str(),
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(plain_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_TRUE(plan->Splits().empty());
    }
}

TEST(FormatTableTest, TestAFailedTargetSizeCheckEndsTheWrite) {
    // The other point inside `Write()` that reaches the file: once a batch is in, the writer is
    // asked whether the file has grown to its target, and answers by measuring what it has written.
    // A refusal there leaves the file part way through just as a refused write does, so the write
    // ends rather than carrying on with a file it can no longer measure.
    //
    // The refusal is armed only after a batch is in. Refusing from the start would stop the write
    // in `OpenFile()` instead - see `FailingWriteFileSystem` - which stages nothing and so is not
    // terminal; `TestAFileThatCannotBeClosedEndsTheWrite` covers that case.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        // The row target is left at its default, so the size check is what runs.
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> created,
                             CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                         {{Options::FILE_FORMAT, file_format}}));
        auto failing_fs = std::make_shared<FailingWriteFileSystem>(
            dir->GetFileSystem(), FailingStreamCall::kGetPos, /*armed=*/false);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl"),
                                                 /*dynamic_options=*/{}));

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                             FormatTableWrite::Create(table, /*pool=*/nullptr));
        // Opens the file and passes the size check, so the next one is what the refusal meets.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> opening,
                             MakeBatch({1}, {"alice"}, "20240101", {}));
        ASSERT_OK(write->Write(std::move(opening)));
        failing_fs->Arm();

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({2}, {"bob"}, "20240101", {}));
        ASSERT_NOK_WITH_MSG(write->Write(std::move(batch)), "injected get position failure");

        // Terminal: the file is part way through, so neither entry point may carry on with it.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> after,
                             MakeBatch({3}, {"carol"}, "20240101", {}));
        ASSERT_NOK(write->Write(std::move(after)));
        ASSERT_NOK(write->PrepareCommit());
        ASSERT_OK(write->Abort());

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> plain_table,
                             FormatTable::Create(dir->GetFileSystem(), dir->Str(),
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(plain_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_TRUE(plan->Splits().empty());
    }
}

TEST(FormatTableTest, TestAFailedOpenClosesTheFileBeforeDeletingIt) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // A compression no codec answers to, so opening the file succeeds and building the writer
    // over it fails: the one path where a temp file exists and nothing owns it yet.
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> created,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                     {{Options::FILE_COMPRESSION, "nonesuch"}}));
    auto recording = std::make_shared<CallOrderFileSystem>(dir->GetFileSystem());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         FormatTable::Create(recording, dir->Str(), Identifier("db", "tbl"),
                                             /*dynamic_options=*/{}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    Status status = write->Write(std::move(batch));
    ASSERT_FALSE(status.ok()) << "a writer was built over a compression nothing implements";

    // Created, closed, and only then deleted. Deleting first would leave the stream to flush
    // afterwards, and on a store that writes from its destructor the file would come back under
    // a hidden name no scan reads and no abort knows about.
    const std::vector<std::string>& calls = recording->Calls();
    ASSERT_EQ(calls.size(), 3u);
    ASSERT_TRUE(StringUtils::StartsWith(calls[0], "create ")) << calls[0];
    ASSERT_TRUE(StringUtils::StartsWith(calls[1], "close ")) << calls[1];
    ASSERT_TRUE(StringUtils::StartsWith(calls[2], "delete ")) << calls[2];
    // All three name the one temp file, which is hidden so that no scan can reach it.
    const std::string temp_path = calls[0].substr(std::string("create ").size());
    ASSERT_EQ(calls[1], "close " + temp_path);
    ASSERT_EQ(calls[2], "delete " + temp_path);
    ASSERT_TRUE(StringUtils::StartsWith(PathUtil::GetName(temp_path), ".")) << temp_path;

    // Nothing is left behind for a later scan to pick up.
    ASSERT_OK_AND_ASSIGN(bool exists, dir->GetFileSystem()->Exists(temp_path));
    ASSERT_FALSE(exists);
}

TEST(FormatTableTest, TestCreateRejectsSchemasThatCouldNeverBeUsed) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // Every one of these can be persisted and loaded, and would otherwise only fail when a reader
    // or writer is built, long after the table looked created.
    ASSERT_NOK_WITH_MSG(CreateTable(dir->GetFileSystem(), dir->Str() + "/a", {},
                                    {{Options::FILE_FORMAT, "nonesuch"}}),
                        "unsupported file format");
    ASSERT_NOK_WITH_MSG(CreateTable(dir->GetFileSystem(), dir->Str() + "/b", {},
                                    {{Options::TARGET_FILE_ROW_NUM, "0"}}),
                        "should be at least 1");
    // A format table format with no reader here is refused by name, rather than failing later
    // with a missing-format-factory error.
    ASSERT_NOK_WITH_MSG(
        CreateTable(dir->GetFileSystem(), dir->Str() + "/c", {}, {{Options::FILE_FORMAT, "csv"}}),
        "not supported by paimon-cpp yet");
    // Every column a partition column leaves the data files with nothing in them.
    ASSERT_NOK_WITH_MSG(
        CreateTable(dir->GetFileSystem(), dir->Str() + "/d", {"id", "name", "dt"}, {}),
        "every one of its columns");
}

TEST(FormatTableTest, TestPrimaryKeysAreRejectedWhenTheTableIsCreated) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    // A format table with primary keys could never be opened, so it is never written either.
    Result<std::unique_ptr<TableSchema>> table_schema = schema_manager.CreateTable(
        MakeSchema(), /*partition_keys=*/{}, /*primary_keys=*/{"id"},
        {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}});
    ASSERT_FALSE(table_schema.ok());
    ASSERT_TRUE(table_schema.status().IsInvalid());
}

TEST(FormatTableTest, TestWriteReadUnpartitioned) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 1);

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 2u);
    ASSERT_EQ(rows[0], "1|alice|20240101");
    ASSERT_EQ(rows[1], "2|bob|20240101");
}

TEST(FormatTableTest, TestBatchesOfOnePartitionShareOneFile) {
    // The directory a partition writes into is derived once and kept, so a second batch for the
    // same partition finds the file the first one opened instead of starting another. A partition
    // that has not been seen before still gets a file of its own.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    for (const auto& [id, name, dt] : std::vector<std::tuple<int32_t, std::string, std::string>>{
             {1, "alice", "20240101"}, {2, "bob", "20240101"}, {3, "carol", "20240102"}}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({id}, {name}, dt, {{"dt", dt}}));
        ASSERT_OK(write->Write(std::move(batch)));
    }

    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    // One file per partition, not one per batch.
    ASSERT_EQ(messages.size(), 2u);
    std::sort(messages.begin(), messages.end(),
              [](const FormatCommitMessage& left, const FormatCommitMessage& right) {
                  return left.file_path < right.file_path;
              });
    ASSERT_EQ(messages[0].partition, (std::map<std::string, std::string>{{"dt", "20240101"}}));
    ASSERT_EQ(messages[0].record_count, 2);
    ASSERT_EQ(messages[1].partition, (std::map<std::string, std::string>{{"dt", "20240102"}}));
    ASSERT_EQ(messages[1].record_count, 1);

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    std::sort(rows.begin(), rows.end());
    ASSERT_EQ(rows,
              (std::vector<std::string>{"1|alice|20240101", "2|bob|20240101", "3|carol|20240102"}));
}

TEST(FormatTableTest, TestWriteReadPartitioned) {
    // Once per format a format table may hold. The name decides everything between the write and
    // the read - which writer builds the file, what the file is called, which reader opens it,
    // and whether the prefetching reader can drive it - and every other test here writes parquet,
    // so this is where orc's path is walked end to end.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"},
                                         {{Options::FILE_FORMAT, file_format}}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                             MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
        ASSERT_OK(WriteAndCommit(table, std::move(first)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second,
                             MakeBatch({2}, {"bob"}, "20240102", {{"dt", "20240102"}}));
        ASSERT_OK(WriteAndCommit(table, std::move(second)));

        // Each partition is its own directory, so each is its own split.
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), 2);

        // The file carries the format in its name, which is what a scan of the directory finds
        // and what another engine reads it as.
        std::shared_ptr<FormatDataSplit> planned =
            std::dynamic_pointer_cast<FormatDataSplit>(plan->Splits()[0]);
        ASSERT_TRUE(planned != nullptr);
        ASSERT_EQ(planned->files.size(), 1u);
        ASSERT_TRUE(
            StringUtils::EndsWith(planned->files[0].file_path, std::string(".") + file_format))
            << planned->files[0].file_path;

        // The partition column is rebuilt from the directory name, not read from the file.
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
        ASSERT_EQ(rows.size(), 2);
        ASSERT_EQ(rows[0], "1|alice|20240101");
        ASSERT_EQ(rows[1], "2|bob|20240102");

        ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
        ASSERT_EQ(partitions.size(), 2);
    }
}

TEST(FormatTableTest, TestAFileIsReadByTheColumnsItHolds) {
    // A format table's files need not have been written by this table. An engine that stored the
    // columns in another order, and a file written before a column was added, are both ordinary
    // content of such a directory: the columns are matched by name, read in the order the file
    // holds them, and a column the file does not hold reads back as null. Both formats care -
    // parquet refuses a column it cannot find, and orc reads its columns by their position in the
    // file - so both are walked.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const std::map<std::string, std::string> options = {{Options::TYPE, "format-table"},
                                                            {Options::FILE_FORMAT, file_format}};

        // The file is written through a table of its own, whose schema is what that other engine
        // stored: `name` in front of `id`, and no `note` at all.
        const std::string written_path = dir->Str() + "/written";
        std::shared_ptr<arrow::Schema> written_schema = arrow::schema(
            {arrow::field("name", arrow::utf8()), arrow::field("id", arrow::int32())});
        SchemaManager written_manager(dir->GetFileSystem(), written_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> written_table_schema,
                             written_manager.CreateTable(written_schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> writer_table,
            FormatTable::Create(dir->GetFileSystem(), written_path, Identifier("db", "written"),
                                /*dynamic_options=*/{}));

        auto make_written_batch = [&written_schema]() -> Result<std::unique_ptr<RecordBatch>> {
            arrow::StringBuilder name_builder;
            arrow::Int32Builder id_builder;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Append("alice"));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
            std::shared_ptr<arrow::Array> name_array;
            std::shared_ptr<arrow::Array> id_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Finish(&name_array));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> struct_array,
                arrow::StructArray::Make({name_array, id_array}, written_schema->fields()));
            auto c_array = std::make_unique<ArrowArray>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
            RecordBatchBuilder builder(c_array.get());
            builder.SetPartition({});
            return builder.Finish();
        };
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, make_written_batch());
        ASSERT_OK(WriteAndCommit(writer_table, std::move(batch)));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> written_scan,
            FormatTableScan::Create(writer_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> written_plan, written_scan->CreatePlan());
        ASSERT_EQ(written_plan->Splits().size(), 1u);
        std::shared_ptr<FormatDataSplit> written_split =
            std::dynamic_pointer_cast<FormatDataSplit>(written_plan->Splits()[0]);
        ASSERT_TRUE(written_split != nullptr);
        ASSERT_EQ(written_split->files.size(), 1u);
        const std::string written_file = written_split->files[0].file_path;

        // The table doing the reading declares three columns, in an order of its own.
        const std::string read_path = dir->Str() + "/read";
        std::shared_ptr<arrow::Schema> table_schema =
            arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8()),
                           arrow::field("note", arrow::utf8())});
        SchemaManager read_manager(dir->GetFileSystem(), read_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> read_table_schema,
                             read_manager.CreateTable(table_schema, /*partition_keys=*/{},
                                                      /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(dir->GetFileSystem(), read_path,
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        // The same file, now sitting in that table's directory: what an external table looks like.
        std::error_code copied;
        std::filesystem::copy_file(
            written_file, PathUtil::JoinPath(read_path, PathUtil::GetName(written_file)), copied);
        ASSERT_FALSE(copied) << copied.message();

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), 1u);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableRead> read,
                             FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                                          /*pool=*/nullptr, /*predicate=*/nullptr,
                                                          /*enable_predicate_filter=*/false));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             read->CreateReader(plan->Splits()));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch read_batch, reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(read_batch));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch,
                             ImportBatch(read_batch));
        ASSERT_EQ(record_batch->num_rows(), 1);
        // The table's own columns, in the order it declares them, behind `_VALUE_KIND`.
        ASSERT_EQ(record_batch->schema()->field(1)->name(), "id");
        ASSERT_EQ(record_batch->schema()->field(2)->name(), "name");
        ASSERT_EQ(record_batch->schema()->field(3)->name(), "note");
        ASSERT_EQ(checked_pointer_cast<arrow::Int32Array>(record_batch->column(1))->Value(0), 1);
        ASSERT_EQ(checked_pointer_cast<arrow::StringArray>(record_batch->column(2))->GetString(0),
                  "alice");
        // The file was written before `note` was a column of anything, so it has no value to give.
        ASSERT_TRUE(record_batch->column(3)->IsNull(0));

        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader->NextBatch());
        ASSERT_TRUE(BatchReader::IsEofBatch(eof));
        reader->Close();
    }
}

TEST(FormatTableTest, TestAFileWhoseColumnTypeIsNotTheTablesIsRefused) {
    // A file storing a column as a type the table does not declare is refused rather than read at
    // the file's type. A `DECIMAL` of another scale is the case that would otherwise slip through;
    // `DescribeFileColumns()` says why. Both formats are walked, since only orc catches it itself.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const std::map<std::string, std::string> options = {{Options::TYPE, "format-table"},
                                                            {Options::FILE_FORMAT, file_format}};

        // The file is written through a table of its own, which stores `amount` to two places.
        const std::string written_path = dir->Str() + "/written";
        std::shared_ptr<arrow::Schema> written_schema = arrow::schema(
            {arrow::field("id", arrow::int32()), arrow::field("amount", arrow::decimal128(10, 2))});
        SchemaManager written_manager(dir->GetFileSystem(), written_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> written_table_schema,
                             written_manager.CreateTable(written_schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> writer_table,
            FormatTable::Create(dir->GetFileSystem(), written_path, Identifier("db", "written"),
                                /*dynamic_options=*/{}));

        auto make_written_batch = [&written_schema]() -> Result<std::unique_ptr<RecordBatch>> {
            arrow::Int32Builder id_builder;
            arrow::Decimal128Builder amount_builder(arrow::decimal128(10, 2));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(amount_builder.Append(arrow::Decimal128(1234)));
            std::shared_ptr<arrow::Array> id_array;
            std::shared_ptr<arrow::Array> amount_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(amount_builder.Finish(&amount_array));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> struct_array,
                arrow::StructArray::Make({id_array, amount_array}, written_schema->fields()));
            auto c_array = std::make_unique<ArrowArray>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
            RecordBatchBuilder builder(c_array.get());
            builder.SetPartition({});
            return builder.Finish();
        };
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, make_written_batch());
        ASSERT_OK(WriteAndCommit(writer_table, std::move(batch)));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> written_scan,
            FormatTableScan::Create(writer_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> written_plan, written_scan->CreatePlan());
        ASSERT_EQ(written_plan->Splits().size(), 1u);
        std::shared_ptr<FormatDataSplit> written_split =
            std::dynamic_pointer_cast<FormatDataSplit>(written_plan->Splits()[0]);
        ASSERT_TRUE(written_split != nullptr);
        ASSERT_EQ(written_split->files.size(), 1u);
        const std::string written_file = written_split->files[0].file_path;

        // The table doing the reading declares that column to three places, which is a type of its
        // own however alike the two look.
        const std::string read_path = dir->Str() + "/read";
        std::shared_ptr<arrow::Schema> table_schema = arrow::schema(
            {arrow::field("id", arrow::int32()), arrow::field("amount", arrow::decimal128(10, 3))});
        SchemaManager read_manager(dir->GetFileSystem(), read_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> read_table_schema,
                             read_manager.CreateTable(table_schema, /*partition_keys=*/{},
                                                      /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(dir->GetFileSystem(), read_path,
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        std::error_code copied;
        std::filesystem::copy_file(
            written_file, PathUtil::JoinPath(read_path, PathUtil::GetName(written_file)), copied);
        ASSERT_FALSE(copied) << copied.message();

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), 1u);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableRead> read,
                             FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                                          /*pool=*/nullptr, /*predicate=*/nullptr,
                                                          /*enable_predicate_filter=*/false));
        // A split's file is opened as it is reached, so the refusal arrives with the first batch,
        // naming the file it came from.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             read->CreateReader(plan->Splits()));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "stores the column 'amount' as");
        reader->Close();

        // The same file read without that column: a projection that leaves a column out never
        // asks the file for it, so its type is not checked.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableRead> projected,
                             FormatTableRead::TEST_Create(table, std::vector<std::string>{"id"},
                                                          /*pool=*/nullptr, /*predicate=*/nullptr,
                                                          /*enable_predicate_filter=*/false));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> projected_reader,
                             projected->CreateReader(plan->Splits()));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch projected_batch, projected_reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(projected_batch));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> projected_record,
                             ImportBatch(projected_batch));
        ASSERT_EQ(projected_record->num_rows(), 1);
        ASSERT_EQ(checked_pointer_cast<arrow::Int32Array>(projected_record->column(1))->Value(0),
                  1);
        projected_reader->Close();
    }
}

TEST(FormatTableTest, TestAPartitionColumnStoredInTheFileNeedNotMatchTheTable) {
    // A partition column's type in the file is never checked, the value coming from the directory
    // name instead; `DescribeFileColumns()` says why. Here an engine wrote `dt` into the file as
    // an `INT` while the table declares it `STRING`.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const std::map<std::string, std::string> options = {{Options::TYPE, "format-table"},
                                                            {Options::FILE_FORMAT, file_format}};

        const std::string written_path = dir->Str() + "/written";
        std::shared_ptr<arrow::Schema> written_schema =
            arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8()),
                           arrow::field("dt", arrow::int32())});
        SchemaManager written_manager(dir->GetFileSystem(), written_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> written_table_schema,
                             written_manager.CreateTable(written_schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> writer_table,
            FormatTable::Create(dir->GetFileSystem(), written_path, Identifier("db", "written"),
                                /*dynamic_options=*/{}));

        auto make_written_batch = [&written_schema]() -> Result<std::unique_ptr<RecordBatch>> {
            arrow::Int32Builder id_builder;
            arrow::StringBuilder name_builder;
            arrow::Int32Builder dt_builder;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Append("alice"));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.Append(20240101));
            std::shared_ptr<arrow::Array> id_array;
            std::shared_ptr<arrow::Array> name_array;
            std::shared_ptr<arrow::Array> dt_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Finish(&name_array));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.Finish(&dt_array));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> struct_array,
                arrow::StructArray::Make({id_array, name_array, dt_array},
                                         written_schema->fields()));
            auto c_array = std::make_unique<ArrowArray>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
            RecordBatchBuilder builder(c_array.get());
            builder.SetPartition({});
            return builder.Finish();
        };
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, make_written_batch());
        ASSERT_OK(WriteAndCommit(writer_table, std::move(batch)));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> written_scan,
            FormatTableScan::Create(writer_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> written_plan, written_scan->CreatePlan());
        ASSERT_EQ(written_plan->Splits().size(), 1u);
        std::shared_ptr<FormatDataSplit> written_split =
            std::dynamic_pointer_cast<FormatDataSplit>(written_plan->Splits()[0]);
        ASSERT_TRUE(written_split != nullptr);
        ASSERT_EQ(written_split->files.size(), 1u);
        const std::string written_file = written_split->files[0].file_path;

        // The reading table is partitioned by `dt`, which it declares `STRING`.
        const std::string read_path = dir->Str() + "/read";
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             CreateTable(dir->GetFileSystem(), read_path, {"dt"},
                                         {{Options::FILE_FORMAT, file_format}}));
        const std::string partition_dir = PathUtil::JoinPath(read_path, "dt=20240101");
        ASSERT_OK(dir->GetFileSystem()->Mkdirs(partition_dir));
        std::error_code copied;
        std::filesystem::copy_file(
            written_file, PathUtil::JoinPath(partition_dir, PathUtil::GetName(written_file)),
            copied);
        ASSERT_FALSE(copied) << copied.message();

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), 1u);
        // The directory decides the value, so it comes back as the `STRING` the table declares.
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
        ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101"}));
    }
}

TEST(FormatTableTest, TestAStructWhoseChildrenTheFileOrdersDifferentlyIsRefused) {
    // A `STRUCT` whose children the file orders differently is refused rather than silently
    // reordered; `FileTypeMatchesTableType()` says why. Both formats are walked: the check runs
    // on the schema the file reports, before either reader is given one to read with, so it is
    // this refusal that either of them meets.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const std::map<std::string, std::string> options = {{Options::TYPE, "format-table"},
                                                            {Options::FILE_FORMAT, file_format}};

        const std::string written_path = dir->Str() + "/written";
        std::shared_ptr<arrow::DataType> written_payload =
            arrow::struct_({arrow::field("b", arrow::int32()), arrow::field("a", arrow::int32())});
        std::shared_ptr<arrow::Schema> written_schema = arrow::schema(
            {arrow::field("id", arrow::int32()), arrow::field("payload", written_payload)});
        SchemaManager written_manager(dir->GetFileSystem(), written_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> written_table_schema,
                             written_manager.CreateTable(written_schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> writer_table,
            FormatTable::Create(dir->GetFileSystem(), written_path, Identifier("db", "written"),
                                /*dynamic_options=*/{}));

        auto make_written_batch = [&]() -> Result<std::unique_ptr<RecordBatch>> {
            arrow::Int32Builder id_builder;
            arrow::Int32Builder b_builder;
            arrow::Int32Builder a_builder;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(b_builder.Append(20));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(a_builder.Append(10));
            std::shared_ptr<arrow::Array> id_array;
            std::shared_ptr<arrow::Array> b_array;
            std::shared_ptr<arrow::Array> a_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(b_builder.Finish(&b_array));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(a_builder.Finish(&a_array));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> payload_array,
                arrow::StructArray::Make({b_array, a_array}, written_payload->fields()));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> struct_array,
                arrow::StructArray::Make({id_array, payload_array}, written_schema->fields()));
            auto c_array = std::make_unique<ArrowArray>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
            RecordBatchBuilder builder(c_array.get());
            builder.SetPartition({});
            return builder.Finish();
        };
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, make_written_batch());
        ASSERT_OK(WriteAndCommit(writer_table, std::move(batch)));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> written_scan,
            FormatTableScan::Create(writer_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> written_plan, written_scan->CreatePlan());
        ASSERT_EQ(written_plan->Splits().size(), 1u);
        std::shared_ptr<FormatDataSplit> written_split =
            std::dynamic_pointer_cast<FormatDataSplit>(written_plan->Splits()[0]);
        ASSERT_TRUE(written_split != nullptr);
        ASSERT_EQ(written_split->files.size(), 1u);
        const std::string written_file = written_split->files[0].file_path;

        // The same children, in the order this table declares them.
        const std::string read_path = dir->Str() + "/read";
        std::shared_ptr<arrow::DataType> read_payload =
            arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32())});
        std::shared_ptr<arrow::Schema> table_schema = arrow::schema(
            {arrow::field("id", arrow::int32()), arrow::field("payload", read_payload)});
        SchemaManager read_manager(dir->GetFileSystem(), read_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> read_table_schema,
                             read_manager.CreateTable(table_schema, /*partition_keys=*/{},
                                                      /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(dir->GetFileSystem(), read_path,
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        std::error_code copied;
        std::filesystem::copy_file(
            written_file, PathUtil::JoinPath(read_path, PathUtil::GetName(written_file)), copied);
        ASSERT_FALSE(copied) << copied.message();

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), 1u);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableRead> read,
                             FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                                          /*pool=*/nullptr, /*predicate=*/nullptr,
                                                          /*enable_predicate_filter=*/false));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             read->CreateReader(plan->Splits()));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "stores the column 'payload' as");
        reader->Close();
    }
}

TEST(FormatTableTest, TestAFileMissingANotNullColumnIsRefused) {
    // A column the file leaves out is read as null, so one the table declares NOT NULL is refused
    // instead. See `DescribeFileColumns()`.
    for (const char* file_format : {"parquet", "orc"}) {
        SCOPED_TRACE(file_format);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        const std::map<std::string, std::string> options = {{Options::TYPE, "format-table"},
                                                            {Options::FILE_FORMAT, file_format}};

        const std::string written_path = dir->Str() + "/written";
        std::shared_ptr<arrow::Schema> written_schema =
            arrow::schema({arrow::field("id", arrow::int32())});
        SchemaManager written_manager(dir->GetFileSystem(), written_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> written_table_schema,
                             written_manager.CreateTable(written_schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> writer_table,
            FormatTable::Create(dir->GetFileSystem(), written_path, Identifier("db", "written"),
                                /*dynamic_options=*/{}));

        auto make_written_batch = [&written_schema]() -> Result<std::unique_ptr<RecordBatch>> {
            arrow::Int32Builder id_builder;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
            std::shared_ptr<arrow::Array> id_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::StructArray> struct_array,
                arrow::StructArray::Make({id_array}, written_schema->fields()));
            auto c_array = std::make_unique<ArrowArray>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
            RecordBatchBuilder builder(c_array.get());
            builder.SetPartition({});
            return builder.Finish();
        };
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, make_written_batch());
        ASSERT_OK(WriteAndCommit(writer_table, std::move(batch)));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> written_scan,
            FormatTableScan::Create(writer_table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> written_plan, written_scan->CreatePlan());
        ASSERT_EQ(written_plan->Splits().size(), 1u);
        std::shared_ptr<FormatDataSplit> written_split =
            std::dynamic_pointer_cast<FormatDataSplit>(written_plan->Splits()[0]);
        ASSERT_TRUE(written_split != nullptr);
        ASSERT_EQ(written_split->files.size(), 1u);
        const std::string written_file = written_split->files[0].file_path;

        // The reading table declares a column the file has no value to give, and refuses null for
        // it.
        const std::string read_path = dir->Str() + "/read";
        std::shared_ptr<arrow::Schema> table_schema =
            arrow::schema({arrow::field("id", arrow::int32()),
                           arrow::field("note", arrow::utf8(), /*nullable=*/false)});
        SchemaManager read_manager(dir->GetFileSystem(), read_path);
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> read_table_schema,
                             read_manager.CreateTable(table_schema, /*partition_keys=*/{},
                                                      /*primary_keys=*/{}, options));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(dir->GetFileSystem(), read_path,
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        std::error_code copied;
        std::filesystem::copy_file(
            written_file, PathUtil::JoinPath(read_path, PathUtil::GetName(written_file)), copied);
        ASSERT_FALSE(copied) << copied.message();

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), 1u);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableRead> read,
                             FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                                          /*pool=*/nullptr, /*predicate=*/nullptr,
                                                          /*enable_predicate_filter=*/false));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader,
                             read->CreateReader(plan->Splits()));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "which the table declares NOT NULL");
        reader->Close();

        // A projection that leaves it out asks the file for nothing it does not hold.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableRead> projected,
                             FormatTableRead::TEST_Create(table, std::vector<std::string>{"id"},
                                                          /*pool=*/nullptr, /*predicate=*/nullptr,
                                                          /*enable_predicate_filter=*/false));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> projected_reader,
                             projected->CreateReader(plan->Splits()));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch projected_batch, projected_reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(projected_batch));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> projected_record,
                             ImportBatch(projected_batch));
        ASSERT_EQ(projected_record->num_rows(), 1);
        projected_reader->Close();
    }
}

namespace {

/// The schema the timestamp tests below use: an `id` and a `ts` in `unit`.
std::shared_ptr<arrow::Schema> MakeTimestampSchema(arrow::TimeUnit::type unit) {
    return arrow::schema(
        {arrow::field("id", arrow::int32()), arrow::field("ts", arrow::timestamp(unit))});
}

/// Creates a format table of `file_format` under `path`, its `ts` column declared in `unit`.
Result<std::shared_ptr<FormatTable>> CreateTimestampTable(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    const std::string& file_format, arrow::TimeUnit::type unit) {
    SchemaManager manager(file_system, path);
    PAIMON_ASSIGN_OR_RAISE(
        [[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
        manager.CreateTable(
            MakeTimestampSchema(unit), /*partition_keys=*/{}, /*primary_keys=*/{},
            {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, file_format}}));
    return FormatTable::Create(file_system, path, Identifier("db", PathUtil::GetName(path)),
                               /*dynamic_options=*/{});
}

/// Builds a one-row batch of `MakeTimestampSchema(unit)` whose `ts` holds `value`, counted in
/// `unit`.
Result<std::unique_ptr<RecordBatch>> MakeTimestampBatch(arrow::TimeUnit::type unit, int64_t value) {
    std::shared_ptr<arrow::Schema> schema = MakeTimestampSchema(unit);
    arrow::Int32Builder id_builder;
    arrow::TimestampBuilder ts_builder(schema->field(1)->type(), arrow::default_memory_pool());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.Append(value));
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> ts_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.Finish(&ts_array));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> struct_array,
        arrow::StructArray::Make({id_array, ts_array}, schema->fields()));
    auto c_array = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
    RecordBatchBuilder builder(c_array.get());
    builder.SetPartition({});
    return builder.Finish();
}

/// One `ts` value a read returned, with the unit it came back in.
struct TimestampRow {
    arrow::TimeUnit::type unit;
    int64_t value;
};

/// One row read whole, with the reader that produced it: a batch borrows memory from its reader,
/// so a caller reads its values out before letting this go.
struct OneRowRead {
    std::unique_ptr<BatchReader> reader;
    std::shared_ptr<arrow::RecordBatch> batch;
};

/// Reads the one row `table` holds, checking that the reader runs out after it - otherwise the
/// row asserted on would be no more than the first of several.
Result<OneRowRead> ReadOneRow(const std::shared_ptr<FormatTable>& table) {
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableRead> read,
                           FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                                        /*pool=*/nullptr, /*predicate=*/nullptr,
                                                        /*enable_predicate_filter=*/false));
    OneRowRead result;
    PAIMON_ASSIGN_OR_RAISE(result.reader, read->CreateReader(plan->Splits()));
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, result.reader->NextBatch());
    if (BatchReader::IsEofBatch(batch)) {
        return Status::Invalid("the table holds no rows");
    }
    PAIMON_ASSIGN_OR_RAISE(result.batch, ImportBatch(batch));
    if (result.batch->num_rows() != 1) {
        return Status::Invalid("the table holds more than the one row that was written");
    }
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch next, result.reader->NextBatch());
    if (!BatchReader::IsEofBatch(next)) {
        return Status::Invalid("the table holds more than the one batch that was written");
    }
    return result;
}

/// The unit and value of a timestamp column a read returned. Both are the point: a value returned
/// under the wrong unit is off by a factor of a thousand and nothing else in the batch says so.
Result<TimestampRow> ToTimestampRow(const std::shared_ptr<arrow::Array>& column) {
    if (column == nullptr || column->type_id() != arrow::Type::TIMESTAMP) {
        return Status::Invalid("the read returned no timestamp column");
    }
    return TimestampRow{checked_pointer_cast<arrow::TimestampType>(column->type())->unit(),
                        checked_pointer_cast<arrow::TimestampArray>(column)->Value(0)};
}

/// The `ts` of the one row `table` holds.
Result<TimestampRow> ReadOneTimestamp(const std::shared_ptr<FormatTable>& table) {
    PAIMON_ASSIGN_OR_RAISE(OneRowRead read, ReadOneRow(table));
    PAIMON_ASSIGN_OR_RAISE(TimestampRow row, ToTimestampRow(read.batch->GetColumnByName("ts")));
    read.reader->Close();
    return row;
}

/// The `payload.ts` of the one row `table` holds, which reaches the type check through its
/// recursive half rather than at the top level.
Result<TimestampRow> ReadOneNestedTimestamp(const std::shared_ptr<FormatTable>& table) {
    PAIMON_ASSIGN_OR_RAISE(OneRowRead read, ReadOneRow(table));
    std::shared_ptr<arrow::Array> payload = read.batch->GetColumnByName("payload");
    if (payload == nullptr || payload->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid("the read returned no struct column named 'payload'");
    }
    PAIMON_ASSIGN_OR_RAISE(
        TimestampRow row,
        ToTimestampRow(checked_pointer_cast<arrow::StructArray>(payload)->field(0)));
    read.reader->Close();
    return row;
}

/// The nested counterpart of `MakeTimestampSchema()`: the `ts` sits inside a `payload` struct.
std::shared_ptr<arrow::Schema> MakeNestedTimestampSchema(arrow::TimeUnit::type unit) {
    return arrow::schema(
        {arrow::field("id", arrow::int32()),
         arrow::field("payload", arrow::struct_({arrow::field("ts", arrow::timestamp(unit))}))});
}

/// `CreateTimestampTable()` for `MakeNestedTimestampSchema()`.
Result<std::shared_ptr<FormatTable>> CreateNestedTimestampTable(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    const std::string& file_format, arrow::TimeUnit::type unit) {
    SchemaManager manager(file_system, path);
    PAIMON_ASSIGN_OR_RAISE(
        [[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
        manager.CreateTable(
            MakeNestedTimestampSchema(unit), /*partition_keys=*/{}, /*primary_keys=*/{},
            {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, file_format}}));
    return FormatTable::Create(file_system, path, Identifier("db", PathUtil::GetName(path)),
                               /*dynamic_options=*/{});
}

/// `MakeTimestampBatch()` for `MakeNestedTimestampSchema()`.
Result<std::unique_ptr<RecordBatch>> MakeNestedTimestampBatch(arrow::TimeUnit::type unit,
                                                              int64_t value) {
    std::shared_ptr<arrow::Schema> schema = MakeNestedTimestampSchema(unit);
    arrow::Int32Builder id_builder;
    arrow::TimestampBuilder ts_builder(arrow::timestamp(unit), arrow::default_memory_pool());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.Append(value));
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> ts_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(ts_builder.Finish(&ts_array));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> payload_array,
        arrow::StructArray::Make({ts_array}, schema->field(1)->type()->fields()));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::StructArray> struct_array,
        arrow::StructArray::Make({id_array, payload_array}, schema->fields()));
    auto c_array = std::make_unique<ArrowArray>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
    RecordBatchBuilder builder(c_array.get());
    builder.SetPartition({});
    return builder.Finish();
}

/// The one data file `table` holds, as its own plan names it.
Result<std::string> OneDataFilePath(const std::shared_ptr<FormatTable>& table) {
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());
    if (plan->Splits().size() != 1) {
        return Status::Invalid("the table holds more than the one split that was written");
    }
    std::shared_ptr<FormatDataSplit> split =
        std::dynamic_pointer_cast<FormatDataSplit>(plan->Splits()[0]);
    if (split == nullptr || split->files.size() != 1) {
        return Status::Invalid("the table holds more than the one file that was written");
    }
    return split->files[0].file_path;
}

}  // namespace

TEST(FormatTableTest, TestAParquetTimestampOfAnotherUnitIsRefusedButTheWritersOwnPairIsNot) {
    // A `SECOND` column survives parquet's round trip through `MILLI`, and a `us` file under an
    // `ms` table is refused rather than relabelled. `TimestampUnitIsCompatible()` says why.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> second_table,
                         CreateTimestampTable(dir->GetFileSystem(), dir->Str() + "/second",
                                              "parquet", arrow::TimeUnit::SECOND));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second_batch,
                         MakeTimestampBatch(arrow::TimeUnit::SECOND, 1700000000));
    ASSERT_OK(WriteAndCommit(second_table, std::move(second_batch)));
    ASSERT_OK_AND_ASSIGN(TimestampRow second_row, ReadOneTimestamp(second_table));
    ASSERT_EQ(second_row.unit, arrow::TimeUnit::SECOND);
    ASSERT_EQ(second_row.value, 1700000000);

    // The same instant written in `us`, whose file the `ms` table below is then handed.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> micro_table,
                         CreateTimestampTable(dir->GetFileSystem(), dir->Str() + "/micro",
                                              "parquet", arrow::TimeUnit::MICRO));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> micro_batch,
                         MakeTimestampBatch(arrow::TimeUnit::MICRO, 1700000000123456));
    ASSERT_OK(WriteAndCommit(micro_table, std::move(micro_batch)));
    ASSERT_OK_AND_ASSIGN(TimestampRow micro_row, ReadOneTimestamp(micro_table));
    ASSERT_EQ(micro_row.unit, arrow::TimeUnit::MICRO);
    ASSERT_EQ(micro_row.value, 1700000000123456);

    const std::string milli_path = dir->Str() + "/milli";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> milli_table,
        CreateTimestampTable(dir->GetFileSystem(), milli_path, "parquet", arrow::TimeUnit::MILLI));
    ASSERT_OK_AND_ASSIGN(std::string micro_file, OneDataFilePath(micro_table));
    std::error_code copied;
    std::filesystem::copy_file(
        micro_file, PathUtil::JoinPath(milli_path, PathUtil::GetName(micro_file)), copied);
    ASSERT_FALSE(copied) << copied.message();
    ASSERT_NOK_WITH_MSG(ReadOneTimestamp(milli_table), "stores the column 'ts' as");
}

TEST(FormatTableTest, TestANestedTimestampFollowsTheSameRuleAsATopLevelOne) {
    // The type check walks into a `STRUCT`, so the per-format unit rule has to hold at depth
    // too, and a refusal has to travel back up out of the recursion.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> orc_table,
                         CreateNestedTimestampTable(dir->GetFileSystem(), dir->Str() + "/orc",
                                                    "orc", arrow::TimeUnit::MILLI));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> orc_batch,
                         MakeNestedTimestampBatch(arrow::TimeUnit::MILLI, 1700000000123));
    ASSERT_OK(WriteAndCommit(orc_table, std::move(orc_batch)));
    ASSERT_OK_AND_ASSIGN(TimestampRow orc_row, ReadOneNestedTimestamp(orc_table));
    ASSERT_EQ(orc_row.unit, arrow::TimeUnit::MILLI);
    ASSERT_EQ(orc_row.value, 1700000000123);

    // parquet keeps the unit it wrote, so a nested `us` under a nested `ms` column is refused as
    // a top-level one is - the recursion carries the refusal back up.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> micro_table,
                         CreateNestedTimestampTable(dir->GetFileSystem(), dir->Str() + "/micro",
                                                    "parquet", arrow::TimeUnit::MICRO));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> micro_batch,
                         MakeNestedTimestampBatch(arrow::TimeUnit::MICRO, 1700000000123456));
    ASSERT_OK(WriteAndCommit(micro_table, std::move(micro_batch)));
    ASSERT_OK_AND_ASSIGN(TimestampRow micro_row, ReadOneNestedTimestamp(micro_table));
    ASSERT_EQ(micro_row.unit, arrow::TimeUnit::MICRO);
    ASSERT_EQ(micro_row.value, 1700000000123456);

    const std::string milli_path = dir->Str() + "/milli";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> milli_table,
                         CreateNestedTimestampTable(dir->GetFileSystem(), milli_path, "parquet",
                                                    arrow::TimeUnit::MILLI));
    ASSERT_OK_AND_ASSIGN(std::string micro_file, OneDataFilePath(micro_table));
    std::error_code copied;
    std::filesystem::copy_file(
        micro_file, PathUtil::JoinPath(milli_path, PathUtil::GetName(micro_file)), copied);
    ASSERT_FALSE(copied) << copied.message();
    ASSERT_NOK_WITH_MSG(ReadOneNestedTimestamp(milli_table), "stores the column 'payload' as");
}

TEST(FormatTableTest, TestAnOrcTimestampIsReadAtTheUnitTheTableDeclares) {
    // Every unit survives orc's round trip, and a file another unit wrote is read at the table's
    // rather than refused. Applying parquet's rule here would refuse every orc `TIMESTAMP(0)`,
    // `(3)` and `(6)`, this library's own writes among them; `TimestampUnitIsCompatible()` says
    // why the two formats differ.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    const std::vector<std::tuple<const char*, arrow::TimeUnit::type, int64_t>> instants = {
        {"second", arrow::TimeUnit::SECOND, 1700000000},
        {"milli", arrow::TimeUnit::MILLI, 1700000000123},
        {"micro", arrow::TimeUnit::MICRO, 1700000000123456},
        {"nano", arrow::TimeUnit::NANO, 1700000000123456789}};
    for (const auto& [name, unit, value] : instants) {
        SCOPED_TRACE(name);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> table,
            CreateTimestampTable(dir->GetFileSystem(), dir->Str() + "/" + name, "orc", unit));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, MakeTimestampBatch(unit, value));
        ASSERT_OK(WriteAndCommit(table, std::move(batch)));
        ASSERT_OK_AND_ASSIGN(TimestampRow row, ReadOneTimestamp(table));
        ASSERT_EQ(row.unit, unit);
        ASSERT_EQ(row.value, value);
    }

    // A file another unit wrote is read at the table's, rather than refused as parquet's is: the
    // seconds and nanoseconds orc stored are rescaled, not relabelled, so the instant survives.
    const std::string milli_path = dir->Str() + "/read_as_milli";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> milli_table,
        CreateTimestampTable(dir->GetFileSystem(), milli_path, "orc", arrow::TimeUnit::MILLI));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> micro_table,
        CreateTimestampTable(dir->GetFileSystem(), dir->Str() + "/written_as_micro", "orc",
                             arrow::TimeUnit::MICRO));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> micro_batch,
                         MakeTimestampBatch(arrow::TimeUnit::MICRO, 1700000000123456));
    ASSERT_OK(WriteAndCommit(micro_table, std::move(micro_batch)));
    ASSERT_OK_AND_ASSIGN(std::string micro_file, OneDataFilePath(micro_table));
    std::error_code copied;
    std::filesystem::copy_file(
        micro_file, PathUtil::JoinPath(milli_path, PathUtil::GetName(micro_file)), copied);
    ASSERT_FALSE(copied) << copied.message();
    ASSERT_OK_AND_ASSIGN(TimestampRow milli_row, ReadOneTimestamp(milli_table));
    ASSERT_EQ(milli_row.unit, arrow::TimeUnit::MILLI);
    ASSERT_EQ(milli_row.value, 1700000000123);
}

TEST(FormatTableTest, TestPartitionsAreListedInAStableOrder) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    // Written out of order, because the listing order must not depend on it: the file system
    // promises none, and a caller comparing two listings would see partitions move.
    for (const char* dt : {"20240103", "20240101", "20240102"}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({1}, {"alice"}, dt, {{"dt", dt}}));
        ASSERT_OK(WriteAndCommit(table, std::move(batch)));
    }

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions.size(), 3u);
    ASSERT_EQ(partitions[0].at("dt"), "20240101");
    ASSERT_EQ(partitions[1].at("dt"), "20240102");
    ASSERT_EQ(partitions[2].at("dt"), "20240103");
}

TEST(FormatTableTest, TestPartitionFilter) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second,
                         MakeBatch({2}, {"bob"}, "20240102", {{"dt", "20240102"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(second)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, {{"dt", "20240102"}}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 1);
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 1);
    ASSERT_EQ(rows[0], "2|bob|20240102");
}

TEST(FormatTableTest, TestPartitionFilterMatchesThroughTheColumnType) {
    // A partition filter names a value, not the text a directory happens to be named with. Java
    // tests a `PartitionPredicate` against the `BinaryRow` a directory parses into and reaches the
    // same answer; comparing the raw strings would make `1` miss a directory called `pt=01`.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<arrow::Schema> int_schema =
        arrow::schema({arrow::field("name", arrow::utf8()), arrow::field("pt", arrow::int32())});
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
                         schema_manager.CreateTable(
                             int_schema, /*partition_keys=*/{"pt"}, /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         FormatTable::Create(dir->GetFileSystem(), dir->Str(),
                                             Identifier("db", "tbl"), /*dynamic_options=*/{}));

    // Written by some other engine, which spelled the value with a leading zero.
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/pt=01"));
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/pt=2"));

    for (const char* spelling : {"1", "01"}) {
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, {{"pt", spelling}}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
        ASSERT_EQ(partitions.size(), 1u) << spelling;
        // The directory's own value, not the normalised one: a format table's partitions are its
        // directories, so a plan reports what is on disk.
        ASSERT_EQ(partitions[0].at("pt"), "01") << spelling;
    }

    // And a value no `INT` column can hold is the caller's mistake, not an empty result.
    ASSERT_NOK_WITH_MSG(FormatTableScan::Create(table, {{"pt", "abc"}}, /*limit=*/std::nullopt),
                        "cannot be read into the partition columns");
}

TEST(FormatTableTest, TestPartitionFilterCanNameTheNullPartition) {
    // A blank partition value stands for null and lands in the `partition.default-name`
    // directory. Both sides of the filter are read into the column type, where that name is null,
    // so naming it selects that partition and nothing else.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> blank,
                         MakeBatch({1}, {"alice"}, "   ", {{"dt", table->PartitionDefaultName()}}));
    ASSERT_OK(WriteAndCommit(table, std::move(blank)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> dated,
                         MakeBatch({2}, {"bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(dated)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableScan> scan,
                         FormatTableScan::Create(table, {{"dt", table->PartitionDefaultName()}},
                                                 /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions, (PartitionList{{{"dt", table->PartitionDefaultName()}}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 1u);
}

TEST(FormatTableTest, TestABlankPartitionDirectoryIsNotTheNullPartition) {
    // A write of a blank value lands in the null partition's directory, but a directory that holds
    // a blank value is still not the null partition: the two are told apart by what the column
    // holds, not by the directory name a write would have chosen for it, as Java tests its
    // `PartitionPredicate` against the parsed value. Rendering both sides back out to a directory
    // name would fold the two together, so `partition.default-name` would select a blank directory
    // too and hand out a blank value the filter never named.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    // Written by some other engine. A tab is escaped in a path, so the directory has a legal name
    // while its value is blank.
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/dt=%09"));
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/dt=" + table->PartitionDefaultName()));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableScan> null_scan,
                         FormatTableScan::Create(table, {{"dt", table->PartitionDefaultName()}},
                                                 /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList null_partitions, null_scan->ListPartitions());
    ASSERT_EQ(null_partitions, (PartitionList{{{"dt", table->PartitionDefaultName()}}}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableScan> blank_scan,
                         FormatTableScan::Create(table, {{"dt", "\t"}}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList blank_partitions, blank_scan->ListPartitions());
    ASSERT_EQ(blank_partitions, (PartitionList{{{"dt", "\t"}}}));
}

TEST(FormatTableTest, TestScanRejectsUnknownPartitionField) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    Result<std::unique_ptr<FormatTableScan>> scan =
        FormatTableScan::Create(table, {{"name", "alice"}}, /*limit=*/std::nullopt);
    ASSERT_FALSE(scan.ok());
    ASSERT_TRUE(scan.status().IsInvalid());
}

TEST(FormatTableTest, TestTheGenericEntryPointsReachAFormatTable) {
    // A caller holding a table path uses the interfaces it uses for every other table. Each one
    // recognises a format table from its schema and dispatches to it, the way Java Paimon serves
    // both kinds through one `ReadBuilder` and one `BatchWriteBuilder`.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));

    WriteContextBuilder write_builder(dir->Str(), "test-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(std::move(write_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1u);
    // Compaction is about manifests and buckets, so it says what it cannot do rather than
    // reporting success for work it never did.
    ASSERT_NOK_WITH_MSG(write->Compact({{"dt", "20240101"}}, /*bucket=*/0,
                                       /*full_compaction=*/false),
                        "cannot be compacted");

    // A write id prefixes a postpone-bucket writer's files; a format table has no buckets.
    WriteContextBuilder write_id_builder(dir->Str(), "test-user");
    write_id_builder.WithWriteId(3);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_id_context, write_id_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(std::move(write_id_context)),
                        "a write id would name nothing");

    // The three `CommitContext` settings that describe snapshot machinery. Each is refused only
    // when it is set away from its default, so an ordinary commit is unaffected.
    CommitContextBuilder empty_commit_builder(dir->Str(), "test-user");
    empty_commit_builder.IgnoreEmptyCommit(false);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> empty_commit_context,
                         empty_commit_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreCommit::Create(std::move(empty_commit_context)),
                        "cannot record an empty commit");

    CommitContextBuilder rest_builder(dir->Str(), "test-user");
    rest_builder.UseRESTCatalogCommit(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> rest_context, rest_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreCommit::Create(std::move(rest_context)), "rest catalog");

    CommitContextBuilder conflict_builder(dir->Str(), "test-user");
    conflict_builder.AppendCommitCheckConflict(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> conflict_context,
                         conflict_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreCommit::Create(std::move(conflict_context)),
                        "no manifests to check a concurrent commit against");

    CommitContextBuilder commit_builder(dir->Str(), "test-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));
    // The same for the snapshot half of the commit interface.
    ASSERT_NOK_WITH_MSG(commit->Expire(), "no snapshots to expire");
    // Closing after a prepared commit must not take back what the commit just published.
    ASSERT_OK(write->Close());

    ScanContextBuilder scan_builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 1u);

    ReadContextBuilder read_builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch read_batch, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(read_batch));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, ImportBatch(read_batch));
    // `_VALUE_KIND` first, then the table's own columns, exactly as the narrower interface gives
    // them.
    ASSERT_EQ(record_batch->num_columns(), 4);
    ASSERT_EQ(record_batch->num_rows(), 2);
    ASSERT_EQ(record_batch->schema()->field(0)->name(), SpecialFields::ValueKind().Name());
    reader->Close();
}

TEST(FormatTableTest, TestTheGenericEntryPointsTakeALoadedFormatTable) {
    // A table a catalog keeps the schema for, the way a rest catalog does. Nothing under its
    // location says that it is a format table, nor that everything below the location is data, so
    // a path is not enough to reach it: the loaded table is handed to the context instead, which
    // is what `FormatTable.newReadBuilder()` and `newBatchWriteBuilder()` do in Java.
    std::unique_ptr<UniqueTestDirectory> schema_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(schema_dir);
    SchemaManager schema_manager(schema_dir->GetFileSystem(), schema_dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> table_schema,
                         schema_manager.CreateTable(
                             MakeSchema(), /*partition_keys=*/{"dt"}, /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));

    std::unique_ptr<UniqueTestDirectory> data_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(data_dir);
    std::shared_ptr<DataSchema> data_schema =
        checked_pointer_cast<DataSchema>(std::shared_ptr<TableSchema>(std::move(table_schema)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> external,
        FormatTable::Create(data_dir->GetFileSystem(), data_dir->Str(), Identifier("db", "tbl"),
                            data_schema,
                            /*location_carries_paimon_metadata=*/false, /*dynamic_options=*/{}));

    // The same location by path alone is not a table at all: there is no schema under it to say
    // what kind of table it is.
    ScanContextBuilder path_builder(data_dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> path_context, path_builder.Finish());
    ASSERT_NOK(TableScan::Create(std::move(path_context)));

    // Options given at the call still win over the ones the schema stored, so a file is rolled
    // per row rather than one file holding all three.
    WriteContextBuilder write_builder(external);
    write_builder.SetOptions({{Options::TARGET_FILE_ROW_NUM, "1"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(std::move(write_context)));
    for (int32_t i = 1; i <= 3; i++) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({i}, {"name"}, "20240101", {{"dt", "20240101"}}));
        ASSERT_OK(write->Write(std::move(batch)));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    ASSERT_EQ(messages.size(), 3u) << "target-file-row-num given at the call was ignored";

    CommitContextBuilder commit_builder(external);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK(write->Close());

    ScanContextBuilder scan_builder(external);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions, (PartitionList{{{"dt", "20240101"}}}));

    ReadContextBuilder read_builder(external);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    int64_t rows = 0;
    while (true) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch read_batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(read_batch)) {
            break;
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch,
                             ImportBatch(read_batch));
        rows += record_batch->num_rows();
    }
    reader->Close();
    ASSERT_EQ(rows, 3);
}

TEST(FormatTableTest, TestAContextBuiltFromAFormatTableRefusesASecondAnswer) {
    // The table already carries its schema and the file system it was loaded through, from a
    // source the context cannot see behind, so a second answer is refused rather than dropped.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest,
                         schema_manager.Latest());
    ASSERT_TRUE(latest.has_value());
    ASSERT_OK_AND_ASSIGN(std::string schema_json, (*latest)->GetJsonSchema());

    ScanContextBuilder schema_builder(table);
    schema_builder.SetTableSchema(schema_json);
    ASSERT_NOK_WITH_MSG(schema_builder.Finish(), "carries its own schema");

    ScanContextBuilder fs_builder(table);
    fs_builder.WithFileSystem(dir->GetFileSystem());
    ASSERT_NOK_WITH_MSG(fs_builder.Finish(), "carries the file system");

    ReadContextBuilder branch_builder(table);
    branch_builder.WithBranch("other");
    ASSERT_NOK_WITH_MSG(branch_builder.Finish(), "has no branches");

    WriteContextBuilder write_fs_builder(table);
    write_fs_builder.WithFileSystem(dir->GetFileSystem());
    ASSERT_NOK_WITH_MSG(write_fs_builder.Finish(), "carries the file system");

    CommitContextBuilder commit_fs_builder(table);
    commit_fs_builder.WithFileSystem(dir->GetFileSystem());
    ASSERT_NOK_WITH_MSG(commit_fs_builder.Finish(), "carries the file system");

    // A null table is the caller's mistake, and says so rather than complaining about a path it
    // was never given.
    ScanContextBuilder null_builder{std::shared_ptr<FormatTable>()};
    ASSERT_NOK_WITH_MSG(null_builder.Finish(), "null format table");
}

TEST(FormatTableTest, TestABranchNamedThroughOptionsIsRefusedAtEveryEntryPoint) {
    // `WithBranch()` is refused on a context built from a format table, and the `branch` option
    // says the same thing by another name: a format table keeps no metadata to branch, and a read
    // or a write would go to its one location whatever the option named. Refusing it is the only
    // answer that is not silently the wrong one - an overwrite would delete the main branch's
    // data while claiming to replace another branch's.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    const std::map<std::string, std::string> branch = {{Options::BRANCH, "dev"}};
    ASSERT_NOK_WITH_MSG(FormatTable::Copy(table, branch), "has no branches");
    ASSERT_NOK_WITH_MSG(
        FormatTable::Create(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl"), branch),
        "has no branches");

    ScanContextBuilder scan_builder(table);
    scan_builder.SetOptions(branch);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), "has no branches");

    ReadContextBuilder read_builder(table);
    read_builder.SetOptions(branch);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)), "has no branches");

    WriteContextBuilder write_builder(table);
    write_builder.SetOptions(branch);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreWrite::Create(std::move(write_context)), "has no branches");

    // The commit is the one that matters most: an overwrite clears what it replaces before it
    // publishes anything, so the refusal has to come before the deleting does.
    CommitContextBuilder commit_builder(table);
    commit_builder.SetOptions(branch);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_NOK_WITH_MSG(FileStoreCommit::Create(std::move(commit_context)), "has no branches");

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101"}));

    // The branch that names no branch at all is the main one, and is left alone: refusing it
    // would refuse the default every context carries.
    for (const char* main_branch : {"main", ""}) {
        ASSERT_OK(FormatTable::Copy(table, {{Options::BRANCH, main_branch}})) << main_branch;
    }
}

TEST(FormatTableTest, TestAFormatTableSchemaOnABranchIsRefusedRatherThanServedFromTheTableRoot) {
    // A context that names a path takes its branch from `WithBranch()`, and the schema of that
    // branch is what the entry point dispatches on. A format table's data is the files under the
    // table path whichever branch named it, so serving one from there under another branch's name
    // would answer a question about `dev` with the rows of `main`.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    // The same table on a branch of its own, which only a schema under `branch` makes reachable.
    SchemaManager branch_schema_manager(dir->GetFileSystem(), dir->Str(), "dev");
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> branch_schema,
                         branch_schema_manager.CreateTable(
                             MakeSchema(), /*partition_keys=*/{"dt"}, /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));

    ReadContextBuilder read_builder(dir->Str());
    read_builder.WithBranch("dev");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)), "has no branches");

    // A scan takes its branch from the option rather than from a `WithBranch()` of its own, which
    // is the same branch by another name and gets the same answer.
    ScanContextBuilder scan_builder(dir->Str());
    scan_builder.SetOptions({{Options::BRANCH, "dev"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(scan_context)), "has no branches");

    // The main branch is untouched by any of it.
    ScanContextBuilder main_builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> main_context, main_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> main_scan,
                         TableScan::Create(std::move(main_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, main_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101"}));
}

TEST(FormatTableTest, TestTheGenericEntryPointsCarryTheContextThrough) {
    // A context promises that options given at the call win over the ones the schema stored, and
    // that a branch and a caller-held schema are honoured. Dispatching to a format table must not
    // quietly drop any of that.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));

    // `target-file-row-num` is stored nowhere in this table's schema, so a file would hold every
    // row. Given at the call it has to roll a file per row, as it does for a managed table.
    WriteContextBuilder write_builder(dir->Str(), "test-user");
    write_builder.SetOptions({{Options::TARGET_FILE_ROW_NUM, "1"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(std::move(write_context)));
    for (int32_t i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({i}, {"name"}, "20240101", {}));
        ASSERT_OK(write->Write(std::move(batch)));
    }
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    ASSERT_EQ(messages.size(), 3u) << "target-file-row-num given at the call was ignored";

    // Published through the generic commit, or nothing below would see the rows: until the commit
    // renames them the files sit in the hidden `_temporary` directory a scan skips.
    CommitContextBuilder generic_commit_builder(dir->Str(), "test-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> generic_commit_context,
                         generic_commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> generic_commit,
                         FileStoreCommit::Create(std::move(generic_commit_context)));
    ASSERT_OK(generic_commit->Commit(messages));
    ASSERT_OK(write->Close());

    // A branch this table has no schema on is a managed table as far as dispatch is concerned, so
    // the format branch must not answer for it.
    ReadContextBuilder branch_builder(dir->Str());
    branch_builder.WithBranch("nosuchbranch");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> branch_context, branch_builder.Finish());
    ASSERT_NOK(TableRead::Create(std::move(branch_context)));

    // A caller-held schema is used instead of reading one from under the path.
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest,
                         schema_manager.Latest());
    ASSERT_TRUE(latest.has_value());
    ASSERT_OK_AND_ASSIGN(std::string schema_json, (*latest)->GetJsonSchema());
    ScanContextBuilder seeded_builder(dir->Str());
    seeded_builder.SetTableSchema(schema_json);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> seeded_context, seeded_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> seeded_scan,
                         TableScan::Create(std::move(seeded_context)));
    // Planning, not just dispatch: handing the schema over must not turn the `schema` directory
    // the table keeps under its own path into data, which a plan would then read as a data file.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> seeded_plan, seeded_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> seeded_rows,
                         ReadAll(table, seeded_plan->Splits()));
    ASSERT_EQ(seeded_rows.size(), 3u);

    // `type` is structural, so an option given at the call must not decide what kind of table
    // this is. Checked by reading the rows back rather than by counting splits: three small files
    // are packed into one split, so a split count says nothing about how many there are.
    ScanContextBuilder retyped_builder(dir->Str());
    retyped_builder.SetOptions({{Options::TYPE, "table"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> retyped_context, retyped_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> retyped_scan,
                         TableScan::Create(std::move(retyped_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> retyped_plan, retyped_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> retyped_rows,
                         ReadAll(table, retyped_plan->Splits()));
    ASSERT_EQ(retyped_rows.size(), 3u);
}

namespace {

/// Counts the lookups a read makes, which is enough to tell that the cache a context carries
/// reached the format reader rather than being dropped on the way; parquet looks its footer up
/// here.
class CountingCache : public Cache {
 public:
    Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) override {
        gets++;
        return supplier(key);
    }
    Status Put(const std::shared_ptr<CacheKey>&, const std::shared_ptr<CacheValue>&) override {
        return Status::OK();
    }
    void Invalidate(const std::shared_ptr<CacheKey>&) override {}
    void InvalidateAll() override {}
    size_t Size() const override {
        return 0;
    }

    int32_t gets = 0;
};

}  // namespace

TEST(FormatTableTest, TestTheGenericReadCarriesPrefetchAndTheCacheThrough) {
    // A format table opens its files through the same component the managed table path opens its
    // own with, so what a `ReadContext` asks for about opening a file applies here too.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        MakeBatch({1, 2, 3}, {"alice", "bob", "carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ScanContextBuilder scan_builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());

    auto read_rows = [&plan](std::unique_ptr<ReadContext> context) -> Result<int64_t> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> read,
                               TableRead::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               read->CreateReader(plan->Splits()));
        int64_t rows = 0;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch read_batch, reader->NextBatch());
            if (BatchReader::IsEofBatch(read_batch)) {
                break;
            }
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::RecordBatch> record_batch,
                arrow::ImportRecordBatch(read_batch.first.get(), read_batch.second.get()));
            rows += record_batch->num_rows();
        }
        reader->Close();
        return rows;
    };

    // Prefetch is honoured rather than refused, and reads back what was written.
    ReadContextBuilder prefetch_builder(dir->Str());
    prefetch_builder.EnablePrefetch(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> prefetch_context, prefetch_builder.Finish());
    ASSERT_OK_AND_ASSIGN(int64_t prefetched_rows, read_rows(std::move(prefetch_context)));
    ASSERT_EQ(prefetched_rows, 3);

    // And the cache the context carries reaches the format reader.
    auto cache = std::make_shared<CountingCache>();
    ReadContextBuilder cache_builder(dir->Str());
    cache_builder.WithCache(cache);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> cache_context, cache_builder.Finish());
    ASSERT_OK_AND_ASSIGN(int64_t cached_rows, read_rows(std::move(cache_context)));
    ASSERT_EQ(cached_rows, 3);
    ASSERT_GT(cache->gets, 0) << "the cache the read context carries never reached the file reader";
}

TEST(FormatTableTest, TestTheGenericEntryPointsRefuseWhatTheyCannotHonour) {
    // Anything a context carries that a format table cannot do is a refusal naming the setting,
    // never a read or a write that quietly did something else.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));

    // A projected read schema can rename a column, prune a nested one and give it metadata of its
    // own; a format table's projection is a list of top-level names.
    ReadContextBuilder read_schema_builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<::ArrowSchema> projected_schema, table->GetArrowSchema());
    read_schema_builder.SetReadSchema(std::move(projected_schema));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_schema_context,
                         read_schema_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_schema_context)),
                        "does not take a projected read schema");

    ScanContextBuilder streaming_builder(dir->Str());
    streaming_builder.WithStreamingMode(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> streaming_context,
                         streaming_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(streaming_context)),
                        "nothing for a streaming scan to follow");

    ScanContextBuilder predicate_builder(dir->Str());
    predicate_builder.SetPredicate(PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(1)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> predicate_context,
                         predicate_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableScan::Create(std::move(predicate_context)),
                        "does not take a predicate");

    CommitContextBuilder commit_builder(dir->Str(), "test-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    // A commit takes batch writes only. The rest of the snapshot half is
    // `TestTheGenericCommitRefusesEveryCallAboutSnapshots`.
    ASSERT_NOK_WITH_MSG(commit->Commit({}, /*commit_identifier=*/7), "commit identifier");
    ASSERT_NOK_WITH_MSG(commit->Commit({}, BATCH_WRITE_COMMIT_IDENTIFIER, /*watermark=*/1),
                        "watermark");

    // A commit message describing manifest files is not published just because the interface
    // takes the base type. `CommitMessage` has no public subclass a test can build, so this
    // stands in for one: anything that is not a `FormatCommitMessage` is refused.
    class NotAFormatMessage : public CommitMessage {};
    ASSERT_NOK_WITH_MSG(commit->Commit({std::make_shared<NotAFormatMessage>()}),
                        "describes files to record in a manifest");
}

TEST(FormatTableTest, TestTheGenericCommitRefusesEveryCallAboutSnapshots) {
    // Each of these describes snapshot or manifest state a format table does not keep, and each
    // says so rather than reporting success for work it never did. A caller moving between table
    // types then finds out at the call rather than from a table that did not change.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    CommitContextBuilder commit_builder(dir->Str(), "test-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));

    ASSERT_NOK_WITH_MSG(commit->CommitWithProgress({}, BATCH_WRITE_COMMIT_IDENTIFIER,
                                                   /*watermark=*/std::nullopt),
                        "real-time offsets");
    ASSERT_NOK_WITH_MSG(commit->FilterAndCommit({}), "which commit identifiers");
    ASSERT_NOK_WITH_MSG(
        commit->FilterAndOverwrite({{"dt", "20240101"}}, {}, BATCH_WRITE_COMMIT_IDENTIFIER),
        "which commit identifiers");
    ASSERT_NOK_WITH_MSG(commit->GetLastCommitTableRequest(), "rest catalog");
    ASSERT_NOK_WITH_MSG(commit->Expire(), "no snapshots to expire");
    ASSERT_NOK_WITH_MSG(commit->RollbackToAsLatest(/*target_snapshot_id=*/1), "roll back to");
    ASSERT_NOK_WITH_MSG(
        commit->DropPartition({{{"dt", "20240101"}}}, BATCH_WRITE_COMMIT_IDENTIFIER),
        "dropping a partition");
    ASSERT_NOK_WITH_MSG(commit->TruncateTable(BATCH_WRITE_COMMIT_IDENTIFIER),
                        "emptying a format table");

    // The one call that cannot refuse, since it returns a reference: it has to be a no-op that
    // hands back the same commit. A format table records no row ids, so there is no conflict.
    ASSERT_EQ(&commit->RowIdCheckConflict(/*row_id_check_from_snapshot=*/std::nullopt),
              commit.get());
    // Empty rather than null, so a caller merging metrics need not tell a table type that keeps
    // none apart from one that does.
    ASSERT_NE(commit->GetCommitMetrics(), nullptr);
}

TEST(FormatTableTest, TestTheGenericCommitOverwritesOnePartition) {
    // `FileStoreCommit::Overwrite()` names the partition to replace, which is the generic form of
    // a static-partition overwrite: it clears that directory and leaves the others alone.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other,
                         MakeBatch({9}, {"zoe"}, "20240102", {{"dt", "20240102"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(other)));

    WriteContextBuilder write_builder(dir->Str(), "test-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(std::move(write_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replacement,
                         MakeBatch({3}, {"carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(write->Write(std::move(replacement)));
    // Empty rather than null on the write side too; what this write produced is on its messages.
    ASSERT_NE(write->GetMetrics(), nullptr);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1u);

    CommitContextBuilder commit_builder(dir->Str(), "test-user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    // Batch writes only, and refused before anything is cleared: an overwrite that failed on its
    // arguments must not have deleted the partition it named.
    ASSERT_NOK_WITH_MSG(commit->Overwrite({{"dt", "20240101"}}, messages, /*commit_identifier=*/7),
                        "commit identifier");
    ASSERT_NOK_WITH_MSG(commit->Overwrite({{"dt", "20240101"}}, messages,
                                          BATCH_WRITE_COMMIT_IDENTIFIER, /*watermark=*/1),
                        "watermark");
    ASSERT_OK(commit->Overwrite({{"dt", "20240101"}}, messages, BATCH_WRITE_COMMIT_IDENTIFIER));
    ASSERT_OK(write->Close());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    std::sort(rows.begin(), rows.end());
    ASSERT_EQ(rows, (std::vector<std::string>{"3|carol|20240101", "9|zoe|20240102"}));
}

TEST(FormatTableTest, TestReadsCarryTheValueKindField) {
    // `BatchReader::NextBatch()` promises a leading `_VALUE_KIND` field, and engines read by field
    // index, so dropping it would shift every column by one. A format table records no row kind,
    // so the field is there and every row is an insert.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());

    auto check = [](const std::shared_ptr<arrow::RecordBatch>& record_batch) {
        // First, and ahead of the table's own columns.
        ASSERT_EQ(record_batch->num_columns(), 4);
        ASSERT_EQ(record_batch->schema()->field(0)->name(), SpecialFields::ValueKind().Name());
        ASSERT_EQ(record_batch->schema()->field(1)->name(), "id");
        auto row_kinds = checked_pointer_cast<arrow::Int8Array>(record_batch->column(0));
        ASSERT_EQ(row_kinds->null_count(), 0);
        for (int64_t i = 0; i < record_batch->num_rows(); i++) {
            ASSERT_EQ(row_kinds->Value(i), RowKind::Insert()->ToByteValue());
        }
    };

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch_read, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch_read));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, ImportBatch(batch_read));
    check(record_batch);
    reader->Close();

    // The same promise holds on the bitmap path, which is the one a caller reaches for when
    // deletion vectors or indexes are in play.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> bitmap_reader,
                         read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                         bitmap_reader->NextBatchWithBitmap());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch_with_bitmap));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> bitmap_batch,
                         ImportBatch(batch_with_bitmap.first));
    check(bitmap_batch);
    bitmap_reader->Close();
}

TEST(FormatTableTest, TestProjectionReordersAndDropsColumns) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        MakeBatch({1, 2, 3}, {"alice", "bob", "carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());

    // A projection may name the partition column and reorder the rest.
    std::vector<std::string> projection = {"dt", "name"};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, projection, /*pool=*/nullptr, /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch_read, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch_read));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, ImportBatch(batch_read));
    // The projected columns, behind the `_VALUE_KIND` field every `BatchReader` puts first.
    ASSERT_EQ(record_batch->num_columns(), 3);
    ASSERT_EQ(record_batch->num_rows(), 3);
    ASSERT_EQ(record_batch->schema()->field(0)->name(), SpecialFields::ValueKind().Name());
    ASSERT_EQ(record_batch->schema()->field(1)->name(), "dt");
    ASSERT_EQ(record_batch->schema()->field(2)->name(), "name");
    ASSERT_EQ(checked_pointer_cast<arrow::StringArray>(record_batch->column(1))->GetString(0),
              "20240101");
    ASSERT_EQ(checked_pointer_cast<arrow::StringArray>(record_batch->column(2))->GetString(1),
              "bob");

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof));
    reader->Close();
}

TEST(FormatTableTest, TestProjectionOfPartitionColumnOnly) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());

    // The partition value is constant, so only the file can say how many rows to repeat it for.
    std::vector<std::string> projection = {"dt"};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, projection, /*pool=*/nullptr, /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch_read, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch_read));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, ImportBatch(batch_read));
    ASSERT_EQ(record_batch->num_columns(), 2);
    ASSERT_EQ(record_batch->num_rows(), 2);
    ASSERT_EQ(record_batch->schema()->field(0)->name(), SpecialFields::ValueKind().Name());
    ASSERT_EQ(record_batch->schema()->field(1)->name(), "dt");
    auto dts = checked_pointer_cast<arrow::StringArray>(record_batch->column(1));
    ASSERT_EQ(dts->GetString(0), "20240101");
    ASSERT_EQ(dts->GetString(1), "20240101");
    reader->Close();
}

TEST(FormatTableTest, TestNullPartitionValue) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    // A null partition value is carried as the default partition name in the batch's partition
    // spec and in the directory it names, while the column itself holds a real null.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatchWithNullPartition(table->PartitionDefaultName()));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 1);

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch_read, reader->NextBatch());
    ASSERT_FALSE(BatchReader::IsEofBatch(batch_read));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::RecordBatch> record_batch, ImportBatch(batch_read));
    ASSERT_EQ(record_batch->num_rows(), 1);
    // The partition column reads back as null, not as the placeholder directory name.
    ASSERT_TRUE(record_batch->column(3)->IsNull(0));
    reader->Close();
}

TEST(FormatTableTest, TestUncommittedFilesAreInvisible) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1);

    // The data is on disk under a hidden name, and a scan does not see it until it is committed.
    ASSERT_OK_AND_ASSIGN(bool temp_exists,
                         dir->GetFileSystem()->Exists(messages[0].temp_file_path));
    ASSERT_TRUE(temp_exists);
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_TRUE(plan->Splits().empty());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> after_commit,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> committed_plan, after_commit->CreatePlan());
    ASSERT_EQ(committed_plan->Splits().size(), 1);
}

TEST(FormatTableTest, TestWriteAbortStillCleansUpAfterPrepareCommit) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1u);

    // A commit that is prepared and then abandoned still has to be cleaned up through the write
    // that staged it. Handing the messages out must not leave the write with nothing to remove
    // while it goes on reporting success.
    ASSERT_OK(write->Abort());
    ASSERT_OK_AND_ASSIGN(bool temp_exists,
                         dir->GetFileSystem()->Exists(messages[0].temp_file_path));
    ASSERT_FALSE(temp_exists);
}

TEST(FormatTableTest, TestAFinishedWriteSaysWhichWayItFinished) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));

    // A prepared write and an aborted one both take no more rows, but the caller has different
    // work to do about each (commit the messages it holds, or start over), so the refusal says
    // which one happened.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> prepared,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK(prepared->PrepareCommit().status());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_NOK_WITH_MSG(prepared->Write(std::move(batch)), "already prepared its commit");
    ASSERT_NOK_WITH_MSG(prepared->PrepareCommit(), "already prepared its commit");

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> aborted,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK(aborted->Abort());
    ASSERT_OK_AND_ASSIGN(batch, MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_NOK_WITH_MSG(aborted->Write(std::move(batch)), "has been aborted");
    ASSERT_NOK_WITH_MSG(aborted->PrepareCommit(), "has been aborted");
    // Aborting is the one call that still works, so a caller cleaning up need not track whether
    // it has already done so.
    ASSERT_OK(aborted->Abort());
}

TEST(FormatTableTest, TestCommitRefusesAMessageFromElsewhere) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));

    // Committing a message renames one path and an overwrite clears the directory around it, and
    // nothing downstream re-checks those paths, so a message that does not describe a file of this
    // table has to be refused here.
    FormatCommitMessage outside(StagedPath("/somewhere/else/data-a-0.parquet"),
                                "/somewhere/else/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({outside}), "not under the table location");
    // `Abort()` is best effort and never fails: it refuses the message, says so in the log and
    // carries on, so that one bad message cannot strand the staged files of the good ones.
    ASSERT_OK(commit->Abort({outside}));

    FormatCommitMessage across_directories(StagedPath(dir->Str() + "/a/data-a-0.parquet"),
                                           dir->Str() + "/b/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({across_directories}), "does not stage its file under");

    FormatCommitMessage not_staged(dir->Str() + "/data-a-0.parquet",
                                   dir->Str() + "/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({not_staged}), "does not stage its file under");

    // A hidden name beside the target is how an earlier paimon-cpp staged its files, before the
    // `_temporary` directory Java Paimon uses. It is still hidden, so nothing about the path says
    // it is wrong; only this check does.
    FormatCommitMessage beside_the_target(dir->Str() + "/.data-a-0.parquet.tmp",
                                          dir->Str() + "/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({beside_the_target}), "does not stage its file under");

    FormatCommitMessage negative(StagedPath(dir->Str() + "/data-a-0.parquet"),
                                 dir->Str() + "/data-a-0.parquet", {}, -1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({negative}), "negative row count");

    // A prefix test alone would let this through: it starts with the table location and still
    // resolves outside it.
    FormatCommitMessage escaping(StagedPath(dir->Str() + "/../victim/data-a-0.parquet"),
                                 dir->Str() + "/../victim/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({escaping}), "does not stay inside");
}

TEST(FormatTableTest, TestAPartitionValueMayBeEscapedMoreThanOneWay) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));

    // A directory another engine wrote may spell a value differently from how this writer would:
    // `100%` and `100%25` name the same value. Comparing the directory string would refuse a
    // perfectly readable partition, so the values are what is compared.
    FormatCommitMessage raw_percent(StagedPath(dir->Str() + "/dt=100%/data-a-0.parquet"),
                                    dir->Str() + "/dt=100%/data-a-0.parquet", {{"dt", "100%"}}, 1,
                                    1);
    ASSERT_OK(commit->Abort({raw_percent}));

    // A file below the partition directory is still that partition's, which is what lets a
    // partition hold its data files in plain subdirectories.
    FormatCommitMessage nested(StagedPath(dir->Str() + "/dt=20240101/part-0/data-a-0.parquet"),
                               dir->Str() + "/dt=20240101/part-0/data-a-0.parquet",
                               {{"dt", "20240101"}}, 1, 1);
    ASSERT_OK(commit->Abort({nested}));
}

TEST(FormatTableTest, TestCommitBindsAFileToThePartitionItClaims) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));

    // The directory is derived from the partition, never trusted: a message whose path says one
    // partition and whose values say another would publish rows under a partition they never had,
    // and would have an overwrite clear the wrong one.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    FormatCommitMessage mismatched(StagedPath(dir->Str() + "/dt=20240102/data-a-0.parquet"),
                                   dir->Str() + "/dt=20240102/data-a-0.parquet",
                                   {{"dt", "20240101"}}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({mismatched}), "but claims");

    // A metadata directory is refused before the partition is even looked at: it is not this
    // table's data at all, whatever partition the message claims for it.
    FormatCommitMessage into_metadata(StagedPath(dir->Str() + "/schema/data-a-0.parquet"),
                                      dir->Str() + "/schema/data-a-0.parquet", {{"dt", "20240101"}},
                                      1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({into_metadata}), "own metadata rather than data");

    // A directory that is neither metadata nor a partition is refused as no partition of this
    // table.
    FormatCommitMessage not_a_partition(StagedPath(dir->Str() + "/plain/data-a-0.parquet"),
                                        dir->Str() + "/plain/data-a-0.parquet",
                                        {{"dt", "20240101"}}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({not_a_partition}), "is not a partition of");

    // And a message must carry every partition key, or nothing says where it belongs.
    FormatCommitMessage no_partition(StagedPath(dir->Str() + "/dt=20240101/data-a-0.parquet"),
                                     dir->Str() + "/dt=20240101/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({no_partition}), "partition values");

    // A static partition bounds what a commit may publish.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> static_commit,
        FormatTableCommit::Create(table, /*overwrite=*/true, {{"dt", "20240101"}}));
    FormatCommitMessage other_partition(StagedPath(dir->Str() + "/dt=20240102/data-a-0.parquet"),
                                        dir->Str() + "/dt=20240102/data-a-0.parquet",
                                        {{"dt", "20240102"}}, 1, 1);
    ASSERT_NOK_WITH_MSG(static_commit->Commit({other_partition}), "static partition");
}

TEST(FormatTableTest, TestCommitRefusesAnUnpublishableMessage) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));

    // Publishing under a hidden name would clear the old data on an overwrite and then succeed at
    // producing a file no scan will ever return.
    FormatCommitMessage hidden_target(StagedPath(dir->Str() + "/.data-a-0.parquet"),
                                      dir->Str() + "/.data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({hidden_target}), "a scan of this table would skip");

    // Two messages aiming at one path would have one silently overwrite the other.
    ASSERT_OK(dir->GetFileSystem()->WriteFile(StagedPath(dir->Str() + "/data-a-0.parquet"), "x",
                                              /*overwrite=*/true));
    FormatCommitMessage first(StagedPath(dir->Str() + "/data-a-0.parquet"),
                              dir->Str() + "/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({first, first}), "would publish");
}

TEST(FormatTableTest, TestCommitChecksTheStagedFileBeforeTouchingAnything) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/true, /*static_partition=*/{}));

    // A message goes stale when its write was aborted, or when the same messages were committed
    // once already. Nothing moves on the strength of a file that is not there.
    FormatCommitMessage gone(StagedPath(dir->Str() + "/data-a-0.parquet"),
                             dir->Str() + "/data-a-0.parquet", {}, 1, 1);
    ASSERT_NOK_WITH_MSG(commit->Commit({gone}), "cannot be read");

    // `rename` moves a directory as readily as a file, and the overwrite has already cleared
    // the old rows by then.
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(StagedPath(dir->Str() + "/data-b-0.parquet")));
    FormatCommitMessage a_directory(StagedPath(dir->Str() + "/data-b-0.parquet"),
                                    dir->Str() + "/data-b-0.parquet", {}, 1, 0);
    ASSERT_NOK_WITH_MSG(commit->Commit({a_directory}), "is a directory, not a file");

    // A length that disagrees with the file means the message and the file are from different
    // writes; publishing it would record a size nothing can rely on.
    ASSERT_OK(dir->GetFileSystem()->WriteFile(StagedPath(dir->Str() + "/data-c-0.parquet"), "12345",
                                              /*overwrite=*/true));
    FormatCommitMessage wrong_size(StagedPath(dir->Str() + "/data-c-0.parquet"),
                                   dir->Str() + "/data-c-0.parquet", {}, 1, 999);
    ASSERT_NOK_WITH_MSG(commit->Commit({wrong_size}), "but the commit message says");

    // Nothing was published, and any old data was never cleared.
    ASSERT_OK_AND_ASSIGN(bool published,
                         dir->GetFileSystem()->Exists(dir->Str() + "/data-c-0.parquet"));
    ASSERT_FALSE(published);
}

TEST(FormatTableTest, TestReadRefusesASplitOverInvisibleFiles) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));

    // A scan skips a hidden name and never descends into one, since that is where an uncommitted
    // job stages its output. A split that did not come from one must not read what a scan would
    // never return.
    auto staged = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/_temporary/a.parquet", 1}},
        std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(staged)),
                        "a scan of this table would skip");

    // Nor this table's own metadata, which a file system catalog keeps under the location.
    auto metadata = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/schema/schema-0", 1}},
        std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(metadata)),
                        "own metadata rather than data");

    // A size no file could have is refused before anything is opened.
    auto negative_size = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/data-a-0.parquet", -1}},
        std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(negative_size)),
                        "negative size");
}

TEST(FormatTableTest, TestAbortCleansUpTheGoodMessagesAmongBadOnes) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1u);

    // Refusing the whole batch at the first bad message would leave the good ones' staged files
    // behind with nothing left to clean them up.
    std::vector<FormatCommitMessage> mixed = {
        FormatCommitMessage(StagedPath("/elsewhere/x"), "/elsewhere/x", {}, 1, 1), messages[0]};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Abort(mixed));
    ASSERT_OK_AND_ASSIGN(bool temp_exists,
                         dir->GetFileSystem()->Exists(messages[0].temp_file_path));
    ASSERT_FALSE(temp_exists);
}

TEST(FormatTableTest, TestReadRefusesASplitItCannotUse) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    // `CreateReader` takes the base `Split`, which a managed table's splits are too. One of those
    // would have this read looking for files where a format table keeps none, so the type is
    // checked rather than assumed, as is the null a caller can always hand over.
    class NotAFormatSplit : public Split {};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::make_shared<NotAFormatSplit>()),
                        "only accepts a FormatDataSplit");
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::shared_ptr<Split>()),
                        "only accepts a FormatDataSplit");

    // A partition value the column type cannot hold. The directory name and the split agree, so
    // nothing before this notices; only reading the value into its type does. A split reaches the
    // read through the base `Split` type, so this has to be a refusal rather than an assumption.
    std::unique_ptr<UniqueTestDirectory> int_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(int_dir);
    auto int_schema =
        arrow::schema({arrow::field("name", arrow::utf8()), arrow::field("pt", arrow::int32())});
    SchemaManager schema_manager(int_dir->GetFileSystem(), int_dir->Str());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> int_table_schema,
                         schema_manager.CreateTable(
                             int_schema, /*partition_keys=*/{"pt"}, /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> int_table,
                         FormatTable::Create(int_dir->GetFileSystem(), int_dir->Str(),
                                             Identifier("db", "tbl"), /*dynamic_options=*/{}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> int_read,
        FormatTableRead::TEST_Create(int_table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));
    auto not_an_int = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{int_dir->Str() + "/pt=abc/data-a-0.parquet", 1}},
        std::map<std::string, std::string>{{"pt", "abc"}});
    ASSERT_NOK(int_read->CreateReader(std::static_pointer_cast<Split>(not_an_int)));
}

TEST(FormatTableTest, TestPathContainmentComparesTheFileSystemAndThePathInsideIt) {
    // A location and a path may be written differently and still name the same place: an absent
    // scheme names the local file system just as `file` does, and `//` collapses. Comparing the
    // raw strings would refuse a path a caller spelled as a `file:` URI as sitting outside the
    // table, and would let one on another file system through on the strength of its path alone.
    const std::string what = "split";
    for (const char* location : {"/tmp/table", "/tmp/table/", "file:///tmp/table"}) {
        for (const char* path : {"/tmp/table/dt=1/a.parquet", "file:///tmp/table/dt=1/a.parquet",
                                 "/tmp//table/dt=1/a.parquet"}) {
            ASSERT_OK(FormatPathValidation::ValidatePathUnderLocation(path, location, what))
                << location << " <- " << path;
        }
        // Another file system, however alike the path reads.
        ASSERT_NOK_WITH_MSG(FormatPathValidation::ValidatePathUnderLocation(
                                "oss://bucket/tmp/table/dt=1/a.parquet", location, what),
                            "not under the table location");
        // A sibling that merely shares the prefix, and a path that climbs back out.
        ASSERT_NOK_WITH_MSG(FormatPathValidation::ValidatePathUnderLocation("/tmp/table2/a.parquet",
                                                                            location, what),
                            "not under the table location");
        ASSERT_NOK_WITH_MSG(FormatPathValidation::ValidatePathUnderLocation(
                                "file:///tmp/table/../victim/a.parquet", location, what),
                            "does not stay inside");
    }

    // An object store location keeps its authority: the bucket is part of where the table is.
    ASSERT_OK(FormatPathValidation::ValidatePathUnderLocation("oss://bucket/table/a.parquet",
                                                              "oss://bucket/table", what));
    ASSERT_NOK_WITH_MSG(FormatPathValidation::ValidatePathUnderLocation(
                            "oss://other/table/a.parquet", "oss://bucket/table", what),
                        "not under the table location");
}

TEST(FormatTableTest, TestTheTableLocationIsRecognisedHoweverItIsWritten) {
    // Whether a directory is the table's own location decides whether `schema` and `branch` below
    // it are metadata or data, so an overwrite at the root would delete the schema if a `file:`
    // URI or a trailing separator made the two compare different. A different file system is a
    // different place, however alike the path reads.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));

    for (const std::string& spelling :
         {dir->Str(), dir->Str() + "/", "file://" + dir->Str(), "file://" + dir->Str() + "/"}) {
        ASSERT_OK_AND_ASSIGN(bool is_location,
                             FormatPathValidation::IsTableLocation(table, spelling));
        ASSERT_TRUE(is_location) << spelling;
    }
    for (const std::string& elsewhere :
         {dir->Str() + "/dt=1", dir->Str() + "-sibling", "oss://bucket" + dir->Str()}) {
        ASSERT_OK_AND_ASSIGN(bool is_location,
                             FormatPathValidation::IsTableLocation(table, elsewhere));
        ASSERT_FALSE(is_location) << elsewhere;
    }
}

TEST(FormatTableTest, TestATableAtARelativeLocationIsWrittenAndReadBack) {
    // A local location may be relative, and the local file system resolves one against the
    // working directory as it opens it: what a scan lists comes back under an absolute path while
    // the table's location is still the relative one. They are the same place, so a split the
    // scan itself produced has to read back rather than be refused as sitting outside the table.
    for (const bool partitioned : {false, true}) {
        SCOPED_TRACE(partitioned);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        // Worked out at run time, so the test depends on no particular working directory.
        const std::string location =
            std::filesystem::relative(dir->Str(), std::filesystem::current_path()).string();
        ASSERT_FALSE(location.empty());
        ASSERT_NE(location.front(), '/');

        std::vector<std::string> partition_keys;
        std::map<std::string, std::string> partition;
        if (partitioned) {
            partition_keys.emplace_back("dt");
            partition.emplace("dt", "20240101");
        }
        // The schema is written through the absolute path and the table opened through the
        // relative one: one directory, spelled the two ways a caller may spell it.
        ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::shared_ptr<FormatTable> absolute_table,
                             CreateTable(dir->GetFileSystem(), dir->Str(), partition_keys));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(dir->GetFileSystem(), location,
                                                 Identifier("db", "tbl"), /*dynamic_options=*/{}));
        ASSERT_EQ(table->Location(), location);

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({1, 2}, {"alice", "bob"}, "20240101", partition));
        ASSERT_OK(WriteAndCommit(table, std::move(batch)));

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
        ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101", "2|bob|20240101"}));
    }
}

TEST(FormatTableTest, TestReadRefusesASplitFromOutsideTheTable) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));

    // Every file a split names is opened, and the split reaching `CreateReader()` may have been
    // planned from somewhere else entirely. Whether a file belongs to this table is a question
    // only the table can answer.
    auto outside = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{"/etc/passwd", 1}},
        std::map<std::string, std::string>{{"dt", "20240101"}});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(outside)),
                        "not under the table location");

    auto escaping = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/../victim/a.parquet", 1}},
        std::map<std::string, std::string>{{"dt", "20240101"}});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(escaping)),
                        "does not stay inside");

    // A file from another partition would read back under partition values it never had.
    auto wrong_partition = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/dt=20240102/a.parquet", 1}},
        std::map<std::string, std::string>{{"dt", "20240101"}});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(wrong_partition)),
                        "but claims");

    // And a split that names no partition at all cannot say where its rows belong.
    auto no_partition = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/dt=20240101/a.parquet", 1}},
        std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::static_pointer_cast<Split>(no_partition)),
                        "partition values");
}

TEST(FormatTableTest, TestAZeroRowBatchWritesNoFile) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    // A writer is created when the first row arrives, so a write that receives none leaves the
    // directory as it found it, rather than a file holding nothing but a footer.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> empty, MakeBatch({}, {}, "20240101", {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK(write->Write(std::move(empty)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_TRUE(messages.empty());
}

TEST(FormatTableTest, TestAbortRemovesWrittenFiles) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Abort(messages));
    ASSERT_OK_AND_ASSIGN(bool temp_exists,
                         dir->GetFileSystem()->Exists(messages[0].temp_file_path));
    ASSERT_FALSE(temp_exists);
    ASSERT_OK_AND_ASSIGN(bool file_exists, dir->GetFileSystem()->Exists(messages[0].file_path));
    ASSERT_FALSE(file_exists);
}

TEST(FormatTableTest, TestWriteRejectsNonInsertRows) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> delete_batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}, {RecordBatch::RowKind::DELETE}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    Status status = write->Write(std::move(delete_batch));
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.IsInvalid());
    ASSERT_OK(write->Abort());
}

TEST(FormatTableTest, TestRowsAreStoredUnderThePartitionTheBatchDeclares) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    // The rows say they belong to 20240102 while the batch declares 20240101. The declaration
    // decides where they go, as `RecordBatch::GetPartition()` does on the managed write path, and
    // the row's own value is not checked against it: the partition columns are not written at
    // all, so the read rebuilds them from the directory name either way.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240102", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions, (PartitionList{{{"dt", "20240101"}}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    // Read back under the partition the batch declared, not the one the row column named.
    ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101"}));
}

TEST(FormatTableTest, TestASplitWeighsTheFilesItNames) {
    // What a split weighs decides how the scan packs it, so it weighs the files it names.
    FormatDataSplit split({{"/tbl/data-a-0.parquet", 4096}, {"/tbl/data-a-1.parquet", 512}}, {});
    ASSERT_EQ(split.files.size(), 2u);
    ASSERT_EQ(split.files[0].file_size, 4096);
    ASSERT_EQ(split.TotalSize(), 4608);

    // The sizes are whatever the split was given, so a total that would not fit an int64
    // saturates rather than wrapping into a negative answer, which the scan would then pack as
    // if the files were empty.
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    FormatDataSplit huge({{"/tbl/a.parquet", kMax}, {"/tbl/b.parquet", kMax}}, {});
    ASSERT_EQ(huge.TotalSize(), kMax);
}

TEST(FormatTableTest, TestASplitIsNotSerializable) {
    // A format table's plan has no cross-runtime encoding, and one of paimon-cpp's own would let
    // a plan made here be handed to a runtime that cannot read it back.
    auto split = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{"/tbl/dt=20240101/data-a-0.parquet", 128}},
        std::map<std::string, std::string>{{"dt", "20240101"}});
    Result<std::string> serialized = Split::Serialize(split, GetDefaultPool());
    ASSERT_FALSE(serialized.ok());
    ASSERT_TRUE(serialized.status().IsNotImplemented());
    ASSERT_NE(serialized.status().message().find("in-memory only"), std::string::npos);
}

TEST(FormatTableTest, TestACommitMessageDescribesItself) {
    // `ToString()` names the message in every commit failure, so it is defined alongside the
    // struct rather than in the commit's own translation unit: a test or a caller that includes
    // the header alone would otherwise link against nothing.
    FormatCommitMessage message("/tbl/dt=1/_temporary/.tmp.abc", "/tbl/dt=1/data-a-0.parquet",
                                {{"dt", "1"}}, /*record_count=*/7, /*file_size=*/1024);
    const std::string described = message.ToString();
    for (const char* expected :
         {"/tbl/dt=1/data-a-0.parquet", "/tbl/dt=1/_temporary/.tmp.abc", "dt", "7", "1024"}) {
        ASSERT_NE(described.find(expected), std::string::npos) << expected << " in " << described;
    }
}

TEST(FormatTableTest, TestACommitMessageIsNotSerializable) {
    // The other half of the same rule: a message names a staged path rather than files to record
    // in a manifest, so there is nothing a manifest-shaped encoding could carry.
    auto message = std::make_shared<FormatCommitMessage>(
        StagedPath("/tbl/dt=20240101/data-a-0.parquet"), "/tbl/dt=20240101/data-a-0.parquet",
        std::map<std::string, std::string>{{"dt", "20240101"}}, /*record_count=*/1,
        /*file_size=*/128);
    Result<std::string> serialized = CommitMessage::Serialize(message, GetDefaultPool());
    ASSERT_FALSE(serialized.ok());
    ASSERT_TRUE(serialized.status().IsNotImplemented());
    ASSERT_NE(serialized.status().message().find("belong to one process"), std::string::npos);

    // And through the list form, which is what a sink hands a batch of messages to.
    Result<std::string> serialized_list = CommitMessage::SerializeList(
        std::vector<std::shared_ptr<CommitMessage>>{message}, GetDefaultPool());
    ASSERT_FALSE(serialized_list.ok());
    ASSERT_TRUE(serialized_list.status().IsNotImplemented());
}

TEST(FormatTableTest, TestOverwriteReplacesTheWholePartitionOfANestedFile) {
    // A commit message may name a file below its partition directory, which is what lets a
    // partition keep its files in plain subdirectories. An overwrite of such a message replaces
    // everything the partition holds, not just the subdirectory the new file lands in: the
    // partition is what the overwrite was asked to replace.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    const std::string partition_dir = PathUtil::JoinPath(dir->Str(), "dt=20240101");

    // Old data at the partition root, and a copy of it in a sibling subdirectory of the partition.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> old_batch,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(old_batch)));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> before,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> before_plan, before->CreatePlan());
    ASSERT_EQ(before_plan->Splits().size(), 1u);
    auto before_split = std::dynamic_pointer_cast<FormatDataSplit>(before_plan->Splits()[0]);
    ASSERT_NE(before_split, nullptr);
    ASSERT_EQ(before_split->files.size(), 1u);
    const std::string old_root_file = before_split->files[0].file_path;
    std::string old_content;
    ASSERT_OK(dir->GetFileSystem()->ReadFile(old_root_file, &old_content));
    const std::string sibling_dir = PathUtil::JoinPath(partition_dir, "part-1");
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(sibling_dir));
    ASSERT_OK(dir->GetFileSystem()->WriteFile(
        PathUtil::JoinPath(sibling_dir, PathUtil::GetName(old_root_file)), old_content,
        /*overwrite=*/true));

    // A new file staged for a subdirectory of the same partition. The bytes are a real data file,
    // written through the table and then staged where a message may legitimately name one.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> new_batch,
                         MakeBatch({3}, {"carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(write->Write(std::move(new_batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> written, write->PrepareCommit());
    ASSERT_EQ(written.size(), 1u);
    std::string new_content;
    ASSERT_OK(dir->GetFileSystem()->ReadFile(written[0].temp_file_path, &new_content));
    ASSERT_OK(write->Abort());

    const std::string nested_target =
        PathUtil::JoinPath(PathUtil::JoinPath(partition_dir, "part-0"), "data-nested-0.parquet");
    const std::string nested_staged = StagedPath(nested_target);
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(PathUtil::GetParentDirPath(nested_staged)));
    ASSERT_OK(dir->GetFileSystem()->WriteFile(nested_staged, new_content, /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(FileStatus staged_status,
                         dir->GetFileSystem()->GetFileStatus(nested_staged));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/true, /*static_partition=*/{}));
    FormatCommitMessage nested(nested_staged, nested_target,
                               std::map<std::string, std::string>{{"dt", "20240101"}},
                               written[0].record_count, staged_status.GetLen());
    ASSERT_OK(commit->Commit({nested}));

    // Only the new row is left: the old file at the partition root and the one in the sibling
    // subdirectory were both replaced.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> after,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> after_plan, after->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, after_plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"3|carol|20240101"}));
}

TEST(FormatTableTest, TestDynamicPartitionOverwriteDecidesWhatAnOverwriteReplaces) {
    // An overwrite naming no partition replaces the partitions it writes when
    // `dynamic-partition-overwrite` is on, its default, and everything the table holds when it is
    // off - the condition Java's `FormatTableCommit.replacesOnlyWrittenPartitions()` and a managed
    // table commit both apply. The two runs differ only in the option, so it is the option and
    // nothing else that decides.
    struct Case {
        const char* dynamic_partition_overwrite;
        std::vector<std::string> expected_rows;
    };
    const std::vector<Case> cases = {// On: the partition this commit never touched is left alone.
                                     {"true", {"3|carol|20240101", "9|zoe|20240102"}},
                                     // Off: it is emptied along with the rest of the table.
                                     {"false", {"3|carol|20240101"}}};
    for (const Case& test_case : cases) {
        SCOPED_TRACE(test_case.dynamic_partition_overwrite);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"},
                                         {{Options::DYNAMIC_PARTITION_OVERWRITE,
                                           test_case.dynamic_partition_overwrite}}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                             MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
        ASSERT_OK(WriteAndCommit(table, std::move(first)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other,
                             MakeBatch({9}, {"zoe"}, "20240102", {{"dt", "20240102"}}));
        ASSERT_OK(WriteAndCommit(table, std::move(other)));

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replacement,
                             MakeBatch({3}, {"carol"}, "20240101", {{"dt", "20240101"}}));
        ASSERT_OK(WriteAndCommit(table, std::move(replacement), /*overwrite=*/true));

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
        std::sort(rows.begin(), rows.end());
        ASSERT_EQ(rows, test_case.expected_rows);
    }
}

TEST(FormatTableTest, TestAStaticPartitionDecidesBeforeDynamicPartitionOverwriteDoes) {
    // A static partition names outright what an overwrite replaces, so it settles the question
    // before `dynamic-partition-overwrite` gets a say: that option only chooses between the
    // partitions a commit wrote and the whole table when no partition was named. Without this,
    // turning the option off would quietly widen a static-partition overwrite to the whole table.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"},
                                     {{Options::DYNAMIC_PARTITION_OVERWRITE, "false"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other,
                         MakeBatch({9}, {"zoe"}, "20240102", {{"dt", "20240102"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(other)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replacement,
                         MakeBatch({3}, {"carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(
        WriteAndCommit(table, std::move(replacement), /*overwrite=*/true, {{"dt", "20240101"}}));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    std::sort(rows.begin(), rows.end());
    // Only the named partition is replaced, with the option off.
    ASSERT_EQ(rows, (std::vector<std::string>{"3|carol|20240101", "9|zoe|20240102"}));
}

TEST(FormatTableTest, TestOverwriteWithAStaticPartitionClearsThatPartition) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other,
                         MakeBatch({9}, {"zoe"}, "20240102", {{"dt", "20240102"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(other)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replacement,
                         MakeBatch({3}, {"carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(
        WriteAndCommit(table, std::move(replacement), /*overwrite=*/true, {{"dt", "20240101"}}));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    std::sort(rows.begin(), rows.end());
    ASSERT_EQ(rows, (std::vector<std::string>{"3|carol|20240101", "9|zoe|20240102"}));
}

TEST(FormatTableTest, TestOverwriteOfAPartitionThatIsNotThereYetSucceeds) {
    // Overwriting a partition that does not exist has nothing to clear, and is how a first write
    // to that partition is spelled. Failing on it would make an overwriting job depend on whether
    // some earlier job had already created the directory.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch), /*overwrite=*/true, {{"dt", "20240101"}}));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101"}));

    // And with nothing to publish either: the partition is left behind empty rather than not
    // created at all, so a later scan lists it.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/true, {{"dt", "20240202"}}));
    ASSERT_OK(commit->Commit({}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> after,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, after->ListPartitions());
    ASSERT_EQ(partitions.size(), 2u);
    ASSERT_EQ(partitions[1].at("dt"), "20240202");
}

TEST(FormatTableTest, TestOverwriteCanEmptyAPartitionWithoutWritingToIt) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));

    // An overwrite that names a partition but writes nothing into it clears that partition and
    // leaves it behind empty, rather than removing it from the table.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/true, {{"dt", "20240101"}}));
    ASSERT_OK(commit->Commit({}));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_TRUE(rows.empty());
    // The partition itself is still there, with no rows in it.
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions.size(), 1u);
    ASSERT_EQ(partitions[0], (std::map<std::string, std::string>{{"dt", "20240101"}}));
}

TEST(FormatTableTest, TestAFailedPublishRollsBackAnAppendButNotAnOverwrite) {
    // A commit publishes file by file, so it can fail with some files already visible. What to do
    // then depends on what it replaced: an append takes its own files back, so the table holds all
    // of this write or none of it; an overwrite has already deleted the data it replaces, so
    // taking them back would leave neither the old rows nor the new, and it keeps them.
    for (const bool overwrite : {false, true}) {
        SCOPED_TRACE(overwrite);
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             CreateTable(dir->GetFileSystem(), dir->Str(), {},
                                         {{Options::TARGET_FILE_ROW_NUM, "1"}}));
        // Something already committed, which the overwriting run replaces.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> existing,
                             MakeBatch({9}, {"zoe"}, "20240101", {}));
        ASSERT_OK(WriteAndCommit(table, std::move(existing)));

        // Two files, so the second rename is the one that fails.
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                             FormatTableWrite::Create(table, /*pool=*/nullptr));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                             MakeBatch({1}, {"alice"}, "20240101", {}));
        ASSERT_OK(write->Write(std::move(first)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second,
                             MakeBatch({2}, {"bob"}, "20240101", {}));
        ASSERT_OK(write->Write(std::move(second)));
        ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
        ASSERT_EQ(messages.size(), 2u);

        auto failing_fs =
            std::make_shared<FailingRenameFileSystem>(dir->GetFileSystem(), /*fail_nth_rename=*/2);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> failing_table,
                             FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl"),
                                                 /*dynamic_options=*/{}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableCommit> commit,
                             FormatTableCommit::Create(failing_table, overwrite,
                                                       /*static_partition=*/{}));
        ASSERT_NOK(commit->Commit(messages));

        // The first file was renamed before the failure. An append takes it back; an overwrite
        // keeps it, since the rows it replaced are already gone.
        ASSERT_OK_AND_ASSIGN(bool first_published,
                             dir->GetFileSystem()->Exists(messages[0].file_path));
        ASSERT_EQ(first_published, overwrite);
        // The one that never got renamed is published in neither case.
        ASSERT_OK_AND_ASSIGN(bool second_published,
                             dir->GetFileSystem()->Exists(messages[1].file_path));
        ASSERT_FALSE(second_published);
    }
}

TEST(FormatTableTest, TestAFailedAppendTakesBackATargetTheRenameLeftBehind) {
    // A rename that reports a failure may have published the file all the same: on an object store
    // it is a copy followed by a delete, and a request that took effect but lost its response
    // looks the same from here. An append that gives up has to take that target back too, or the
    // rows would be there for the next scan to read - and there again after the write is retried.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        CreateTable(dir->GetFileSystem(), dir->Str(), {}, {{Options::TARGET_FILE_ROW_NUM, "1"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> existing,
                         MakeBatch({9}, {"zoe"}, "20240101", {}));
    ASSERT_OK(WriteAndCommit(table, std::move(existing)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK(write->Write(std::move(first)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second,
                         MakeBatch({2}, {"bob"}, "20240101", {}));
    ASSERT_OK(write->Write(std::move(second)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_EQ(messages.size(), 2u);

    auto failing_fs = std::make_shared<FailingRenameFileSystem>(
        dir->GetFileSystem(), /*fail_nth_rename=*/2, RenameFailure::kAfterPublishing);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> failing_table,
                         FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl"),
                                             /*dynamic_options=*/{}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(failing_table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_NOK(commit->Commit(messages));

    // Neither the file whose rename said it worked nor the one whose rename published it and then
    // said it had not.
    for (const FormatCommitMessage& message : messages) {
        ASSERT_OK_AND_ASSIGN(bool published, dir->GetFileSystem()->Exists(message.file_path));
        ASSERT_FALSE(published) << message.file_path;
    }
    // So what a scan reads is what was there before this commit.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"9|zoe|20240101"}));
}

TEST(FormatTableTest, TestReplacingTheTableLeavesWhatAScanCannotSee) {
    // Replacing the table clears what the table holds, and what it holds is what a scan reads.
    // A directory whose name is not a partition of this table, and a file above the partition
    // level, are read by no scan - so an overwrite must leave them where they are. Deleting them
    // would take out another engine's data on the strength of it sharing a directory.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"},
                                     {{Options::DYNAMIC_PARTITION_OVERWRITE, "false"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));

    // None of these three is this table's data: a file above the partition level, a directory
    // whose name is no `key=value` at all, and one naming another table's partition key.
    const std::string orphan = dir->Str() + "/orphan.parquet";
    const std::string backup = dir->Str() + "/backup/file.parquet";
    const std::string other_key = dir->Str() + "/other=1/file.parquet";
    ASSERT_OK(dir->GetFileSystem()->WriteFile(orphan, "x", /*overwrite=*/true));
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/backup"));
    ASSERT_OK(dir->GetFileSystem()->WriteFile(backup, "x", /*overwrite=*/true));
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/other=1"));
    ASSERT_OK(dir->GetFileSystem()->WriteFile(other_key, "x", /*overwrite=*/true));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replacement,
                         MakeBatch({2}, {"bob"}, "20240102", {{"dt", "20240102"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(replacement), /*overwrite=*/true));

    for (const std::string& kept : {orphan, backup, other_key}) {
        ASSERT_OK_AND_ASSIGN(bool exists, dir->GetFileSystem()->Exists(kept));
        ASSERT_TRUE(exists) << kept;
    }
    // The table's own partition was replaced all the same.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows, (std::vector<std::string>{"2|bob|20240102"}));
}

TEST(FormatTableTest, TestOverwriteReplacingTheTableEmptiesItWithNothingToPublish) {
    // A statement whose query returns nothing still empties what it overwrites, so an overwrite
    // that replaces the table cannot take the directories to clear from its commit messages.
    for (const char* dynamic_partition_overwrite : {"false", "true"}) {
        std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        // Unpartitioned, so the option has no partition to select and the table is replaced
        // whichever way it is set.
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> table,
            CreateTable(dir->GetFileSystem(), dir->Str(), {},
                        {{Options::DYNAMIC_PARTITION_OVERWRITE, dynamic_partition_overwrite}}));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                             MakeBatch({1, 2}, {"alice", "bob"}, "20240101", {}));
        ASSERT_OK(WriteAndCommit(table, std::move(first)));

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableCommit> commit,
            FormatTableCommit::Create(table, /*overwrite=*/true, /*static_partition=*/{}));
        ASSERT_OK(commit->Commit({}));

        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
        ASSERT_TRUE(rows.empty()) << dynamic_partition_overwrite;
    }
}

TEST(FormatTableTest, TestOverwritingATableWhoseDirectoryIsNotThereYet) {
    // A table whose schema lives in a metastore is loaded from that schema alone, and nothing in it
    // says the data directory has been created. An overwrite that replaces the whole table reaches
    // that directory before anything has written to it, so a missing directory has to read as no
    // data to replace rather than as a failure, as Java's `previousDataFiles()` makes of it. The
    // two ways to get there are an unpartitioned table and a partitioned one with
    // `dynamic-partition-overwrite` off.
    for (const bool partitioned : {false, true}) {
        SCOPED_TRACE(partitioned);
        std::unique_ptr<UniqueTestDirectory> schema_dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(schema_dir);
        std::vector<std::string> partition_keys;
        if (partitioned) {
            partition_keys.push_back("dt");
        }
        SchemaManager schema_manager(schema_dir->GetFileSystem(), schema_dir->Str());
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<TableSchema> table_schema,
            schema_manager.CreateTable(MakeSchema(), partition_keys, /*primary_keys=*/{},
                                       {{Options::TYPE, "format-table"},
                                        {Options::FILE_FORMAT, "parquet"},
                                        {Options::DYNAMIC_PARTITION_OVERWRITE, "false"}}));
        std::shared_ptr<DataSchema> data_schema =
            checked_pointer_cast<DataSchema>(std::shared_ptr<TableSchema>(std::move(table_schema)));

        // Never created: the schema says where the data goes, not that anything is there yet.
        std::unique_ptr<UniqueTestDirectory> data_dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(data_dir);
        const std::string location = data_dir->Str() + "/not-there-yet";
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                             FormatTable::Create(data_dir->GetFileSystem(), location,
                                                 Identifier("db", "tbl"), data_schema,
                                                 /*location_carries_paimon_metadata=*/false,
                                                 /*dynamic_options=*/{}));
        ASSERT_OK_AND_ASSIGN(bool exists, data_dir->GetFileSystem()->Exists(location));
        ASSERT_FALSE(exists);

        // A statement whose query returned nothing, over a table nothing has written to.
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableCommit> commit,
            FormatTableCommit::Create(table, /*overwrite=*/true, /*static_partition=*/{}));
        ASSERT_OK(commit->Commit({}));

        // And the table still takes what is written to it afterwards.
        std::map<std::string, std::string> partition;
        if (partitioned) {
            partition["dt"] = "20240101";
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             MakeBatch({1}, {"alice"}, "20240101", partition));
        ASSERT_OK(WriteAndCommit(table, std::move(batch), /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FormatTableScan> scan,
            FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
        ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101"}));
    }
}

TEST(FormatTableTest, TestDataFilePrefixReachesTheWrittenFiles) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        CreateTable(dir->GetFileSystem(), dir->Str(), {}, {{Options::DATA_FILE_PREFIX, "part-"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK(write->Write(std::move(batch)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_TRUE(StringUtils::StartsWith(PathUtil::GetName(messages[0].file_path), "part-"))
        << messages[0].file_path;
    ASSERT_OK(write->Abort());
}

TEST(FormatTableTest, TestCommitWithoutOverwriteAddsToWhatIsThere) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second,
                         MakeBatch({2}, {"bob"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(second)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    std::sort(rows.begin(), rows.end());
    ASSERT_EQ(rows, (std::vector<std::string>{"1|alice|20240101", "2|bob|20240101"}));
}

TEST(FormatTableTest, TestAStaticPartitionMayNameOnlyTheLeadingPartitionKeys) {
    // Every other test here partitions by one key, which leaves the nested layout and the whole
    // "leading keys" rule unexercised: a static partition naming only `year` stands for every
    // month below it, and clearing it has to descend the level the spec did not name.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<arrow::Schema> nested_schema =
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("year", arrow::utf8()),
                       arrow::field("month", arrow::utf8())});
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
                         schema_manager.CreateTable(
                             nested_schema, /*partition_keys=*/{"year", "month"},
                             /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         FormatTable::Create(dir->GetFileSystem(), dir->Str(),
                                             Identifier("db", "tbl"), /*dynamic_options=*/{}));

    auto make_batch = [&nested_schema](
                          int32_t id, const std::string& year,
                          const std::string& month) -> Result<std::unique_ptr<RecordBatch>> {
        arrow::Int32Builder id_builder;
        arrow::StringBuilder year_builder;
        arrow::StringBuilder month_builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(id));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(year_builder.Append(year));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(month_builder.Append(month));
        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> year_array;
        std::shared_ptr<arrow::Array> month_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(year_builder.Finish(&year_array));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(month_builder.Finish(&month_array));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> struct_array,
            arrow::StructArray::Make({id_array, year_array, month_array}, nested_schema->fields()));
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
        RecordBatchBuilder builder(c_array.get());
        builder.SetPartition({{"year", year}, {"month", month}});
        return builder.Finish();
    };
    auto write_and_commit =
        [&table](std::unique_ptr<RecordBatch> batch, bool overwrite,
                 const std::map<std::string, std::string>& static_partition) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableWrite> write,
                               FormatTableWrite::Create(table, /*pool=*/nullptr));
        PAIMON_RETURN_NOT_OK(write->Write(std::move(batch)));
        PAIMON_ASSIGN_OR_RAISE(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableCommit> commit,
                               FormatTableCommit::Create(table, overwrite, static_partition));
        return commit->Commit(messages);
    };

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> january, make_batch(1, "2025", "01"));
    ASSERT_OK(write_and_commit(std::move(january), /*overwrite=*/false, {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> february, make_batch(2, "2025", "02"));
    ASSERT_OK(write_and_commit(std::move(february), /*overwrite=*/false, {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> other_year, make_batch(3, "2024", "12"));
    ASSERT_OK(write_and_commit(std::move(other_year), /*overwrite=*/false, {}));

    // Nested one level below a partition directory, where no partition key belongs. Clearing the
    // `year=2025` prefix descends the month level, so it must tell a month from anything else.
    const std::string foreign = dir->Str() + "/year=2025/backup/file.parquet";
    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/year=2025/backup"));
    ASSERT_OK(dir->GetFileSystem()->WriteFile(foreign, "x", /*overwrite=*/true));

    // Names only the leading key, so it stands for every month of 2025 - including February,
    // which this commit writes nothing to.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> replacement, make_batch(4, "2025", "01"));
    ASSERT_OK(write_and_commit(std::move(replacement), /*overwrite=*/true, {{"year", "2025"}}));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    // Both months of 2025 are still partitions of the table; February simply holds nothing now.
    ASSERT_EQ(partitions.size(), 3u);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 2u);
    // The other year is untouched, and `backup` was never this table's data to clear.
    ASSERT_OK_AND_ASSIGN(bool foreign_kept, dir->GetFileSystem()->Exists(foreign));
    ASSERT_TRUE(foreign_kept);
}

TEST(FormatTableTest, TestStaticPartitionMustNameLeadingKeysOfAPartitionedTable) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_NOK_WITH_MSG(FormatTableCommit::Create(table, /*overwrite=*/true, {{"nope", "1"}}),
                        "is not a partition key");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> unpartitioned,
                         CreateTable(dir->GetFileSystem(), dir->Str() + "/plain", {}));
    ASSERT_NOK_WITH_MSG(
        FormatTableCommit::Create(unpartitioned, /*overwrite=*/true, {{"dt", "20240101"}}),
        "is not partitioned");
}

TEST(FormatTableTest, TestReadRefusesASplitWhoseFileChanged) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {}));
    ASSERT_OK(dir->GetFileSystem()->WriteFile(dir->Str() + "/data-x-0.parquet", "not a data file",
                                              /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                     /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false));

    // The size in a split is the caller's and `Open` trusts it, so a stale length would truncate
    // an object-store read. A file is opened only when it is reached, so the check runs then
    // rather than when the reader is built.
    auto wrong_size = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/data-x-0.parquet", 999999}},
        std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> wrong_size_reader,
                         read->CreateReader(std::static_pointer_cast<Split>(wrong_size)));
    Result<BatchReader::ReadBatch> stale = wrong_size_reader->NextBatch();
    ASSERT_NOK_WITH_MSG(stale, "different version of the file");
    // And it says which file, since a split holds a whole partition of them.
    ASSERT_NOK_WITH_MSG(stale, "data-x-0.parquet");
    wrong_size_reader->Close();

    ASSERT_OK(dir->GetFileSystem()->Mkdirs(dir->Str() + "/adir"));
    auto a_directory = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{dir->Str() + "/adir", 0}},
        std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> directory_reader,
                         read->CreateReader(std::static_pointer_cast<Split>(a_directory)));
    Result<BatchReader::ReadBatch> not_a_file = directory_reader->NextBatch();
    ASSERT_NOK_WITH_MSG(not_a_file, "names a directory, not a data file");
    ASSERT_NOK_WITH_MSG(not_a_file, "adir");
    directory_reader->Close();
}

TEST(FormatTableTest, TestPredicateFilterKeepsOnlyMatchingRows) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        MakeBatch({1, 2, 3}, {"alice", "bob", "carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    std::shared_ptr<Predicate> id_gt_1 = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(1));

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits(), id_gt_1,
                                                                /*enable_predicate_filter=*/true));
    ASSERT_EQ(rows, (std::vector<std::string>{"2|bob|20240101", "3|carol|20240101"}));
}

TEST(FormatTableTest, TestPredicateWithoutFilterIsOnlyPushedDown) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        MakeBatch({1, 2, 3}, {"alice", "bob", "carol"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(batch)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    std::shared_ptr<Predicate> id_gt_1 = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(1));

    // A one-sided contract: what the predicate keeps is never lost, what it rejects may still
    // come back. Asserting a row count would assert the format's statistics granularity.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits(), id_gt_1));
    ASSERT_NE(std::find(rows.begin(), rows.end(), "2|bob|20240101"), rows.end());
    ASSERT_NE(std::find(rows.begin(), rows.end(), "3|carol|20240101"), rows.end());
    ASSERT_LE(rows.size(), 3u);

    // Exactness is what `enable_predicate_filter` is for, and then the rejected row is gone.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> filtered,
                         ReadAll(table, plan->Splits(), id_gt_1,
                                 /*enable_predicate_filter=*/true));
    ASSERT_EQ(filtered, (std::vector<std::string>{"2|bob|20240101", "3|carol|20240101"}));
}

TEST(FormatTableTest, TestPredicateOnPartitionColumnIsAppliedToTheBatch) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> first,
                         MakeBatch({1}, {"alice"}, "20240101", {{"dt", "20240101"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(first)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> second,
                         MakeBatch({2}, {"bob"}, "20240102", {{"dt", "20240102"}}));
    ASSERT_OK(WriteAndCommit(table, std::move(second)));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    // The column lives in the directory name, not in the file, so only the batch can be tested
    // against it, which is what the filter layer does.
    const std::string wanted_dt = "20240102";
    std::shared_ptr<Predicate> dt_equal = PredicateBuilder::Equal(
        /*field_index=*/2, /*field_name=*/"dt", FieldType::STRING,
        Literal(FieldType::STRING, wanted_dt.data(), wanted_dt.size(), /*own_data=*/true));
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits(), dt_equal,
                                                                /*enable_predicate_filter=*/true));
    ASSERT_EQ(rows, (std::vector<std::string>{"2|bob|20240102"}));
}

TEST(FormatTableTest, TestProjectionIsCheckedAgainstTheTable) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));

    // Reading the column once and naming the result twice would collide in the arrow schema, so
    // a repeated column is refused rather than silently returned once.
    std::vector<std::string> projection = {"id", "name", "id"};
    ASSERT_NOK_WITH_MSG(
        FormatTableRead::TEST_Create(table, projection, /*pool=*/nullptr, /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false),
        "appears more than once in the projection");

    // A partition column is read from the directory rather than from the file, and repeats the
    // same way.
    std::vector<std::string> repeated_partition = {"dt", "dt"};
    ASSERT_NOK_WITH_MSG(
        FormatTableRead::TEST_Create(table, repeated_partition, /*pool=*/nullptr,
                                     /*predicate=*/nullptr, /*enable_predicate_filter=*/false),
        "appears more than once in the projection");

    // A column the table does not have has nothing to read: the projection is refused where it is
    // given rather than coming back as a batch quietly missing a column.
    ASSERT_NOK_WITH_MSG(
        FormatTableRead::TEST_Create(table, std::vector<std::string>{"id", "nosuchcolumn"},
                                     /*pool=*/nullptr, /*predicate=*/nullptr,
                                     /*enable_predicate_filter=*/false),
        "is not a column of table");

    // And a projection has to name something.
    ASSERT_NOK_WITH_MSG(FormatTableRead::TEST_Create(table, std::vector<std::string>{},
                                                     /*pool=*/nullptr, /*predicate=*/nullptr,
                                                     /*enable_predicate_filter=*/false),
                        "requires at least one column to read");
}

TEST(FormatTableTest, TestPredicateIsCheckedTheWayEveryOtherReadPathChecksIt) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));

    // The same two checks `TableRead` runs, through the same validator: a caller that moves a
    // predicate between the managed and the format table path should not find one of them
    // accepting what the other refuses.
    std::shared_ptr<Predicate> literal_mismatch = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(int64_t{1}));
    ASSERT_NOK_WITH_MSG(FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                                     /*pool=*/nullptr, literal_mismatch,
                                                     /*enable_predicate_filter=*/false),
                        "mismatch field type");

    std::shared_ptr<Predicate> schema_mismatch = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT, Literal(int64_t{1}));
    Result<std::unique_ptr<FormatTableRead>> wrong_type =
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                     /*pool=*/nullptr, schema_mismatch,
                                     /*enable_predicate_filter=*/false);
    ASSERT_FALSE(wrong_type.ok());
    ASSERT_TRUE(wrong_type.status().IsInvalid()) << wrong_type.status().ToString();

    // The field index a predicate carries is not one of the checks: everything downstream
    // resolves a field by name, and a caller that built the predicate against the table cannot
    // know where a projection will put the column.
    std::shared_ptr<Predicate> wrong_index = PredicateBuilder::Equal(
        /*field_index=*/7, /*field_name=*/"id", FieldType::INT, Literal(1));
    ASSERT_OK(FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt,
                                           /*pool=*/nullptr, wrong_index,
                                           /*enable_predicate_filter=*/false));
}

TEST(FormatTableTest, TestPredicateMayOnlyNameColumnsTheReadProduces) {
    // One rule, the one `InternalReadContext` applies to a managed table: validated against the
    // read schema. An unknown field and a dropped one are refused alike, since nothing
    // downstream could evaluate either.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));

    // Not a column of the table at all.
    std::shared_ptr<Predicate> unknown = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"nope", FieldType::INT, Literal(1));
    ASSERT_NOK_WITH_MSG(
        FormatTableRead::TEST_Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr, unknown,
                                     /*enable_predicate_filter=*/false),
        "does not exist in schema");

    // A column of the table that this read does not produce.
    std::shared_ptr<Predicate> id_gt_1 = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(1));
    std::vector<std::string> projection = {"name", "dt"};
    ASSERT_NOK_WITH_MSG(FormatTableRead::TEST_Create(table, projection, /*pool=*/nullptr, id_gt_1,
                                                     /*enable_predicate_filter=*/true),
                        "does not exist in schema");
    ASSERT_NOK(FormatTableRead::TEST_Create(table, projection, /*pool=*/nullptr, id_gt_1,
                                            /*enable_predicate_filter=*/false));

    // Projected back in, and the same predicate is accepted.
    ASSERT_OK(FormatTableRead::TEST_Create(table, std::vector<std::string>{"id", "name", "dt"},
                                           /*pool=*/nullptr, id_gt_1,
                                           /*enable_predicate_filter=*/true));
}

TEST(FormatTableTest, TestAPartitionValueIsReadIntoItsColumnTypeBeforeItNamesADirectory) {
    // The declared partition is text, but the directory is named after the value it stands for:
    // read into the column's type and rendered back, as Java Paimon's writer does. So two
    // spellings of one value are one partition, not two half-filled directories.
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto int_schema =
        arrow::schema({arrow::field("name", arrow::utf8()), arrow::field("pt", arrow::int32())});
    SchemaManager schema_manager(dir->GetFileSystem(), dir->Str());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
                         schema_manager.CreateTable(
                             int_schema, /*partition_keys=*/{"pt"}, /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         FormatTable::Create(dir->GetFileSystem(), dir->Str(),
                                             Identifier("db", "tbl"), /*dynamic_options=*/{}));

    auto make_batch = [&int_schema](
                          const std::string& name,
                          const std::string& declared) -> Result<std::unique_ptr<RecordBatch>> {
        arrow::StringBuilder name_builder;
        arrow::Int32Builder pt_builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Append(name));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(pt_builder.Append(7));
        std::shared_ptr<arrow::Array> name_array;
        std::shared_ptr<arrow::Array> pt_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(name_builder.Finish(&name_array));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(pt_builder.Finish(&pt_array));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> struct_array,
            arrow::StructArray::Make({name_array, pt_array}, int_schema->fields()));
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
        RecordBatchBuilder builder(c_array.get());
        builder.SetPartition({{"pt", declared}});
        return builder.Finish();
    };

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    // `007` and `7` are the same partition once read into the column type, so both batches belong
    // to one directory however the caller spelled the value.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> padded, make_batch("alice", "007"));
    ASSERT_OK(write->Write(std::move(padded)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> plain, make_batch("bob", "7"));
    ASSERT_OK(write->Write(std::move(plain)));
    ASSERT_OK_AND_ASSIGN(std::vector<FormatCommitMessage> messages, write->PrepareCommit());

    // One partition, so one open file and one message, named after the value rather than after
    // either spelling.
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_EQ(messages[0].partition, (std::map<std::string, std::string>{{"pt", "7"}}));
    ASSERT_EQ(messages[0].record_count, 2);
    ASSERT_EQ(PathUtil::GetName(PathUtil::GetParentDirPath(messages[0].file_path)), "pt=7");

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableCommit> commit,
        FormatTableCommit::Create(table, /*overwrite=*/false, /*static_partition=*/{}));
    ASSERT_OK(commit->Commit(messages));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions, (PartitionList{{{"pt", "7"}}}));
}

TEST(FormatTableTest, TestLegacyPartitionNameDecidesHowADateIsWritten) {
    // `partition.legacy-name` decides how the declared partition is rendered back out, and DATE
    // is the only partition type allowed here that reads back differently under it: the day count
    // on, `YYYY-MM-DD` off. The table decides the directory, not the caller's spelling.
    constexpr int32_t kDaysTo20240101 = 19723;
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto date_schema =
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("dt", arrow::date32())});

    // `kDaysTo20240101` is a constant expression, so it is read without being captured.
    auto write_one = [&dir, &date_schema](
                         const std::string& path,
                         const std::map<std::string, std::string>& extra_options,
                         const std::string& declared) -> Result<FormatCommitMessage> {
        std::map<std::string, std::string> options = {{Options::TYPE, "format-table"},
                                                      {Options::FILE_FORMAT, "parquet"}};
        for (const auto& [key, value] : extra_options) {
            options[key] = value;
        }
        SchemaManager schema_manager(dir->GetFileSystem(), path);
        PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] std::unique_ptr<TableSchema> table_schema,
                               schema_manager.CreateTable(date_schema, /*partition_keys=*/{"dt"},
                                                          /*primary_keys=*/{}, options));
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FormatTable> table,
            FormatTable::Create(dir->GetFileSystem(), path, Identifier("db", "tbl"),
                                /*dynamic_options=*/{}));

        arrow::Int32Builder id_builder;
        arrow::Date32Builder dt_builder;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Append(1));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.Append(kDaysTo20240101));
        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> dt_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(id_builder.Finish(&id_array));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(dt_builder.Finish(&dt_array));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> struct_array,
            arrow::StructArray::Make({id_array, dt_array}, date_schema->fields()));
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, c_array.get()));
        RecordBatchBuilder batch_builder(c_array.get());
        batch_builder.SetPartition({{"dt", declared}});
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());

        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableWrite> write,
                               FormatTableWrite::Create(table, /*pool=*/nullptr));
        PAIMON_RETURN_NOT_OK(write->Write(std::move(batch)));
        PAIMON_ASSIGN_OR_RAISE(std::vector<FormatCommitMessage> messages, write->PrepareCommit());
        if (messages.size() != 1) {
            return Status::Invalid("expected exactly one written file");
        }
        FormatCommitMessage message = messages[0];
        PAIMON_RETURN_NOT_OK(write->Abort());
        return message;
    };

    // On by default, as everywhere else in paimon: a DATE reads back as its day count, so both
    // spellings name `dt=19723` and the commit message says so too.
    for (const char* declared : {"19723", "2024-01-01"}) {
        ASSERT_OK_AND_ASSIGN(FormatCommitMessage message,
                             write_one(dir->Str() + "/legacy-" + declared, {}, declared));
        ASSERT_EQ(message.partition, (std::map<std::string, std::string>{{"dt", "19723"}}))
            << declared;
        ASSERT_EQ(PathUtil::GetName(PathUtil::GetParentDirPath(message.file_path)), "dt=19723")
            << declared;
    }

    // Off, and the same two spellings name `dt=2024-01-01` instead.
    const std::map<std::string, std::string> not_legacy = {
        {Options::PARTITION_GENERATE_LEGACY_NAME, "false"}};
    for (const char* declared : {"19723", "2024-01-01"}) {
        ASSERT_OK_AND_ASSIGN(FormatCommitMessage message,
                             write_one(dir->Str() + "/iso-" + declared, not_legacy, declared));
        ASSERT_EQ(message.partition, (std::map<std::string, std::string>{{"dt", "2024-01-01"}}))
            << declared;
        ASSERT_EQ(PathUtil::GetName(PathUtil::GetParentDirPath(message.file_path)), "dt=2024-01-01")
            << declared;
    }

    // A value that is not a date at all still has nothing to name a directory after.
    ASSERT_NOK(write_one(dir->Str() + "/nonsense", {}, "not-a-date"));
}

TEST(FormatTableTest, TestWriteRejectsWrongPartitionSpec) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    // The batch names no partition, but the table is partitioned by one field.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240101", {}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    Status status = write->Write(std::move(batch));
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.IsInvalid());
    ASSERT_OK(write->Abort());
}

}  // namespace paimon::test
