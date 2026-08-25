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
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
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

/// The one call a `FailingWriteFileSystem`'s streams refuse.
enum class FailingStreamCall { kGetPos, kFlush, kClose };

/// Wraps a file system and hands out streams that fail one call, so that a write can be stopped
/// where a real store would stop it: after the writer is finished and while the file it produced
/// is still hidden.
class FailingWriteFileSystem : public FileSystem {
 public:
    FailingWriteFileSystem(const std::shared_ptr<FileSystem>& delegate, FailingStreamCall failing)
        : delegate_(delegate), failing_(failing) {}

    using FileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return delegate_->Open(path);
    }
    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                               delegate_->Create(path, overwrite));
        return std::unique_ptr<OutputStream>(new FailingOutputStream(std::move(out), failing_));
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
        FailingOutputStream(std::unique_ptr<OutputStream> delegate, FailingStreamCall failing)
            : delegate_(std::move(delegate)), failing_(failing) {}

        Result<int64_t> Write(const char* buffer, int64_t size) override {
            return delegate_->Write(buffer, size);
        }
        Status Flush() override {
            if (failing_ == FailingStreamCall::kFlush) {
                return Status::IOError("injected flush failure");
            }
            return delegate_->Flush();
        }
        Result<int64_t> GetPos() const override {
            if (failing_ == FailingStreamCall::kGetPos) {
                return Status::IOError("injected get position failure");
            }
            return delegate_->GetPos();
        }
        Result<std::string> GetUri() const override {
            return delegate_->GetUri();
        }
        Status Close() override {
            if (failing_ == FailingStreamCall::kClose) {
                // Closed all the same, so the file it wrote can still be removed.
                [[maybe_unused]] Status closed = delegate_->Close();
                return Status::IOError("injected close failure");
            }
            return delegate_->Close();
        }

     private:
        std::unique_ptr<OutputStream> delegate_;
        FailingStreamCall failing_;
    };

    std::shared_ptr<FileSystem> delegate_;
    FailingStreamCall failing_;
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
    return FormatTable::Create(file_system, path, Identifier("db", "tbl"));
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
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr, predicate,
                                enable_predicate_filter));
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
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        FormatTable::Create(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
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
    ASSERT_FALSE(created.ok());
    ASSERT_NE(std::string::npos, created.status().ToString().find("cannot be partitioned"))
        << created.status().ToString();

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
                            /*location_carries_paimon_metadata=*/true);
    ASSERT_FALSE(opened.ok());
    ASSERT_NE(std::string::npos, opened.status().ToString().find("cannot be partitioned"))
        << opened.status().ToString();
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
                            /*location_carries_paimon_metadata=*/false),
        "requires a location");

    // The file system root is its own separator, so what is below it starts one character in.
    // Reading it two characters in would take `/data.parquet` for `ata.parquet`, and
    // `/.hidden.parquet` for a name that is not hidden at all.
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> root_table,
        FormatTable::Create(dir->GetFileSystem(), "/", Identifier("db", "tbl"), data_schema,
                            /*location_carries_paimon_metadata=*/false));
    ASSERT_EQ(root_table->Location(), "/");
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::Create(root_table, /*projection=*/std::nullopt, /*pool=*/nullptr,
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> trailing,
                         FormatTable::Create(dir->GetFileSystem(), dir->Str() + "/",
                                             Identifier("db", "tbl"), data_schema,
                                             /*location_carries_paimon_metadata=*/false));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> trailing_read,
        FormatTableRead::Create(trailing, /*projection=*/std::nullopt, /*pool=*/nullptr,
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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         FormatTable::Create(dir->GetFileSystem(), dir->Str() + "/",
                                             Identifier("db", "tbl"), data_schema,
                                             /*location_carries_paimon_metadata=*/true));

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
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> external,
                         FormatTable::Create(data_dir->GetFileSystem(), data_dir->Str(),
                                             Identifier("db", "tbl"), data_schema,
                                             /*location_carries_paimon_metadata=*/false));
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
    Result<std::shared_ptr<FormatTable>> table =
        FormatTable::Create(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl"));
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
                             FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl")));

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
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FormatTable> plain_table,
            FormatTable::Create(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));
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
                         FormatTable::Create(failing_fs, dir->Str(), Identifier("db", "tbl")));
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
                         FormatTable::Create(recording, dir->Str(), Identifier("db", "tbl")));

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

    // Each partition is its own directory, so each is its own split.
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->Splits().size(), 2);

    // The partition column is rebuilt from the directory name, not read from the file.
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    ASSERT_EQ(rows.size(), 2);
    ASSERT_EQ(rows[0], "1|alice|20240101");
    ASSERT_EQ(rows[1], "2|bob|20240102");

    ASSERT_OK_AND_ASSIGN(PartitionList partitions, scan->ListPartitions());
    ASSERT_EQ(partitions.size(), 2);
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
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
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
        FormatTableRead::Create(table, projection, /*pool=*/nullptr, /*predicate=*/nullptr,
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
        FormatTableRead::Create(table, projection, /*pool=*/nullptr, /*predicate=*/nullptr,
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
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
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
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
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
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                /*predicate=*/nullptr,
                                /*enable_predicate_filter=*/false));
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::make_shared<NotAFormatSplit>()),
                        "only accepts a FormatDataSplit");
    ASSERT_NOK_WITH_MSG(read->CreateReader(std::shared_ptr<Split>()),
                        "only accepts a FormatDataSplit");

    // A partition value the column type cannot hold. The directory name and the split agree, so
    // nothing before this notices; only reading the value into its type does. A split is a public
    // struct, so this has to be a refusal rather than an assumption.
    std::unique_ptr<UniqueTestDirectory> int_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(int_dir);
    auto int_schema =
        arrow::schema({arrow::field("name", arrow::utf8()), arrow::field("pt", arrow::int32())});
    SchemaManager schema_manager(int_dir->GetFileSystem(), int_dir->Str());
    ASSERT_OK_AND_ASSIGN([[maybe_unused]] std::unique_ptr<TableSchema> int_table_schema,
                         schema_manager.CreateTable(
                             int_schema, /*partition_keys=*/{"pt"}, /*primary_keys=*/{},
                             {{Options::TYPE, "format-table"}, {Options::FILE_FORMAT, "parquet"}}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> int_table,
        FormatTable::Create(int_dir->GetFileSystem(), int_dir->Str(), Identifier("db", "tbl")));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> int_read,
        FormatTableRead::Create(int_table, /*projection=*/std::nullopt, /*pool=*/nullptr,
                                /*predicate=*/nullptr,
                                /*enable_predicate_filter=*/false));
    auto not_an_int = std::make_shared<FormatDataSplit>(
        std::vector<FormatDataSplit::FileMeta>{{int_dir->Str() + "/pt=abc/data-a-0.parquet", 1}},
        std::map<std::string, std::string>{{"pt", "abc"}});
    ASSERT_NOK(int_read->CreateReader(std::static_pointer_cast<Split>(not_an_int)));
}

TEST(FormatTableTest, TestReadRefusesASplitFromOutsideTheTable) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableRead> read,
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
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

TEST(FormatTableTest, TestWriteRejectsRowsFromAnotherPartition) {
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatTable> table,
                         CreateTable(dir->GetFileSystem(), dir->Str(), {"dt"}));
    // The rows say they belong to 20240102 while the batch declares 20240101. The partition
    // column is not written, so accepting this would file the rows under the wrong partition and
    // lose their real value.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         MakeBatch({1}, {"alice"}, "20240102", {{"dt", "20240101"}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatTableWrite> write,
                         FormatTableWrite::Create(table, /*pool=*/nullptr));
    Status status = write->Write(std::move(batch));
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.IsInvalid());
    ASSERT_OK(write->Abort());
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

TEST(FormatTableTest, TestOverwriteReplacesThePartitionsItWrites) {
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
    ASSERT_OK(WriteAndCommit(table, std::move(replacement), /*overwrite=*/true));

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FormatTableScan> scan,
        FormatTableScan::Create(table, /*partition_filter=*/{}, /*limit=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> rows, ReadAll(table, plan->Splits()));
    std::sort(rows.begin(), rows.end());
    // Only the partition written to is replaced; the one this commit never touched is left alone.
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
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr,
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
        FormatTableRead::Create(table, projection, /*pool=*/nullptr, /*predicate=*/nullptr,
                                /*enable_predicate_filter=*/false),
        "appears more than once in the projection");

    // A partition column is read from the directory rather than from the file, and repeats the
    // same way.
    std::vector<std::string> repeated_partition = {"dt", "dt"};
    ASSERT_NOK_WITH_MSG(
        FormatTableRead::Create(table, repeated_partition, /*pool=*/nullptr, /*predicate=*/nullptr,
                                /*enable_predicate_filter=*/false),
        "appears more than once in the projection");

    // A column the table does not have has nothing to read: the projection is refused where it is
    // given rather than coming back as a batch quietly missing a column.
    ASSERT_NOK_WITH_MSG(
        FormatTableRead::Create(table, std::vector<std::string>{"id", "nosuchcolumn"},
                                /*pool=*/nullptr, /*predicate=*/nullptr,
                                /*enable_predicate_filter=*/false),
        "is not a column of table");

    // And a projection has to name something.
    ASSERT_NOK_WITH_MSG(FormatTableRead::Create(table, std::vector<std::string>{},
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
    ASSERT_NOK_WITH_MSG(FormatTableRead::Create(table, /*projection=*/std::nullopt,
                                                /*pool=*/nullptr, literal_mismatch,
                                                /*enable_predicate_filter=*/false),
                        "mismatch field type");

    std::shared_ptr<Predicate> schema_mismatch = PredicateBuilder::Equal(
        /*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT, Literal(int64_t{1}));
    Result<std::unique_ptr<FormatTableRead>> wrong_type =
        FormatTableRead::Create(table, /*projection=*/std::nullopt,
                                /*pool=*/nullptr, schema_mismatch,
                                /*enable_predicate_filter=*/false);
    ASSERT_FALSE(wrong_type.ok());
    ASSERT_TRUE(wrong_type.status().IsInvalid()) << wrong_type.status().ToString();

    // The field index a predicate carries is not one of the checks: everything downstream
    // resolves a field by name, and a caller that built the predicate against the table cannot
    // know where a projection will put the column.
    std::shared_ptr<Predicate> wrong_index = PredicateBuilder::Equal(
        /*field_index=*/7, /*field_name=*/"id", FieldType::INT, Literal(1));
    ASSERT_OK(FormatTableRead::Create(table, /*projection=*/std::nullopt,
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
        FormatTableRead::Create(table, /*projection=*/std::nullopt, /*pool=*/nullptr, unknown,
                                /*enable_predicate_filter=*/false),
        "does not exist in schema");

    // A column of the table that this read does not produce.
    std::shared_ptr<Predicate> id_gt_1 = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(1));
    std::vector<std::string> projection = {"name", "dt"};
    ASSERT_NOK_WITH_MSG(FormatTableRead::Create(table, projection, /*pool=*/nullptr, id_gt_1,
                                                /*enable_predicate_filter=*/true),
                        "does not exist in schema");
    ASSERT_NOK(FormatTableRead::Create(table, projection, /*pool=*/nullptr, id_gt_1,
                                       /*enable_predicate_filter=*/false));

    // Projected back in, and the same predicate is accepted.
    ASSERT_OK(FormatTableRead::Create(table, std::vector<std::string>{"id", "name", "dt"},
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
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FormatTable> table,
        FormatTable::Create(dir->GetFileSystem(), dir->Str(), Identifier("db", "tbl")));

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
    // Aliased, or the comma inside the type would read as a second macro argument.
    using PartitionList = std::vector<std::map<std::string, std::string>>;
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
            FormatTable::Create(dir->GetFileSystem(), path, Identifier("db", "tbl")));

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
