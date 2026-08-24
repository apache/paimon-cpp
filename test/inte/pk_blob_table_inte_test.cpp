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
#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/managed_blob_reference_file.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/data/blob.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

/// One read-back row of the (pk, v, b) table; nullopt marks a NULL cell.
using PkBlobRow = std::tuple<std::string, std::optional<int32_t>, std::optional<std::string>>;

/// End-to-end tests for table-managed BLOB storage in primary-key tables: payloads are
/// externalized to `.managed.blob` packs at write time, data files carry `.blobref` sidecars,
/// reads resolve descriptors back to payload bytes, and compaction rewrites descriptors
/// verbatim while rebuilding the exact reference set of the surviving rows.
class PkBlobTableInteTest : public ::testing::Test,
                            public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create("local");
        fields_ = {arrow::field("pk", arrow::utf8(), /*nullable=*/false),
                   arrow::field("v", arrow::int32()),
                   BlobUtils::ToArrowField("b", /*nullable=*/true)};
    }

    void TearDown() override {
        dir_.reset();
    }

    std::string FileFormat() const {
        return GetParam();
    }

    void CreateTable(const std::map<std::string, std::string>& extra_options) {
        std::map<std::string, std::string> options = {{Options::FILE_FORMAT, FileFormat()},
                                                      {Options::FILE_SYSTEM, "local"},
                                                      {Options::BUCKET, "1"}};
        options.insert(extra_options.begin(), extra_options.end());
        auto schema = arrow::schema(fields_);
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir_->Str(), options));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                       /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, options,
                                       /*ignore_if_exists=*/false));
    }

    std::string TablePath() const {
        return PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    }

    std::string BucketPath() const {
        return PathUtil::JoinPath(TablePath(), "bucket-0");
    }

    std::shared_ptr<arrow::Array> MakeArray(const std::vector<PkBlobRow>& rows) {
        arrow::StringBuilder pk_builder;
        arrow::Int32Builder v_builder;
        arrow::LargeBinaryBuilder b_builder;
        for (const auto& [pk, v, blob] : rows) {
            EXPECT_TRUE(pk_builder.Append(pk).ok());
            if (v) {
                EXPECT_TRUE(v_builder.Append(v.value()).ok());
            } else {
                EXPECT_TRUE(v_builder.AppendNull().ok());
            }
            if (blob) {
                EXPECT_TRUE(b_builder.Append(blob.value()).ok());
            } else {
                EXPECT_TRUE(b_builder.AppendNull().ok());
            }
        }
        std::shared_ptr<arrow::Array> pk_array;
        std::shared_ptr<arrow::Array> v_array;
        std::shared_ptr<arrow::Array> b_array;
        EXPECT_TRUE(pk_builder.Finish(&pk_array).ok());
        EXPECT_TRUE(v_builder.Finish(&v_array).ok());
        EXPECT_TRUE(b_builder.Finish(&b_array).ok());
        return arrow::StructArray::Make({pk_array, v_array, b_array}, fields_).ValueOrDie();
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArray(
        const std::shared_ptr<arrow::Array>& write_array, int64_t commit_identifier,
        const std::vector<RecordBatch::RowKind>& row_kinds = {}) const {
        WriteContextBuilder write_builder(TablePath(), "commit_user_1");
        write_builder.WithStreamingMode(true)
            .WithTempDirectory(PathUtil::JoinPath(dir_->Str(), "tmp"))
            .AddOption(Options::WRITE_ONLY, "true");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*write_array, &c_array));
        auto record_batch = std::make_unique<RecordBatch>(std::map<std::string, std::string>(),
                                                          /*bucket=*/0, row_kinds, &c_array);
        PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> commit_msgs,
                               file_store_write->PrepareCommit(
                                   /*wait_compaction=*/false, commit_identifier));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return commit_msgs;
    }

    Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        CommitContextBuilder commit_builder(TablePath(), "commit_user_1");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        return file_store_commit->Commit(commit_msgs);
    }

    Status WriteAndCommit(const std::vector<PkBlobRow>& rows, int64_t commit_identifier,
                          const std::vector<RecordBatch::RowKind>& row_kinds = {}) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> commit_msgs,
                               WriteArray(MakeArray(rows), commit_identifier, row_kinds));
        return Commit(commit_msgs);
    }

    /// Full-compacts the single bucket and returns the commit messages *without* committing
    /// them. `also_write`, when set, is written through the same writer first, so one message
    /// carries both newly written files and a compaction output.
    Result<std::vector<std::shared_ptr<CommitMessage>>> FullCompact(
        int64_t commit_identifier,
        const std::shared_ptr<arrow::Array>& also_write = nullptr) const {
        WriteContextBuilder write_builder(TablePath(), "commit_user_1");
        write_builder.WithStreamingMode(true).WithTempDirectory(
            PathUtil::JoinPath(dir_->Str(), "tmp"));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        if (also_write != nullptr) {
            ArrowArray c_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*also_write, &c_array));
            auto record_batch =
                std::make_unique<RecordBatch>(std::map<std::string, std::string>(), /*bucket=*/0,
                                              std::vector<RecordBatch::RowKind>(), &c_array);
            PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        }
        PAIMON_RETURN_NOT_OK(file_store_write->Compact(/*partition=*/{}, /*bucket=*/0,
                                                       /*full_compaction=*/true));
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::shared_ptr<CommitMessage>> commit_messages,
            file_store_write->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return commit_messages;
    }

    Status FullCompactAndCommit(int64_t commit_identifier) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> commit_messages,
                               FullCompact(commit_identifier));
        return Commit(commit_messages);
    }

    /// Reads every row through the public read path (rows come back in key order within the
    /// single bucket). With `blob_as_descriptor` the managed blob column holds the serialized
    /// descriptors instead of the payload bytes. With `prefetch` the data file is read through
    /// the prefetch reader and the shared read-ahead cache instead of a plain stream.
    void ReadRows(bool blob_as_descriptor, std::vector<PkBlobRow>* rows, bool prefetch = false) {
        rows->clear();
        std::map<std::string, std::string> options = {{Options::FILE_SYSTEM, "local"}};
        if (blob_as_descriptor) {
            options.emplace(Options::BLOB_AS_DESCRIPTOR, "true");
        }
        ScanContextBuilder scan_context_builder(TablePath());
        scan_context_builder.WithStreamingMode(false).SetOptions(options).AddOption(
            Options::SCAN_MODE, StartupMode::LatestFull().ToString());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                             scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> result_plan, table_scan->CreatePlan());

        ReadContextBuilder read_context_builder(TablePath());
        read_context_builder.SetOptions(options);
        if (prefetch) {
            read_context_builder.EnablePrefetch(true).SetReadAheadCacheEnabled(true);
        }
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                             TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> batch_reader,
                             table_read->CreateReader(result_plan->Splits()));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        // The collector reports an empty read (no batches at all) as a null chunked array.
        if (result == nullptr) {
            return;
        }
        // Copy the cell values out while the reader that owns the buffers is still alive.
        for (const std::shared_ptr<arrow::Array>& chunk : result->chunks()) {
            auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(chunk);
            ASSERT_TRUE(struct_array != nullptr);
            auto pk_array =
                std::dynamic_pointer_cast<arrow::StringArray>(struct_array->GetFieldByName("pk"));
            auto v_array =
                std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->GetFieldByName("v"));
            auto b_array = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(
                struct_array->GetFieldByName("b"));
            ASSERT_TRUE(pk_array != nullptr);
            ASSERT_TRUE(v_array != nullptr);
            ASSERT_TRUE(b_array != nullptr);
            for (int64_t row = 0; row < struct_array->length(); row++) {
                std::optional<int32_t> v;
                if (!v_array->IsNull(row)) {
                    v = v_array->Value(row);
                }
                std::optional<std::string> blob;
                if (!b_array->IsNull(row)) {
                    blob = std::string(b_array->GetView(row));
                }
                rows->emplace_back(std::string(pk_array->GetView(row)), v, std::move(blob));
            }
        }
    }

    /// The live data files of the single bucket, from a fresh scan.
    void CollectDataFiles(std::vector<std::shared_ptr<DataFileMeta>>* files) {
        files->clear();
        std::map<std::string, std::string> options = {{Options::FILE_SYSTEM, "local"}};
        ScanContextBuilder scan_context_builder(TablePath());
        scan_context_builder.WithStreamingMode(false).SetOptions(options).AddOption(
            Options::SCAN_MODE, StartupMode::LatestFull().ToString());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context,
                             scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> result_plan, table_scan->CreatePlan());
        for (const auto& split : result_plan->Splits()) {
            auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
            ASSERT_TRUE(split_impl != nullptr);
            files->insert(files->end(), split_impl->DataFiles().begin(),
                          split_impl->DataFiles().end());
        }
    }

    /// Reads the `.blobref` sidecar carried in `file`'s extra files.
    void ReadSidecar(const std::shared_ptr<DataFileMeta>& file,
                     std::vector<ManagedBlobReferenceFile::Reference>* references) {
        references->clear();
        ASSERT_EQ(file->extra_files.size(), 1);
        ASSERT_TRUE(file->extra_files[0].has_value());
        const std::string& sidecar_name = file->extra_files[0].value();
        ASSERT_TRUE(
            StringUtils::EndsWith(sidecar_name, ManagedBlobReferenceFile::kReferenceFileSuffix));
        ASSERT_EQ(sidecar_name, file->file_name + ManagedBlobReferenceFile::kReferenceFileSuffix);
        ASSERT_OK_AND_ASSIGN(*references, ManagedBlobReferenceFile::Read(
                                              dir_->GetFileSystem(),
                                              PathUtil::JoinPath(BucketPath(), sidecar_name)));
    }

    /// Asserts the bucket holds a single data file whose sidecar references exactly
    /// `expected_packs`. Reads deduplicate, so the set is the full reference list.
    void AssertSingleFileSidecar(const std::set<std::string>& expected_packs) {
        std::vector<std::shared_ptr<DataFileMeta>> files;
        CollectDataFiles(&files);
        ASSERT_EQ(files.size(), 1);
        std::vector<ManagedBlobReferenceFile::Reference> references;
        ReadSidecar(files[0], &references);
        std::set<std::string> referenced;
        for (const auto& reference : references) {
            referenced.insert(reference.ToString());
        }
        ASSERT_EQ(referenced, expected_packs);
    }

    bool FileExists(const std::string& path) {
        return dir_->GetFileSystem()->GetFileStatus(path).ok();
    }

    /// The bucket's files whose name ends with `suffix`, sorted, as bare file names.
    std::vector<std::string> BucketFilesWithSuffix(const std::string& suffix) {
        std::vector<BasicFileStatus> statuses;
        EXPECT_TRUE(dir_->GetFileSystem()->ListDir(BucketPath(), &statuses).ok());
        std::vector<std::string> names;
        for (const auto& status : statuses) {
            std::string name = PathUtil::GetName(status.GetPath());
            if (!status.IsDir() && StringUtils::EndsWith(name, suffix)) {
                names.push_back(std::move(name));
            }
        }
        std::sort(names.begin(), names.end());
        return names;
    }

    Status Abort(const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        CommitContextBuilder commit_builder(TablePath(), "commit_user_1");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        return file_store_commit->Abort(commit_msgs);
    }

    /// Resolves serialized descriptor bytes to (payload, pack uri).
    Result<std::string> ResolveDescriptor(const std::string& descriptor_bytes, std::string* uri) {
        PAIMON_ASSIGN_OR_RAISE(
            bool is_descriptor,
            BlobDescriptor::IsBlobDescriptor(descriptor_bytes.data(), descriptor_bytes.size()));
        if (!is_descriptor) {
            return Status::Invalid("value is not a serialized blob descriptor");
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<Blob> blob,
            Blob::FromDescriptor(descriptor_bytes.data(), descriptor_bytes.size()));
        *uri = blob->Uri();
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> payload,
                               blob->ToData(dir_->GetFileSystem(), pool_));
        return std::string(payload->data(), payload->size());
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    arrow::FieldVector fields_;
};

TEST_P(PkBlobTableInteTest, TestWriteAndReadManagedBlob) {
    CreateTable({});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("payload-1")}, {"k2", 2, std::nullopt}},
                             /*commit_identifier=*/0));

    // The default read resolves the managed blob descriptors back to payload bytes.
    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 1, std::string("payload-1")},
                                       {"k2", 2, std::nullopt}};
    ASSERT_EQ(rows, expected);

    // With blob-as-descriptor the column holds serialized descriptors that resolve to the
    // same payload through a ranged read of the pack.
    ReadRows(/*blob_as_descriptor=*/true, &rows);
    ASSERT_EQ(rows.size(), 2);
    ASSERT_TRUE(std::get<2>(rows[0]).has_value());
    std::string pack_uri;
    ASSERT_OK_AND_ASSIGN(std::string payload,
                         ResolveDescriptor(std::get<2>(rows[0]).value(), &pack_uri));
    ASSERT_EQ(payload, "payload-1");
    ASSERT_TRUE(StringUtils::EndsWith(PathUtil::GetName(pack_uri),
                                      ManagedBlobReferenceFile::kManagedBlobSuffix));
    ASSERT_TRUE(FileExists(pack_uri));
    ASSERT_FALSE(std::get<2>(rows[1]).has_value());

    // The single data file carries a sidecar listing exactly the referenced pack.
    AssertSingleFileSidecar({pack_uri});
}

TEST_P(PkBlobTableInteTest, TestReadWithPrefetchAndReadAheadCache) {
    // The data file is read through the prefetch reader and the shared read-ahead cache, while
    // the managed packs are opened directly - `AbstractSplitRead::CreateFileBatchReader` keeps
    // the blob format off the prefetch path. Descriptor resolution sits above both, so neither
    // may change what a read hands back.
    CreateTable({});
    // The last payload is larger than one blob copy buffer, so externalizing and resolving it
    // both span several chunks.
    std::string large_payload(64 * 1024, 'x');
    ASSERT_OK(WriteAndCommit(
        {{"k1", 1, std::string("payload-1")}, {"k2", 2, std::nullopt}, {"k3", 3, large_payload}},
        /*commit_identifier=*/0));

    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows, /*prefetch=*/true);
    std::vector<PkBlobRow> expected = {
        {"k1", 1, std::string("payload-1")}, {"k2", 2, std::nullopt}, {"k3", 3, large_payload}};
    ASSERT_EQ(rows, expected);

    // Asking for the descriptors instead resolves to the very same bytes, through a pack the
    // prefetch path never touched.
    ReadRows(/*blob_as_descriptor=*/true, &rows, /*prefetch=*/true);
    ASSERT_EQ(rows.size(), 3);
    ASSERT_TRUE(std::get<2>(rows[2]).has_value());
    std::string pack_uri;
    ASSERT_OK_AND_ASSIGN(std::string payload,
                         ResolveDescriptor(std::get<2>(rows[2]).value(), &pack_uri));
    ASSERT_EQ(payload, large_payload);
    ASSERT_TRUE(FileExists(pack_uri));
    ASSERT_FALSE(std::get<2>(rows[1]).has_value());
}

TEST_P(PkBlobTableInteTest, TestCompactionRebuildsExactBlobReferences) {
    // A one-byte blob target size seals a pack per value, so the reference sets of the
    // surviving and the overwritten payloads are distinguishable.
    CreateTable({{Options::BLOB_TARGET_FILE_SIZE, "1"}});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("v1-old")}}, /*commit_identifier=*/0));
    ASSERT_OK(WriteAndCommit({{"k1", 2, std::string("v1-new")}, {"k2", 3, std::string("k2-val")}},
                             /*commit_identifier=*/1));

    // The merged pre-compaction descriptors: the newest version wins.
    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/true, &rows);
    ASSERT_EQ(rows.size(), 2);
    ASSERT_TRUE(std::get<2>(rows[0]).has_value());
    ASSERT_TRUE(std::get<2>(rows[1]).has_value());
    std::string k1_descriptor_before = std::get<2>(rows[0]).value();

    ASSERT_OK(FullCompactAndCommit(/*commit_identifier=*/2));

    // The compacted file rewrites descriptors verbatim: same descriptor bytes, same pack.
    ReadRows(/*blob_as_descriptor=*/true, &rows);
    ASSERT_EQ(rows.size(), 2);
    ASSERT_TRUE(std::get<2>(rows[0]).has_value());
    ASSERT_EQ(std::get<2>(rows[0]).value(), k1_descriptor_before);
    std::string k1_uri;
    std::string k2_uri;
    ASSERT_OK_AND_ASSIGN(std::string k1_payload,
                         ResolveDescriptor(std::get<2>(rows[0]).value(), &k1_uri));
    ASSERT_OK_AND_ASSIGN(std::string k2_payload,
                         ResolveDescriptor(std::get<2>(rows[1]).value(), &k2_uri));
    ASSERT_EQ(k1_payload, "v1-new");
    ASSERT_EQ(k2_payload, "k2-val");

    // The rebuilt sidecar lists exactly the packs of the surviving rows; the overwritten
    // payload's pack is no longer referenced.
    AssertSingleFileSidecar({k1_uri, k2_uri});

    // The full read still resolves the payloads.
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 2, std::string("v1-new")},
                                       {"k2", 3, std::string("k2-val")}};
    ASSERT_EQ(rows, expected);
}

TEST_P(PkBlobTableInteTest, TestFirstRowManagedBlobKeepsFirstValue) {
    CreateTable({{Options::MERGE_ENGINE, "first-row"}, {Options::BLOB_TARGET_FILE_SIZE, "1"}});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("first")}}, /*commit_identifier=*/0));
    ASSERT_OK(WriteAndCommit({{"k1", 2, std::string("second")}}, /*commit_identifier=*/1));

    // Batch reads of a first-row table only see compacted (level > 0) files, so the
    // write-only level-0 commits are not visible yet.
    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    ASSERT_TRUE(rows.empty());

    ASSERT_OK(FullCompactAndCommit(/*commit_identifier=*/2));

    // first-row keeps the first payload; the later write only produced an unused pack.
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 1, std::string("first")}};
    ASSERT_EQ(rows, expected);

    // After compaction the sidecar holds exactly the first payload's pack: the reference to
    // the losing write's pack is gone.
    ReadRows(/*blob_as_descriptor=*/true, &rows);
    ASSERT_EQ(rows.size(), 1);
    ASSERT_TRUE(std::get<2>(rows[0]).has_value());
    std::string first_uri;
    ASSERT_OK_AND_ASSIGN(std::string payload,
                         ResolveDescriptor(std::get<2>(rows[0]).value(), &first_uri));
    ASSERT_EQ(payload, "first");
    AssertSingleFileSidecar({first_uri});
}

TEST_P(PkBlobTableInteTest, TestPartialUpdateManagedBlob) {
    CreateTable({{Options::MERGE_ENGINE, "partial-update"}});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("payload")}}, /*commit_identifier=*/0));
    // Partial update: a NULL cell leaves the previous value in place, so the merged row keeps
    // the descriptor externalized by the first write. The second write produces no pack.
    ASSERT_OK(WriteAndCommit({{"k1", 2, std::nullopt}}, /*commit_identifier=*/1));

    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 2, std::string("payload")}};
    ASSERT_EQ(rows, expected);

    ASSERT_OK(FullCompactAndCommit(/*commit_identifier=*/2));

    ReadRows(/*blob_as_descriptor=*/false, &rows);
    ASSERT_EQ(rows, expected);

    // The compacted file's sidecar lists exactly the surviving payload's pack.
    ReadRows(/*blob_as_descriptor=*/true, &rows);
    ASSERT_EQ(rows.size(), 1);
    ASSERT_TRUE(std::get<2>(rows[0]).has_value());
    std::string pack_uri;
    ASSERT_OK_AND_ASSIGN(std::string payload,
                         ResolveDescriptor(std::get<2>(rows[0]).value(), &pack_uri));
    ASSERT_EQ(payload, "payload");
    AssertSingleFileSidecar({pack_uri});
}

TEST_P(PkBlobTableInteTest, TestSnapshotExpirationRemovesSidecar) {
    CreateTable({});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("one")}}, /*commit_identifier=*/0));
    ASSERT_OK(WriteAndCommit({{"k2", 2, std::string("two")}}, /*commit_identifier=*/1));

    std::vector<std::shared_ptr<DataFileMeta>> files;
    CollectDataFiles(&files);
    ASSERT_EQ(files.size(), 2);
    std::vector<std::string> old_data_paths;
    std::vector<std::string> old_sidecar_paths;
    std::set<std::string> pack_paths;
    for (const auto& file : files) {
        old_data_paths.push_back(PathUtil::JoinPath(BucketPath(), file->file_name));
        std::vector<ManagedBlobReferenceFile::Reference> references;
        ReadSidecar(file, &references);
        // ReadSidecar's assertions only return from the helper, so stop here rather than
        // dereference the extra file it just failed to validate.
        ASSERT_FALSE(HasFatalFailure());
        old_sidecar_paths.push_back(PathUtil::JoinPath(BucketPath(), file->extra_files[0].value()));
        for (const auto& reference : references) {
            pack_paths.insert(reference.ToString());
        }
    }
    ASSERT_EQ(pack_paths.size(), 2);

    // Rewrite both files into one; snapshot 3 marks the two originals as deleted.
    ASSERT_OK(FullCompactAndCommit(/*commit_identifier=*/2));

    CommitContextBuilder commit_builder(TablePath(), "commit_user_1");
    commit_builder.SetOptions({{Options::FILE_SYSTEM, "local"},
                               {Options::SNAPSHOT_NUM_RETAINED_MAX, "1"},
                               {Options::SNAPSHOT_NUM_RETAINED_MIN, "1"}});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, commit_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> file_store_commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK_AND_ASSIGN(int32_t expired_snapshots, file_store_commit->Expire());
    ASSERT_EQ(expired_snapshots, 2);

    // The expired data files took their `.blobref` sidecars with them, while the shared packs
    // are only referenced and never deleted by snapshot expiration: the compacted file still
    // resolves its payloads from them.
    for (const auto& path : old_data_paths) {
        EXPECT_FALSE(FileExists(path));
    }
    for (const auto& path : old_sidecar_paths) {
        EXPECT_FALSE(FileExists(path));
    }
    for (const auto& path : pack_paths) {
        EXPECT_TRUE(FileExists(path));
    }
    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 1, std::string("one")},
                                       {"k2", 2, std::string("two")}};
    ASSERT_EQ(rows, expected);
}

TEST_P(PkBlobTableInteTest, TestDeleteDropsRow) {
    CreateTable({});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("payload")}}, /*commit_identifier=*/0));

    // A retract row never keeps a payload; the delete removes the key entirely on read.
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::nullopt}}, /*commit_identifier=*/1,
                             {RecordBatch::RowKind::DELETE}));

    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    ASSERT_TRUE(rows.empty());
}

TEST_P(PkBlobTableInteTest, TestAbortOfACompactionKeepsTheHistoricalPacks) {
    // The counterpart of the append rollback below, and the reason a rollback cannot derive
    // pack ownership from the `.blobref` sidecars of the files it removes: compaction rewrites
    // blob descriptors verbatim, so its output references the packs of the files it merged.
    // Deleting those would break the snapshot the compaction was planned against, which the
    // rollback never touched.
    CreateTable({{Options::BLOB_TARGET_FILE_SIZE, "1"}});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("v1")}}, /*commit_identifier=*/0));
    ASSERT_OK(WriteAndCommit({{"k2", 2, std::string("v2")}}, /*commit_identifier=*/1));
    std::vector<std::string> packs_before =
        BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix);
    ASSERT_EQ(packs_before.size(), 2);

    // The compaction produces an output file but no pack of its own, so it owns nothing to roll
    // back.
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> compact_msgs,
                         FullCompact(/*commit_identifier=*/2));
    ASSERT_OK(Abort(compact_msgs));

    ASSERT_EQ(BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix), packs_before);
    // The pre-compaction snapshot is still the latest one, and its payloads still resolve.
    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 1, std::string("v1")}, {"k2", 2, std::string("v2")}};
    ASSERT_EQ(rows, expected);
}

TEST_P(PkBlobTableInteTest, TestAbortOfAWriteAndCompactionKeepsOnlyTheHistoricalPacks) {
    // One message carrying both: newly written rows, whose packs this writer created, and a
    // compaction output, whose packs it only inherited. The rollback has to split them.
    CreateTable({{Options::BLOB_TARGET_FILE_SIZE, "1"}});
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("v1")}}, /*commit_identifier=*/0));
    ASSERT_OK(WriteAndCommit({{"k2", 2, std::string("v2")}}, /*commit_identifier=*/1));
    std::vector<std::string> packs_before =
        BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix);
    ASSERT_EQ(packs_before.size(), 2);

    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        FullCompact(/*commit_identifier=*/2, MakeArray({{"k3", 3, std::string("v3")}})));
    // The write created a third pack before the rollback runs.
    ASSERT_EQ(BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix).size(), 3);

    ASSERT_OK(Abort(messages));
    // Exactly the new one is gone; the two the compaction merely referenced stay.
    ASSERT_EQ(BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix), packs_before);
    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 1, std::string("v1")}, {"k2", 2, std::string("v2")}};
    ASSERT_EQ(rows, expected);
}

TEST_P(PkBlobTableInteTest, TestAbortDeletesTheManagedBlobPacksItRollsBack) {
    CreateTable({});
    // PrepareCommit seals the packs and hands them over: from here on the writer no longer
    // deletes them, so whoever gives up on the commit has to. Nothing else ever collects a
    // `.managed.blob` — orphan file cleaning skips them on purpose, because a pack may be
    // shared by several data files — so a pack the rollback leaves behind leaks for good.
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> commit_msgs,
                         WriteArray(MakeArray({{"k1", 1, std::string("payload-1")},
                                               {"k2", 2, std::string("payload-2")}}),
                                    /*commit_identifier=*/0));
    std::vector<std::string> packs =
        BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix);
    ASSERT_FALSE(packs.empty());
    ASSERT_FALSE(BucketFilesWithSuffix(ManagedBlobReferenceFile::kReferenceFileSuffix).empty());

    // The snapshot never lands. Aborting must leave the bucket as it was before the write.
    ASSERT_OK(Abort(commit_msgs));
    ASSERT_TRUE(BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix).empty());
    ASSERT_TRUE(BucketFilesWithSuffix(ManagedBlobReferenceFile::kReferenceFileSuffix).empty());

    // The table is still usable and the next commit brings its own packs.
    ASSERT_OK(WriteAndCommit({{"k1", 1, std::string("payload-3")}}, /*commit_identifier=*/1));
    std::vector<PkBlobRow> rows;
    ReadRows(/*blob_as_descriptor=*/false, &rows);
    std::vector<PkBlobRow> expected = {{"k1", 1, std::string("payload-3")}};
    ASSERT_EQ(rows, expected);
    ASSERT_EQ(BucketFilesWithSuffix(ManagedBlobReferenceFile::kManagedBlobSuffix).size(), 1);
}

std::vector<std::string> GetTestValuesForPkBlobTableInteTest() {
    std::vector<std::string> values;
    values.emplace_back("parquet");
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc");
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, PkBlobTableInteTest,
                         ::testing::ValuesIn(GetTestValuesForPkBlobTableInteTest()));

}  // namespace paimon::test
