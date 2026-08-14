/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
#include <algorithm>
#include <set>
#include <tuple>
#include <utility>

#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/append/append_compact_coordinator.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/io/cache/lru_cache.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/indexed_split.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/deletion_vector_test_helper.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
using DataEvolutionTableParam = std::tuple<std::string, bool>;

// This is a sdk end-to-end test for data evolution
class DataEvolutionTableTest : public ::testing::Test,
                               public ::testing::WithParamInterface<DataEvolutionTableParam> {
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
    }
    void TearDown() override {
        dir_.reset();
    }

    void CreateTable(const arrow::FieldVector& fields,
                     const std::vector<std::string>& partition_keys,
                     const std::map<std::string, std::string>& options) const {
        auto schema = arrow::schema(fields);
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());

        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir_->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, partition_keys,
                                       /*primary_keys=*/{}, options,
                                       /*ignore_if_exists=*/false));
    }

    void CreateTable(const std::vector<std::string>& partition_keys,
                     const std::map<std::string, std::string>& options) const {
        CreateTable(fields_, partition_keys, options);
    }

    void CreateTable(const std::vector<std::string>& partition_keys) const {
        std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                      {Options::FILE_FORMAT, FileFormat()},
                                                      {Options::FILE_SYSTEM, "local"},
                                                      {Options::ROW_TRACKING_ENABLED, "true"},
                                                      {Options::DATA_EVOLUTION_ENABLED, "true"}};
        return CreateTable(partition_keys, options);
    }

    void CreateTable() const {
        return CreateTable(/*partition_keys=*/{});
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArray(
        const std::string& table_path, const std::map<std::string, std::string>& partition,
        const std::vector<std::string>& write_cols,
        const std::shared_ptr<arrow::Array>& write_array) const {
        // write
        WriteContextBuilder write_builder(table_path, "commit_user_1");
        write_builder.WithWriteSchema(write_cols);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, write_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*write_array, &c_array).ok());
        auto record_batch = std::make_unique<RecordBatch>(
            partition, /*bucket=*/0,
            /*row_kinds=*/std::vector<RecordBatch::RowKind>(), &c_array);
        PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs,
                               file_store_write->PrepareCommit(
                                   /*wait_compaction=*/false, /*commit_identifier=*/0));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return commit_msgs;
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteArray(
        const std::string& table_path, const std::vector<std::string>& write_cols,
        const std::shared_ptr<arrow::Array>& write_array) const {
        return WriteArray(table_path, /*partition=*/{}, write_cols, write_array);
    }

    /// Stamps `reset_first_row_id` on the metas the caller holds. The commit assigns row ids
    /// onto its own copies, so a caller that later looks a file up by row id range, to find a
    /// row range group's anchor, has to mirror the assignment here.
    void SetFirstRowId(int64_t reset_first_row_id,
                       std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        for (auto& commit_msg : commit_msgs) {
            auto commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msg);
            ASSERT_TRUE(commit_msg_impl);
            for (auto& file : commit_msg_impl->data_increment_.new_files_) {
                file->AssignFirstRowId(reset_first_row_id);
            }
        }
    }

    Status Commit(const std::string& table_path,
                  const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs) const {
        // commit
        CommitContextBuilder commit_builder(table_path, "commit_user_1");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        return file_store_commit->Commit(commit_msgs);
    }

    /// Creates the default (f0, f1, f2) data evolution table and returns the options it was
    /// created with, which a test that later evolves the schema has to pass along.
    /// `extra_options` is merged in for tests that also pin a read batch or split target size.
    std::map<std::string, std::string> CreateDataEvolutionTable(
        bool deletion_vectors_enabled,
        const std::map<std::string, std::string>& extra_options = {}) const {
        std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                      {Options::FILE_FORMAT, FileFormat()},
                                                      {Options::FILE_SYSTEM, "local"},
                                                      {Options::ROW_TRACKING_ENABLED, "true"},
                                                      {Options::DATA_EVOLUTION_ENABLED, "true"}};
        if (deletion_vectors_enabled) {
            options.emplace(Options::DELETION_VECTORS_ENABLED, "true");
        }
        options.insert(extra_options.begin(), extra_options.end());
        CreateTable(/*partition_keys=*/{}, options);
        return options;
    }

    /// DeletionVectorTestHelper::CreateDeletionVectorCommitMessage, documented there, plus the
    /// commit of the message it returns.
    Result<std::shared_ptr<CommitMessage>> CommitDeletionVectors(
        const std::string& table_path, const std::shared_ptr<CommitMessage>& base_commit_msg,
        const std::map<std::string, std::vector<int64_t>>& deleted_positions_by_anchor,
        const std::shared_ptr<CommitMessage>& replaced_commit_msg = nullptr) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<CommitMessage> commit_message,
            DeletionVectorTestHelper::CreateDeletionVectorCommitMessage(
                dir_->GetFileSystem(), table_path, /*file_format_identifier=*/FileFormat(),
                base_commit_msg, deleted_positions_by_anchor, GetDefaultPool(),
                replaced_commit_msg));
        PAIMON_RETURN_NOT_OK(Commit(table_path, {commit_message}));
        return commit_message;
    }

    /// Writes one row range group holding `f0_values`, starting at `first_row_id`: a full-row
    /// write plus a partial f2 write over the same rows, so the group's split merges columns
    /// from two files and is not raw convertible. When `partition` is set the files land in that
    /// partition and every row carries its value in f1, the partition key.
    ///
    /// The f2 write starts only once the full-row write is committed, which is what makes the
    /// f2 file the newer of the pair. Writing both before either commit left the column merge
    /// free to serve f2 from the full-row write instead.
    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteAndCommitGroup(
        const std::string& table_path, int64_t first_row_id, const std::vector<int32_t>& f0_values,
        const std::map<std::string, std::string>& partition = {}) const {
        auto partition_f1 = partition.find("f1");
        std::string base_json = "[";
        std::string f2_json = "[";
        for (size_t i = 0; i < f0_values.size(); i++) {
            if (i > 0) {
                base_json += ", ";
                f2_json += ", ";
            }
            std::string f1_value =
                partition_f1 == partition.end() ? fmt::format("a{}", i) : partition_f1->second;
            base_json += fmt::format(R"([{}, "{}", "x{}"])", f0_values[i], f1_value, i);
            f2_json += fmt::format(R"(["y{}"])", i);
        }
        base_json += "]";
        f2_json += "]";

        auto base_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), base_json)
                .ValueOrDie());
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<std::shared_ptr<CommitMessage>> base_msgs,
            WriteArray(table_path, partition, arrow::schema(fields_)->field_names(), base_array));
        // both writes are stamped with the same first row id, so their files cover the same row
        // id range and form one row range group
        SetFirstRowId(first_row_id, base_msgs);
        PAIMON_RETURN_NOT_OK(Commit(table_path, base_msgs));

        arrow::FieldVector f2_fields = {fields_[2]};
        auto f2_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(f2_fields), f2_json)
                .ValueOrDie());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> f2_msgs,
                               WriteArray(table_path, partition, {"f2"}, f2_array));
        SetFirstRowId(first_row_id, f2_msgs);
        PAIMON_RETURN_NOT_OK(Commit(table_path, f2_msgs));

        std::vector<std::shared_ptr<CommitMessage>> group_msgs;
        group_msgs.insert(group_msgs.end(), base_msgs.begin(), base_msgs.end());
        group_msgs.insert(group_msgs.end(), f2_msgs.begin(), f2_msgs.end());
        return group_msgs;
    }

    /// Flattens the f0 column of a read result, looked up by name so the row kind column the
    /// reader prepends does not shift it.
    static Result<std::vector<int32_t>> CollectF0Values(
        const std::shared_ptr<arrow::ChunkedArray>& rows) {
        std::vector<int32_t> values;
        if (!rows) {
            return values;
        }
        for (const std::shared_ptr<arrow::Array>& chunk : rows->chunks()) {
            auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(chunk);
            if (!struct_array) {
                return Status::Invalid("read result chunk is not a struct array");
            }
            auto f0_array =
                std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->GetFieldByName("f0"));
            if (!f0_array) {
                return Status::Invalid("read result has no int32 f0 column");
            }
            for (int64_t i = 0; i < f0_array->length(); i++) {
                values.push_back(f0_array->Value(i));
            }
        }
        return values;
    }

    struct LimitScanResult {
        /// The read that produced `rows`. Its memory pool owns the buffers behind them, so it
        /// has to outlive them: these two members are declared first on purpose, since members
        /// are destroyed in reverse order.
        std::unique_ptr<TableRead> table_read;
        std::unique_ptr<BatchReader> batch_reader;
        /// The splits the limit push down kept in the plan.
        std::vector<std::shared_ptr<Split>> splits;
        /// The rows reading those splits produced, null when the read returned nothing. The
        /// read does not truncate to the limit, so this is everything the kept splits hold, or
        /// everything that survives the predicate when `enable_predicate_filter` is set.
        std::shared_ptr<arrow::ChunkedArray> rows;
    };

    /// Scans with a pushed-down row limit and reads the planned splits back. The push down only
    /// prunes splits, so a correct plan must still expose at least `limit` rows to the read.
    ///
    /// `enable_predicate_filter` turns the predicate into a row filter while reading. It is off
    /// by default, matching the read context default: a predicate then only prunes while
    /// planning, and the read returns every row the kept splits hold.
    Result<LimitScanResult> ScanAndReadWithLimit(
        const std::string& table_path, const std::vector<std::string>& read_schema, int32_t limit,
        const std::shared_ptr<Predicate>& predicate = nullptr,
        const std::vector<Range>& row_ranges = {}, bool enable_predicate_filter = false) const {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetLimit(limit);
        scan_context_builder.SetPredicate(predicate);
        if (!row_ranges.empty()) {
            scan_context_builder.SetGlobalIndexResult(
                BitmapGlobalIndexResult::FromRanges(row_ranges));
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                               FinishScanContext(scan_context_builder));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                               TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> result_plan, table_scan->CreatePlan());

        LimitScanResult result;
        result.splits = result_plan->Splits();
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetReadFieldNames(read_schema)
            .SetPredicate(predicate)
            .EnablePredicateFilter(enable_predicate_filter);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                               read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(result.table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(result.batch_reader, result.table_read->CreateReader(result.splits));
        PAIMON_ASSIGN_OR_RAISE(result.rows,
                               ReadResultCollector::CollectResult(result.batch_reader.get()));
        return result;
    }

    /// Plans the table without any push down and returns the planned splits, so a test can
    /// assert what the scan handed the read: which data file each deletion file landed on, and
    /// the row count derived from them. Reading the splits back is ScanAndReadWithLimit's job,
    /// which keeps the reader that owns the returned rows alive.
    Result<std::vector<std::shared_ptr<Split>>> PlanSplits(const std::string& table_path) const {
        ScanContextBuilder scan_context_builder(table_path);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                               FinishScanContext(scan_context_builder));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                               TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
        return plan->Splits();
    }

    /// Anchor file name of every row range group of the table, ordered by ascending first row
    /// id, derived from the planned splits.
    ///
    /// Deriving them from the same metas the read groups keeps the two in step. Deriving them
    /// from the metas a write hands back uses pre-commit copies, and a vector keyed by a file
    /// the read does not consider the anchor is never found, silently leaving rows undeleted.
    Result<std::vector<std::string>> PlannedAnchorFileNames(const std::string& table_path) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Split>> splits, PlanSplits(table_path));
        std::vector<std::shared_ptr<DataFileMeta>> data_files;
        for (const std::shared_ptr<Split>& split : splits) {
            auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(split);
            if (!split_impl) {
                return Status::Invalid("split cannot cast to DataSplitImpl");
            }
            for (const std::shared_ptr<DataFileMeta>& file : split_impl->DataFiles()) {
                data_files.push_back(file);
            }
        }
        return DeletionVectorTestHelper::RetrieveAnchorFileNames(data_files);
    }

    /// PlannedAnchorFileNames for a table holding a single row range group.
    Result<std::string> PlannedAnchorFileName(const std::string& table_path) const {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> anchor_file_names,
                               PlannedAnchorFileNames(table_path));
        if (anchor_file_names.size() != 1) {
            return Status::Invalid("expected exactly one row range group in the table");
        }
        return anchor_file_names[0];
    }

    using DeletionCardinalityMap = std::map<std::string, int64_t>;

    /// Maps every data file of `split` that carries a deletion file to that file's cardinality.
    static Result<DeletionCardinalityMap> DeletionCardinalityByDataFile(
        const std::shared_ptr<Split>& split) {
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(split);
        if (!split_impl) {
            return Status::Invalid("split cannot cast to DataSplitImpl");
        }
        const std::vector<std::shared_ptr<DataFileMeta>>& data_files = split_impl->DataFiles();
        const std::vector<std::optional<DeletionFile>>& deletion_files =
            split_impl->DeletionFiles();
        if (!deletion_files.empty() && deletion_files.size() != data_files.size()) {
            return Status::Invalid("deletion files are not aligned with data files");
        }
        DeletionCardinalityMap cardinality_by_file;
        for (size_t i = 0; i < deletion_files.size(); i++) {
            if (deletion_files[i] == std::nullopt) {
                continue;
            }
            if (deletion_files[i].value().cardinality == std::nullopt) {
                return Status::Invalid("deletion file is missing its cardinality");
            }
            cardinality_by_file[data_files[i]->file_name] =
                deletion_files[i].value().cardinality.value();
        }
        return cardinality_by_file;
    }

    Status CommitWithRowIdCheckFromSnapshot(
        const std::string& table_path,
        const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs,
        std::optional<int64_t> row_id_check_from_snapshot) const {
        CommitContextBuilder commit_builder(table_path, "commit_user_1");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                               commit_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> file_store_commit,
                               FileStoreCommit::Create(std::move(commit_context)));
        file_store_commit->RowIdCheckConflict(row_id_check_from_snapshot);
        return file_store_commit->Commit(commit_msgs);
    }

    Status ScanAndRead(const std::string& table_path, const std::vector<std::string>& read_schema,
                       const std::shared_ptr<arrow::StructArray>& expected_array,
                       const std::shared_ptr<Predicate>& predicate = nullptr,
                       const std::vector<Range>& row_ranges = {},
                       bool check_scan_plan_when_empty_result = true) const {
        // scan
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetPredicate(predicate);
        if (!row_ranges.empty()) {
            auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
            scan_context_builder.SetGlobalIndexResult(global_index_result);
        }
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, FinishScanContext(scan_context_builder));
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        if (!expected_array && check_scan_plan_when_empty_result) {
            if (!result_plan->Splits().empty()) {
                return Status::Invalid("check_scan_plan_when_empty_result but plan is not empty");
            }
        }

        // read
        auto splits = result_plan->Splits();
        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetReadFieldNames(read_schema).SetPredicate(predicate);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                               read_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_read, TableRead::Create(std::move(read_context)));
        PAIMON_ASSIGN_OR_RAISE(auto batch_reader, table_read->CreateReader(splits));
        PAIMON_ASSIGN_OR_RAISE(auto read_result,
                               ReadResultCollector::CollectResult(batch_reader.get()));

        if (!expected_array) {
            if (read_result) {
                return Status::Invalid("expected array is empty, but read result is not empty");
            }
            return Status::OK();
        }
        // add row kind array for expected array
        auto row_kind_scalar =
            std::make_shared<arrow::Int8Scalar>(RowKind::Insert()->ToByteValue());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto row_kind_array,
            arrow::MakeArrayFromScalar(*row_kind_scalar, expected_array->length()));

        arrow::ArrayVector expected_with_row_kind_fields = expected_array->fields();
        std::vector<std::string> expected_with_row_kind_field_names =
            arrow::schema(expected_array->type()->fields())->field_names();
        expected_with_row_kind_fields.insert(expected_with_row_kind_fields.begin(), row_kind_array);
        expected_with_row_kind_field_names.insert(expected_with_row_kind_field_names.begin(),
                                                  "_VALUE_KIND");

        // check read result
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            auto expected_with_row_kind_array,
            arrow::StructArray::Make(expected_with_row_kind_fields,
                                     expected_with_row_kind_field_names));
        auto expected_chunk_array =
            std::make_shared<arrow::ChunkedArray>(expected_with_row_kind_array);
        if (!expected_chunk_array->Equals(read_result)) {
            std::cout << "result=" << read_result->ToString() << std::endl
                      << "expected=" << expected_chunk_array->ToString() << std::endl;
            return Status::Invalid("expected array and result array not equal");
        }
        return Status::OK();
    }

    void CheckScanResult(const std::string& table_path, const std::shared_ptr<Predicate>& predicate,
                         const std::vector<Range>& row_ranges,
                         const std::vector<std::optional<int64_t>>& expected_first_row_ids,
                         const std::vector<int64_t>& expected_row_counts) {
        ASSERT_EQ(expected_first_row_ids.size(), expected_row_counts.size());
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetPredicate(predicate);
        if (!row_ranges.empty()) {
            auto global_index_result = BitmapGlobalIndexResult::FromRanges(row_ranges);
            scan_context_builder.SetGlobalIndexResult(global_index_result);
        }
        ASSERT_OK_AND_ASSIGN(auto scan_context, FinishScanContext(scan_context_builder));
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        const auto& result_splits = result_plan->Splits();
        if (expected_first_row_ids.empty()) {
            ASSERT_EQ(result_splits.size(), 0);
            return;
        }
        ASSERT_EQ(result_splits.size(), 1);
        std::shared_ptr<DataSplitImpl> data_split;
        if (auto indexed_split = std::dynamic_pointer_cast<IndexedSplit>(result_splits[0])) {
            data_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
        } else {
            data_split = std::dynamic_pointer_cast<DataSplitImpl>(result_splits[0]);
        }
        ASSERT_TRUE(data_split);
        std::vector<std::optional<int64_t>> result_first_row_ids;
        std::vector<int64_t> result_row_counts;
        for (const auto& meta : data_split->DataFiles()) {
            result_first_row_ids.push_back(meta->first_row_id);
            result_row_counts.push_back(meta->row_count);
        }
        ASSERT_EQ(result_first_row_ids, expected_first_row_ids);
        ASSERT_EQ(result_row_counts, expected_row_counts);
    }

    Result<std::unique_ptr<ScanContext>> FinishScanContext(ScanContextBuilder& builder) const {
        if (EnableSnapshotLiveManifestCache()) {
            if (!snapshot_live_manifest_cache_) {
                snapshot_live_manifest_cache_ =
                    std::make_shared<LruCache>(/*max_weight=*/64 * 1024 * 1024);
            }
            builder.AddOption(Options::SCAN_MANIFEST_ENTRY_CACHE_MAX_SNAPSHOTS, "3")
                .WithCache(snapshot_live_manifest_cache_);
        }
        return builder.Finish();
    }

    std::string FileFormat() const {
        return std::get<0>(GetParam());
    }

    bool EnableSnapshotLiveManifestCache() const {
        return std::get<1>(GetParam());
    }

    std::shared_ptr<arrow::StructArray> PrepareBulkData(
        int32_t write_batch_size, std::function<std::string(int32_t)> data_generator,
        const arrow::FieldVector& fields) const {
        std::string data_str = "[";
        for (int32_t i = 0; i < write_batch_size; i++) {
            data_str.append("[");
            auto row_str = data_generator(i);
            data_str.append(row_str);
            data_str.append("],");
        }
        data_str.pop_back();
        data_str.append("]");
        return std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), data_str)
                .ValueOrDie());
    }

 private:
    std::unique_ptr<UniqueTestDirectory> dir_;
    mutable std::shared_ptr<Cache> snapshot_live_manifest_cache_;
    arrow::FieldVector fields_ = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::utf8()),
    };
};

TEST_P(DataEvolutionTableTest, TestBasic) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                            SpecialFields::RowId().field_, fields_[2]}),
            R"([
        ["a", 1, 2, 0, "c"]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                          expected_row_tracking_array));

    // read score but not indexed split
    ASSERT_NOK_WITH_MSG(
        ScanAndRead(table_path, {"f0", "f1", "_INDEX_SCORE"}, expected_row_tracking_array,
                    /*predicate=*/nullptr,
                    /*row_ranges=*/{}),
        "Invalid read schema, read _INDEX_SCORE while split cannot cast to IndexedSplit");
}

TEST_P(DataEvolutionTableTest, TestCommitConflictOnOverlappedRowIdAndWriteColumns) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Snapshot 1: initialize row id range [0, 0].
    std::vector<std::string> init_write_cols = {"f0", "f1", "f2"};
    auto init_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto init_msgs, WriteArray(table_path, init_write_cols, init_array));
    ASSERT_OK(Commit(table_path, init_msgs));

    // Snapshot 2: update f2 at row id 0.
    std::vector<std::string> write_cols = {"f2"};
    auto src_array_1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_1, WriteArray(table_path, write_cols, src_array_1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs_1);
    ASSERT_OK(Commit(table_path, commit_msgs_1));

    // Snapshot 3 attempt: update f2 at row id 0 again, and check history from snapshot 1.
    // This should conflict with snapshot 2 because row-id range and write columns overlap.
    auto src_array_2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2, WriteArray(table_path, write_cols, src_array_2));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs_2);
    ASSERT_NOK_WITH_MSG(
        CommitWithRowIdCheckFromSnapshot(table_path, commit_msgs_2,
                                         /*row_id_check_from_snapshot=*/1),
        "multiple 'MERGE INTO' operations have encountered conflicts, updating the same file");
}

TEST_P(DataEvolutionTableTest, TestMultipleAppends) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, "a"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(10, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(10, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // write field: f0, f1
    std::vector<std::string> write_cols3 = {"f0", "f1"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [2, "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    SetFirstRowId(11, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // write field: f2
    std::vector<std::string> write_cols4 = {"f2"};
    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs4, WriteArray(table_path, write_cols4, src_array4));
    SetFirstRowId(11, commit_msgs4);
    ASSERT_OK(Commit(table_path, commit_msgs4));

    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(11l, 11l)};
        // test with row ids
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 11, 11}, /*expected_row_counts=*/{10, 1, 1});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                      fields_[0],
                                                      fields_[1],
                                                      fields_[2],
                                                      SpecialFields::RowId().field_,
                                                      SpecialFields::SequenceNumber().field_,
                                                  }),
                                                  R"([
        [1, "a", "b", 0, 1],
        [1, "a", "b", 1, 1],
        [1, "a", "b", 2, 1],
        [1, "a", "b", 3, 1],
        [1, "a", "b", 4, 1],
        [1, "a", "b", 5, 1],
        [1, "a", "b", 6, 1],
        [1, "a", "b", 7, 1],
        [1, "a", "b", 8, 1],
        [1, "a", "b", 9, 1],
        [1, "a", "b", 10, 2],
        [2, "c", "d", 11, 4]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestOnlySomeColumns) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0
    std::vector<std::string> write_cols0 = {"f0"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0]}), R"([
        [1]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f1
    std::vector<std::string> write_cols1 = {"f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1]}), R"([
        ["a"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);
    ASSERT_OK(Commit(table_path, commit_msgs2));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                      fields_[0],
                                                      fields_[1],
                                                      fields_[2],
                                                      SpecialFields::RowId().field_,
                                                      SpecialFields::SequenceNumber().field_,
                                                  }),
                                                  R"([
        [1, "a", "b", 0, 3]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestMultipleSharedShreddingMapsPartialOverwrite) {
    if (FileFormat() == "avro") {
        return;
    }

    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("map1", map_type),
        arrow::field("map2", map_type),
    };
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, FileFormat()},
        {Options::FILE_SYSTEM, "local"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {"fields.map1.map.storage-layout", "shared-shredding"},
        {"fields.map1.map.shared-shredding.max-columns", "1"},
        {"fields.map2.map.storage-layout", "shared-shredding"},
        {"fields.map2.map.shared-shredding.max-columns", "1"},
    };
    CreateTable(fields, /*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields);

    std::vector<std::string> write_cols0 = {"id", "map1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[0], fields[1]}), R"([
        [1, [["a", 10], ["b", 20]]],
        [11, [["a", 11], ["b", 21]]]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs0, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs0));

    std::vector<std::string> write_cols1 = {"id", "map2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[0], fields[2]}), R"([
        [2, [["c", 30], ["d", 40]]],
        [12, [["c", 31], ["d", 41]]]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    std::vector<std::string> write_cols2 = {"map1"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[1]}), R"([
        [[["b", 200], ["a", 100]]],
        [[["b", 201], ["a", 101]]]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs2);
    ASSERT_OK(Commit(table_path, commit_msgs2));

    // Read all columns and merge values from all partial files.
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [2, [["a", 100], ["b", 200]], [["c", 30], ["d", 40]]],
        [12, [["a", 101], ["b", 201]], [["c", 31], ["d", 41]]]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // Read a subset of columns and recall only the requested shared-shredding MAP column.
    auto expected_column_pruned_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[0], fields[2]}), R"([
        [2, [["c", 30], ["d", 40]]],
        [12, [["c", 31], ["d", 41]]]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"id", "map2"}, expected_column_pruned_array));

    // Read selected keys from both shared-shredding MAP columns after partial overwrite merge.
    {
        auto map1_selected_keys =
            arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"b"});
        auto map2_selected_keys =
            arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"d"});
        auto read_schema = arrow::schema({
            fields[0],
            fields[1]->WithMetadata(map1_selected_keys),
            fields[2]->WithMetadata(map2_selected_keys),
        });
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_schema.get()).ok());

        ScanContextBuilder scan_context_builder(table_path);
        ASSERT_OK_AND_ASSIGN(auto scan_context, FinishScanContext(scan_context_builder));
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.SetReadSchema(std::move(c_schema));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context,
                             read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
        ASSERT_OK_AND_ASSIGN(auto actual, ReadResultCollector::CollectResult(batch_reader.get()));

        auto expected_type = arrow::struct_({
            SpecialFields::ValueKind().field_,
            fields[0],
            fields[1],
            fields[2],
        });
        auto expected = arrow::ipc::internal::json::ArrayFromJSON(expected_type, R"([
            [0, 2, [["b", 200]], [["d", 40]]],
            [0, 12, [["b", 201]], [["d", 41]]]
        ])")
                            .ValueOrDie();
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(expected_chunked->Equals(actual))
            << "actual=" << actual->ToString() << "\nexpected=" << expected_chunked->ToString();
    }

    // Read a subset of rows after merging values from all partial files.
    auto expected_partial_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [12, [["a", 101], ["b", 201]], [["c", 31], ["d", 41]]]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_partial_array,
                          /*predicate=*/nullptr,
                          /*row_ranges=*/{Range(1l, 1l)}));

    // Read row tracking fields and verify the latest partial overwrite sequence number.
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields[0], fields[1], fields[2], SpecialFields::RowId().field_,
                            SpecialFields::SequenceNumber().field_}),
            R"([
        [2, [["a", 100], ["b", 200]], [["c", 30], ["d", 40]], 0, 3],
        [12, [["a", 101], ["b", 201]], [["c", 31], ["d", 41]], 1, 3]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"id", "map1", "map2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestNullValues) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, null]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // Commit 2: Overwrite with non-null
    // write field: f2
    std::vector<std::string> write_cols3 = {"f2"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    SetFirstRowId(0, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, null, "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                      fields_[0],
                                                      fields_[1],
                                                      fields_[2],
                                                      SpecialFields::RowId().field_,
                                                      SpecialFields::SequenceNumber().field_,
                                                  }),
                                                  R"([
        [1, null, "c", 0, 2]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestMultipleAppendsDifferentFirstRowIds) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // First commit, firstRowId = 0
    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, "a"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // Second commit, firstRowId = 1
    // write field: f0, f1
    std::vector<std::string> write_cols3 = {"f0", "f1"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [2, "c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    SetFirstRowId(1, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // Third commit
    // write field: f2
    std::vector<std::string> write_cols4 = {"f2"};
    auto src_array4 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs4, WriteArray(table_path, write_cols4, src_array4));
    SetFirstRowId(1, commit_msgs4);
    ASSERT_OK(Commit(table_path, commit_msgs4));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({
                                                      fields_[0],
                                                      fields_[1],
                                                      fields_[2],
                                                      SpecialFields::RowId().field_,
                                                      SpecialFields::SequenceNumber().field_,
                                                  }),
                                                  R"([
        [1, "a", "b", 0, 1],
        [2, "c", "d", 1, 3]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestMoreData) {
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1
    std::vector<std::string> write_cols1 = {"f0", "f1"};
    // row0: 0, a0; row1: 1, a1 ...
    auto src_array1 = PrepareBulkData(
        10000, [](int32_t i) { return std::to_string(i) + ", \"a" + std::to_string(i) + "\""; },
        {fields_[0], fields_[1]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(0, commit_msgs1);

    // write field: f2
    std::vector<std::string> write_cols2 = {"f2"};
    // row0: b0; row1: b1 ...
    auto src_array2 = PrepareBulkData(
        10000, [](int32_t i) { return "\"b" + std::to_string(i) + "\""; }, {fields_[2]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols2, src_array2));
    SetFirstRowId(0, commit_msgs2);

    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    total_msgs.insert(total_msgs.end(), commit_msgs2.begin(), commit_msgs2.end());
    ASSERT_OK(Commit(table_path, total_msgs));

    // write field: f2
    std::vector<std::string> write_cols3 = {"f2"};
    // row0: c0; row1: c1 ...
    auto src_array3 = PrepareBulkData(
        10000, [](int32_t i) { return "\"c" + std::to_string(i) + "\""; }, {fields_[2]});
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, WriteArray(table_path, write_cols3, src_array3));
    ASSERT_OK(Commit(table_path, commit_msgs3));

    // row0: 0, a0, c0; row1: 1, a1, c1 ...
    auto expected_array = PrepareBulkData(10000,
                                          [](int32_t i) {
                                              return std::to_string(i) + ", \"a" +
                                                     std::to_string(i) + "\", \"c" +
                                                     std::to_string(i) + "\"";
                                          },
                                          {fields_[0], fields_[1], fields_[2]});
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
}

TEST_P(DataEvolutionTableTest, TestOnlyRowTrackingEnabled) {
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, FileFormat()},
        {Options::FILE_SYSTEM, "local"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "false"},
    };
    CreateTable(/*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"],
        [2, "c", "d"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                            SpecialFields::RowId().field_, fields_[2]}),
            R"([
        ["a", 1, 1, 0, "b"],
        ["c", 2, 1, 1, "d"]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestExternalPath) {
    // create external path dir
    auto external_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = external_dir->Str();

    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, FileFormat()},
        {Options::FILE_SYSTEM, "local"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::DATA_FILE_EXTERNAL_PATHS, "FILE://" + external_test_dir},
    };
    CreateTable(/*partition_keys=*/{}, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1
    std::vector<std::string> write_cols0 = {"f0", "f1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [1, "a"],
        [2, "c"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs0, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs0));

    // write field: f0, f2
    std::vector<std::string> write_cols1 = {"f0", "f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[2]}), R"([
        [10, "b"],
        [20, "d"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [10, "a", "b"],
        [20, "c", "d"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[1], fields_[0], fields_[2], SpecialFields::RowId().field_,
                            SpecialFields::SequenceNumber().field_}),
            R"([
        ["a", 10, "b", 0, 2],
        ["c", 20, "d", 1, 2]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestWithPartitionSimple) {
    std::vector<std::string> partition_keys = {"f1"};
    CreateTable(partition_keys);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2 for f1=2024
    std::map<std::string, std::string> partition0 = {{"f1", "2024"}};
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "2024", "b1"],
        [2, "2024", "b2"],
        [3, "2024", "b3"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, partition0, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2 for f1=2024
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c1"],
        ["c2"],
        ["c3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, partition0, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write fields: f1, f2 for f1=2025
    std::map<std::string, std::string> partition2 = {{"f1", "2025"}};
    std::vector<std::string> write_cols2 = {"f1", "f2"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1], fields_[2]}), R"([
        ["2025", "d1"],
        ["2025", "d2"],
        ["2025", "d3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, partition2, write_cols2, src_array2));
    SetFirstRowId(/*reset_first_row_id=*/3, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    // test read all fields
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "2024", "c1"],
        [2, "2024", "c2"],
        [3, "2024", "c3"],
        [null, "2025", "d1"],
        [null, "2025", "d2"],
        [null, "2025", "d3"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // test only read partition fields
    auto expected_array_only_partition = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[1]}), R"([
        ["2024"],
        ["2024"],
        ["2024"],
        ["2025"],
        ["2025"],
        ["2025"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f1"}, expected_array_only_partition));

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_,
                            SpecialFields::SequenceNumber().field_}),
            R"([
        [1, "2024", "c1", 0, 2],
        [2, "2024", "c2", 1, 2],
        [3, "2024", "c3", 2, 2],
        [null, "2025", "d1", 3, 3],
        [null, "2025", "d2", 4, 3],
        [null, "2025", "d3", 5, 3]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));

    // read only read partition fields and row tracking
    auto expected_partition_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[1], SpecialFields::RowId().field_,
                            SpecialFields::SequenceNumber().field_}),
            R"([
        ["2024", 0, 2],
        ["2024", 1, 2],
        ["2024", 2, 2],
        ["2025", 3, 3],
        ["2025", 4, 3],
        ["2025", 5, 3]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f1", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_partition_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestWithPartitionWithoutPartitionFieldsInFile) {
    std::vector<std::string> partition_keys = {"f1"};
    CreateTable(partition_keys);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f2 for f1=2024
    std::map<std::string, std::string> partition0 = {{"f1", "2024"}};
    std::vector<std::string> write_cols0 = {"f0", "f2"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[2]}), R"([
        [1, "b1"],
        [2, "b2"],
        [3, "b3"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, partition0, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2 for f1=2024
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c1"],
        ["c2"],
        ["c3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, partition0, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "2024", "c1"],
        [2, "2024", "c2"],
        [3, "2024", "c3"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        // test with row ids
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{3, 3});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [2, "2024", "c2"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }

    // read with row tracking
    auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_,
                            SpecialFields::SequenceNumber().field_}),
            R"([
        [1, "2024", "c1", 0, 2],
        [2, "2024", "c2", 1, 2],
        [3, "2024", "c3", 2, 2]
    ])")
            .ValueOrDie());

    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                          expected_row_tracking_array));
}

TEST_P(DataEvolutionTableTest, TestPartitionWithPredicate) {
    auto file_format = FileFormat();
    if (file_format == "avro") {
        return;
    }
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},         {Options::FILE_FORMAT, FileFormat()},
        {Options::FILE_SYSTEM, "local"},           {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"}, {"parquet.write.max-row-group-length", "1"}};

    CreateTable(partition_keys, options);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1 for partition f1 = "2024"
    std::vector<std::string> write_cols0 = {"f0", "f1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [11, "2024"],
        [12, "2024"],
        [13, "2024"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs0,
                         WriteArray(table_path, {{"f1", "2024"}}, write_cols0, src_array0));

    // write field: f2 for partition f1 = "2024"
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["a"],
        ["b"],
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, {{"f1", "2024"}}, write_cols1, src_array1));
    std::vector<std::shared_ptr<CommitMessage>> total_msgs;
    total_msgs.insert(total_msgs.end(), commit_msgs0.begin(), commit_msgs0.end());
    total_msgs.insert(total_msgs.end(), commit_msgs1.begin(), commit_msgs1.end());
    SetFirstRowId(0, total_msgs);
    ASSERT_OK(Commit(table_path, total_msgs));

    // write field: f0, f1 for partition f1 = "2025"
    std::vector<std::string> write_cols3 = {"f0", "f1"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [21, "2025"],
        [22, "2025"],
        [23, "2025"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs3,
                         WriteArray(table_path, {{"f1", "2025"}}, write_cols3, src_array3));
    SetFirstRowId(3, commit_msgs3);
    ASSERT_OK(Commit(table_path, commit_msgs3));
    {
        // only set data field predicate, predicate only takes effective in file skip
        // read will not push down
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(11));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [11, "2024", "a"],
        [12, "2024", "b"],
        [13, "2024", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, equal));
    }
    {
        // only set partition predicate
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2024", 4));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [11, "2024", "a"],
        [12, "2024", "b"],
        [13, "2024", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, equal));
    }
    {
        // set partition predicate and data field predicate
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2024", 4));
        auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f0",
                                                          FieldType::INT, Literal(100));
        ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, greater_than}));
        ASSERT_OK(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate));
    }
    {
        // read with row tracking
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2024", 4));

        auto expected_row_tracking_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_,
                                SpecialFields::SequenceNumber().field_}),
                R"([
        [11, "2024", "a", 0, 1],
        [12, "2024", "b", 1, 1],
        [13, "2024", "c", 2, 1]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID", "_SEQUENCE_NUMBER"},
                              expected_row_tracking_array, equal));
    }
    {
        // test with row ids
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{3, 3});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [12, "2024", "b"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids and partition predicate
        auto equal =
            PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                    Literal(FieldType::STRING, "2025", 4));
        CheckScanResult(table_path, /*predicate=*/equal, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              /*predicate=*/equal,
                              /*row_ranges=*/row_ranges));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids and non-partition predicate
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(11));
        CheckScanResult(table_path, /*predicate=*/equal, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{3, 3});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [12, "2024", "b"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/equal,
                              /*row_ranges=*/row_ranges));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids and data predicate
        auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                             Literal(50));
        CheckScanResult(table_path, /*predicate=*/equal, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              /*predicate=*/equal,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestAlterTable) {
    auto file_format = FileFormat();
    if (file_format == "avro") {
        return;
    }
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/append_table_alter_table_with_cast_with_data_evolution.db/"
                             "append_table_alter_table_with_cast_with_data_evolution";
    std::vector<DataField> read_fields = {
        DataField(6, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::int32())),
        DataField(2, arrow::field("f3", arrow::int32())),
        DataField(3, arrow::field("f1", arrow::utf8())),
        DataField(4, arrow::field("f2", arrow::decimal128(6, 3))),
        DataField(5, arrow::field("f0", arrow::boolean())),
        DataField(8, arrow::field("f6", arrow::int32())),
        SpecialFields::RowId(),
        SpecialFields::SequenceNumber()};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    {
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, null, 4, 1],
["2024-11-26T06:38:56.054000154", 0, 1, 150, "2024-11-26 15:28:31", "55.002", true, 56, 5, 3],
["2024-11-26T06:38:56.064000164", 0, 1, 160, "2024-11-26 15:28:41", "666.012", false, 66, 6, 3],
["2024-11-26T06:38:56.074000174", 0, 1, 170, "2024-11-26 15:28:51", "-77.022", true, 76, 7, 3],
["2024-11-26T06:38:56.084000184", 0, 1, 180, "2024-11-26 15:29:01", "8.032", true, -86, 8, 3],
["2024-11-26T06:38:56.094000194", 0, 1, 190, "I'm strange", "-999.420", false, 96, 9, 3]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array));
    }
    {
        // only files with schema-1 will be skipped, while files with schema-0 will be reserved
        // as type change
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                       FieldType::INT, Literal(200));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, null, 4, 1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // files with schema-0 will be reserved as f6 does not exist in schema-0 (null count =
        // null), files with schema-1 will also be reserved
        auto predicate =
            PredicateBuilder::IsNotNull(/*field_index=*/7, /*field_name=*/"f6", FieldType::INT);
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1970-01-05T00:00", 0, 1, 100, "2024-11-26 06:38:56.001000001", "0.020", true, null, 0, 1],
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1],
["1971-03-21T00:00", 0, 1, 120, "2024-11-26 06:38:56.021000021", "22.220", false, null, 2, 1],
["1957-11-01T00:00", 0, 1, 130, "2024-11-26 06:38:56.031000031", "333.320", false, null, 3, 1],
["2091-09-07T00:00", 0, 1, 140, "2024-11-26 06:38:56.041000041", "444.420", true, null, 4, 1],
["2024-11-26T06:38:56.054000154", 0, 1, 150, "2024-11-26 15:28:31", "55.002", true, 56, 5, 3],
["2024-11-26T06:38:56.064000164", 0, 1, 160, "2024-11-26 15:28:41", "666.012", false, 66, 6, 3],
["2024-11-26T06:38:56.074000174", 0, 1, 170, "2024-11-26 15:28:51", "-77.022", true, 76, 7, 3],
["2024-11-26T06:38:56.084000184", 0, 1, 180, "2024-11-26 15:29:01", "8.032", true, -86, 8, 3],
["2024-11-26T06:38:56.094000194", 0, 1, 190, "I'm strange", "-999.420", false, 96, 9, 3]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        std::vector<Range> row_ranges = {Range(1l, 1l)};
        // test with row ids
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{5, 5});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
["1969-11-18T00:00", 0, 1, 110, "2024-11-26 06:38:56.011000011", "11.120", true, null, 1, 1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestReadCompactFiles) {
    auto file_format = FileFormat();
    if (file_format == "avro") {
        return;
    }
    std::string table_path =
        paimon::test::GetDataDir() + file_format +
        "/append_table_row_tracking_with_compact.db/append_table_row_tracking_with_compact";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64())),
                                          SpecialFields::RowId(),
                                          SpecialFields::SequenceNumber()};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 12, 2.1, 0, 1],
        ["Alice", 3, 13, 3.1, 1, 1],
        ["Bob", 4, 14, 4.1, 2, 2],
        ["David", 5, 15, 5.1, 3, 2]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                          expected_array));
}

TEST_P(DataEvolutionTableTest, TestReadTableWithDenseStats) {
    auto file_format = FileFormat();
    if (file_format == "avro") {
        return;
    }
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/data_evolution_with_dense_stats.db/data_evolution_with_dense_stats";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64())),
                                          SpecialFields::RowId(),
                                          SpecialFields::SequenceNumber()};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1, 0, 2],
        ["Alice", 4, 104, 3.1, 1, 2]
    ])")
            .ValueOrDie());
    {
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array));
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(102));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(12));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              /*expected_array=*/nullptr, predicate));
    }
    {
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f1",
                                                       FieldType::INT, Literal(6));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              /*expected_array=*/nullptr, predicate));
    }
    {
        // f3 does not have stats, therefore data will not be filtered
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                       FieldType::DOUBLE, Literal(5.1));
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(0l, 0l)};
        auto predicate = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1",
                                                 FieldType::INT, Literal(5));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              /*expected_array=*/nullptr, predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(0l, 0l)};
        auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                       FieldType::DOUBLE, Literal(5.1));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{2, 2});
        auto expected_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1, 0, 2]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array0, predicate,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestScanAndReadWithIndex) {
    auto file_format = FileFormat();
    if (file_format == "avro") {
        return;
    }
    // only f2 has index
    std::string table_path = paimon::test::GetDataDir() + file_format +
                             "/data_evolution_with_index.db/data_evolution_with_index";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};

    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(read_fields);
    {
        // first 4 records are two file with the same first row id with data evolution
        // last 2 rows only in one file
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1],
        ["David", 8, 108, 5.1],
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array));
    }
    {
        // first 4 records read with data evolution, ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(102));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1],
        ["David", 8, 108, 5.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // f2 has bitmap index, but data evolution scan and read ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(103));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1],
        ["David", 8, 108, 5.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // f2 has bitmap index, data evolution scan will ignore index => not empty plan
        // data evolution split read will also ignore index => not empty read batch
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(203));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate,
                              /*row_ranges=*/{},
                              /*check_scan_plan_when_empty_result=*/true));
    }
    {
        // f2 has bitmap index, data evolution split read will ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(202));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        auto predicate =
            PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING);
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(0l, 2l)};
        // row id = {0, 1, 2}, while data evolution split read will ignore index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(106));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 0}, /*expected_row_counts=*/{4, 4});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        ["Lily", 2, 102, 2.1],
        ["Alice", 4, 104, 3.1],
        ["Bob", 6, 106, 4.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row id with predicate
        std::vector<Range> row_ranges = {Range(4l, 5l)};
        // row id = {4, 5}, data evolution split read will ignore bitmap index
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f2",
                                                 FieldType::INT, Literal(204));
        CheckScanResult(table_path, /*predicate=*/predicate, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{4}, /*expected_row_counts=*/{2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [null, null, 202, 6.1],
        [null, null, 204, 7.1]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, arrow::schema(arrow_data_type->fields())->field_names(),
                              expected_array, predicate,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestPredicate) {
    if (FileFormat() == "avro") {
        // Avro does not have stats.
        return;
    }
    CreateTable();
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // write field: f0, f1, f2
    std::vector<std::string> write_cols0 = schema->field_names();
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "b"]
    ])")
            .ValueOrDie());

    ASSERT_OK_AND_ASSIGN(auto commit_msgs, WriteArray(table_path, write_cols0, src_array0));
    ASSERT_OK(Commit(table_path, commit_msgs));

    // write field: f2
    std::vector<std::string> write_cols1 = {"f2"};
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["c"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(commit_msgs, WriteArray(table_path, write_cols1, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));
    {
        // test no predicate
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        // test predicate with f2
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                       Literal(FieldType::STRING, "b", 1));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "c"]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, predicate));
    }
    {
        // test predicate with f1
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                       Literal(FieldType::STRING, "a", 1));
        ASSERT_OK(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate));
    }
    {
        // test predicate with f2
        auto predicate =
            PredicateBuilder::NotEqual(/*field_index=*/2, /*field_name=*/"f2", FieldType::STRING,
                                       Literal(FieldType::STRING, "c", 1));
        ASSERT_OK(
            ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr, predicate));
    }
}

TEST_P(DataEvolutionTableTest, TestIOException) {
    std::string table_path;
    // write and commit with I/O exception
    bool write_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 2000; i += paimon::test::RandomNumber(20, 30)) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        dir_ = UniqueTestDirectory::Create("local");
        CreateTable();
        table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
        auto schema = arrow::schema(fields_);

        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        // write field: f0, f1, f2
        std::vector<std::string> write_cols0 = schema->field_names();
        auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [10, "a", "b"],
        [20, "aa", "bb"],
        [23, "aaa", "bbb"]
    ])")
                .ValueOrDie());
        auto commit_msgs0_result = WriteArray(table_path, write_cols0, src_array0);
        CHECK_HOOK_STATUS(commit_msgs0_result.status(), i);
        CHECK_HOOK_STATUS(Commit(table_path, commit_msgs0_result.value()), i);

        // write field: f2, f0
        std::vector<std::string> write_cols1 = {"f2", "f0"};
        auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2], fields_[0]}), R"([
        ["c", 100],
        ["cc", 200],
        ["ccc", 300]
    ])")
                .ValueOrDie());
        auto commit_msgs1_result = WriteArray(table_path, write_cols1, src_array1);
        CHECK_HOOK_STATUS(commit_msgs1_result.status(), i);
        SetFirstRowId(/*reset_first_row_id=*/0,
                      const_cast<std::vector<std::shared_ptr<paimon::CommitMessage>>&>(
                          commit_msgs1_result.value()));
        CHECK_HOOK_STATUS(Commit(table_path, commit_msgs1_result.value()), i);
        write_run_complete = true;
        break;
    }
    ASSERT_TRUE(write_run_complete);

    // scan and read with I/O exception
    bool read_run_complete = false;
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                            SpecialFields::RowId().field_, fields_[2]}),
            R"([
        ["a", 100, 2, 0, "c"],
        ["aa", 200, 2, 1, "cc"],
        ["aaa", 300, 2, 2, "ccc"]
    ])")
            .ValueOrDie());

    for (size_t i = 0; i < 2000; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        CHECK_HOOK_STATUS(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                                      expected_array),
                          i);
        read_run_complete = true;
        break;
    }
    ASSERT_TRUE(read_run_complete);
}

TEST_P(DataEvolutionTableTest, TestWithRowIds) {
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, FileFormat()},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"}};
    CreateTable(/*partition_keys=*/{}, options);

    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // turn 0: write field: f0, f1
    std::vector<std::string> write_cols = {"f0", "f1"};
    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [0, "a"],
        [1, "b"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs0, WriteArray(table_path, write_cols, src_array0));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs0);
    ASSERT_OK(Commit(table_path, commit_msgs0));

    // turn 1: write field: f0, f1
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [2, "c"],
        [3, "d"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, write_cols, src_array1));
    SetFirstRowId(/*reset_first_row_id=*/2, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    // turn 2: write field: f0, f1
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[0], fields_[1]}), R"([
        [4, "e"],
        [5, "f"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, WriteArray(table_path, write_cols, src_array2));
    SetFirstRowId(/*reset_first_row_id=*/4, commit_msgs2);
    ASSERT_OK(Commit(table_path, commit_msgs2));

    {
        // test without row ids
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/{},
                        /*expected_first_row_ids=*/{0, 2, 4}, /*expected_row_counts=*/{2, 2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a", null],
        [1, "b", null],
        [2, "c", null],
        [3, "d", null],
        [4, "e", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));
    }
    {
        // test row ids in first file
        std::vector<Range> row_ranges = {Range(0l, 1l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0}, /*expected_row_counts=*/{2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a", null],
        [1, "b", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr, /*row_ranges=*/row_ranges));
    }
    {
        // test row ids in last file
        std::vector<Range> row_ranges = {Range(4l, 4l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{4}, /*expected_row_counts=*/{2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [4, "e", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr, /*row_ranges=*/row_ranges));
    }
    {
        // test row ids in multiple files
        std::vector<Range> row_ranges = {Range(1l, 1l), Range(4l, 4l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 4}, /*expected_row_counts=*/{2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "b", null],
        [4, "e", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test all row ids
        std::vector<Range> row_ranges = {Range(0l, 5l)};
        CheckScanResult(table_path, /*predicate=*/nullptr, /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 2, 4}, /*expected_row_counts=*/{2, 2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a", null],
        [1, "b", null],
        [2, "c", null],
        [3, "d", null],
        [4, "e", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr, /*row_ranges=*/row_ranges));
    }
    {
        // test unordered row ids
        std::vector<Range> row_ranges = {Range(5l, 5l), Range(3l, 3l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{2, 4}, /*expected_row_counts=*/{2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [3, "d", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row ids which partially exist
        std::vector<Range> row_ranges = {Range(100l, 100l), Range(5l, 5l), Range(3l, 3l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{2, 4}, /*expected_row_counts=*/{2, 2});
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [3, "d", null],
        [5, "f", null]
    ])")
                .ValueOrDie());
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row ids which do not exist
        std::vector<Range> row_ranges = {Range(100l, 100l), Range(200l, 200l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
    if (FileFormat() == "avro") {
        // Avro does not support stats.
        return;
    }
    {
        // test row id with predicate
        // row ids {0, 1, 5} and predicate f0 = 2
        // scan will filter out all files
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(1l, 1l), Range(5l, 5l)};
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::INT, Literal(2));
        CheckScanResult(table_path, /*predicate=*/predicate,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{}, /*expected_row_counts=*/{});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), /*expected_array=*/nullptr,
                              predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test row id with predicate
        // row ids {0, 1, 5} and predicate f0 = 5
        // scan will filter out file0 and file1
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(1l, 1l), Range(5l, 5l)};
        auto predicate = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0",
                                                 FieldType::INT, Literal(5));
        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [5, "f", null]
    ])")
                .ValueOrDie());
        CheckScanResult(table_path, /*predicate=*/predicate,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{4}, /*expected_row_counts=*/{2});
        ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array, predicate,
                              /*row_ranges=*/row_ranges));
    }
    {
        // test with row tracking fields
        std::vector<Range> row_ranges = {Range(0l, 0l), Range(1l, 1l), Range(5l, 5l)};
        CheckScanResult(table_path, /*predicate=*/nullptr,
                        /*row_ranges=*/row_ranges,
                        /*expected_first_row_ids=*/{0, 4}, /*expected_row_counts=*/{2, 2});

        auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({fields_[1], fields_[0], SpecialFields::SequenceNumber().field_,
                                SpecialFields::RowId().field_, fields_[2]}),
                R"([
        ["a", 0, 1, 0, null],
        ["b", 1, 1, 1, null],
        ["f", 5, 3, 5, null]
    ])")
                .ValueOrDie());

        ASSERT_OK(ScanAndRead(table_path, {"f1", "f0", "_SEQUENCE_NUMBER", "_ROW_ID", "f2"},
                              expected_array, /*predicate=*/nullptr,
                              /*row_ranges=*/row_ranges));
    }
}

TEST_P(DataEvolutionTableTest, TestReadWithDeletionVectors) {
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // full-row write assigns row ids 0-3, producing the anchor file of the row range group
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "z"],
        [4, "d", "w"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs0,
                         WriteArray(table_path, schema->field_names(), src_array));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs0);
    ASSERT_OK(Commit(table_path, commit_msgs0));

    // partial write of f2 over the same row range: the group merges columns from two files
    arrow::FieldVector f2_fields = {fields_[2]};
    auto update_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(f2_fields), R"([
        ["x2"],
        ["y2"],
        ["z2"],
        ["w2"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, WriteArray(table_path, {"f2"}, update_array));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    auto expected_all = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x2"],
        [2, "b", "y2"],
        [3, "c", "z2"],
        [4, "d", "w2"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_all));

    ASSERT_OK_AND_ASSIGN(std::string anchor_file_name, PlannedAnchorFileName(table_path));
    ASSERT_OK(CommitDeletionVectors(table_path, commit_msgs0[0],
                                    {{anchor_file_name, /*deleted_positions=*/{1, 3}}}));

    // both files of the group must drop the same rows to keep the column merge aligned
    auto expected_deleted = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x2"],
        [3, "c", "z2"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_deleted));

    auto expected_with_row_id = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_}),
            R"([
        [1, "a", "x2", 0],
        [3, "c", "z2", 2]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_with_row_id));

    // a row-range selection composes with the deletion vector: rows {1, 2} minus deleted {1}
    auto expected_selected = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [3, "c", "z2"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_selected,
                          /*predicate=*/nullptr, /*row_ranges=*/{Range(1, 2)}));
}

TEST_P(DataEvolutionTableTest, TestReadWithDeletionVectorsAcrossReadBatches) {
    // the 12 rows below span several read batches, so the deletion vector empties a whole
    // batch of every file of the group: each file reader then skips that batch entirely and
    // the column merge has to stay aligned on the surviving row count alone
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true, {{Options::READ_BATCH_SIZE, "4"}});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> group_msgs,
                         WriteAndCommitGroup(table_path, /*first_row_id=*/0,
                                             /*f0_values=*/{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}));

    // positions 4-7 cover a whole read batch, position 9 only part of the next one
    ASSERT_OK_AND_ASSIGN(std::string anchor_file_name, PlannedAnchorFileName(table_path));
    ASSERT_OK(CommitDeletionVectors(table_path, group_msgs[0],
                                    {{anchor_file_name, /*deleted_positions=*/{4, 5, 6, 7, 9}}}));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a0", "y0"],
        [1, "a1", "y1"],
        [2, "a2", "y2"],
        [3, "a3", "y3"],
        [8, "a8", "y8"],
        [10, "a10", "y10"],
        [11, "a11", "y11"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    arrow::FieldVector row_id_fields = {SpecialFields::RowId().field_};
    auto expected_row_ids = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(row_id_fields), R"([
        [0], [1], [2], [3], [8], [10], [11]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"_ROW_ID"}, expected_row_ids));

    // a row-range selection that spans the fully deleted batch composes with it
    auto expected_selected = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [3, "a3", "y3"],
        [8, "a8", "y8"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_selected,
                          /*predicate=*/nullptr, /*row_ranges=*/{Range(3, 8)}));
}

TEST_P(DataEvolutionTableTest, TestReadWithDeletionVectorsOnPartOfRowRangeGroups) {
    // one split per row range group, so a group's deletion file must not reach the other
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true,
                             {{Options::SOURCE_SPLIT_TARGET_SIZE, "1"}});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    auto src_array0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "z"],
        [4, "d", "w"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs0,
                         WriteArray(table_path, schema->field_names(), src_array0));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs0);
    ASSERT_OK(Commit(table_path, commit_msgs0));

    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [5, "e", "v"],
        [6, "f", "u"],
        [7, "g", "t"],
        [8, "h", "s"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1,
                         WriteArray(table_path, schema->field_names(), src_array1));
    SetFirstRowId(/*reset_first_row_id=*/4, commit_msgs1);
    ASSERT_OK(Commit(table_path, commit_msgs1));

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> anchor_file_names,
                         PlannedAnchorFileNames(table_path));
    ASSERT_EQ(anchor_file_names.size(), 2);
    ASSERT_OK(CommitDeletionVectors(table_path, commit_msgs0[0],
                                    {{anchor_file_names[0], /*deleted_positions=*/{1, 3}}}));

    // the deletion vector applies to its own group only, the other group keeps every row
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x"],
        [3, "c", "z"],
        [5, "e", "v"],
        [6, "f", "u"],
        [7, "g", "t"],
        [8, "h", "s"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    // a projection-only read drops the same rows
    arrow::FieldVector f0_fields = {fields_[0]};
    auto expected_f0 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(f0_fields), R"([
        [1], [3], [5], [6], [7], [8]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f0"}, expected_f0));

    arrow::FieldVector row_id_fields = {SpecialFields::RowId().field_};
    auto expected_row_ids = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(row_id_fields), R"([
        [0], [2], [4], [5], [6], [7]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"_ROW_ID"}, expected_row_ids));
}

TEST_P(DataEvolutionTableTest, TestReadWithDeletionVectorsOnEveryRowRangeGroup) {
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // one split (the default target size keeps both groups together), so a single split
    // deletion vector factory serves two groups anchored at different row ids
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> group_msgs0,
        WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{0, 1, 2, 3}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/4, /*f0_values=*/{4, 5, 6, 7}));

    ASSERT_OK_AND_ASSIGN(std::vector<std::string> anchor_file_names,
                         PlannedAnchorFileNames(table_path));
    ASSERT_EQ(anchor_file_names.size(), 2);

    // Positions are anchor-relative, so the groups deliberately delete different ones: group 0
    // drops {1, 3} of row ids 0-3, group 1 drops {0, 2} of row ids 4-7. Reading a group with the
    // other group's vector, or with its anchor range as the shift base, cannot match below.
    ASSERT_OK(CommitDeletionVectors(table_path, group_msgs0[0],
                                    {{anchor_file_names[0], /*deleted_positions=*/{1, 3}},
                                     {anchor_file_names[1], /*deleted_positions=*/{0, 2}}}));

    // The read looks a group's deletion vector up by its anchor file name, so the scan has to
    // hand it exactly that. Asserting it separates a scan-side mix-up from a read-side one.
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> planned_splits,
                         PlanSplits(table_path));
    ASSERT_EQ(planned_splits.size(), 1);
    auto planned_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(planned_splits[0]);
    ASSERT_TRUE(planned_split_impl);

    ASSERT_OK_AND_ASSIGN(DeletionCardinalityMap cardinality_by_file,
                         DeletionCardinalityByDataFile(planned_splits[0]));
    DeletionCardinalityMap expected_cardinalities = {{anchor_file_names[0], 2},
                                                     {anchor_file_names[1], 2}};
    ASSERT_EQ(cardinality_by_file, expected_cardinalities);

    // the same split reports the surviving row count the limit push down prunes on: the two
    // groups hold 4 rows each and each deletion vector drops 2 of them
    ASSERT_OK_AND_ASSIGN(std::optional<int64_t> merged_row_count,
                         planned_split_impl->MergedRowCount());
    ASSERT_EQ(std::optional<int64_t>(4), merged_row_count);

    // a count query answers from that metadata alone, never reading a row, so the deletion
    // vectors have to reach it too: without them it reports the 8 rows the files hold
    ReadContextBuilder count_context_builder(table_path);
    count_context_builder.SetReadFieldNames(schema->field_names());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> count_context,
                         count_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> count_table_read,
                         TableRead::Create(std::move(count_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CountReader> count_reader,
                         count_table_read->CreateCountReader(planned_splits));
    ASSERT_OK_AND_ASSIGN(int64_t counted_rows, count_reader->CountRows());
    ASSERT_EQ(counted_rows, 4);

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [0, "a0", "y0"],
        [2, "a2", "y2"],
        [5, "a1", "y1"],
        [7, "a3", "y3"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    arrow::FieldVector row_id_fields = {SpecialFields::RowId().field_};
    auto expected_row_ids = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(row_id_fields), R"([
        [0], [2], [5], [7]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"_ROW_ID"}, expected_row_ids));

    // a row-range selection straddling the group boundary composes with both deletion vectors:
    // row ids {2, 3, 4, 5} minus the deleted {3, 4}
    auto expected_selected = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [2, "a2", "y2"],
        [5, "a1", "y1"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_selected,
                          /*predicate=*/nullptr, /*row_ranges=*/{Range(2, 5)}));
}

TEST_P(DataEvolutionTableTest, TestReadWithFullyDeletedRowRangeGroup) {
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    // one split (the default target size keeps both groups together), so the emptied group's
    // readers are concatenated with the surviving group's
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> group_msgs0,
        WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{0, 1, 2, 3}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/4, /*f0_values=*/{4, 5, 6, 7}));

    // both file readers of the first group then yield nothing, and its column merge must
    // produce no rows at all instead of misaligning
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> anchor_file_names,
                         PlannedAnchorFileNames(table_path));
    ASSERT_EQ(anchor_file_names.size(), 2);
    ASSERT_OK(CommitDeletionVectors(table_path, group_msgs0[0],
                                    {{anchor_file_names[0], /*deleted_positions=*/{0, 1, 2, 3}}}));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [4, "a0", "y0"],
        [5, "a1", "y1"],
        [6, "a2", "y2"],
        [7, "a3", "y3"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_array));

    arrow::FieldVector row_id_fields = {SpecialFields::RowId().field_};
    auto expected_row_ids = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(row_id_fields), R"([
        [4], [5], [6], [7]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"_ROW_ID"}, expected_row_ids));

    // a row-range selection spanning both groups keeps only what survives in the second one
    auto expected_selected = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [4, "a0", "y0"],
        [5, "a1", "y1"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_selected,
                          /*predicate=*/nullptr, /*row_ranges=*/{Range(0, 5)}));

    // a selection covering only deleted rows returns nothing. The plan is asserted non-empty
    // too: the scan cannot prune the split on row ids alone, so the emptiness comes from the
    // deletion vector rather than from a plan with nothing to read.
    ASSERT_OK_AND_ASSIGN(LimitScanResult only_deleted,
                         ScanAndReadWithLimit(table_path, schema->field_names(), /*limit=*/100,
                                              /*predicate=*/nullptr,
                                              /*row_ranges=*/{Range(0, 3)}));
    ASSERT_FALSE(only_deleted.splits.empty());
    ASSERT_FALSE(only_deleted.rows);
}

TEST_P(DataEvolutionTableTest, TestReadAfterUpdatingDeletionVectors) {
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    auto schema = arrow::schema(fields_);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "z"],
        [4, "d", "w"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, schema->field_names(), src_array));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    ASSERT_OK_AND_ASSIGN(std::string anchor_file_name, PlannedAnchorFileName(table_path));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CommitMessage> first_dv_msg,
                         CommitDeletionVectors(table_path, commit_msgs[0],
                                               {{anchor_file_name, /*deleted_positions=*/{1}}}));

    auto expected_after_first = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x"],
        [3, "c", "z"],
        [4, "d", "w"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_after_first));

    // a second deletion vector replaces the first one instead of both staying live
    ASSERT_OK(CommitDeletionVectors(table_path, commit_msgs[0],
                                    {{anchor_file_name, /*deleted_positions=*/{1, 3}}},
                                    /*replaced_commit_msg=*/first_dv_msg));

    auto expected_after_update = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x"],
        [3, "c", "z"]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, schema->field_names(), expected_after_update));
}

TEST_P(DataEvolutionTableTest, TestReadWithDeletionVectorsAfterAddingColumn) {
    if (FileFormat() == "avro") {
        GTEST_SKIP() << "Avro has no stats, which the added column's scan pruning relies on";
    }
    std::map<std::string, std::string> options =
        CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a", "x"],
        [2, "b", "y"],
        [3, "c", "z"],
        [4, "d", "w"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         WriteArray(table_path, arrow::schema(fields_)->field_names(), src_array));
    SetFirstRowId(/*reset_first_row_id=*/0, commit_msgs);
    ASSERT_OK(Commit(table_path, commit_msgs));

    // add column f3, then fill it for the same row range: the group merges columns from two
    // files written under different schema ids
    auto f3 = arrow::field("f3", arrow::int64());
    ASSERT_OK(TestHelper::WriteNextSchema(dir_->GetFileSystem(), table_path,
                                          {DataField(0, fields_[0]), DataField(1, fields_[1]),
                                           DataField(2, fields_[2]), DataField(3, f3)},
                                          /*highest_field_id=*/3, options));

    auto f3_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f3}), R"([
        [10], [20], [30], [40]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto f3_commit_msgs, WriteArray(table_path, {"f3"}, f3_array));
    SetFirstRowId(/*reset_first_row_id=*/0, f3_commit_msgs);
    ASSERT_OK(Commit(table_path, f3_commit_msgs));

    // the deletion vector is still anchored on the oldest normal file, written before the
    // column was added
    ASSERT_OK_AND_ASSIGN(std::string anchor_file_name, PlannedAnchorFileName(table_path));
    ASSERT_OK(CommitDeletionVectors(table_path, commit_msgs[0],
                                    {{anchor_file_name, /*deleted_positions=*/{1, 3}}}));

    arrow::FieldVector evolved_fields = {fields_[0], fields_[1], fields_[2], f3};
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(evolved_fields), R"([
        [1, "a", "x", 10],
        [3, "c", "z", 30]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "f3"}, expected_array));

    // projecting only the added column keeps the same surviving rows
    auto expected_f3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({f3}), R"([
        [10], [30]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f3"}, expected_f3));
}

TEST_P(DataEvolutionTableTest, TestLimitPushDownWithHeavilyDeletedFirstRowRangeGroup) {
    // one split per row range group, so the limit has to span both to be satisfied
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true,
                             {{Options::SOURCE_SPLIT_TARGET_SIZE, "1"}});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> group_msgs0,
        WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{0, 1, 2, 3}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/4, /*f0_values=*/{4, 5, 6, 7}));

    // the first group keeps a single surviving row
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> anchor_file_names,
                         PlannedAnchorFileNames(table_path));
    ASSERT_EQ(anchor_file_names.size(), 2);
    ASSERT_OK(CommitDeletionVectors(table_path, group_msgs0[0],
                                    {{anchor_file_names[0], /*deleted_positions=*/{0, 1, 2}}}));

    // the first split alone satisfies a limit of 1: it still holds the one row that survived
    // the deletion vector
    ASSERT_OK_AND_ASSIGN(LimitScanResult limit_1,
                         ScanAndReadWithLimit(table_path, {"f0"}, /*limit=*/1));
    ASSERT_EQ(limit_1.splits.size(), 1);
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> limit_1_values, CollectF0Values(limit_1.rows));
    ASSERT_EQ(limit_1_values, (std::vector<int32_t>{3}));

    // the first split contributes only one surviving row, so a limit of 3 needs the second one
    ASSERT_OK_AND_ASSIGN(LimitScanResult limit_3,
                         ScanAndReadWithLimit(table_path, {"f0"}, /*limit=*/3));
    ASSERT_EQ(limit_3.splits.size(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> limit_3_values, CollectF0Values(limit_3.rows));
    ASSERT_EQ(limit_3_values, (std::vector<int32_t>{3, 4, 5, 6, 7}));
}

TEST_P(DataEvolutionTableTest, TestLimitPushDownDisabledByNonPartitionFilter) {
    // one split per row range group, so the plan can drop the group holding the matches
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false,
                             {{Options::SOURCE_SPLIT_TARGET_SIZE, "1"}});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // First row range group: no f0 value lies in [100, 200], but the 300 keeps the group's stats
    // range straddling the filter so the scan cannot prune it. It therefore reaches the plan
    // reporting four rows, and contributes none of them to the result.
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{0, 1, 2, 300}));
    // second row range group: every f0 value matches
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/4,
                                  /*f0_values=*/{100, 101, 102, 103}));

    // the metadata row count of the first split alone satisfies the limit, but the filter runs
    // while reading and drops every one of its rows. Pruning the plan on the metadata count
    // would return nothing, so the push down has to be skipped and both splits kept.
    auto at_least_100 = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                         FieldType::INT, Literal(100));
    auto at_most_200 = PredicateBuilder::LessOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                     FieldType::INT, Literal(200));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({at_least_100, at_most_200}));
    // filtering while reading is what makes the metadata count an upper bound, so the read has
    // to opt into it for the rows below to show what the push down would have thrown away
    ASSERT_OK_AND_ASSIGN(LimitScanResult limited,
                         ScanAndReadWithLimit(table_path, {"f0"}, /*limit=*/2, predicate,
                                              /*row_ranges=*/{},
                                              /*enable_predicate_filter=*/true));
    ASSERT_EQ(limited.splits.size(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> values, CollectF0Values(limited.rows));
    ASSERT_EQ(values, (std::vector<int32_t>{100, 101, 102, 103}));
}

TEST_P(DataEvolutionTableTest, TestLimitPushDownKeptByPartitionFilter) {
    std::vector<std::string> partition_keys = {"f1"};
    CreateTable(partition_keys);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // one row range group per partition, each holding four rows with its own f0 range. A split
    // never spans partitions, so the plan holds one split per partition for the push down to
    // prune.
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{0, 1, 2, 3},
                                  /*partition=*/{{"f1", "p0"}}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/4,
                                  /*f0_values=*/{10, 11, 12, 13}, /*partition=*/{{"f1", "p1"}}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/8,
                                  /*f0_values=*/{20, 21, 22, 23}, /*partition=*/{{"f1", "p2"}}));

    // a predicate on the partition key alone is evaluated while planning, never while reading,
    // so every row a surviving split reports is actually returned and the push down stays on
    auto not_p0 =
        PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                   Literal(FieldType::STRING, "p0", 2));

    // baseline: a limit no split combination can reach prunes nothing, so this only shows which
    // splits the partition filter itself leaves behind
    ASSERT_OK_AND_ASSIGN(LimitScanResult unpruned,
                         ScanAndReadWithLimit(table_path, {"f0", "f1"}, /*limit=*/100, not_p0));
    ASSERT_EQ(unpruned.splits.size(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> unpruned_values, CollectF0Values(unpruned.rows));
    std::sort(unpruned_values.begin(), unpruned_values.end());
    ASSERT_EQ(unpruned_values, (std::vector<int32_t>{10, 11, 12, 13, 20, 21, 22, 23}));

    // the first matching split already holds four rows, so a limit of 2 drops the second one.
    // Treating the partition filter as a read-time filter would skip the push down and keep both.
    ASSERT_OK_AND_ASSIGN(LimitScanResult limited,
                         ScanAndReadWithLimit(table_path, {"f0", "f1"}, /*limit=*/2, not_p0));
    ASSERT_EQ(limited.splits.size(), 1);
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> limited_values, CollectF0Values(limited.rows));
    std::sort(limited_values.begin(), limited_values.end());
    ASSERT_TRUE(limited_values == (std::vector<int32_t>{10, 11, 12, 13}) ||
                limited_values == (std::vector<int32_t>{20, 21, 22, 23}))
        << "unexpected kept rows for limit 2";

    // one non-partition conjunct added to the same partition filter: the predicate is no longer
    // settled while planning, so the push down is skipped and the split the limit would have
    // dropped stays
    auto at_least_10 = PredicateBuilder::GreaterOrEqual(/*field_index=*/0, /*field_name=*/"f0",
                                                        FieldType::INT, Literal(10));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> mixed_predicate,
                         PredicateBuilder::And({not_p0, at_least_10}));
    ASSERT_OK_AND_ASSIGN(LimitScanResult mixed, ScanAndReadWithLimit(table_path, {"f0", "f1"},
                                                                     /*limit=*/2, mixed_predicate));
    ASSERT_EQ(mixed.splits.size(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> mixed_values, CollectF0Values(mixed.rows));
    std::sort(mixed_values.begin(), mixed_values.end());
    ASSERT_EQ(mixed_values, (std::vector<int32_t>{10, 11, 12, 13, 20, 21, 22, 23}));
}

TEST_P(DataEvolutionTableTest, TestLimitPushDownDisabledByRowRangeIndex) {
    // one split per row range group, so the plan can drop the group holding the selection
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false,
                             {{Options::SOURCE_SPLIT_TARGET_SIZE, "1"}});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{0, 1, 2, 3}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/4,
                                  /*f0_values=*/{100, 101, 102, 103}));

    // the index selects one row of the first group and two of the second, but the first split
    // still reports four rows. Pruning on that count would drop the second group and return a
    // single row for a limit of 2, so the push down has to be skipped.
    ASSERT_OK_AND_ASSIGN(LimitScanResult limited,
                         ScanAndReadWithLimit(table_path, {"f0"}, /*limit=*/2,
                                              /*predicate=*/nullptr,
                                              /*row_ranges=*/{Range(3, 3), Range(5, 6)}));
    ASSERT_EQ(limited.splits.size(), 2);
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> values, CollectF0Values(limited.rows));
    ASSERT_EQ(values, (std::vector<int32_t>{3, 101, 102}));
}

TEST_P(DataEvolutionTableTest, TestCompactAcrossEvolvedFieldGroups) {
    std::map<std::string, std::string> options =
        CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Two evolved field groups (row id ranges [0, 1] and [2, 3]), each made of a full-column
    // file and a newer partial f2 file over the exact same rows.
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{1, 2}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/2, /*f0_values=*/{3, 4}));

    // The coordinator merges the four contiguous files into one full-column file.
    std::map<std::string, std::string> compact_options = options;
    compact_options[Options::COMPACTION_MIN_FILE_NUM] = "2";
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(messages.size(), 1);
    auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message_impl);
    const CompactIncrement& compact_increment = message_impl->GetCompactIncrement();
    ASSERT_EQ(compact_increment.CompactBefore().size(), 4);
    ASSERT_EQ(compact_increment.CompactAfter().size(), 1);
    const std::shared_ptr<DataFileMeta>& compacted_file = compact_increment.CompactAfter()[0];
    // Row ids and the merged sequence number range of the inputs are preserved, so the rewrite
    // does not disturb row tracking metadata.
    ASSERT_OK_AND_ASSIGN(int64_t compacted_first_row_id, compacted_file->NonNullFirstRowId());
    ASSERT_EQ(compacted_first_row_id, 0);
    ASSERT_EQ(compacted_file->row_count, 4);
    ASSERT_EQ(compacted_file->min_sequence_number, 1);
    ASSERT_EQ(compacted_file->max_sequence_number, 4);
    ASSERT_TRUE(compacted_file->file_source.has_value());
    ASSERT_EQ(compacted_file->file_source.value(), FileSource::Compact());
    ASSERT_EQ(compacted_file->write_cols, std::nullopt);
    // Java's contract for data-evolution compact messages: total buckets stay unset.
    ASSERT_EQ(message_impl->TotalBuckets(), std::nullopt);

    ASSERT_OK(Commit(table_path, messages));

    // The merged rows keep the newest f2 values and their row ids. The read now serves from
    // the single compacted file.
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_}),
            R"([
        [1, "a0", "y0", 0],
        [2, "a1", "y1", 1],
        [3, "a0", "y0", 2],
        [4, "a1", "y1", 3]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_array));

    // The compacted file carries the merged sequence range, so every row now reads the group
    // maximum sequence number (4), where rows 0-1 read 2 before the compaction.
    auto expected_sequence_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], SpecialFields::RowId().field_,
                            SpecialFields::SequenceNumber().field_}),
            R"([
        [1, 0, 4],
        [2, 1, 4],
        [3, 2, 4],
        [4, 3, 4]
    ])")
            .ValueOrDie());
    ASSERT_OK(
        ScanAndRead(table_path, {"f0", "_ROW_ID", "_SEQUENCE_NUMBER"}, expected_sequence_array));

    // A second coordinator run finds a single file and plans nothing.
    ASSERT_OK_AND_ASSIGN(messages, AppendCompactCoordinator::Run(
                                       table_path, compact_options,
                                       /*partitions=*/{}, dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_TRUE(messages.empty());
}

TEST_P(DataEvolutionTableTest, TestCompactRejectsDeletionVectorTables) {
    std::map<std::string, std::string> options =
        CreateDataEvolutionTable(/*deletion_vectors_enabled=*/true);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{1, 2}));

    // Deletion vectors are keyed by the row range group's anchor file; without the DV
    // migration of Java's DataEvolutionCompactDeletionVectorRewriter the coordinator keeps
    // rejecting such tables instead of dropping the deletes.
    ASSERT_NOK_WITH_MSG(AppendCompactCoordinator::Run(table_path, options, /*partitions=*/{},
                                                      dir_->GetFileSystem(), GetDefaultPool())
                            .status(),
                        "does not support deletion vectors");

    // Nor can the rejection be slipped past by overriding deletion vectors off — the rewrite
    // neither reads nor migrates the deletion files, so the immutable-option guard fires
    // first.
    std::map<std::string, std::string> compact_options = options;
    compact_options[Options::DELETION_VECTORS_ENABLED] = "false";
    ASSERT_NOK_WITH_MSG(
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool())
            .status(),
        "immutable table options");
}

TEST_P(DataEvolutionTableTest, TestCompactRejectsImmutableOptionOverrides) {
    // Turning data evolution off at the compaction entry point would route the table into
    // the plain append rewrite, which reorders row ids; the override is rejected before any
    // scanning. The same guard covers the other path-deciding and layout options, checked
    // here against the same table.
    std::map<std::string, std::string> options =
        CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{1, 2}));

    const std::vector<std::pair<std::string, std::string>> overrides = {
        {Options::DATA_EVOLUTION_ENABLED, "false"},
        {Options::ROW_TRACKING_ENABLED, "false"},
        {Options::BUCKET, "2"},
        {Options::BLOB_FIELD, "f0"},
    };
    for (const auto& [key, value] : overrides) {
        SCOPED_TRACE(key);
        std::map<std::string, std::string> compact_options = options;
        compact_options[key] = value;
        ASSERT_NOK_WITH_MSG(
            AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                          dir_->GetFileSystem(), GetDefaultPool())
                .status(),
            "immutable table options");
    }
}

TEST_P(DataEvolutionTableTest, TestCompactRejectsLegacyRewriteRowIdsOption) {
    std::map<std::string, std::string> options =
        CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{1, 2}));

    // The legacy row-id rewriting mode was removed in Java; the coordinator honors the same
    // contract instead of silently ignoring the option.
    std::map<std::string, std::string> compact_options = options;
    compact_options.emplace("data-evolution.compaction.rewrite-row-ids", "true");
    ASSERT_NOK_WITH_MSG(
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool())
            .status(),
        "no longer supported");
}

TEST_P(DataEvolutionTableTest, TestCompactAcrossEvolvedFieldGroupsWithPartitions) {
    CreateTable(/*partition_keys=*/{"f1"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // One evolved field group per partition, each made of a full-column file and a newer
    // partial f2 file over the same rows.
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{1, 2},
                                  /*partition=*/{{"f1", "2024"}}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/2, /*f0_values=*/{3, 4},
                                  /*partition=*/{{"f1", "2025"}}));

    std::map<std::string, std::string> compact_options = {{Options::COMPACTION_MIN_FILE_NUM, "2"}};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool()));
    // Bins never span partitions: one task per partition, each merging the two files of its
    // field group.
    ASSERT_EQ(messages.size(), 2);
    std::set<int64_t> compacted_first_row_ids;
    for (const auto& message : messages) {
        auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(message);
        ASSERT_TRUE(message_impl);
        const CompactIncrement& compact_increment = message_impl->GetCompactIncrement();
        ASSERT_EQ(compact_increment.CompactBefore().size(), 2);
        ASSERT_EQ(compact_increment.CompactAfter().size(), 1);
        ASSERT_OK_AND_ASSIGN(int64_t compacted_first_row_id,
                             compact_increment.CompactAfter()[0]->NonNullFirstRowId());
        compacted_first_row_ids.insert(compacted_first_row_id);
    }
    ASSERT_EQ(compacted_first_row_ids, (std::set<int64_t>{0, 2}));

    ASSERT_OK(Commit(table_path, messages));

    // Split order across partitions does not follow row ids, so each partition is verified
    // through its own partition predicate.
    auto read_type =
        arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_});
    auto expected_2024 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [1, "2024", "y0", 0],
        [2, "2024", "y1", 1]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(
        table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_2024,
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                Literal(FieldType::STRING, "2024", 4))));
    auto expected_2025 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [3, "2025", "y0", 2],
        [4, "2025", "y1", 3]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(
        table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_2025,
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                Literal(FieldType::STRING, "2025", 4))));
}

TEST_P(DataEvolutionTableTest, TestCompactWithPartitionFilter) {
    CreateTable(/*partition_keys=*/{"f1"});
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{1, 2},
                                  /*partition=*/{{"f1", "2024"}}));
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/2, /*f0_values=*/{3, 4},
                                  /*partition=*/{{"f1", "2025"}}));

    // Only the selected partition is planned; the other partition's field group stays as it
    // is and keeps serving reads from its original files.
    std::map<std::string, std::string> compact_options = {{Options::COMPACTION_MIN_FILE_NUM, "2"}};
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> messages,
                         AppendCompactCoordinator::Run(table_path, compact_options,
                                                       /*partitions=*/{{{"f1", "2024"}}},
                                                       dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(messages.size(), 1);
    auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message_impl);
    const CompactIncrement& compact_increment = message_impl->GetCompactIncrement();
    ASSERT_EQ(compact_increment.CompactBefore().size(), 2);
    ASSERT_EQ(compact_increment.CompactAfter().size(), 1);
    ASSERT_OK_AND_ASSIGN(int64_t compacted_first_row_id,
                         compact_increment.CompactAfter()[0]->NonNullFirstRowId());
    ASSERT_EQ(compacted_first_row_id, 0);

    ASSERT_OK(Commit(table_path, messages));

    // Split order across partitions does not follow row ids, so each partition is verified
    // through its own partition predicate: the compacted 2024 rows and the untouched 2025
    // rows read back unchanged.
    auto read_type =
        arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_});
    auto expected_2024 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [1, "2024", "y0", 0],
        [2, "2024", "y1", 1]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(
        table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_2024,
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                Literal(FieldType::STRING, "2024", 4))));
    auto expected_2025 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [3, "2025", "y0", 2],
        [4, "2025", "y1", 3]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(
        table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_2025,
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::STRING,
                                Literal(FieldType::STRING, "2025", 4))));
}

TEST_P(DataEvolutionTableTest, TestCompactAcrossSchemaEvolution) {
    std::map<std::string, std::string> options =
        CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Schema 0: two full-row commits, row ids [0, 1] and [2, 3], sequence numbers 1 and 2.
    for (const std::string& rows_json : {std::string(R"([[1, "a0", "x0"], [2, "a1", "x1"]])"),
                                         std::string(R"([[3, "a2", "x2"], [4, "a3", "x3"]])")}) {
        auto schema0_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), rows_json)
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> schema0_msgs,
                             WriteArray(table_path, {"f0", "f1", "f2"}, schema0_array));
        ASSERT_OK(Commit(table_path, schema0_msgs));
    }

    // Evolve the schema: change f0 INT -> BIGINT (keeps field id 0, a type change) and add
    // f3 INT with the fresh field id 3.
    arrow::FieldVector evolved_fields = {
        arrow::field("f0", arrow::int64()),
        arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::utf8()),
        arrow::field("f3", arrow::int32()),
    };
    ASSERT_OK(TestHelper::WriteNextSchema(
        dir_->GetFileSystem(), table_path,
        {DataField(0, evolved_fields[0]), DataField(1, evolved_fields[1]),
         DataField(2, evolved_fields[2]), DataField(3, evolved_fields[3])},
        /*highest_field_id=*/3, options));

    // Schema 1: two more full rows, row ids [4, 5], sequence number 3 (rows [2, 3] from the
    // second schema-0 commit above stay untouched and keep their added f3 as NULL).
    auto schema1_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(evolved_fields), R"([
        [5, "a4", "x4", 50],
        [6, "a5", "x5", 60]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> schema1_msgs,
                         WriteArray(table_path, {"f0", "f1", "f2", "f3"}, schema1_array));
    ASSERT_OK(Commit(table_path, schema1_msgs));

    // A schema-1 partial write fills the added f3 for the first schema-0 rows: the [0, 1]
    // field group now merges files written under different schema ids, sequence number 4.
    auto f3_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({evolved_fields[3]}), R"([
        [100],
        [200]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> f3_msgs,
                         WriteArray(table_path, {"f3"}, f3_array));
    SetFirstRowId(/*reset_first_row_id=*/0, f3_msgs);
    ASSERT_OK(Commit(table_path, f3_msgs));

    // Pre-compaction read through the latest schema: f0 is cast to BIGINT everywhere, the
    // added f3 is NULL for the untouched schema-0 rows [2, 3] and filled through the
    // cross-schema field group for rows [0, 1].
    auto read_type =
        arrow::struct_({evolved_fields[0], evolved_fields[1], evolved_fields[2], evolved_fields[3],
                        SpecialFields::RowId().field_, SpecialFields::SequenceNumber().field_});
    std::vector<std::string> read_names = {"f0", "f1", "f2", "f3", "_ROW_ID", "_SEQUENCE_NUMBER"};
    auto expected_before = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [1, "a0", "x0", 100, 0, 4],
        [2, "a1", "x1", 200, 1, 4],
        [3, "a2", "x2", null, 2, 2],
        [4, "a3", "x3", null, 3, 2],
        [5, "a4", "x4", 50, 4, 3],
        [6, "a5", "x5", 60, 5, 3]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, read_names, expected_before));

    // The coordinator merges all four files — schema-0, schema-1 and the cross-schema
    // partial — into one file written with the latest schema.
    std::map<std::string, std::string> compact_options = {{Options::COMPACTION_MIN_FILE_NUM, "2"}};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(messages.size(), 1);
    auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message_impl);
    const CompactIncrement& compact_increment = message_impl->GetCompactIncrement();
    ASSERT_EQ(compact_increment.CompactBefore().size(), 4);
    ASSERT_EQ(compact_increment.CompactAfter().size(), 1);
    const std::shared_ptr<DataFileMeta>& compacted_file = compact_increment.CompactAfter()[0];
    // The rewritten file holds every column of the latest schema and carries its schema id;
    // row ids and the merged sequence range of the inputs are preserved.
    ASSERT_EQ(compacted_file->schema_id, 1);
    ASSERT_EQ(compacted_file->write_cols, std::nullopt);
    ASSERT_OK_AND_ASSIGN(int64_t compacted_first_row_id, compacted_file->NonNullFirstRowId());
    ASSERT_EQ(compacted_first_row_id, 0);
    ASSERT_EQ(compacted_file->row_count, 6);
    ASSERT_EQ(compacted_file->min_sequence_number, 1);
    ASSERT_EQ(compacted_file->max_sequence_number, 4);

    ASSERT_OK(Commit(table_path, messages));

    // After the rewrite the merged values survive: the cast f0, the NULL-filled f3 and the
    // cross-schema f3 fill are now materialized in the compacted file, and every row reads
    // the group's maximum sequence number.
    auto expected_after = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [1, "a0", "x0", 100, 0, 4],
        [2, "a1", "x1", 200, 1, 4],
        [3, "a2", "x2", null, 2, 4],
        [4, "a3", "x3", null, 3, 4],
        [5, "a4", "x4", 50, 4, 4],
        [6, "a5", "x5", 60, 5, 4]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, read_names, expected_after));
}

TEST_P(DataEvolutionTableTest, TestCompactAfterDropColumn) {
    std::map<std::string, std::string> options =
        CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // Schema 0: two full rows, row ids [0, 1], sequence number 1.
    auto schema0_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [1, "a0", "x0"],
        [2, "a1", "x1"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> schema0_msgs,
                         WriteArray(table_path, {"f0", "f1", "f2"}, schema0_array));
    ASSERT_OK(Commit(table_path, schema0_msgs));

    // Drop f2. The surviving fields keep their ids, and the highest field id stays 2 so a
    // later added column cannot recycle the dropped id.
    ASSERT_OK(TestHelper::WriteNextSchema(dir_->GetFileSystem(), table_path,
                                          {DataField(0, fields_[0]), DataField(1, fields_[1])},
                                          /*highest_field_id=*/2, options));

    // Schema 1: two more rows without f2, row ids [2, 3], sequence number 2.
    arrow::FieldVector remaining_fields = {fields_[0], fields_[1]};
    auto schema1_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(remaining_fields), R"([
        [3, "a2"],
        [4, "a3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> schema1_msgs,
                         WriteArray(table_path, {"f0", "f1"}, schema1_array));
    ASSERT_OK(Commit(table_path, schema1_msgs));

    auto read_type =
        arrow::struct_({remaining_fields[0], remaining_fields[1], SpecialFields::RowId().field_});
    std::vector<std::string> read_names = {"f0", "f1", "_ROW_ID"};
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [1, "a0", 0],
        [2, "a1", 1],
        [3, "a2", 2],
        [4, "a3", 3]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, read_names, expected_array));

    // The rewrite reads the schema-0 file with the dropped column projected away and writes
    // the latest two-column schema.
    std::map<std::string, std::string> compact_options = {{Options::COMPACTION_MIN_FILE_NUM, "2"}};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(messages.size(), 1);
    auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(messages[0]);
    ASSERT_TRUE(message_impl);
    const CompactIncrement& compact_increment = message_impl->GetCompactIncrement();
    ASSERT_EQ(compact_increment.CompactBefore().size(), 2);
    ASSERT_EQ(compact_increment.CompactAfter().size(), 1);
    const std::shared_ptr<DataFileMeta>& compacted_file = compact_increment.CompactAfter()[0];
    ASSERT_EQ(compacted_file->schema_id, 1);
    ASSERT_EQ(compacted_file->write_cols, std::nullopt);
    ASSERT_OK_AND_ASSIGN(int64_t compacted_first_row_id, compacted_file->NonNullFirstRowId());
    ASSERT_EQ(compacted_first_row_id, 0);
    ASSERT_EQ(compacted_file->row_count, 4);

    ASSERT_OK(Commit(table_path, messages));
    ASSERT_OK(ScanAndRead(table_path, read_names, expected_array));
}

TEST_P(DataEvolutionTableTest, TestStaleCompactMessagePreservesConcurrentPartialUpdate) {
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    // One evolved field group over rows [0, 1]: a full-column file and a newer partial f2
    // file ("y0"/"y1").
    ASSERT_OK(WriteAndCommitGroup(table_path, /*first_row_id=*/0, /*f0_values=*/{1, 2}));

    // Plan and execute the compaction but hold its commit message back, so the commit below
    // races it with a stale view of the files.
    std::map<std::string, std::string> compact_options = {{Options::COMPACTION_MIN_FILE_NUM, "2"}};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(messages.size(), 1);

    // A concurrent partial update lands on the same rows after the compaction has read its
    // input.
    auto concurrent_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["z0"],
        ["z1"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> concurrent_msgs,
                         WriteArray(table_path, {"f2"}, concurrent_array));
    SetFirstRowId(/*reset_first_row_id=*/0, concurrent_msgs);
    ASSERT_OK(Commit(table_path, concurrent_msgs));

    // The stale compact message still commits: its before-files are still live, and the
    // rewritten file keeps the input group's sequence range, so the newer update stays on
    // top instead of being swallowed. Mirrors Java's
    // testCompactPreservesConcurrentPartialUpdateWithinCandidateRange.
    ASSERT_OK(Commit(table_path, messages));

    // f2 serves from the concurrent update (newest sequence), the other columns from the
    // compacted file, and row ids are unchanged.
    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_}),
            R"([
        [1, "a0", "z0", 0],
        [2, "a1", "z1", 1]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_array));
}

TEST_P(DataEvolutionTableTest, TestSmallFileCompactConflictsWithConcurrentPartialUpdate) {
    // Mirrors Java's testSmallFileCompactConflictsWithConcurrentPartialUpdate: merging the
    // groups [0, 0] and [1, 1] into one [0, 1] file moves the field group boundary, so a
    // concurrent partial update aligned with the OLD boundary must conflict — committing the
    // stale compact would leave the update's file misaligned with its new group.
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    for (const std::string& row_json :
         {std::string(R"([[10, "a0", "b0"]])"), std::string(R"([[11, "a1", "b1"]])")}) {
        auto row_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), row_json)
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> row_msgs,
                             WriteArray(table_path, {"f0", "f1", "f2"}, row_array));
        ASSERT_OK(Commit(table_path, row_msgs));
    }

    std::map<std::string, std::string> compact_options = {{Options::COMPACTION_MIN_FILE_NUM, "2"}};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(messages.size(), 1);

    // The concurrent update lands on row 0 only, aligned with the pre-compaction boundary.
    auto concurrent_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields_[2]}), R"([
        ["upd0"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> concurrent_msgs,
                         WriteArray(table_path, {"f2"}, concurrent_array));
    SetFirstRowId(/*reset_first_row_id=*/0, concurrent_msgs);
    ASSERT_OK(Commit(table_path, concurrent_msgs));

    // The compact-kind commit always runs the row range alignment check on data-evolution
    // tables, so the stale message is rejected and the table keeps serving the update.
    ASSERT_NOK_WITH_MSG(Commit(table_path, messages),
                        "'COMPACT' operations have encountered conflicts");

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_}),
            R"([
        [10, "a0", "upd0", 0],
        [11, "a1", "b1", 1]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_array));
}

TEST_P(DataEvolutionTableTest, TestCompactKeepsConcurrentAppendForNextSmallFileMerge) {
    // Mirrors Java's testCompactKeepsConcurrentAppendForNextSmallFileMerge: an append outside
    // the task's row id range neither conflicts with nor is consumed by the stale compact
    // message; the next coordinator round merges it with the compacted file.
    CreateDataEvolutionTable(/*deletion_vectors_enabled=*/false);
    std::string table_path = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");

    for (const std::string& row_json :
         {std::string(R"([[10, "a0", "b0"]])"), std::string(R"([[11, "a1", "b1"]])")}) {
        auto row_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), row_json)
                .ValueOrDie());
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> row_msgs,
                             WriteArray(table_path, {"f0", "f1", "f2"}, row_array));
        ASSERT_OK(Commit(table_path, row_msgs));
    }

    std::map<std::string, std::string> compact_options = {{Options::COMPACTION_MIN_FILE_NUM, "2"}};
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> messages,
        AppendCompactCoordinator::Run(table_path, compact_options, /*partitions=*/{},
                                      dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(messages.size(), 1);

    // The concurrent append takes the next row id range [2, 2], outside the task's range.
    auto append_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields_), R"([
        [12, "a2", "b2"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> append_msgs,
                         WriteArray(table_path, {"f0", "f1", "f2"}, append_array));
    ASSERT_OK(Commit(table_path, append_msgs));

    // The stale compact message still commits: the appended range does not overlap the
    // rewritten one.
    ASSERT_OK(Commit(table_path, messages));

    auto expected_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields_[0], fields_[1], fields_[2], SpecialFields::RowId().field_}),
            R"([
        [10, "a0", "b0", 0],
        [11, "a1", "b1", 1],
        [12, "a2", "b2", 2]
    ])")
            .ValueOrDie());
    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_array));

    // The next round packs the compacted file [0, 1] and the appended file [2, 2] — adjacent
    // ranges — into one task and merges them.
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> next_messages,
        AppendCompactCoordinator::Run(table_path, compact_options,
                                      /*partitions=*/{}, dir_->GetFileSystem(), GetDefaultPool()));
    ASSERT_EQ(next_messages.size(), 1);
    auto next_message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(next_messages[0]);
    ASSERT_TRUE(next_message_impl);
    const CompactIncrement& next_increment = next_message_impl->GetCompactIncrement();
    ASSERT_EQ(next_increment.CompactBefore().size(), 2);
    ASSERT_EQ(next_increment.CompactAfter().size(), 1);
    ASSERT_OK_AND_ASSIGN(int64_t next_first_row_id,
                         next_increment.CompactAfter()[0]->NonNullFirstRowId());
    ASSERT_EQ(next_first_row_id, 0);
    ASSERT_EQ(next_increment.CompactAfter()[0]->row_count, 3);
    ASSERT_OK(Commit(table_path, next_messages));
    ASSERT_OK(ScanAndRead(table_path, {"f0", "f1", "f2", "_ROW_ID"}, expected_array));
}

std::vector<DataEvolutionTableParam> GetTestValuesForDataEvolutionTableTest() {
    std::vector<DataEvolutionTableParam> values;
    for (bool enable_snapshot_live_manifest_cache : {false, true}) {
        values.emplace_back("parquet", enable_snapshot_live_manifest_cache);
#ifdef PAIMON_ENABLE_ORC
        values.emplace_back("orc", enable_snapshot_live_manifest_cache);
#endif
#ifdef PAIMON_ENABLE_AVRO
        values.emplace_back("avro", enable_snapshot_live_manifest_cache);
#endif
    }
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, DataEvolutionTableTest,
                         ::testing::ValuesIn(GetTestValuesForDataEvolutionTableTest()));

}  // namespace paimon::test
