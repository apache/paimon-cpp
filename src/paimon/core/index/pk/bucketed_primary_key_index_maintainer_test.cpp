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

#include "paimon/core/index/pk/bucketed_primary_key_index_maintainer.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_policy.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/record_batch.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {
namespace {

constexpr char kCommitUser[] = "pk-index-maintenance-test";

std::vector<std::shared_ptr<IndexFileMeta>> BTreeIndexFiles(const CommitMessageImpl& message,
                                                            bool added) {
    std::vector<std::shared_ptr<IndexFileMeta>> result;
    const std::vector<std::shared_ptr<IndexFileMeta>>& data_files =
        added ? message.GetNewFilesIncrement().NewIndexFiles()
              : message.GetNewFilesIncrement().DeletedIndexFiles();
    const std::vector<std::shared_ptr<IndexFileMeta>>& compact_files =
        added ? message.GetCompactIncrement().NewIndexFiles()
              : message.GetCompactIncrement().DeletedIndexFiles();
    for (const std::shared_ptr<IndexFileMeta>& file : data_files) {
        if (file != nullptr && file->IndexType() == "btree") {
            result.push_back(file);
        }
    }
    for (const std::shared_ptr<IndexFileMeta>& file : compact_files) {
        if (file != nullptr && file->IndexType() == "btree") {
            result.push_back(file);
        }
    }
    return result;
}

std::vector<PrimaryKeyIndexSourceFile> ExpectedSources(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    std::vector<PrimaryKeyIndexSourceFile> result;
    for (const std::shared_ptr<DataFileMeta>& file : files) {
        if (file != nullptr && PrimaryKeyIndexSourcePolicy::ShouldRead(*file)) {
            result.emplace_back(file->file_name, file->row_count);
        }
    }
    std::sort(result.begin(), result.end(),
              [](const PrimaryKeyIndexSourceFile& left, const PrimaryKeyIndexSourceFile& right) {
                  return left.file_name < right.file_name;
              });
    return result;
}

int64_t TotalRows(const std::vector<PrimaryKeyIndexSourceFile>& sources) {
    int64_t result = 0;
    for (const PrimaryKeyIndexSourceFile& source : sources) {
        result += source.row_count;
    }
    return result;
}

Result<std::shared_ptr<IndexFileMeta>> MakeSourceBackedBTreePayload(
    const std::string& file_name, int32_t field_id, const std::shared_ptr<MemoryPool>& pool) {
    constexpr int64_t kRowCount = 1;
    PAIMON_ASSIGN_OR_RAISE(
        PrimaryKeyIndexSourceMeta source_meta,
        PrimaryKeyIndexSourceMeta::Create(/*data_level=*/1, {{"source.data", kRowCount}}));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> source_meta_bytes, source_meta.Serialize(pool));
    return std::make_shared<IndexFileMeta>(
        "btree", file_name, /*file_size=*/1, kRowCount, /*dv_ranges=*/std::nullopt,
        /*external_path=*/std::nullopt,
        GlobalIndexMeta(/*row_range_start=*/0, /*row_range_end=*/0, field_id,
                        /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr,
                        source_meta_bytes));
}

std::shared_ptr<IndexFileMeta> MakeDataEvolutionBTreePayload(
    const std::string& file_name, int32_t field_id, const std::shared_ptr<MemoryPool>& pool) {
    constexpr char kSourceMeta[] =
        "\x44\x45\x49\x58\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07";
    auto source_meta =
        std::make_shared<Bytes>(std::string(kSourceMeta, sizeof(kSourceMeta) - 1), pool.get());
    return std::make_shared<IndexFileMeta>(
        "btree", file_name, /*file_size=*/1, /*row_count=*/1, /*dv_ranges=*/std::nullopt,
        /*external_path=*/std::nullopt,
        GlobalIndexMeta(/*row_range_start=*/0, /*row_range_end=*/0, field_id,
                        /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr, source_meta));
}

std::shared_ptr<IndexFileMeta> MakeMalformedPrimaryKeyBTreePayload(
    const std::string& file_name, int32_t field_id, const std::shared_ptr<MemoryPool>& pool) {
    auto source_meta = std::make_shared<Bytes>("malformed", pool.get());
    return std::make_shared<IndexFileMeta>(
        "btree", file_name, /*file_size=*/1, /*row_count=*/1, /*dv_ranges=*/std::nullopt,
        /*external_path=*/std::nullopt,
        GlobalIndexMeta(/*row_range_start=*/0, /*row_range_end=*/0, field_id,
                        /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr, source_meta));
}

}  // namespace

TEST(BucketedPrimaryKeyIndexMaintainerStandaloneTest,
     DeletesRestoredBTreePayloadWhenNoDefinitionRemains) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BucketedPrimaryKeyIndexMaintainer::Factory> factory,
        BucketedPrimaryKeyIndexMaintainer::Factory::Create(
            /*root_path=*/"", /*branch=*/"main", /*table_schema=*/nullptr,
            /*definitions=*/{}, /*path_factory=*/nullptr, /*index_file_handler=*/nullptr, options,
            /*io_manager=*/nullptr, /*enable_multi_thread_spill=*/false, /*executor=*/nullptr,
            GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<IndexFileMeta> stale_payload,
        MakeSourceBackedBTreePayload("stale-btree.index", /*field_id=*/7, GetDefaultPool()));
    std::shared_ptr<IndexFileMeta> data_evolution_payload = MakeDataEvolutionBTreePayload(
        "data-evolution-btree.index", /*field_id=*/7, GetDefaultPool());
    std::shared_ptr<IndexFileMeta> malformed_pk_payload = MakeMalformedPrimaryKeyBTreePayload(
        "malformed-pk-btree.index", /*field_id=*/7, GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BucketedPrimaryKeyIndexMaintainer> maintainer,
        factory->CreateMaintainer(BinaryRow::EmptyRow(), /*bucket=*/0,
                                  /*restored_data_files=*/{},
                                  {stale_payload, data_evolution_payload, malformed_pk_payload}));
    CommitIncrement increment(DataIncrement({}, {}, {}), CompactIncrement({}, {}, {}), nullptr);
    ASSERT_OK(maintainer->PrepareCommit(&increment));
    ASSERT_EQ(increment.GetNewFilesIncrement().DeletedIndexFiles(),
              (std::vector<std::shared_ptr<IndexFileMeta>>{stale_payload, malformed_pk_payload}));
}

TEST(BucketedPrimaryKeyIndexMaintainerStandaloneTest,
     PreservesDataEvolutionBTreePayloadForOwnedField) {
    constexpr int32_t kFieldId = 7;
    PrimaryKeyIndexDefinition definition("value", kFieldId, "btree",
                                         PrimaryKeyIndexDefinition::Family::BTREE, {});
    std::vector<BucketedPrimaryKeyIndexMaintainer::FieldMaintainer> fields = {
        {std::move(definition), /*builder=*/nullptr}};
    std::shared_ptr<IndexFileMeta> data_evolution_payload =
        MakeDataEvolutionBTreePayload("data-evolution-btree.index", kFieldId, GetDefaultPool());
    BucketedPrimaryKeyIndexMaintainer maintainer(std::move(fields), /*active_data_files=*/{},
                                                 {data_evolution_payload});
    CommitIncrement increment(DataIncrement({}, {}, {}), CompactIncrement({}, {}, {}), nullptr);
    ASSERT_OK(maintainer.PrepareCommit(&increment));
    ASSERT_TRUE(increment.GetNewFilesIncrement().DeletedIndexFiles().empty());
}

class BucketedPrimaryKeyIndexMaintainerTest : public ::testing::TestWithParam<std::string> {
 protected:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_NE(nullptr, directory_);
        schema_ = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                 arrow::field("value", arrow::utf8(), /*nullable=*/false)});
        options_ = {{Options::BUCKET, "1"},
                    {Options::DELETION_VECTORS_ENABLED, "true"},
                    {Options::FILE_FORMAT, GetParam()},
                    {Options::NUM_LEVELS, "3"},
                    {Options::PK_BTREE_INDEX_COLUMNS, "value"},
                    {Options::TARGET_FILE_ROW_NUM, "2"},
                    {Options::WRITE_BUFFER_SIZE, "1"},
                    {Options::WRITE_BATCH_SIZE, "2"}};
        if (GetParam() == "orc") {
            options_["orc.dictionary-key-size-threshold"] = "1.0";
            options_["orc.read.enable-lazy-decoding"] = "true";
        }

        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema_, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog,
                             Catalog::Create(directory_->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("db", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("db", "table"), &c_schema,
                                       /*partition_keys=*/{}, /*primary_keys=*/{"id"}, options_,
                                       /*ignore_if_exists=*/false));
        table_path_ = PathUtil::JoinPath(directory_->Str(), "db.db/table");
    }

    Result<std::unique_ptr<RecordBatch>> MakeBatch(const std::string& json) const {
        std::shared_ptr<arrow::DataType> struct_type = arrow::struct_(schema_->fields());
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ipc::internal::json::ArrayFromJSON(struct_type, json));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        RecordBatchBuilder builder(&c_array);
        return builder.SetBucket(0).Finish();
    }

    Result<std::unique_ptr<FileStoreWrite>> CreateWriter(bool with_temp_directory = true,
                                                         bool write_buffer_spillable = true) const {
        WriteContextBuilder builder(table_path_, kCommitUser);
        std::map<std::string, std::string> writer_options = options_;
        writer_options[Options::WRITE_BUFFER_SPILLABLE] = write_buffer_spillable ? "true" : "false";
        builder.SetOptions(writer_options).WithStreamingMode(true);
        if (with_temp_directory) {
            builder.WithTempDirectory(directory_->Str());
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> context, builder.Finish());
        return FileStoreWrite::Create(std::move(context));
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> WriteAndPrepare(
        const std::string& json, int64_t commit_identifier) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> writer, CreateWriter());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch, MakeBatch(json));
        PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> messages,
                               writer->PrepareCommit(/*wait_compaction=*/false, commit_identifier));
        PAIMON_RETURN_NOT_OK(writer->Close());
        return messages;
    }

    Result<std::vector<std::shared_ptr<CommitMessage>>> CompactAndPrepare(
        bool full_compaction, int64_t commit_identifier) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> writer, CreateWriter());
        PAIMON_RETURN_NOT_OK(writer->Compact(/*partition=*/{}, /*bucket=*/0, full_compaction));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<CommitMessage>> messages,
                               writer->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
        PAIMON_RETURN_NOT_OK(writer->Close());
        return messages;
    }

    Status Commit(const std::vector<std::shared_ptr<CommitMessage>>& messages,
                  int64_t commit_identifier) const {
        CommitContextBuilder builder(table_path_, kCommitUser);
        builder.SetOptions(options_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->Commit(messages, commit_identifier);
    }

    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    std::string table_path_;
};

TEST_P(BucketedPrimaryKeyIndexMaintainerTest, BuildsRestoresAndReplacesCompactedLevelPayload) {
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> initial_messages,
                         WriteAndPrepare(R"([[4, "b"], [1, "z"], [3, "a"], [2, "y"]])",
                                         /*commit_identifier=*/0));
    ASSERT_EQ(1, initial_messages.size());
    std::shared_ptr<CommitMessageImpl> initial =
        std::dynamic_pointer_cast<CommitMessageImpl>(initial_messages[0]);
    ASSERT_NE(nullptr, initial);
    ASSERT_TRUE(BTreeIndexFiles(*initial, /*added=*/true).empty());
    ASSERT_OK(Commit(initial_messages, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> first_compact_messages,
                         CompactAndPrepare(/*full_compaction=*/true,
                                           /*commit_identifier=*/1));
    ASSERT_EQ(1, first_compact_messages.size());
    std::shared_ptr<CommitMessageImpl> first_compact =
        std::dynamic_pointer_cast<CommitMessageImpl>(first_compact_messages[0]);
    ASSERT_NE(nullptr, first_compact);
    const std::vector<std::shared_ptr<DataFileMeta>>& first_compact_after =
        first_compact->GetCompactIncrement().CompactAfter();
    std::vector<PrimaryKeyIndexSourceFile> first_expected_sources =
        ExpectedSources(first_compact_after);
    ASSERT_FALSE(first_expected_sources.empty());
    ASSERT_EQ(first_compact_after.size(), first_expected_sources.size());

    std::vector<std::shared_ptr<IndexFileMeta>> first_payloads =
        BTreeIndexFiles(*first_compact, /*added=*/true);
    ASSERT_EQ(1, first_payloads.size());
    ASSERT_TRUE(BTreeIndexFiles(*first_compact, /*added=*/false).empty());
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta first_source_meta,
                         PrimaryKeyIndexSourceMeta::FromIndexFile(*first_payloads[0]));
    ASSERT_EQ(first_expected_sources, first_source_meta.SourceFiles());
    ASSERT_EQ(first_compact_after[0]->level, first_source_meta.DataLevel());
    int64_t first_row_count = TotalRows(first_expected_sources);
    ASSERT_EQ(first_row_count, first_payloads[0]->RowCount());
    ASSERT_TRUE(first_payloads[0]->GetGlobalIndexMeta().has_value());
    ASSERT_EQ(0, first_payloads[0]->GetGlobalIndexMeta()->row_range_start);
    ASSERT_EQ(first_row_count - 1, first_payloads[0]->GetGlobalIndexMeta()->row_range_end);
    ASSERT_OK(Commit(first_compact_messages, /*commit_identifier=*/1));

    const std::string indexed_value = "a";
    std::shared_ptr<Predicate> value_predicate = PredicateBuilder::Equal(
        /*field_index=*/1, "value", FieldType::STRING,
        Literal(FieldType::STRING, indexed_value.data(), indexed_value.size()));
    ScanContextBuilder scan_builder(table_path_);
    scan_builder.SetPredicate(value_predicate).AddOption(Options::GLOBAL_INDEX_ENABLED, "true");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    int32_t indexed_split_count = 0;
    int32_t data_split_count = 0;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        if (std::dynamic_pointer_cast<IndexedSplitImpl>(split) != nullptr) {
            indexed_split_count++;
        } else if (std::dynamic_pointer_cast<DataSplitImpl>(split) != nullptr) {
            data_split_count++;
        }
    }
    ASSERT_GT(indexed_split_count, 0);
    ASSERT_EQ(0, data_split_count);

    ReadContextBuilder read_builder(table_path_);
    // The source build above keeps lazy decoding enabled. Disable it only for the final data-row
    // assertion, whose predicate reader is outside the maintenance path covered by this test.
    read_builder.SetReadFieldNames({"id", "value"})
        .SetPredicate(value_predicate)
        .AddOption("orc.read.enable-lazy-decoding", "false")
        .EnablePredicateFilter(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> batch_reader,
                         table_read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(batch_reader.get()));
    ASSERT_NE(nullptr, result);
    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(result->type(),
                                                                 {R"([[0, 3, "a"]])"}, &expected)
                    .ok());
    ASSERT_TRUE(result->Equals(expected)) << result->ToString();

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> unchanged_messages,
                         CompactAndPrepare(/*full_compaction=*/false,
                                           /*commit_identifier=*/2));
    ASSERT_EQ(1, unchanged_messages.size());
    std::shared_ptr<CommitMessageImpl> unchanged =
        std::dynamic_pointer_cast<CommitMessageImpl>(unchanged_messages[0]);
    ASSERT_NE(nullptr, unchanged);
    ASSERT_TRUE(unchanged->IsEmpty());
    ASSERT_TRUE(BTreeIndexFiles(*unchanged, /*added=*/true).empty());
    ASSERT_TRUE(BTreeIndexFiles(*unchanged, /*added=*/false).empty());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> additional_messages,
                         WriteAndPrepare(R"([[6, "f"], [5, "e"]])", /*commit_identifier=*/2));
    ASSERT_EQ(1, additional_messages.size());
    std::shared_ptr<CommitMessageImpl> additional =
        std::dynamic_pointer_cast<CommitMessageImpl>(additional_messages[0]);
    ASSERT_NE(nullptr, additional);
    ASSERT_TRUE(BTreeIndexFiles(*additional, /*added=*/true).empty());
    ASSERT_TRUE(BTreeIndexFiles(*additional, /*added=*/false).empty());
    ASSERT_OK(Commit(additional_messages, /*commit_identifier=*/2));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> second_compact_messages,
                         CompactAndPrepare(/*full_compaction=*/true,
                                           /*commit_identifier=*/3));
    ASSERT_EQ(1, second_compact_messages.size());
    std::shared_ptr<CommitMessageImpl> second_compact =
        std::dynamic_pointer_cast<CommitMessageImpl>(second_compact_messages[0]);
    ASSERT_NE(nullptr, second_compact);
    std::vector<std::shared_ptr<IndexFileMeta>> replacement_payloads =
        BTreeIndexFiles(*second_compact, /*added=*/true);
    std::vector<std::shared_ptr<IndexFileMeta>> deleted_payloads =
        BTreeIndexFiles(*second_compact, /*added=*/false);
    ASSERT_EQ(1, replacement_payloads.size());
    ASSERT_EQ(1, deleted_payloads.size());
    ASSERT_EQ(first_payloads[0]->FileName(), deleted_payloads[0]->FileName());
    ASSERT_NE(first_payloads[0]->FileName(), replacement_payloads[0]->FileName());

    std::vector<PrimaryKeyIndexSourceFile> second_expected_sources =
        ExpectedSources(second_compact->GetCompactIncrement().CompactAfter());
    ASSERT_FALSE(second_expected_sources.empty());
    ASSERT_OK_AND_ASSIGN(PrimaryKeyIndexSourceMeta second_source_meta,
                         PrimaryKeyIndexSourceMeta::FromIndexFile(*replacement_payloads[0]));
    ASSERT_EQ(second_expected_sources, second_source_meta.SourceFiles());
    ASSERT_EQ(TotalRows(second_expected_sources), replacement_payloads[0]->RowCount());
}

TEST_P(BucketedPrimaryKeyIndexMaintainerTest, ContinuesCompactionWhenIndexBuildFails) {
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> initial_messages,
                         WriteAndPrepare(R"([[4, "b"], [1, "z"], [3, "a"], [2, "y"]])",
                                         /*commit_identifier=*/0));
    ASSERT_OK(Commit(initial_messages, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         CreateWriter(/*with_temp_directory=*/true,
                                      /*write_buffer_spillable=*/false));
    ASSERT_OK(writer->Compact(/*partition=*/{}, /*bucket=*/0, /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> compact_messages,
                         writer->PrepareCommit(/*wait_compaction=*/true,
                                               /*commit_identifier=*/1));
    ASSERT_OK(writer->Close());

    ASSERT_EQ(1, compact_messages.size());
    std::shared_ptr<CommitMessageImpl> compact =
        std::dynamic_pointer_cast<CommitMessageImpl>(compact_messages[0]);
    ASSERT_NE(nullptr, compact);
    ASSERT_FALSE(compact->GetCompactIncrement().CompactBefore().empty());
    ASSERT_FALSE(compact->GetCompactIncrement().CompactAfter().empty());
    ASSERT_TRUE(BTreeIndexFiles(*compact, /*added=*/true).empty());
    ASSERT_TRUE(BTreeIndexFiles(*compact, /*added=*/false).empty());
    ASSERT_OK(Commit(compact_messages, /*commit_identifier=*/1));

    const std::string indexed_value = "a";
    std::shared_ptr<Predicate> value_predicate = PredicateBuilder::Equal(
        /*field_index=*/1, "value", FieldType::STRING,
        Literal(FieldType::STRING, indexed_value.data(), indexed_value.size()));
    ScanContextBuilder scan_builder(table_path_);
    scan_builder.SetPredicate(value_predicate).AddOption(Options::GLOBAL_INDEX_ENABLED, "true");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, scan_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                         TableScan::Create(std::move(scan_context)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
    int32_t indexed_split_count = 0;
    int32_t data_split_count = 0;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        if (std::dynamic_pointer_cast<IndexedSplitImpl>(split) != nullptr) {
            indexed_split_count++;
        } else if (std::dynamic_pointer_cast<DataSplitImpl>(split) != nullptr) {
            data_split_count++;
        }
    }
    ASSERT_EQ(0, indexed_split_count);
    ASSERT_GT(data_split_count, 0);

    ReadContextBuilder read_builder(table_path_);
    read_builder.SetReadFieldNames({"id", "value"})
        .SetPredicate(value_predicate)
        .AddOption("orc.read.enable-lazy-decoding", "false")
        .EnablePredicateFilter(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> read_context, read_builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> batch_reader,
                         table_read->CreateReader(plan->Splits()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(batch_reader.get()));
    ASSERT_NE(nullptr, result);
    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(result->type(),
                                                                 {R"([[0, 3, "a"]])"}, &expected)
                    .ok());
    ASSERT_TRUE(result->Equals(expected)) << result->ToString();

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> repair_messages,
                         CompactAndPrepare(/*full_compaction=*/false,
                                           /*commit_identifier=*/2));
    ASSERT_EQ(1, repair_messages.size());
    std::shared_ptr<CommitMessageImpl> repair =
        std::dynamic_pointer_cast<CommitMessageImpl>(repair_messages[0]);
    ASSERT_NE(nullptr, repair);
    ASSERT_EQ(1, BTreeIndexFiles(*repair, /*added=*/true).size());
    ASSERT_TRUE(BTreeIndexFiles(*repair, /*added=*/false).empty());
}

TEST_P(BucketedPrimaryKeyIndexMaintainerTest, DeletesPayloadAfterLastDefinitionIsRemoved) {
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> initial_messages,
                         WriteAndPrepare(R"([[4, "b"], [1, "z"], [3, "a"], [2, "y"]])",
                                         /*commit_identifier=*/0));
    ASSERT_OK(Commit(initial_messages, /*commit_identifier=*/0));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> compact_messages,
                         CompactAndPrepare(/*full_compaction=*/true,
                                           /*commit_identifier=*/1));
    ASSERT_EQ(1, compact_messages.size());
    std::shared_ptr<CommitMessageImpl> compact =
        std::dynamic_pointer_cast<CommitMessageImpl>(compact_messages[0]);
    ASSERT_NE(nullptr, compact);
    std::vector<std::shared_ptr<IndexFileMeta>> payloads =
        BTreeIndexFiles(*compact, /*added=*/true);
    ASSERT_EQ(1, payloads.size());
    ASSERT_OK(Commit(compact_messages, /*commit_identifier=*/1));

    std::map<std::string, std::string> evolved_options = options_;
    evolved_options.erase(Options::PK_BTREE_INDEX_COLUMNS);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> evolved_schema,
                         TableSchema::Create(/*schema_id=*/1, schema_, /*partition_keys=*/{},
                                             /*primary_keys=*/{"id"}, evolved_options));
    ASSERT_OK_AND_ASSIGN(std::string schema_json, evolved_schema->ToJsonString());
    ASSERT_OK_AND_ASSIGN(CoreOptions evolved_core_options, CoreOptions::FromMap(evolved_options));
    ASSERT_OK(evolved_core_options.GetFileSystem()->WriteFile(
        PathUtil::JoinPath(table_path_, "schema/schema-1"), schema_json,
        /*overwrite=*/false));

    WriteContextBuilder builder(table_path_, kCommitUser);
    builder.SetOptions(evolved_options)
        .WithStreamingMode(true)
        .WithTempDirectory(directory_->Str());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> writer,
                         FileStoreWrite::Create(std::move(context)));
    ASSERT_OK(writer->Compact(/*partition=*/{}, /*bucket=*/0, /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> cleanup_messages,
                         writer->PrepareCommit(/*wait_compaction=*/true,
                                               /*commit_identifier=*/2));
    ASSERT_OK(writer->Close());
    ASSERT_EQ(1, cleanup_messages.size());
    std::shared_ptr<CommitMessageImpl> cleanup =
        std::dynamic_pointer_cast<CommitMessageImpl>(cleanup_messages[0]);
    ASSERT_NE(nullptr, cleanup);
    std::vector<std::shared_ptr<IndexFileMeta>> deleted_payloads =
        BTreeIndexFiles(*cleanup, /*added=*/false);
    ASSERT_EQ(1, deleted_payloads.size());
    ASSERT_EQ(payloads[0]->FileName(), deleted_payloads[0]->FileName());
    ASSERT_TRUE(BTreeIndexFiles(*cleanup, /*added=*/true).empty());
}

INSTANTIATE_TEST_SUITE_P(FileFormats, BucketedPrimaryKeyIndexMaintainerTest,
                         ::testing::Values("parquet", "orc"));

}  // namespace paimon::test
