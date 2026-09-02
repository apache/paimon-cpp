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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/append/bucketed_append_compact_manager.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/operation/restore_files.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/executor.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/result.h"
#include "paimon/table/source/table_read.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/data_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class AppendCompactionInteTest : public testing::Test,
                                 public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void PrepareSimpleAppendData(const std::shared_ptr<DataGenerator>& gen, bool with_dv,
                                 TestHelper* helper, int64_t* identifier) {
        auto& commit_identifier = *identifier;
        std::vector<BinaryRow> datas_1;
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Alice"), 10, 1, 11.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Bob"), 10, 0, 12.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Emily"), 10, 0, 13.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Tony"), 10, 0, 14.1}, pool_.get()));
        datas_1.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Lucy"), 20, 1, 14.1}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches_1, gen->SplitArrayByPartitionAndBucket(datas_1));
        ASSERT_EQ(3, batches_1.size());
        ASSERT_OK_AND_ASSIGN(
            auto commit_msgs,
            helper->WriteAndCommit(std::move(batches_1), commit_identifier++, std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
        ASSERT_TRUE(snapshot1);
        ASSERT_EQ(1, snapshot1.value().Id());
        ASSERT_EQ(5, snapshot1.value().TotalRecordCount());
        ASSERT_EQ(5, snapshot1.value().DeltaRecordCount());

        std::vector<BinaryRow> datas_2;
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Emily"), 10, 0, 15.1}, pool_.get()));
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Bob"), 10, 0, 12.1}, pool_.get()));
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Alex"), 10, 0, 16.1}, pool_.get()));
        datas_2.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Paul"), 20, 1, NullType()}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches_2, gen->SplitArrayByPartitionAndBucket(datas_2));
        ASSERT_EQ(2, batches_2.size());
        ASSERT_OK_AND_ASSIGN(
            auto commit_msgs_2,
            helper->WriteAndCommit(std::move(batches_2), commit_identifier++, std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
        ASSERT_TRUE(snapshot2);
        ASSERT_EQ(2, snapshot2.value().Id());
        ASSERT_EQ(9, snapshot2.value().TotalRecordCount());
        ASSERT_EQ(4, snapshot2.value().DeltaRecordCount());

        std::vector<BinaryRow> datas_3;
        datas_3.push_back(
            BinaryRowGenerator::GenerateRow({std::string("David"), 10, 0, 17.1}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches_3, gen->SplitArrayByPartitionAndBucket(datas_3));
        ASSERT_EQ(1, batches_3.size());
        ASSERT_OK_AND_ASSIGN(
            auto commit_msgs_3,
            helper->WriteAndCommit(std::move(batches_3), commit_identifier++, std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot3, helper->LatestSnapshot());
        ASSERT_TRUE(snapshot3);
        ASSERT_EQ(3, snapshot3.value().Id());
        ASSERT_EQ(10, snapshot3.value().TotalRecordCount());
        ASSERT_EQ(1, snapshot3.value().DeltaRecordCount());

        // @note: for append-only tables in Spark, native row-level deletes aren't supported during
        // writing. Instead, deletions are expressed by committing a Deletion Vector (DV) file
        // externally.
        if (with_dv) {
            auto partition = BinaryRowGenerator::GenerateRow({10}, pool_.get());
            int32_t bucket = 1;
            auto abstract_write = dynamic_cast<AbstractFileStoreWrite*>(helper->write_.get());
            ASSERT_NE(abstract_write, nullptr);
            ASSERT_OK_AND_ASSIGN(auto restore_files,
                                 abstract_write->ScanExistingFileMetas(partition, bucket));
            ASSERT_OK_AND_ASSIGN(
                auto dv_maintainer,
                abstract_write->dv_maintainer_factory_->Create(
                    partition, bucket, std::vector<std::shared_ptr<IndexFileMeta>>{}));
            for (const auto& data_file : restore_files->DataFiles()) {
                ASSERT_OK(dv_maintainer->NotifyNewDeletion(data_file->file_name, 0));
            }
            ASSERT_OK_AND_ASSIGN(auto index_file_meta, dv_maintainer->WriteDeletionVectorsIndex());

            auto commit_message = std::make_shared<CommitMessageImpl>(
                partition, bucket, 2, DataIncrement({}, {}, {}, {index_file_meta.value()}, {}),
                CompactIncrement({}, {}, {}));
            std::vector<std::shared_ptr<CommitMessage>> commit_messages;
            commit_messages.push_back(commit_message);
            ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier++));
        }
    }

    // Reports the struct type of the first batch `path` yields when read through the format layer
    // with `options`. The encoding a reader hands over only exists between the reader and its
    // consumer - ReadResultCollector decodes it on the way out - so the raw batch is imported here
    // rather than going through the table read path.
    void ReadFirstBatchType(const std::shared_ptr<FileSystem>& file_system, const std::string& path,
                            const std::string& file_format,
                            const std::map<std::string, std::string>& options,
                            std::shared_ptr<arrow::DataType>* batch_type) {
        ASSERT_OK_AND_ASSIGN(auto unique_stream, file_system->Open(path));
        std::shared_ptr<InputStream> stream(std::move(unique_stream));
        ASSERT_OK_AND_ASSIGN(auto format, FileFormatFactory::Get(file_format, options));
        ASSERT_OK_AND_ASSIGN(auto reader_builder, format->CreateReaderBuilder(10));
        ASSERT_OK_AND_ASSIGN(auto reader, reader_builder->Build(stream));
        ASSERT_OK_AND_ASSIGN(auto c_schema, reader->GetFileSchema());
        ASSERT_OK(reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        auto& [c_array, c_array_schema] = batch;
        std::shared_ptr<arrow::Array> array =
            arrow::ImportArray(c_array.get(), c_array_schema.get()).ValueOrDie();
        *batch_type = array->type();
    }

    static std::string DataFilePath(const std::shared_ptr<DataSplitImpl>& split, size_t index = 0) {
        return PathUtil::JoinPath(split->BucketPath(), split->DataFiles()[index]->file_name);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

std::vector<std::string> GetTestValuesForAppendCompactionInteTest() {
    std::vector<std::string> values;
    values.emplace_back("parquet");
#ifdef PAIMON_ENABLE_MOSAIC
    values.emplace_back("mosaic");
#endif
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc");
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro");
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, AppendCompactionInteTest,
                         ::testing::ValuesIn(GetTestValuesForAppendCompactionInteTest()));

TEST_P(AppendCompactionInteTest, TestAppendTableStreamWriteFullCompaction) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleAppendData(gen, /*with_dv=*/false, helper.get(), &commit_identifier);
    std::vector<BinaryRow> datas_4;
    datas_4.push_back(
        BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_4, gen->SplitArrayByPartitionAndBucket(datas_4));
    ASSERT_EQ(1, batches_4.size());

    ASSERT_OK(helper->write_->Write(std::move(batches_4[0])));
    ASSERT_OK(helper->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot5, helper->LatestSnapshot());
    ASSERT_EQ(5, snapshot5.value().Id());
    ASSERT_EQ(11, snapshot5.value().TotalRecordCount());
    ASSERT_EQ(0, snapshot5.value().DeltaRecordCount());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lily", 10, 0, 17.1]
])";

    expected_datas[std::make_pair("f1=20/", 0)] = R"([
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
])";

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    for (const auto& split : data_splits) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }
}

TEST_P(AppendCompactionInteTest, TestAppendTableStreamWriteFullCompactionWithMapSharedShredding) {
    auto file_format = GetParam();
    if (file_format == "avro" || file_format == "mosaic") {
        return;
    }

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    auto schema = arrow::schema(fields);

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_ROW_NUM, "1"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, "local"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "64"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options,
                                                         /*is_streaming_mode=*/true));

    ASSERT_OK_AND_ASSIGN(auto batch_0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [1, [["a", 10], ["b", 20]]],
        [2, [["c", 30]]]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    int64_t commit_identifier = 0;
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_0), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [3, [["a", 40], ["d", 50]]],
        [4, null]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [5, [["e", 60], ["f", 70], ["g", 80], ["h", 90]]]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK(helper->write_->Compact(/*partition=*/{}, /*bucket=*/0,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_FALSE(commit_messages.empty());
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot.value().GetCommitKind());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 1);
    {
        // Compaction ignores target-file-row-num and creates one five-row output file. It also
        // creates a fresh shared-shredding writer, so the file starts from K_max.
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(data_splits[0]);
        ASSERT_TRUE(data_split);
        ASSERT_EQ(data_split->DataFiles().size(), 1);
        auto compact_file = data_split->DataFiles()[0];
        std::string compact_file_path =
            PathUtil::JoinPath(data_split->BucketPath(), compact_file->file_name);
        ASSERT_OK_AND_ASSIGN(auto unique_input_stream,
                             dir->GetFileSystem()->Open(compact_file_path));
        std::shared_ptr<InputStream> input_stream(std::move(unique_input_stream));
        ASSERT_OK_AND_ASSIGN(auto file_format_obj, FileFormatFactory::Get(file_format, options));
        ASSERT_OK_AND_ASSIGN(auto reader_builder, file_format_obj->CreateReaderBuilder(10));
        ASSERT_OK_AND_ASSIGN(auto reader, reader_builder->Build(input_stream));
        ASSERT_OK_AND_ASSIGN(auto c_file_schema, reader->GetFileSchema());
        auto file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
        auto tags_field = file_schema->GetFieldByName("tags");
        ASSERT_TRUE(tags_field);
        ASSERT_TRUE(tags_field->metadata());
        ASSERT_OK_AND_ASSIGN(auto tags_meta, MapSharedShreddingUtils::DeserializeMetadata(
                                                 tags_field->metadata()->Copy()));
        ASSERT_EQ(64, tags_meta.num_columns);
        ASSERT_EQ(4, tags_meta.max_row_width);
    }
    {
        // recall all fields
        arrow::FieldVector fields_with_row_kind = fields;
        fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                    arrow::field("_VALUE_KIND", arrow::int8()));
        auto data_type = arrow::struct_(fields_with_row_kind);
        ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits,
                                                                      R"([
        [0, 1, [["a", 10], ["b", 20]]],
        [0, 2, [["c", 30]]],
        [0, 3, [["a", 40], ["d", 50]]],
        [0, 4, null],
        [0, 5, [["e", 60], ["f", 70], ["g", 80], ["h", 90]]]
    ])"));
        ASSERT_TRUE(success);
    }
    {
        // recall only "a,f" sub-key in map
        auto selected_keys_meta =
            arrow::KeyValueMetadata::Make({DataField::MAP_SELECTED_KEYS}, {"a,f"});
        auto read_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type)->WithMetadata(selected_keys_meta),
        });
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_schema.get()).ok());

        ReadContextBuilder read_context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"));
        read_context_builder.SetOptions(options).SetReadSchema(std::move(c_schema));
        ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
        ASSERT_OK_AND_ASSIGN(auto actual,
                             ReadResultCollector::CollectResult(std::move(batch_reader)));

        auto expected_type = arrow::struct_({
            arrow::field("_VALUE_KIND", arrow::int8()),
            arrow::field("id", arrow::int32()),
            arrow::field("tags", map_type),
        });
        auto expected = arrow::ipc::internal::json::ArrayFromJSON(expected_type, R"([
        [0, 1, [["a", 10]]],
        [0, 2, []],
        [0, 3, [["a", 40]]],
        [0, 4, null],
        [0, 5, [["f", 70]]]
    ])")
                            .ValueOrDie();
        auto expected_chunked = std::make_shared<arrow::ChunkedArray>(expected);
        ASSERT_TRUE(expected_chunked->Equals(actual))
            << "actual=" << actual->ToString() << "\nexpected=" << expected_chunked->ToString();
    }
}

TEST_P(AppendCompactionInteTest,
       TestOrcAppendTableFullCompactionWithMapSharedShreddingStringValue) {
    auto file_format = GetParam();
    if (file_format != "orc") {
        return;
    }

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto map_type = arrow::map(arrow::utf8(), arrow::utf8());
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("tags", map_type),
    };
    auto schema = arrow::schema(fields);

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "orc"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, "local"},
        {"orc.read.enable-lazy-decoding", "true"},
        {"orc.dictionary-key-size-threshold", "1"},
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "1"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options,
                                                         /*is_streaming_mode=*/true));

    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto batch_0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [1, [["a", "shared"], ["b", "hot"]]],
        [2, [["c", "shared"]]]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_0), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [3, [["a", "shared"], ["d", "hot"]]],
        [4, null]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK_AND_ASSIGN(auto batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields),
                                                     R"([
        [5, [["e", "shared"], ["f", "hot"], ["g", "shared"]]]
    ])",
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK(helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                     /*expected_commit_messages=*/std::nullopt));

    ASSERT_OK(helper->write_->Compact(/*partition=*/{}, /*bucket=*/0,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_FALSE(commit_messages.empty());
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot.value().GetCommitKind());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 1);
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type, data_splits,
                                                                  R"([
        [0, 1, [["a", "shared"], ["b", "hot"]]],
        [0, 2, [["c", "shared"]]],
        [0, 3, [["a", "shared"], ["d", "hot"]]],
        [0, 4, null],
        [0, 5, [["e", "shared"], ["f", "hot"], ["g", "shared"]]]
    ])"));
    ASSERT_TRUE(success);
}

TEST_P(AppendCompactionInteTest, TestAppendTableStreamWriteFullCompactionWithDv) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleAppendData(gen, /*with_dv=*/true, helper.get(), &commit_identifier);
    std::vector<BinaryRow> datas_4;
    datas_4.push_back(
        BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_4, gen->SplitArrayByPartitionAndBucket(datas_4));
    ASSERT_EQ(1, batches_4.size());

    ASSERT_OK_AND_ASSIGN(
        auto helper2, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                         /*is_streaming_mode=*/true, /*ignore_if_exists=*/true));

    ASSERT_OK(helper2->write_->Write(std::move(batches_4[0])));
    ASSERT_OK(helper2->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                       /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper2->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_OK(helper2->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot5, helper2->LatestSnapshot());
    ASSERT_EQ(6, snapshot5.value().Id());
    ASSERT_EQ(8, snapshot5.value().TotalRecordCount());
    ASSERT_EQ(-3, snapshot5.value().DeltaRecordCount());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper2->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "Lily", 10, 0, 17.1]
])";

    expected_datas[std::make_pair("f1=20/", 0)] = R"([
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
])";

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    for (const auto& split : data_splits) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper2->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper2->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }
}

TEST_P(AppendCompactionInteTest, TestAppendTableStreamWriteBestEffortCompaction) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::COMPACTION_MIN_FILE_NUM, "3"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleAppendData(gen, /*with_dv=*/false, helper.get(), &commit_identifier);
    std::vector<BinaryRow> datas_4;
    datas_4.push_back(
        BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_4, gen->SplitArrayByPartitionAndBucket(datas_4));
    ASSERT_EQ(1, batches_4.size());

    ASSERT_OK(helper->write_->Write(std::move(batches_4[0])));
    ASSERT_OK(helper->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                      /*full_compaction=*/false));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot5, helper->LatestSnapshot());
    ASSERT_EQ(5, snapshot5.value().Id());
    ASSERT_EQ(11, snapshot5.value().TotalRecordCount());
    ASSERT_EQ(0, snapshot5.value().DeltaRecordCount());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lily", 10, 0, 17.1]
])";

    expected_datas[std::make_pair("f1=20/", 0)] = R"([
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
])";

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    for (const auto& split : data_splits) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }
}

TEST_P(AppendCompactionInteTest, TestAppendTableStreamWriteCompactionWithExternalPath) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto external_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = "FILE://" + external_dir->Str();

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
        {Options::FILE_SYSTEM, "local"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
        {Options::DATA_FILE_EXTERNAL_PATHS, external_test_dir},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);
    auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
    int64_t commit_identifier = 0;
    PrepareSimpleAppendData(gen, /*with_dv=*/false, helper.get(), &commit_identifier);
    std::vector<BinaryRow> datas_4;
    datas_4.push_back(
        BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_4, gen->SplitArrayByPartitionAndBucket(datas_4));
    ASSERT_EQ(1, batches_4.size());

    ASSERT_OK(helper->write_->Write(std::move(batches_4[0])));
    ASSERT_OK(helper->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                      /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot5, helper->LatestSnapshot());
    ASSERT_EQ(5, snapshot5.value().Id());
    ASSERT_EQ(11, snapshot5.value().TotalRecordCount());
    ASSERT_EQ(0, snapshot5.value().DeltaRecordCount());
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot5.value().GetCommitKind());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas;
    expected_datas[std::make_pair("f1=10/", 0)] = R"([
[0, "Alice", 10, 1, 11.1]
])";

    expected_datas[std::make_pair("f1=10/", 1)] = R"([
[0, "Bob", 10, 0, 12.1],
[0, "Emily", 10, 0, 13.1],
[0, "Tony", 10, 0, 14.1],
[0, "Emily", 10, 0, 15.1],
[0, "Bob", 10, 0, 12.1],
[0, "Alex", 10, 0, 16.1],
[0, "David", 10, 0, 17.1],
[0, "Lily", 10, 0, 17.1]
])";

    expected_datas[std::make_pair("f1=20/", 0)] = R"([
[0, "Lucy", 20, 1, 14.1],
[0, "Paul", 20, 1, null]
])";

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    for (const auto& split : data_splits) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }
}

TEST_F(AppendCompactionInteTest, TestAppendTableCompactionWithIOException) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);

    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::DELETION_VECTORS_ENABLED, "true"}};

    bool compaction_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 2000; ++i) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);

        ASSERT_OK_AND_ASSIGN(auto helper,
                             TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys,
                                                options, /*is_streaming_mode=*/true));
        ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                             helper->LatestSchema());
        ASSERT_TRUE(table_schema);

        auto gen = std::make_shared<DataGenerator>(table_schema.value(), pool_);
        int64_t commit_identifier = 0;
        PrepareSimpleAppendData(gen, /*with_dv=*/true, helper.get(), &commit_identifier);

        std::vector<BinaryRow> data;
        data.push_back(
            BinaryRowGenerator::GenerateRow({std::string("Lily"), 10, 0, 17.1}, pool_.get()));
        ASSERT_OK_AND_ASSIGN(auto batches, gen->SplitArrayByPartitionAndBucket(data));
        ASSERT_EQ(1, batches.size());

        ASSERT_OK_AND_ASSIGN(
            auto helper2,
            TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                               /*is_streaming_mode=*/true, /*ignore_if_exists=*/true));

        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);

        CHECK_HOOK_STATUS(helper2->write_->Write(std::move(batches[0])), i);
        CHECK_HOOK_STATUS(helper2->write_->Compact(/*partition=*/{{"f1", "10"}}, /*bucket=*/1,
                                                   /*full_compaction=*/true),
                          i);

        Result<std::vector<std::shared_ptr<CommitMessage>>> commit_messages =
            helper2->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier);
        CHECK_HOOK_STATUS(commit_messages.status(), i);
        CHECK_HOOK_STATUS(helper2->commit_->Commit(commit_messages.value(), commit_identifier), i);

        compaction_run_complete = true;
        io_hook->Clear();

        ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> latest_snapshot, helper2->LatestSnapshot());
        ASSERT_TRUE(latest_snapshot);
        ASSERT_EQ(Snapshot::CommitKind::Compact(), latest_snapshot->GetCommitKind());
        break;
    }

    ASSERT_TRUE(compaction_run_complete);
}

// Rewriting through the dictionary passthrough has to produce the same table as rewriting through
// materialized values, whatever encoding each input file happens to carry. The interesting part is
// the chain the unit tests cannot reach on their own: CompactRewrite hands the batch to the file
// index writer and to the format writer, and both of them recover each column's encoding from the
// batch layout after the type has been dropped by the C data interface.
//
// Both parameters set `parquet.read.enable-dictionary-passthrough` on the table, which is what the
// rewrite requires - it never enables the passthrough by itself. Parquet then forwards the
// encoding; ORC has the option vetoed because its writer cannot take a dictionary-encoded batch.
// ORC lazy decoding is on throughout, which makes the ORC reader hand over
// `dictionary(int64, large_utf8)` regardless, so the ORC parameter covers decoding a shape no
// layout can resolve. ArrowUtilsTest.TestFlattenUnresolvableDictionaries separately covers a
// vetoed writer receiving the layout-recoverable dictionary(int32, utf8) shape.
TEST_P(AppendCompactionInteTest, TestAppendTableCompactionDictionaryPassthrough) {
    auto file_format = GetParam();
    if (file_format != "parquet" && file_format != "orc") {
        GTEST_SKIP() << file_format << " has no dictionary encoding to forward or to suppress";
    }
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    // `s` is low-cardinality STRING and comes back encoded, so the rewrite carries an encoded and
    // a materialized column at once. The other three are each excluded for their own reason: `id`
    // is INT32, which the gate excludes by physical type; `b` is BINARY, which Parquet stores in
    // the same BYTE_ARRAY leaf as STRING but which no value accessor can read as a dictionary; `u`
    // holds a distinct value per row, the shape passthrough saves least on. The cardinality-driven
    // half of the gate - a column that starts dictionary-encoded and falls back to plain partway
    // through a file - needs more rows than a readable fixture holds and is covered by
    // ParquetFileBatchReaderTest.TestDictionaryPassthroughSkipsFallbackToPlain instead.
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()), arrow::field("s", arrow::utf8()),
        arrow::field("b", arrow::binary()), arrow::field("u", arrow::utf8())};
    auto schema = arrow::schema(fields);

    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, "local"},
        // Opt in explicitly: the rewrite never turns the passthrough on by itself. On the ORC
        // parameter this doubles as the assertion that the option alone is not enough.
        {"parquet.read.enable-dictionary-passthrough", "true"},
        {"orc.read.enable-lazy-decoding", "true"},
        // Above the distinct/total ratio of every column here, so ORC dictionary-encodes rather
        // than leaving it to its own heuristic and making the assertion below data-dependent.
        {"orc.dictionary-key-size-threshold", "0.9"},
        {"file-index.bitmap.columns", "s"},
        {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options,
                                                         /*is_streaming_mode=*/true));

    // Each commit becomes its own input file with its own dictionary, so the rewrite has to carry
    // several different dictionaries into one output column chunk.
    const std::vector<std::string> batches = {
        R"([[1, "aa", "p", "distinct_value_1"],
            [2, "bb", "q", "distinct_value_2"],
            [3, null, "p", "distinct_value_3"],
            [4, "aa", "q", "distinct_value_4"]])",
        R"([[5, "cc", "r", "distinct_value_5"],
            [6, "dd", "r", "distinct_value_6"],
            [7, "cc", "s", "distinct_value_7"],
            [8, "dd", "s", "distinct_value_8"]])",
        R"([[9, "aa", "p", "distinct_value_9"],
            [10, "ee", "t", "distinct_value_10"]])",
    };
    int64_t commit_identifier = 0;
    for (const std::string& data : batches) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                         /*partition_map=*/{}, /*bucket=*/0, {}));
        ASSERT_OK(helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                         /*expected_commit_messages=*/std::nullopt));
    }

    // The rewrite must not change these, and neither must reading them back encoded.
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto read_type = arrow::struct_(fields_with_row_kind);
    const std::string expected_rows = R"([
        [0, 1, "aa", "p", "distinct_value_1"],
        [0, 2, "bb", "q", "distinct_value_2"],
        [0, 3, null, "p", "distinct_value_3"],
        [0, 4, "aa", "q", "distinct_value_4"],
        [0, 5, "cc", "r", "distinct_value_5"],
        [0, 6, "dd", "r", "distinct_value_6"],
        [0, 7, "cc", "s", "distinct_value_7"],
        [0, 8, "dd", "s", "distinct_value_8"],
        [0, 9, "aa", "p", "distinct_value_9"],
        [0, 10, "ee", "t", "distinct_value_10"]
    ])";

    {
        // What the rewrite is about to read, checked on an input file with the same options the
        // rewrite uses. Asserting the read type here is what makes the two directions facts of
        // this test rather than assumptions: on Parquet `s` arrives as DictionaryArray while `id`
        // and `b` do not; on ORC `s` arrives as `dictionary(int64, large_utf8)`, the shape no
        // ArrowArray layout can resolve and the one FlattenUnresolvableDictionaries has to decode
        // before the batch reaches the writer.
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> input_splits,
                             helper->NewScan(StartupMode::LatestFull(),
                                             /*snapshot_id=*/std::nullopt));
        ASSERT_EQ(1, input_splits.size());
        auto input_split = std::dynamic_pointer_cast<DataSplitImpl>(input_splits[0]);
        ASSERT_TRUE(input_split);
        ASSERT_EQ(3, input_split->DataFiles().size());
        std::shared_ptr<arrow::DataType> input_type;
        ASSERT_NO_FATAL_FAILURE(ReadFirstBatchType(dir->GetFileSystem(), DataFilePath(input_split),
                                                   file_format, options, &input_type));
        ASSERT_EQ(arrow::Type::INT32, input_type->field(0)->type()->id()) << "column id";
        ASSERT_EQ(arrow::Type::DICTIONARY, input_type->field(1)->type()->id()) << "column s";
        if (file_format == "parquet") {
            ASSERT_TRUE(input_type->field(1)->type()->Equals(
                *arrow::dictionary(arrow::int32(), arrow::utf8())))
                << input_type->field(1)->type()->ToString();
            // `b` is dictionary-encoded in the file exactly as `s` is - same physical type, same
            // pages - and is still handed over materialized, because the gate stops at STRING.
            ASSERT_EQ(arrow::Type::BINARY, input_type->field(2)->type()->id()) << "column b";
        } else {
            // The ORC adapter only dictionary-encodes STRING, so `b` stays materialized, and it
            // widens the values: `dictionary(int64, large_utf8)` is exactly the shape whose index
            // and offset widths an ArrowArray layout cannot report.
            ASSERT_TRUE(input_type->field(1)->type()->Equals(
                *arrow::dictionary(arrow::int64(), arrow::large_utf8())))
                << input_type->field(1)->type()->ToString();
        }

        // The same files through the whole read path rather than the raw format reader: with the
        // option on, this is where an encoded batch meets the projection, the concatenation and
        // the collector. It only works here - after the rewrite the output column has fallen back
        // to plain, so the read at the end of this test no longer carries a dictionary at all.
        ASSERT_OK_AND_ASSIGN(bool input_read_success,
                             helper->ReadAndCheckResult(read_type, input_splits, expected_rows));
        ASSERT_TRUE(input_read_success);
    }

    ASSERT_OK(helper->write_->Compact(/*partition=*/{}, /*bucket=*/0, /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_FALSE(commit_messages.empty());
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(Snapshot::CommitKind::Compact(), snapshot.value().GetCommitKind());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(1, data_splits.size());
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(data_splits[0]);
    ASSERT_TRUE(data_split);
    ASSERT_EQ(1, data_split->DataFiles().size());

    if (file_format == "parquet") {
        // What the passthrough costs, on the file it just produced. Three input files brought
        // three different dictionaries into one output row group, so the writer kept the first and
        // wrote the rest plain; the gate then declines the output column, which is what makes it
        // ineligible for the next compaction round and bigger than a rewrite from materialized
        // values. TestAppendTableCompactionDictionaryPassthroughDefaultOff is the other side.
        std::shared_ptr<arrow::DataType> output_type;
        ASSERT_NO_FATAL_FAILURE(ReadFirstBatchType(dir->GetFileSystem(), DataFilePath(data_split),
                                                   file_format, options, &output_type));
        ASSERT_NE(arrow::Type::DICTIONARY, output_type->field(1)->type()->id())
            << "column s: " << output_type->field(1)->type()->ToString();
    }

    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(read_type, data_splits, expected_rows));
    ASSERT_TRUE(success);

    // The bitmap index on `s` is built from the rewritten batch, which reaches the index writer
    // still encoded when the passthrough is on. Reading through the index is what proves it was
    // decoded against the right values rather than against its indices.
    std::string indexed_value = "cc";
    auto predicate = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"s", FieldType::STRING,
        Literal(FieldType::STRING, indexed_value.data(), indexed_value.size()));
    ReadContextBuilder read_context_builder(PathUtil::JoinPath(dir->Str(), "foo.db/bar"));
    read_context_builder.SetOptions(options).SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(data_splits));
    ASSERT_OK_AND_ASSIGN(auto filtered, ReadResultCollector::CollectResult(batch_reader.get()));
    auto expected = std::make_shared<arrow::ChunkedArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [0, 5, "cc", "r", "distinct_value_5"],
        [0, 7, "cc", "s", "distinct_value_7"]
    ])")
            .ValueOrDie());
    ASSERT_TRUE(expected->Equals(filtered)) << "actual=" << filtered->ToString();
}

// The same rewrite on a table that says nothing about the passthrough, which is the default and
// the shape of every table that has not opted in. It has to land on the same rows as
// TestAppendTableCompactionDictionaryPassthrough, so the option is a performance knob and never a
// correctness one - and on a *better* encoded output, which is why it is not turned on by default.
TEST_F(AppendCompactionInteTest, TestAppendTableCompactionDictionaryPassthroughDefaultOff) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("s", arrow::utf8())};
    auto schema = arrow::schema(fields);
    // No `parquet.read.enable-dictionary-passthrough` here on purpose: absence is what is under
    // test. An explicit `false` reaches the same code, since the reader resolves the option
    // against the same default.
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options,
                                                         /*is_streaming_mode=*/true));

    // Three files, which is what full compaction needs before it rewrites anything, and three
    // different dictionaries for the writer to reconcile.
    const std::vector<std::string> batches = {
        R"([[1, "aa"], [2, "bb"]])", R"([[3, "cc"], [4, "aa"]])", R"([[5, "dd"], [6, "cc"]])"};
    int64_t commit_identifier = 0;
    for (const std::string& data : batches) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                         /*partition_map=*/{}, /*bucket=*/0, {}));
        ASSERT_OK(helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                         /*expected_commit_messages=*/std::nullopt));
    }

    ASSERT_OK(helper->write_->Compact(/*partition=*/{}, /*bucket=*/0, /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_FALSE(commit_messages.empty());
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(1, data_splits.size());
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(data_splits[0]);
    ASSERT_TRUE(data_split);
    ASSERT_EQ(1, data_split->DataFiles().size());

    // The point of leaving the passthrough off: the writer built one dictionary covering the whole
    // row group, so `s` is dictionary-encoded end to end and the gate accepts it - unlike the same
    // rewrite with the passthrough on, which leaves the column partly plain. Read with the option
    // on, since that is what makes the encoding visible; it does not change what is in the file.
    std::map<std::string, std::string> passthrough_options = options;
    passthrough_options["parquet.read.enable-dictionary-passthrough"] = "true";
    std::shared_ptr<arrow::DataType> output_type;
    ASSERT_NO_FATAL_FAILURE(ReadFirstBatchType(dir->GetFileSystem(), DataFilePath(data_split),
                                               "parquet", passthrough_options, &output_type));
    ASSERT_EQ(arrow::Type::DICTIONARY, output_type->field(1)->type()->id())
        << "column s: " << output_type->field(1)->type()->ToString();

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(
                                           arrow::struct_(fields_with_row_kind), data_splits, R"([
        [0, 1, "aa"],
        [0, 2, "bb"],
        [0, 3, "cc"],
        [0, 4, "aa"],
        [0, 5, "dd"],
        [0, 6, "cc"]
    ])"));
    ASSERT_TRUE(success);
}

// The veto in GetDictionaryPassthroughVetoReason() that no other test reaches: a shredding writer
// reshapes every batch against a fixed physical schema, so the rewrite forces the read option off
// even though the table asks for it. The format veto is covered by the ORC parameter of
// TestAppendTableCompactionDictionaryPassthrough; the `parquet.enable-dictionary` one is left
// uncovered on purpose, since a writer with dictionaries disabled densifies whatever it is handed
// and the rewritten file is identical either way.
//
// `s` is what makes this one visible: it reaches the rewrite dictionary-encoded - asserted on an
// input file first, so the test cannot pass merely because there was nothing to forward - and comes
// back dictionary-encoded end to end, which only a rewrite from materialized values produces. Had
// the option been honoured it would have gone plain after the first input file's dictionary, the
// way it does in TestAppendTableCompactionDictionaryPassthrough.
TEST_F(AppendCompactionInteTest, TestAppendTableCompactionDictionaryPassthroughVetoedByShredding) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto map_type = arrow::map(arrow::utf8(), arrow::int64());
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("s", arrow::utf8()), arrow::field("tags", map_type)};
    auto schema = arrow::schema(fields);
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, "parquet"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "id"},
        {Options::FILE_SYSTEM, "local"},
        {"parquet.read.enable-dictionary-passthrough", "true"},
        // The shredding plan, which is what makes the rewrite veto the option above.
        {"fields.tags.map.storage-layout", "shared-shredding"},
        {"fields.tags.map.shared-shredding.max-columns", "64"},
    };
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                                         /*primary_keys=*/{}, options,
                                                         /*is_streaming_mode=*/true));

    // Three files, each with its own `s` dictionary, which is what full compaction needs before it
    // rewrites anything.
    const std::vector<std::string> batches = {
        R"([[1, "aa", [["a", 10]]], [2, "bb", [["b", 20]]]])",
        R"([[3, "cc", [["a", 30]]], [4, "aa", null]])",
        R"([[5, "dd", [["c", 40]]], [6, "cc", [["a", 50], ["d", 60]]]])",
    };
    int64_t commit_identifier = 0;
    for (const std::string& data : batches) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                         /*partition_map=*/{}, /*bucket=*/0, {}));
        ASSERT_OK(helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                         /*expected_commit_messages=*/std::nullopt));
    }

    {
        // Without this the veto could not be told apart from an input the gate declines anyway.
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> input_splits,
                             helper->NewScan(StartupMode::LatestFull(),
                                             /*snapshot_id=*/std::nullopt));
        ASSERT_EQ(1, input_splits.size());
        auto input_split = std::dynamic_pointer_cast<DataSplitImpl>(input_splits[0]);
        ASSERT_TRUE(input_split);
        ASSERT_EQ(3, input_split->DataFiles().size());
        std::shared_ptr<arrow::DataType> input_type;
        ASSERT_NO_FATAL_FAILURE(ReadFirstBatchType(dir->GetFileSystem(), DataFilePath(input_split),
                                                   "parquet", options, &input_type));
        ASSERT_EQ("s", input_type->field(1)->name());
        ASSERT_EQ(arrow::Type::DICTIONARY, input_type->field(1)->type()->id())
            << "column s: " << input_type->field(1)->type()->ToString();
    }

    ASSERT_OK(helper->write_->Compact(/*partition=*/{}, /*bucket=*/0, /*full_compaction=*/true));
    ASSERT_OK_AND_ASSIGN(
        std::vector<std::shared_ptr<CommitMessage>> commit_messages,
        helper->write_->PrepareCommit(/*wait_compaction=*/true, commit_identifier));
    ASSERT_FALSE(commit_messages.empty());
    ASSERT_OK(helper->commit_->Commit(commit_messages, commit_identifier));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(1, data_splits.size());
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(data_splits[0]);
    ASSERT_TRUE(data_split);
    ASSERT_EQ(1, data_split->DataFiles().size());

    std::shared_ptr<arrow::DataType> output_type;
    ASSERT_NO_FATAL_FAILURE(ReadFirstBatchType(dir->GetFileSystem(), DataFilePath(data_split),
                                               "parquet", options, &output_type));
    ASSERT_EQ("s", output_type->field(1)->name());
    ASSERT_EQ(arrow::Type::DICTIONARY, output_type->field(1)->type()->id())
        << "column s: " << output_type->field(1)->type()->ToString();

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(
                                           arrow::struct_(fields_with_row_kind), data_splits, R"([
        [0, 1, "aa", [["a", 10]]],
        [0, 2, "bb", [["b", 20]]],
        [0, 3, "cc", [["a", 30]]],
        [0, 4, "aa", null],
        [0, 5, "dd", [["c", 40]]],
        [0, 6, "cc", [["a", 50], ["d", 60]]]
    ])"));
    ASSERT_TRUE(success);
}

}  // namespace paimon::test
