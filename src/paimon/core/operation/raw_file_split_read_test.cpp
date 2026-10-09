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

#include "paimon/core/operation/raw_file_split_read.h"

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/data/timestamp.h"
#include "paimon/executor.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
#include "paimon/testing/mock/mock_search_file_index.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class RawFileSplitReadTest : public ::testing::Test {
 protected:
    std::vector<std::shared_ptr<DataSplit>> PrepareDataSplits() const {
        auto meta1 = std::make_shared<DataFileMeta>(
            "data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc", /*file_size=*/575, /*row_count=*/3,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                              {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/2, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1728497439433ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
        DataSplitImpl::Builder builder1(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/orc/multi_partition_append_table.db/"
                                            "multi_partition_append_table/f1=10/f2=0/bucket-0",
                                        {meta1});
        EXPECT_OK_AND_ASSIGN(
            auto data_split1,
            builder1.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

        auto meta2 = std::make_shared<DataFileMeta>(
            "data-b79de94d-abe4-47d6-8e6c-911816487252-0.orc", /*file_size=*/541, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1, 14.1},
                                              {std::string("Lucy"), 20, 1, 14.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1728497439453ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
        DataSplitImpl::Builder builder2(BinaryRowGenerator::GenerateRow({20, 1}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/orc/multi_partition_append_table.db/"
                                            "multi_partition_append_table/f1=20/f2=1/bucket-0",
                                        {meta2});
        EXPECT_OK_AND_ASSIGN(
            auto data_split2,
            builder2.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

        auto meta3 = std::make_shared<DataFileMeta>(
            "data-955cbedd-ffcc-4234-8b98-4c3f08f78309-0.orc", /*file_size=*/543, /*row_count=*/1,
            /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
            /*key_stats=*/SimpleStats::EmptyStats(),
            BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1, 11.1},
                                              {std::string("Alice"), 10, 1, 11.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1728497439469ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
        DataSplitImpl::Builder builder3(BinaryRowGenerator::GenerateRow({10, 1}, pool_.get()),
                                        /*bucket=*/0, /*bucket_path=*/
                                        paimon::test::GetDataDir() +
                                            "/orc/multi_partition_append_table.db/"
                                            "multi_partition_append_table/f1=10/f2=1/bucket-0",
                                        {meta3});
        EXPECT_OK_AND_ASSIGN(
            auto data_split3,
            builder3.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());
        std::vector<std::shared_ptr<DataSplit>> data_splits = {data_split1, data_split2,
                                                               data_split3};
        return data_splits;
    }

    Result<std::shared_ptr<arrow::ChunkedArray>> ReadSearchWithDeletionVector(
        const std::shared_ptr<ReadContext>& read_context, const std::string& index_type,
        const RoaringBitmap32& deleted_rows, std::shared_ptr<TableSchema> table_schema) const {
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        if (!table_schema) {
            PAIMON_ASSIGN_OR_RAISE(table_schema, schema_manager.ReadSchema(0));
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<InternalReadContext> internal_context,
            InternalReadContext::Create(read_context, table_schema, table_schema->Options()));
        auto split = std::dynamic_pointer_cast<DataSplitImpl>(PrepareDataSplits().front());
        if (!split) {
            return Status::Invalid("Raw search test split must be a DataSplitImpl");
        }
        PAIMON_ASSIGN_OR_RAISE(
            split->DataFiles().front()->embedded_index,
            MockSearchFileIndex::MakeIndex("f3", index_type, {{0, 0.25f}, {2, 0.75f}}, pool_));

        const CoreOptions& options = internal_context->GetCoreOptions();
        std::shared_ptr<arrow::Schema> schema =
            DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               options.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_path,
                               options.CreateGlobalIndexExternalPath());
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                read_context->GetPath(), schema, table_schema->PartitionKeys(),
                options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
                options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
                global_index_path, options.IndexFileInDataFileDir(), pool_));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Executor> executor,
                               CreateDefaultExecutor(/*thread_count=*/2));
        std::shared_ptr<InternalReadContext> shared_context(std::move(internal_context));
        RawFileSplitRead split_read(path_factory, shared_context, pool_, executor);
        auto dv = std::make_shared<BitmapDeletionVector>(deleted_rows);
        DeletionVector::Factory dv_factory =
            [dv](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
            return std::shared_ptr<DeletionVector>(dv);
        };
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BatchReader> reader,
            split_read.CreateReader(split->Partition(), split->Bucket(), split->DataFiles(),
                                    dv_factory, std::nullopt));
        return ReadResultCollector::CollectResult(std::move(reader));
    }

    void CheckReadResult(const std::shared_ptr<arrow::Schema>& read_schema,
                         const std::shared_ptr<arrow::ChunkedArray>& expected_array,
                         DeletionVector::Factory dv_factory = {}) const {
        std::string path = paimon::test::GetDataDir() +
                           "/orc/multi_partition_append_table.db/"
                           "multi_partition_append_table";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadFieldNames(read_schema->field_names());
        if (dv_factory) {
            context_builder.AddOption(Options::FILE_INDEX_READ_ENABLED, "true");
            context_builder.SetPredicate(PredicateBuilder::IsNotNull(0, "f0", FieldType::STRING));
        }
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(read_context, table_schema, table_schema->Options()));
        auto data_splits = PrepareDataSplits();
        const auto& core_options = internal_context->GetCoreOptions();
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                             core_options.CreateExternalPaths());
        ASSERT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                             core_options.CreateGlobalIndexExternalPath());

        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<FileStorePathFactory> path_factory,
            FileStorePathFactory::Create(
                internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
                core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
                core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
                external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
                pool_));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor,
                             CreateDefaultExecutor(/*thread_count=*/2));
        auto split_read = std::make_unique<RawFileSplitRead>(
            path_factory, std::move(internal_context), pool_, executor);

        std::vector<std::unique_ptr<BatchReader>> batch_readers;
        batch_readers.reserve(data_splits.size());
        for (const auto& split : data_splits) {
            auto impl = std::dynamic_pointer_cast<DataSplitImpl>(split);
            if (dv_factory) {
                // A DV64 read must bypass file-index evaluation even for a small file.
                for (const auto& file : impl->DataFiles()) {
                    file->embedded_index = std::make_shared<Bytes>("invalid index", pool_.get());
                }
            }
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<BatchReader> reader,
                dv_factory ? split_read->CreateReader(impl->Partition(), impl->Bucket(),
                                                      impl->DataFiles(), dv_factory, std::nullopt)
                           : split_read->CreateReader(split));
            batch_readers.emplace_back(std::move(reader));
        }
        auto batch_reader =
            std::make_unique<ConcatBatchReader>(std::move(batch_readers), GetArrowPool(pool_));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(std::move(batch_reader)));
        ASSERT_TRUE(result_array->Equals(expected_array));
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

// test simple, recall all columns with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReader) {
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, "Bob", 10, 0, 12.1],
      [0, "Emily", 10, 0, 13.1],
      [0, "Tony", 10, 0, 14.1],
      [0, "Lucy", 20, 1, 14.1],
      [0, "Alice", 10, 1, 11.1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall all columns with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithReserveSequence) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 12.1, 0, 10, "Bob"],
      [0, 13.1, 0, 10, "Emily"],
      [0, 14.1, 0, 10, "Tony"],
      [0, 14.1, 1, 20, "Lucy"],
      [0, 11.1, 1, 10, "Alice"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall all partition columns with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartitionKeys) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 10, 0],
      [0, 10, 0],
      [0, 10, 0],
      [0, 20, 1],
      [0, 10, 1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall all partition columns with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartitionKeysWithReverseSequence) {
    std::vector<DataField> read_fields = {DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(1, arrow::field("f1", arrow::int32()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 0, 10],
      [0, 0, 10],
      [0, 0, 10],
      [0, 1, 20],
      [0, 1, 10]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall part partition keys with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartPartition) {
    std::vector<DataField> read_fields = {DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 0, 12.1],
      [0, 0, 13.1],
      [0, 0, 14.1],
      [0, 1, 14.1],
      [0, 1, 11.1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall part partition keys with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithPartPartitionWithReserveSequence) {
    std::vector<DataField> read_fields = {DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 10, "Bob"],
      [0, 10, "Emily"],
      [0, 10, "Tony"],
      [0, 20, "Lucy"],
      [0, 10, "Alice"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall non partition columns with sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithNonPartition) {
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, "Bob", 12.1],
      [0, "Emily", 13.1],
      [0, "Tony", 14.1],
      [0, "Lucy", 14.1],
      [0, "Alice", 11.1]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

// recall non partition columns with reverse sequence in data
TEST_F(RawFileSplitReadTest, TestCreateReaderWithNonPartitionWithReserveSequence) {
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto fields_with_row_kind = read_schema->fields();
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(fields_with_row_kind), {R"([
      [0, 12.1, "Bob"],
      [0, 13.1, "Emily"],
      [0, 14.1, "Tony"],
      [0, 14.1, "Lucy"],
      [0, 11.1, "Alice"]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckReadResult(read_schema, expected_array);
}

TEST_F(RawFileSplitReadTest, TestVectorSearchScoreProjection) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/multi_partition_append_table.db/multi_partition_append_table";
    std::shared_ptr<VectorSearch> vector_search = std::make_shared<VectorSearch>(
        "f3", /*limit=*/2, std::vector<float>{1.0f}, nullptr, nullptr,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"_INDEX_SCORE", "f0"}).SetVectorSearch(vector_search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        auto internal_context,
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()));

    auto data_splits = PrepareDataSplits();
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(data_splits.front());
    ASSERT_TRUE(data_split);
    ASSERT_OK_AND_ASSIGN(
        data_split->DataFiles().front()->embedded_index,
        MockSearchFileIndex::MakeIndex("f3", MockSearchFileIndex::VECTOR_IDENTIFIER,
                                       {{0, 0.25f}, {2, 0.75f}}, pool_));

    const CoreOptions& core_options = internal_context->GetCoreOptions();
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                         core_options.CreateExternalPaths());
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                         core_options.CreateGlobalIndexExternalPath());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            path, data_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool_));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor,
                         CreateDefaultExecutor(/*thread_count=*/2));
    auto split_read = std::make_unique<RawFileSplitRead>(path_factory, std::move(internal_context),
                                                         pool_, executor);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, split_read->CreateReader(data_split));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(std::move(reader)));

    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()),
                                    arrow::field("_INDEX_SCORE", arrow::float32()),
                                    arrow::field("f0", arrow::utf8())}),
                    {R"([[0, 0.25, "Bob"], [0, 0.75, "Tony"]])"}, &expected)
                    .ok());
    ASSERT_TRUE(result->Equals(expected));

    ReadContextBuilder without_score_builder(path);
    without_score_builder.SetReadFieldNames({"f0"}).SetVectorSearch(vector_search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> without_score_context,
                         without_score_builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        auto without_score_internal_context,
        InternalReadContext::Create(without_score_context, table_schema, table_schema->Options()));
    auto without_score_split_read = std::make_unique<RawFileSplitRead>(
        path_factory, std::move(without_score_internal_context), pool_, executor);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> without_score_reader,
                         without_score_split_read->CreateReader(data_split));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> without_score_result,
                         ReadResultCollector::CollectResult(std::move(without_score_reader)));
    std::shared_ptr<arrow::ChunkedArray> without_score_expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()),
                                    arrow::field("f0", arrow::utf8())}),
                    {R"([[0, "Bob"], [0, "Tony"]])"}, &without_score_expected)
                    .ok());
    ASSERT_TRUE(without_score_result->Equals(without_score_expected));
}

TEST_F(RawFileSplitReadTest, TestVectorSearchFiltersDeletionVectorBeforeTopK) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/multi_partition_append_table.db/multi_partition_append_table";
    auto user_filter = [](int64_t row_id) { return row_id != 1; };
    std::shared_ptr<VectorSearch> search = std::make_shared<VectorSearch>(
        "f3", /*limit=*/1, std::vector<float>{1.0f}, user_filter, nullptr,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    ReadContextBuilder builder(path);
    builder.SetReadFieldNames({"_INDEX_SCORE", "f0"}).SetVectorSearch(search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::ChunkedArray> result,
        ReadSearchWithDeletionVector(read_context, MockSearchFileIndex::VECTOR_IDENTIFIER,
                                     RoaringBitmap32::From({0}), /*table_schema=*/nullptr));
    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()),
                                    arrow::field("_INDEX_SCORE", arrow::float32()),
                                    arrow::field("f0", arrow::utf8())}),
                    {R"([[0, 0.75, "Tony"]])"}, &expected)
                    .ok());
    EXPECT_TRUE(result->Equals(expected));
}

TEST_F(RawFileSplitReadTest, TestFullTextSearchFiltersDeletionVectorBeforeLimit) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/multi_partition_append_table.db/multi_partition_append_table";
    RoaringBitmap64 user_filter;
    user_filter.Add(0);
    user_filter.Add(2);
    auto search = std::make_shared<FullTextSearch>(
        "f3", /*limit=*/1, "match", FullTextSearch::SearchType::MATCH_ALL, user_filter);
    ReadContextBuilder builder(path);
    builder.SetReadFieldNames({"f0"}).SetFullTextSearch(search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::ChunkedArray> result,
        ReadSearchWithDeletionVector(read_context, MockSearchFileIndex::FULL_TEXT_IDENTIFIER,
                                     RoaringBitmap32::From({0}), /*table_schema=*/nullptr));
    std::shared_ptr<arrow::ChunkedArray> expected;
    ASSERT_TRUE(arrow::ipc::internal::json::ChunkedArrayFromJSON(
                    arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()),
                                    arrow::field("f0", arrow::utf8())}),
                    {R"([[0, "Tony"]])"}, &expected)
                    .ok());
    EXPECT_TRUE(result->Equals(expected));
}

TEST_F(RawFileSplitReadTest, TestVectorSearchRenamedUnprojectedFieldInOldFile) {
    const std::string path =
        GetDataDir() + "/orc/multi_partition_append_table.db/multi_partition_append_table";
    SchemaManager schemas(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(auto original_schema, schemas.ReadSchema(0));
    auto fields = original_schema->Fields();
    ASSERT_EQ("f3", fields[3].Name());
    fields[3] = DataField(fields[3].Id(), fields[3].ArrowField()->WithName("renamed_f3"));
    fields.emplace_back(4, arrow::field("f3", arrow::float64()));

    const std::string identifier = "raw-options-vector";
    auto* factories = FactoryCreator::GetInstance();
    if (!factories->Create(identifier)) {
        factories->Register(identifier,
                            std::make_unique<OptionsCheckingMockSearchFileIndexFactory>(
                                identifier, SearchIndexType::VECTOR,
                                std::map<std::string, std::string>{{"token", "current-options"}})
                                .release());
    }
    auto options = original_schema->Options();
    options["file-index.raw-options-vector.columns"] = "renamed_f3";
    options["file-index.raw-options-vector.renamed_f3.token"] = "current-options";
    options[Options::READ_BATCH_SIZE] = "1";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> current_schema,
                         TableSchema::Create(1, DataField::ConvertDataFieldsToArrowSchema(fields),
                                             original_schema->PartitionKeys(), {}, options));
    auto search = std::make_shared<VectorSearch>("renamed_f3", 2, std::vector<float>{1.0f}, nullptr,
                                                 nullptr, VectorSearch::DistanceType::EUCLIDEAN,
                                                 std::map<std::string, std::string>{});
    ReadContextBuilder builder(path);
    builder.SetOptions(options).SetReadFieldNames({"f0", "_INDEX_SCORE"}).SetVectorSearch(search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());

    auto expected_type = arrow::struct_({arrow::field("_VALUE_KIND", arrow::int8()),
                                         arrow::field("f0", arrow::utf8()),
                                         arrow::field("_INDEX_SCORE", arrow::float32())});
    const std::vector<std::pair<RoaringBitmap32, std::string>> cases = {
        {RoaringBitmap32(), R"([[0,"Bob",0.25],[0,"Tony",0.75]])"},
        {RoaringBitmap32::From({0}), R"([[0,"Tony",0.75]])"}};
    for (const auto& [deleted, expected_json] : cases) {
        SCOPED_TRACE(deleted.ToString());
        ASSERT_OK_AND_ASSIGN(auto actual, ReadSearchWithDeletionVector(context, identifier, deleted,
                                                                       current_schema));
        auto expected_result =
            arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_json);
        ASSERT_TRUE(expected_result.ok()) << expected_result.status().ToString();
        ASSERT_TRUE(actual);
        ASSERT_TRUE(actual->Equals(arrow::ChunkedArray(expected_result.ValueOrDie())))
            << actual->ToString();
    }
    ASSERT_EQ("renamed_f3", search->field_name);
    ASSERT_FALSE(search->pre_filter);
    ASSERT_EQ("current-options",
              current_schema->Options().at("file-index.raw-options-vector.renamed_f3.token"));

    // The old file's f3 is field ID 3; the current table's f3 is a different field with ID 4.
    auto missing_search = std::make_shared<VectorSearch>(*search);
    missing_search->field_name = "f3";
    ReadContextBuilder missing_field_builder(path);
    missing_field_builder.SetOptions(options).SetReadFieldNames({"f0"}).SetVectorSearch(
        missing_search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> missing_context,
                         missing_field_builder.Finish());
    ASSERT_NOK_WITH_MSG(ReadSearchWithDeletionVector(missing_context, identifier, RoaringBitmap32(),
                                                     current_schema),
                        "has no matching field ID");
}

TEST_F(RawFileSplitReadTest, TestFullTextSearchRenamedUnprojectedFieldInOldFile) {
    const std::string path =
        GetDataDir() + "/orc/multi_partition_append_table.db/multi_partition_append_table";
    SchemaManager schemas(std::make_shared<LocalFileSystem>(), path);
    ASSERT_OK_AND_ASSIGN(auto original_schema, schemas.ReadSchema(0));
    auto fields = original_schema->Fields();
    ASSERT_EQ("f3", fields[3].Name());
    fields[3] = DataField(fields[3].Id(), fields[3].ArrowField()->WithName("renamed_f3"));
    fields.emplace_back(4, arrow::field("f3", arrow::float64()));

    const std::string identifier = "raw-options-full-text";
    auto* factories = FactoryCreator::GetInstance();
    if (!factories->Create(identifier)) {
        factories->Register(identifier,
                            std::make_unique<OptionsCheckingMockSearchFileIndexFactory>(
                                identifier, SearchIndexType::FULL_TEXT,
                                std::map<std::string, std::string>{{"token", "current-options"}})
                                .release());
    }
    auto options = original_schema->Options();
    options["file-index.raw-options-full-text.columns"] = "renamed_f3";
    options["file-index.raw-options-full-text.renamed_f3.token"] = "current-options";
    options[Options::READ_BATCH_SIZE] = "1";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> current_schema,
                         TableSchema::Create(1, DataField::ConvertDataFieldsToArrowSchema(fields),
                                             original_schema->PartitionKeys(), {}, options));
    auto search = std::make_shared<FullTextSearch>(
        "renamed_f3", 2, "match", FullTextSearch::SearchType::MATCH_ALL, std::nullopt);
    ReadContextBuilder builder(path);
    builder.SetOptions(options).SetReadFieldNames({"f0"}).SetFullTextSearch(search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> context, builder.Finish());

    auto expected_type = arrow::struct_(
        {arrow::field("_VALUE_KIND", arrow::int8()), arrow::field("f0", arrow::utf8())});
    const std::vector<std::pair<RoaringBitmap32, std::string>> cases = {
        {RoaringBitmap32(), R"([[0,"Bob"],[0,"Tony"]])"},
        {RoaringBitmap32::From({0}), R"([[0,"Tony"]])"}};
    for (const auto& [deleted, expected_json] : cases) {
        SCOPED_TRACE(deleted.ToString());
        ASSERT_OK_AND_ASSIGN(auto actual, ReadSearchWithDeletionVector(context, identifier, deleted,
                                                                       current_schema));
        auto expected_result =
            arrow::ipc::internal::json::ArrayFromJSON(expected_type, expected_json);
        ASSERT_TRUE(expected_result.ok()) << expected_result.status().ToString();
        ASSERT_TRUE(actual);
        ASSERT_TRUE(actual->Equals(arrow::ChunkedArray(expected_result.ValueOrDie())))
            << actual->ToString();
    }
    ASSERT_EQ("renamed_f3", search->field_name);
    ASSERT_FALSE(search->pre_filter);
    ASSERT_EQ("current-options",
              current_schema->Options().at("file-index.raw-options-full-text.renamed_f3.token"));

    // The old file's f3 is field ID 3; the current table's f3 is a different field with ID 4.
    auto missing_search = std::make_shared<FullTextSearch>(*search);
    missing_search->field_name = "f3";
    ReadContextBuilder missing_field_builder(path);
    missing_field_builder.SetOptions(options).SetReadFieldNames({"f0"}).SetFullTextSearch(
        missing_search);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> missing_context,
                         missing_field_builder.Finish());
    ASSERT_NOK_WITH_MSG(ReadSearchWithDeletionVector(missing_context, identifier, RoaringBitmap32(),
                                                     current_schema),
                        "has no matching field ID");
}

TEST_F(RawFileSplitReadTest, TestEmptyPlan) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/multi_partition_append_table.db/"
                       "multi_partition_append_table";
    ReadContextBuilder context_builder(path);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));

    ASSERT_OK_AND_ASSIGN(
        auto internal_context,
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()));
    const auto& core_options = internal_context->GetCoreOptions();
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    ASSERT_OK_AND_ASSIGN(std::vector<std::string> external_paths,
                         core_options.CreateExternalPaths());
    ASSERT_OK_AND_ASSIGN(std::optional<std::string> global_index_external_path,
                         core_options.CreateGlobalIndexExternalPath());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            pool_));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor,
                         CreateDefaultExecutor(/*thread_count=*/2));
    auto split_read = std::make_unique<RawFileSplitRead>(path_factory, std::move(internal_context),
                                                         pool_, executor);
    DataSplitImpl::Builder builder(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()),
                                   /*bucket=*/0, /*bucket_path=*/
                                   paimon::test::GetDataDir() +
                                       "/orc/multi_partition_append_table.db/"
                                       "multi_partition_append_table/f1=10/f2=0/bucket-0",
                                   {});
    ASSERT_OK_AND_ASSIGN(auto data_split,
                         builder.WithSnapshot(1).IsStreaming(false).RawConvertible(true).Build());

    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, split_read->CreateReader(data_split));
    batch_readers.push_back(std::move(reader));
    auto batch_reader =
        std::make_unique<ConcatBatchReader>(std::move(batch_readers), GetArrowPool(pool_));
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ReadResultCollector::CollectResult(std::move(batch_reader)));
    ASSERT_EQ(result_array, nullptr);
}

TEST_F(RawFileSplitReadTest, TestMatch) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_total_buckets.db/pk_table_with_total_buckets";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldNames({"f0", "f1", "f2", "f3"});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<InternalReadContext> internal_context,
        InternalReadContext::Create(read_context, table_schema, table_schema->Options()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor,
                         CreateDefaultExecutor(/*thread_count=*/2));
    auto split_read = std::make_unique<RawFileSplitRead>(
        /*path_factory=*/nullptr, std::move(internal_context), pool_, executor);
    auto create_data_split = [this](bool is_streaming, bool raw_convertible,
                                    std::optional<int64_t> delete_row_count =
                                        0) -> std::shared_ptr<DataSplit> {
        auto meta = std::make_shared<DataFileMeta>(
            "data-d7725088-6bd4-4e70-9ce6-714ae93b47cc-0.orc", /*file_size=*/863, /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Alice"), 1}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("Alice"), 1}, {std::string("Alice"), 1},
                                              {0, 0}, pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("Alice"), 10, 1, 11.1},
                                              {std::string("Alice"), 10, 1, 11.1}, {0, 0, 0, 0},
                                              pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1743525392885ll, 0), delete_row_count,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
        DataSplitImpl::Builder builder(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()),
                                       /*bucket=*/0, /*bucket_path=*/
                                       paimon::test::GetDataDir() +
                                           "/orc/multi_partition_append_table.db/"
                                           "multi_partition_append_table/f1=10/f2=0/bucket-0",
                                       {meta});
        return builder.WithSnapshot(1)
            .IsStreaming(is_streaming)
            .RawConvertible(raw_convertible)
            .Build()
            .value();
    };
    {
        auto data_split = create_data_split(/*is_streaming=*/false, /*raw_convertible=*/true);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/true));
        ASSERT_FALSE(match_result);
    }
    {
        auto data_split = create_data_split(/*is_streaming=*/false, /*raw_convertible=*/true);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/false));
        ASSERT_TRUE(match_result);
    }
    {
        auto data_split = create_data_split(/*is_streaming=*/true, /*raw_convertible=*/true);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/false));
        ASSERT_FALSE(match_result);
    }
    {
        auto data_split = create_data_split(/*is_streaming=*/false, /*raw_convertible=*/false);
        auto split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
        ASSERT_TRUE(split_impl);
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(data_split, /*force_keep_delete=*/false));
        ASSERT_FALSE(match_result);
    }
    {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(
            create_data_split(/*is_streaming=*/false, /*raw_convertible=*/false));
        auto indexed_split = std::make_shared<IndexedSplitImpl>(
            data_split, std::vector<Range>{Range(0, 0)}, std::vector<float>{0.5F});
        ASSERT_NOK_WITH_MSG(split_read->CreateReader(indexed_split),
                            "Primary-key reads do not support scored indexed splits yet");
    }
    {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(
            create_data_split(/*is_streaming=*/false, /*raw_convertible=*/false));
        auto indexed_split =
            std::make_shared<IndexedSplitImpl>(data_split, std::vector<Range>{Range(0, 1)});
        ASSERT_NOK_WITH_MSG(split_read->CreateReader(indexed_split),
                            "Invalid file-local row range [0, 1]");
    }
    {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(create_data_split(
            /*is_streaming=*/false, /*raw_convertible=*/false, /*delete_row_count=*/std::nullopt));
        auto indexed_split =
            std::make_shared<IndexedSplitImpl>(data_split, std::vector<Range>{Range(0, 0)});
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(indexed_split, /*force_keep_delete=*/false));
        ASSERT_FALSE(match_result);
    }
    {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(create_data_split(
            /*is_streaming=*/false, /*raw_convertible=*/false, /*delete_row_count=*/1));
        auto indexed_split =
            std::make_shared<IndexedSplitImpl>(data_split, std::vector<Range>{Range(0, 0)});
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(indexed_split, /*force_keep_delete=*/false));
        ASSERT_FALSE(match_result);
    }
    {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(create_data_split(
            /*is_streaming=*/false, /*raw_convertible=*/false, /*delete_row_count=*/0));
        auto indexed_split =
            std::make_shared<IndexedSplitImpl>(data_split, std::vector<Range>{Range(0, 0)});
        ASSERT_OK_AND_ASSIGN(bool match_result,
                             split_read->Match(indexed_split, /*force_keep_delete=*/false));
        ASSERT_TRUE(match_result);
    }
    ASSERT_NOK(split_read->Match(nullptr, /*force_keep_delete=*/false));
}

TEST_F(RawFileSplitReadTest, TestBitmap64DeletionVectorFallback) {
    auto read_schema =
        arrow::schema({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
                       arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())});
    auto result_fields = read_schema->fields();
    result_fields.insert(result_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    auto expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(result_fields), R"([[0, "Bob", 10, 0, 12.1], [0, "Tony", 10, 0, 14.1]])")
            .ValueOrDie();
    DeletionVector::Factory factory =
        [](const std::string& name) -> Result<std::shared_ptr<DeletionVector>> {
        auto dv = std::make_shared<Bitmap64DeletionVector>();
        PAIMON_RETURN_NOT_OK(dv->Delete(name.find("01b6a930") != std::string::npos ? 1 : 0));
        return dv;
    };
    CheckReadResult(read_schema, std::make_shared<arrow::ChunkedArray>(expected), factory);
}

}  // namespace paimon::test
