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

#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"

#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "fmt/format.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_index_writer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/core/realtime/data_file_key_offset_index_writer.h"
#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/defs.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<DataFileMeta> MakeDataFile(const std::string& file_name, int64_t row_count) {
    return std::make_shared<DataFileMeta>(
        file_name, /*file_size=*/0, row_count, DataFileMeta::EmptyMinKey(),
        DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        DataFileMeta::DUMMY_LEVEL, /*extra_files=*/std::vector<std::optional<std::string>>(),
        Timestamp(0, 0), /*delete_row_count=*/std::nullopt, /*embedded_index=*/nullptr,
        /*file_source=*/std::nullopt, /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
}

std::shared_ptr<arrow::StructArray> MakeKeys(const std::shared_ptr<arrow::Field>& key_field) {
    return checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({key_field}), R"([[1], [2]])")
            .ValueOrDie());
}

}  // namespace

class RealtimeOffsetFileIndexLookupTest : public testing::Test {
 public:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_NE(nullptr, directory_);
        path_factory_ = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory_->Init(directory_->Str(), /*format_identifier=*/"parquet",
                                      /*data_file_prefix=*/"data-",
                                      /*external_path_provider=*/nullptr));
        key_field_ = DataField::ConvertDataFieldToArrowField(
            DataField(1, arrow::field("id", arrow::int64(), /*nullable=*/false)));
        schema_ = DataField::ConvertDataFieldsToArrowSchema(
            {DataField(0, SpecialFields::RealtimeOffset().ArrowField()),
             DataField(1, arrow::field("id", arrow::int64(), /*nullable=*/false))});
        schema_manager_ =
            std::make_shared<SchemaManager>(directory_->GetFileSystem(), directory_->Str());
        options_ = {{"file-index.bitmap.columns", SpecialFields::RealtimeOffset().Name()},
                    {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}};
    }

    Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>> CreateLookup(
        const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
        int64_t data_schema_id = 0) const {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                               CoreOptions::FromMap(options_, directory_->GetFileSystem()));
        return RealtimeOffsetFileIndexLookup::Create(
            schema_, data_schema_id, key_field_, data_files, path_factory_,
            directory_->GetFileSystem(), options, schema_manager_, GetDefaultPool());
    }

    Result<std::shared_ptr<TableSchema>> WriteHistoricalSchema(
        const std::shared_ptr<arrow::Schema>& schema) const {
        std::map<std::string, std::string> options = options_;
        options[Options::REALTIME_DEDUPLICATE_KEY_FIELDS] = "id";
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> table_schema,
                               TableSchema::Create(0, schema, {}, {}, options));
        PAIMON_RETURN_NOT_OK(
            directory_->GetFileSystem()->Mkdirs(schema_manager_->SchemaDirectory()));
        PAIMON_ASSIGN_OR_RAISE(std::string content, table_schema->ToJsonString());
        PAIMON_RETURN_NOT_OK(directory_->GetFileSystem()->AtomicStore(
            schema_manager_->SchemaDirectory() + "/schema-0", content));
        return table_schema;
    }

    Result<std::shared_ptr<DataFileMeta>> CreateKeyIndexedDataFile(const std::string& file_name,
                                                                   const std::string& json) const {
        std::shared_ptr<arrow::StructArray> batch = checked_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
                               DataFileKeyOffsetIndexWriter::Create(
                                   key_field_, directory_->GetFileSystem(), GetDefaultPool()));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(batch));
        std::shared_ptr<DataFileMeta> file = MakeDataFile(file_name, batch->length());
        PAIMON_ASSIGN_OR_RAISE(std::string sidecar,
                               writer->Finish(path_factory_->ToPath(file), file->row_count));
        return file->CopyWithExtraFiles({sidecar});
    }

    Result<std::shared_ptr<DataFileMeta>> CreateIndexedDataFile(
        const std::string& file_name, const std::string& json, int64_t min_offset,
        int64_t max_offset, int64_t schema_id = 0, bool name_value_stats = true) const {
        std::shared_ptr<arrow::StructArray> batch = checked_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie());
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options,
                               CoreOptions::FromMap(options_, directory_->GetFileSystem()));
        PAIMON_ASSIGN_OR_RAISE(FileIndexOptions file_index_options,
                               FileIndexOptions::FromCoreOptions(options));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<DataFileIndexWriter> writer,
            DataFileIndexWriter::Create(schema_, file_index_options, directory_->GetFileSystem(),
                                        path_factory_, GetDefaultPool()));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(batch));
        PAIMON_ASSIGN_OR_RAISE(FileIndexWriteResult index,
                               writer->Finish(path_factory_->ToPath(file_name)));
        if (!index.embedded_index || !index.extra_files.empty()) {
            return Status::Invalid("test bitmap index was not embedded");
        }
        const SimpleStats value_stats = BinaryRowGenerator::GenerateStats(
            {min_offset}, {max_offset}, {0}, GetDefaultPool().get());
        const std::optional<std::vector<std::string>> value_stats_cols =
            name_value_stats
                ? std::optional<std::vector<std::string>>({SpecialFields::RealtimeOffset().Name()})
                : std::nullopt;
        return std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/0, batch->length(), DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), value_stats,
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, schema_id,
            DataFileMeta::DUMMY_LEVEL, index.extra_files, Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt, index.embedded_index,
            /*file_source=*/std::nullopt, value_stats_cols,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
    }

 protected:
    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<arrow::Field> key_field_;
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<SchemaManager> schema_manager_;
};

TEST_F(RealtimeOffsetFileIndexLookupTest, TestLookupFilePositionsAcrossDataFiles) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> first,
        CreateIndexedDataFile("data-0.parquet", R"([[10, 1], [12, 2], [14, 3], [15, 4]])",
                              /*min_offset=*/10, /*max_offset=*/15));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> second,
        CreateIndexedDataFile("data-1.parquet", R"([[20, 5], [21, 6], [22, 7], [23, 8]])",
                              /*min_offset=*/20, /*max_offset=*/23));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> lookup,
                         CreateLookup({first, second}));

    RoaringBitmap64 deleted_offsets;
    deleted_offsets.Add(10);
    deleted_offsets.Add(14);
    deleted_offsets.Add(21);
    deleted_offsets.Add(23);
    deleted_offsets.Add(1000);
    std::map<std::string, RoaringBitmap32> positions;
    ASSERT_OK_AND_ASSIGN(positions, lookup->LookupFilePositions(deleted_offsets));
    ASSERT_EQ(2, positions.size());
    ASSERT_EQ("{0,2}", positions.at("data-0.parquet").ToString());
    ASSERT_EQ("{1,3}", positions.at("data-1.parquet").ToString());

    RoaringBitmap64 outside_file_stats;
    outside_file_stats.Add(16);
    outside_file_stats.Add(100);
    ASSERT_OK_AND_ASSIGN(positions, lookup->LookupFilePositions(outside_file_stats));
    ASSERT_TRUE(positions.empty());
}

TEST_F(RealtimeOffsetFileIndexLookupTest, TestMixedExactIndexTypes) {
    std::vector<std::shared_ptr<DataFileMeta>> files;
    int64_t offset = 0;
    for (const std::string& type : {"bitmap", "bsi", "range-bitmap"}) {
        options_ = {{"file-index." + type + ".columns", "_REALTIME_OFFSET"},
                    {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}};
        const std::string json = fmt::format("[[{}, 1]]", offset);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> file,
                             CreateIndexedDataFile(type + ".parquet", json, offset, offset));
        files.push_back(file);
        ++offset;
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> lookup,
                         CreateLookup(files));
    std::map<std::string, RoaringBitmap32> positions;
    ASSERT_OK_AND_ASSIGN(positions, lookup->LookupFilePositions(RoaringBitmap64::From({0, 1, 2})));
    ASSERT_EQ(3, positions.size());
    for (const auto& entry : positions) {
        ASSERT_EQ("{0}", entry.second.ToString());
    }
}

TEST_F(RealtimeOffsetFileIndexLookupTest, TestCreateAndCloneDataFiles) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> lookup,
                         CreateLookup(/*data_files=*/{}));
    ASSERT_TRUE(lookup->DataFiles().empty());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> key_lookup,
                         lookup->CreateKeyOffsetLookup());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 offsets, key_lookup->LookupOffsets(MakeKeys(key_field_)));
    ASSERT_TRUE(offsets.IsEmpty());

    std::shared_ptr<DataFileMeta> data_file = MakeDataFile("data-0.parquet", /*row_count=*/2);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> cloned,
                         lookup->WithDataFiles({data_file}));
    ASSERT_TRUE(lookup->DataFiles().empty());
    ASSERT_EQ(1, cloned->DataFiles().size());
    ASSERT_EQ(data_file, cloned->DataFiles()[0]);

    ASSERT_NOK_WITH_MSG(lookup->WithDataFiles({nullptr}), "invalid data file");
    ASSERT_NOK_WITH_MSG(lookup->WithDataFiles({MakeDataFile("negative.parquet", -1)}),
                        "invalid data file");
    ASSERT_NOK_WITH_MSG(
        lookup->WithDataFiles({MakeDataFile("too-large.parquet",
                                            static_cast<int64_t>(RoaringBitmap32::MAX_VALUE) + 1)}),
        "invalid data file");
}

TEST_F(RealtimeOffsetFileIndexLookupTest, TestReusesUnchangedFileKeyReaders) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> first,
                         CreateKeyIndexedDataFile("first.parquet", R"([[10, 1], [12, 2]])"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> second,
                         CreateKeyIndexedDataFile("second.parquet", R"([[20, 1], [22, 2]])"));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> initial,
                         CreateLookup({first}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> old_keys,
                         initial->CreateKeyOffsetLookup());
    const std::string first_path = path_factory_->ToPath(first);
    const std::string second_path = path_factory_->ToPath(second);
    const auto first_reader = initial->key_readers_.at(first_path).reader;

    auto first_copy = std::make_shared<DataFileMeta>(*first);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> next,
                         initial->WithDataFiles({first_copy, second}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> next_keys,
                         next->CreateKeyOffsetLookup());
    ASSERT_EQ(first_reader, next->key_readers_.at(first_path).reader);
    auto reading_old = std::async(std::launch::async,
                                  [&]() { return old_keys->LookupOffsets(MakeKeys(key_field_)); });
    auto reading_next = std::async(
        std::launch::async, [&]() { return next_keys->LookupOffsets(MakeKeys(key_field_)); });
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 old_found, reading_old.get());
    ASSERT_EQ("{10,12}", old_found.ToString());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 next_found, reading_next.get());
    ASSERT_EQ("{10,12,20,22}", next_found.ToString());
    ASSERT_EQ(1, initial->key_readers_.size());
    ASSERT_EQ(2, next->key_readers_.size());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> repeated,
                         next->CreateKeyOffsetLookup());
    ASSERT_EQ(first_reader, next->key_readers_.at(first_path).reader);
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 found, next_keys->LookupOffsets(MakeKeys(key_field_)));
    ASSERT_EQ("{10,12,20,22}", found.ToString());
    ASSERT_OK_AND_ASSIGN(found, old_keys->LookupOffsets(MakeKeys(key_field_)));
    ASSERT_EQ("{10,12}", found.ToString());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> removed,
                         next->WithDataFiles({second}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> removed_keys,
                         removed->CreateKeyOffsetLookup());
    ASSERT_EQ(1, removed->key_readers_.size());
    ASSERT_EQ(next->key_readers_.at(second_path).reader,
              removed->key_readers_.at(second_path).reader);
    ASSERT_OK_AND_ASSIGN(found, removed_keys->LookupOffsets(MakeKeys(key_field_)));
    ASSERT_EQ("{20,22}", found.ToString());
    ASSERT_OK_AND_ASSIGN(found, next_keys->LookupOffsets(MakeKeys(key_field_)));
    ASSERT_EQ("{10,12,20,22}", found.ToString());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> replacement,
                         CreateKeyIndexedDataFile("replacement.parquet", R"([[30, 1], [32, 2]])"));
    std::shared_ptr<DataFileMeta> changed = first->CopyWithExtraFiles(replacement->extra_files);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> rewritten,
                         initial->WithDataFiles({changed}));
    ASSERT_TRUE(rewritten->key_readers_.empty());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> rewritten_keys,
                         rewritten->CreateKeyOffsetLookup());
    ASSERT_NE(first_reader, rewritten->key_readers_.at(first_path).reader);
    ASSERT_OK_AND_ASSIGN(found, rewritten_keys->LookupOffsets(MakeKeys(key_field_)));
    ASSERT_EQ("{30,32}", found.ToString());
}

TEST_F(RealtimeOffsetFileIndexLookupTest, TestDifferentSchemaDoesNotUseCurrentStatsPosition) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> old_schema,
                         WriteHistoricalSchema(arrow::schema(
                             {arrow::field("id", arrow::int64(), false),
                              arrow::field("_REALTIME_OFFSET", arrow::int64(), false)})));
    key_field_ = DataField::ConvertDataFieldToArrowField(old_schema->Fields()[0]);
    schema_ = arrow::schema(
        {DataField::ConvertDataFieldToArrowField(old_schema->Fields()[1]), key_field_});
    // Offset is at position 1 in the old schema, but position 0 in the current schema.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> old_schema_file,
                         CreateIndexedDataFile("data-0.parquet", R"([[10, 1], [12, 2]])",
                                               /*min_offset=*/100, /*max_offset=*/200,
                                               /*schema_id=*/0, /*name_value_stats=*/false));
    old_schema_file->value_stats =
        BinaryRowGenerator::GenerateStats({100, 10}, {200, 12}, {0, 0}, GetDefaultPool().get());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> lookup,
                         CreateLookup({old_schema_file}, /*data_schema_id=*/1));

    RoaringBitmap64 deleted_offsets;
    deleted_offsets.Add(10);
    std::map<std::string, RoaringBitmap32> positions;
    ASSERT_OK_AND_ASSIGN(positions, lookup->LookupFilePositions(deleted_offsets));
    ASSERT_EQ(1, positions.size());
    ASSERT_EQ("{0}", positions.at("data-0.parquet").ToString());

    // The old schema's actual offset bounds can also prune irrelevant lookups.
    old_schema_file->embedded_index =
        std::make_shared<Bytes>("invalid index", GetDefaultPool().get());
    RoaringBitmap64 outside;
    outside.Add(100);
    ASSERT_OK_AND_ASSIGN(positions, lookup->LookupFilePositions(outside));
    ASSERT_TRUE(positions.empty());
}

TEST_F(RealtimeOffsetFileIndexLookupTest, TestHistoricalKeyRenameAndIncompatibleKey) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> old_schema,
                         WriteHistoricalSchema(
                             arrow::schema({arrow::field("_REALTIME_OFFSET", arrow::int64(), false),
                                            arrow::field("id", arrow::int64(), false)})));
    schema_ = DataField::ConvertDataFieldsToArrowSchema(old_schema->Fields());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> file,
                         CreateIndexedDataFile("data-0.parquet", R"([[10, 1], [12, 2]])", 10, 12));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<DataFileKeyOffsetIndexWriter> writer,
                         DataFileKeyOffsetIndexWriter::Create(
                             schema_->field(1), directory_->GetFileSystem(), GetDefaultPool()));
    auto batch = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()),
                                                  R"([[10, 1], [12, 2]])")
            .ValueOrDie());
    ASSERT_OK(writer->AddBatch(batch));
    ASSERT_OK_AND_ASSIGN(std::string sidecar,
                         writer->Finish(path_factory_->ToPath(file), file->row_count));
    file = file->CopyWithExtraFiles({sidecar});
    key_field_ = schema_->field(1)->WithName("renamed_id");
    schema_ = arrow::schema({schema_->field(0), key_field_});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> lookup,
                         CreateLookup({file}, /*data_schema_id=*/1));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const KeyOffsetLookup> keys,
                         lookup->CreateKeyOffsetLookup());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 found, keys->LookupOffsets(MakeKeys(key_field_)));
    ASSERT_EQ("{10,12}", found.ToString());

    key_field_ = key_field_->WithType(arrow::int32());
    ASSERT_OK_AND_ASSIGN(lookup, CreateLookup({file}, /*data_schema_id=*/1));
    ASSERT_NOK_WITH_MSG(lookup->CreateKeyOffsetLookup(), "deduplicate key is incompatible");
    key_field_ = DataField::ConvertDataFieldToArrowField(
        DataField(2, arrow::field("renamed_id", arrow::int64(), false)));
    ASSERT_OK_AND_ASSIGN(lookup, CreateLookup({file}, /*data_schema_id=*/1));
    ASSERT_NOK_WITH_MSG(lookup->CreateKeyOffsetLookup(), "deduplicate key is incompatible");

    schema_ = DataField::ConvertDataFieldsToArrowSchema(
        {DataField(3, arrow::field("_REALTIME_OFFSET", arrow::int64(), false)),
         DataField(1, arrow::field("id", arrow::int64(), false))});
    ASSERT_OK_AND_ASSIGN(lookup, CreateLookup({file}, /*data_schema_id=*/1));
    RoaringBitmap64 offsets;
    offsets.Add(10);
    ASSERT_NOK_WITH_MSG(lookup->LookupFilePositions(offsets), "offset field ID is incompatible");
}

TEST_F(RealtimeOffsetFileIndexLookupTest, TestCreateValidation) {
    ASSERT_OK_AND_ASSIGN(CoreOptions valid_options,
                         CoreOptions::FromMap({{"file-index.bitmap.columns",
                                                SpecialFields::RealtimeOffset().Name()}}));
    ASSERT_NOK_WITH_MSG(RealtimeOffsetFileIndexLookup::Create(
                            schema_, /*data_schema_id=*/0, key_field_, {nullptr}, path_factory_,
                            directory_->GetFileSystem(), valid_options,
                            /*schema_manager=*/nullptr, GetDefaultPool()),
                        "invalid data file");
    ASSERT_NOK_WITH_MSG(
        RealtimeOffsetFileIndexLookup::Create(schema_, /*data_schema_id=*/0, key_field_, {},
                                              nullptr, directory_->GetFileSystem(), valid_options,
                                              /*schema_manager=*/nullptr, GetDefaultPool()),
        "missing a dependency");
}

}  // namespace paimon::test
