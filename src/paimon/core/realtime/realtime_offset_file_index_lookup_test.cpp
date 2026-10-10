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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_index_writer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/defs.h"
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
        key_field_ = arrow::field("id", arrow::int64());
        schema_ = arrow::schema(
            {DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset()), key_field_});
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
            directory_->GetFileSystem(), GetDefaultPool(), options);
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

TEST_F(RealtimeOffsetFileIndexLookupTest, TestDifferentSchemaDoesNotUseCurrentStatsPosition) {
    // These unnamed stats represent another field at position 0 in the old schema. The current
    // schema has _REALTIME_OFFSET at position 0, so interpreting them with the current layout would
    // incorrectly prune offset 10 before consulting its bitmap index.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> old_schema_file,
                         CreateIndexedDataFile("data-0.parquet", R"([[10, 1], [12, 2]])",
                                               /*min_offset=*/100, /*max_offset=*/200,
                                               /*schema_id=*/0, /*name_value_stats=*/false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<RealtimeOffsetFileIndexLookup> lookup,
                         CreateLookup({old_schema_file}, /*data_schema_id=*/1));

    RoaringBitmap64 deleted_offsets;
    deleted_offsets.Add(10);
    std::map<std::string, RoaringBitmap32> positions;
    ASSERT_OK_AND_ASSIGN(positions, lookup->LookupFilePositions(deleted_offsets));
    ASSERT_EQ(1, positions.size());
    ASSERT_EQ("{0}", positions.at("data-0.parquet").ToString());
}

TEST_F(RealtimeOffsetFileIndexLookupTest, TestCreateValidation) {
    ASSERT_OK_AND_ASSIGN(CoreOptions valid_options,
                         CoreOptions::FromMap({{"file-index.bitmap.columns",
                                                SpecialFields::RealtimeOffset().Name()}}));
    ASSERT_NOK_WITH_MSG(RealtimeOffsetFileIndexLookup::Create(
                            schema_, /*data_schema_id=*/0, key_field_, {nullptr}, path_factory_,
                            directory_->GetFileSystem(), GetDefaultPool(), valid_options),
                        "invalid data file");
    ASSERT_NOK_WITH_MSG(RealtimeOffsetFileIndexLookup::Create(
                            schema_, /*data_schema_id=*/0, key_field_, {}, nullptr,
                            directory_->GetFileSystem(), GetDefaultPool(), valid_options),
                        "missing a dependency");
}

}  // namespace paimon::test
