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

#include "paimon/core/realtime/realtime_file_schema.h"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {

class RealtimeFileSchemaTest : public testing::Test {
 public:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_NE(nullptr, directory_);
        schema_ = DataField::ConvertDataFieldsToArrowSchema(
            {DataField(0, SpecialFields::RealtimeOffset().ArrowField()),
             DataField(1, arrow::field("id", arrow::int64(), /*nullable=*/false))});
        schema_manager_ =
            std::make_shared<SchemaManager>(directory_->GetFileSystem(), directory_->Str());
    }

 protected:
    std::shared_ptr<DataFileMeta> MakeDataFile() const {
        return std::make_shared<DataFileMeta>(
            "data-0.parquet", /*file_size=*/0, /*row_count=*/2, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            DataFileMeta::DUMMY_LEVEL, /*extra_files=*/std::vector<std::optional<std::string>>(),
            Timestamp(0, 0), /*delete_row_count=*/std::nullopt, /*embedded_index=*/nullptr,
            /*file_source=*/std::nullopt, /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
    }

    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<SchemaManager> schema_manager_;
};

TEST_F(RealtimeFileSchemaTest, TestOffsetStatsBoundsAndConservativeFallback) {
    std::shared_ptr<DataFileMeta> file = MakeDataFile();
    file->value_stats = BinaryRowGenerator::GenerateStats({10}, {20}, {0}, GetDefaultPool().get());
    file->value_stats_cols = std::vector<std::string>{"_REALTIME_OFFSET"};
    RealtimeFileSchema file_schema(schema_, 0, schema_manager_);
    RoaringBitmap64 offsets;
    for (int64_t offset : {9, 10, 20, 21}) {
        offsets.Add(offset);
    }
    ASSERT_OK_AND_ASSIGN(std::vector<Literal> selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(std::vector<Literal>({Literal(int64_t{10}), Literal(int64_t{20})}), selected);

    file->value_stats_cols = std::vector<std::string>{"id"};
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(4, selected.size());
    file->value_stats_cols = std::vector<std::string>{"_REALTIME_OFFSET"};
    file->value_stats = BinaryRowGenerator::GenerateStats({20}, {10}, {0}, GetDefaultPool().get());
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(4, selected.size());
    file->value_stats = SimpleStats::EmptyStats();
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(4, selected.size());
    file->value_stats = BinaryRowGenerator::GenerateStats({10}, {20}, {0}, GetDefaultPool().get());
    file->value_stats_cols.reset();
    file->write_cols = std::vector<std::string>{"id"};
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(4, selected.size());
}

TEST_F(RealtimeFileSchemaTest, TestWriteColumnsStatsProjection) {
    const auto payload = DataField::ConvertDataFieldToArrowField(
        DataField(2, arrow::field("payload", arrow::int64())));
    const auto schema = arrow::schema({schema_->field(0), schema_->field(1), payload});
    RealtimeFileSchema file_schema(schema, 0, schema_manager_);
    std::shared_ptr<DataFileMeta> file = MakeDataFile();
    const RoaringBitmap64 offsets = RoaringBitmap64::From({9, 10, 20, 21, 100, 200});
    const std::vector<Literal> expected = {Literal(int64_t{10}), Literal(int64_t{20})};

    file->write_cols = std::vector<std::string>{"payload", "_REALTIME_OFFSET"};
    file->value_stats =
        BinaryRowGenerator::GenerateStats({100, 10}, {200, 20}, {0, 0}, GetDefaultPool().get());
    ASSERT_OK_AND_ASSIGN(std::vector<Literal> selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);

    file->value_stats_cols = std::vector<std::string>{"_REALTIME_OFFSET", "payload"};
    file->value_stats =
        BinaryRowGenerator::GenerateStats({10, 100}, {20, 200}, {0, 0}, GetDefaultPool().get());
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);

    file->value_stats_cols.reset();
    file->write_cols = std::vector<std::string>{"_REALTIME_OFFSET"};
    file->value_stats = BinaryRowGenerator::GenerateStats({10}, {20}, {0}, GetDefaultPool().get());
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);
}

TEST_F(RealtimeFileSchemaTest, TestWriteColumnsMissingOffsetStatsFallback) {
    RealtimeFileSchema file_schema(schema_, 0, schema_manager_);
    std::shared_ptr<DataFileMeta> file = MakeDataFile();
    const RoaringBitmap64 offsets = RoaringBitmap64::From({9, 10, 20, 21});
    const std::vector<Literal> expected = {Literal(int64_t{9}), Literal(int64_t{10}),
                                           Literal(int64_t{20}), Literal(int64_t{21})};
    file->write_cols = std::vector<std::string>{"id", "_REALTIME_OFFSET"};
    file->value_stats_cols = std::vector<std::string>{"id"};
    file->value_stats = BinaryRowGenerator::GenerateStats({10}, {20}, {0}, GetDefaultPool().get());
    ASSERT_OK_AND_ASSIGN(std::vector<Literal> selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);

    file->value_stats_cols.reset();
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);

    file->write_cols = std::vector<std::string>{"id"};
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);
}

TEST_F(RealtimeFileSchemaTest, TestHistoricalKeyRenameAndFieldReorder) {
    const std::map<std::string, std::string> options = {
        {Options::REALTIME_DEDUPLICATE_KEY_FIELDS, "id"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> historical_schema,
                         TableSchema::Create(0, schema_, {}, {}, options));
    ASSERT_OK(directory_->GetFileSystem()->Mkdirs(schema_manager_->SchemaDirectory()));
    ASSERT_OK_AND_ASSIGN(std::string content, historical_schema->ToJsonString());
    ASSERT_OK(directory_->GetFileSystem()->AtomicStore(
        schema_manager_->SchemaDirectory() + "/schema-0", content));

    const auto historical_arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(historical_schema->Fields());
    const auto renamed_key = historical_arrow_schema->field(1)->WithName("renamed_id");
    // Swap key and offset positions without changing their field IDs or the offset name.
    const auto current_schema = arrow::schema({renamed_key, historical_arrow_schema->field(0)});
    RealtimeFileSchema file_schema(current_schema, /*schema_id=*/1, schema_manager_);
    std::shared_ptr<DataFileMeta> file = MakeDataFile();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Schema> resolved_schema,
                         file_schema.GetFileSchema(*file));
    ASSERT_TRUE(resolved_schema->Equals(*historical_arrow_schema, /*check_metadata=*/true));
    ASSERT_OK(file_schema.ValidateKey(*file, renamed_key));

    // Historical statistics are [offset, id], not the current [renamed_id, offset].
    file->value_stats =
        BinaryRowGenerator::GenerateStats({10, 100}, {20, 200}, {0, 0}, GetDefaultPool().get());
    RoaringBitmap64 offsets;
    for (int64_t offset : {9, 10, 20, 21, 100, 200}) {
        offsets.Add(offset);
    }
    ASSERT_OK_AND_ASSIGN(std::vector<Literal> selected, file_schema.SelectOffsets(*file, offsets));
    const std::vector<Literal> expected = {Literal(int64_t{10}), Literal(int64_t{20})};
    ASSERT_EQ(expected, selected);

    // Explicit statistics column names take precedence over the historical schema order.
    file->value_stats_cols = std::vector<std::string>{"id", "_REALTIME_OFFSET"};
    file->value_stats =
        BinaryRowGenerator::GenerateStats({100, 10}, {200, 20}, {0, 0}, GetDefaultPool().get());
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);

    file->value_stats_cols.reset();
    file->write_cols = std::vector<std::string>{"id", "_REALTIME_OFFSET"};
    ASSERT_OK_AND_ASSIGN(selected, file_schema.SelectOffsets(*file, offsets));
    ASSERT_EQ(expected, selected);
}

TEST_F(RealtimeFileSchemaTest, TestHistoricalSchemaMustBeAvailable) {
    std::shared_ptr<DataFileMeta> file = MakeDataFile();
    RoaringBitmap64 offsets;
    offsets.Add(10);
    RealtimeFileSchema without_manager(schema_, /*schema_id=*/1, nullptr);
    ASSERT_NOK_WITH_MSG(without_manager.SelectOffsets(*file, offsets), "requires a schema manager");
    RealtimeFileSchema missing_schema(schema_, /*schema_id=*/1, schema_manager_);
    ASSERT_NOK(missing_schema.SelectOffsets(*file, offsets));
}

}  // namespace paimon::test
