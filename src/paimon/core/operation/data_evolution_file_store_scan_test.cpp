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

#include "paimon/core/operation/data_evolution_file_store_scan.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/reader/data_evolution_array.h"
#include "paimon/common/reader/data_evolution_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class DataEvolutionFileStoreScanTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    std::shared_ptr<TableSchema> CreateTableSchema(int64_t schema_id,
                                                   const std::vector<DataField>& fields) const {
        EXPECT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::InitSchema(schema_id, fields, /*highest_field_id=*/fields.size(),
                                    /*partition_keys=*/{},
                                    /*primary_keys=*/{}, /*options=*/{}, /*comment=*/std::nullopt,
                                    /*time_millis=*/0));
        return table_schema;
    }
    ManifestEntry CreateManifestEntry(
        int64_t schema_id, const SimpleStats& stats,
        const std::optional<std::vector<std::string>>& value_stats_cols,
        const std::optional<std::vector<std::string>>& write_cols, int64_t sequence_number = 0,
        const std::string& file_name = "test-file.parquet", int64_t row_count = 100,
        int64_t first_row_id = 0) const {
        EXPECT_OK_AND_ASSIGN(
            auto meta,
            DataFileMeta::ForAppend(file_name, /*file_size=*/100l, row_count, stats,
                                    /*min_sequence_number=*/sequence_number,
                                    /*max_sequence_number=*/sequence_number, schema_id,
                                    /*file_source=*/FileSource::Append(), value_stats_cols,
                                    /*external_path=*/std::nullopt, first_row_id, write_cols));
        return ManifestEntry(FileKind::Add(), /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0,
                             /*total_buckets=*/1, meta);
    }

    ManifestEntry CreateBlobManifestEntry(
        int64_t schema_id, const SimpleStats& stats,
        const std::optional<std::vector<std::string>>& value_stats_cols,
        const std::optional<std::vector<std::string>>& write_cols, int64_t first_row_id,
        int64_t row_count) const {
        EXPECT_OK_AND_ASSIGN(
            auto meta,
            DataFileMeta::ForAppend(
                /*file_name=*/"test-file.blob", /*file_size=*/100l, /*row_count=*/100l, stats,
                /*min_sequence_number=*/0l,
                /*max_sequence_number=*/0l, schema_id,
                /*file_source=*/FileSource::Append(), value_stats_cols,
                /*external_path=*/std::nullopt, /*first_row_id=*/0, write_cols));
        return ManifestEntry(FileKind::Add(), /*partition=*/BinaryRow::EmptyRow(), /*bucket=*/0,
                             /*total_buckets=*/1, meta);
    }

    void CheckFieldCountAndArraySize(const std::shared_ptr<DataEvolutionRow>& min_row,
                                     const std::shared_ptr<DataEvolutionRow>& max_row,
                                     const std::shared_ptr<DataEvolutionArray>& null_counts,
                                     int32_t expected) const {
        ASSERT_EQ(min_row->GetFieldCount(), expected);
        ASSERT_EQ(max_row->GetFieldCount(), expected);
        ASSERT_EQ(null_counts->Size(), expected);
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsSingleFile) {
    std::vector<DataField> fields = {DataField(0, arrow::field("f0", arrow::int32())),
                                     DataField(1, arrow::field("f1", arrow::utf8()))};
    auto table_schema = CreateTableSchema(/*schema_id=*/0, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        assert(schema_id == 0);
        return table_schema;
    };

    auto entry = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {5, std::string("z")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry}, table_schema, schema_fetcher));
    auto result_row_count = result_stats.first;
    ASSERT_EQ(result_row_count, 100);
    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 2);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 5);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "z");

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsMultipleFiles) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
    };

    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields);
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, {fields[0], fields[2]});

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a"), 10}, {3, std::string("c"), 30},
                                          {0, 1, 0}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2, 20}, {4, 40}, {1, 2}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry1, entry0}, table_schema0, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 2);
    ASSERT_EQ(max_row->GetInt(0), 4);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "c");
    ASSERT_EQ(min_row->GetInt(2), 20);
    ASSERT_EQ(max_row->GetInt(2), 40);

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 1);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_EQ(null_counts->GetLong(2), 2);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithSchemaEvolution) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
    };

    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, {fields[0], fields[1]});
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {3, std::string("c")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2, std::string("b"), 20}, {4, std::string("d"), 40},
                                          {1, 0, 1}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry0, entry1}, table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 3);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "c");
    ASSERT_EQ(min_row->GetInt(2), 20);
    ASSERT_EQ(max_row->GetInt(2), 40);

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_EQ(null_counts->GetLong(2), 1);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithWriteColsNotEqualToValueStatsCols) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
    };

    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields);
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {3, std::string("c")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f0", "f1"}),
        /*write_cols=*/std::vector<std::string>({"f0", "f1", "f2"}));
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2, 20}, {4, 40}, {1, 0}, pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f0", "f2"}),
        /*write_cols=*/std::vector<std::string>({"f0", "f2"}));

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry0, entry1}, table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 3);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "c");
    ASSERT_TRUE(min_row->IsNullAt(2));
    ASSERT_TRUE(max_row->IsNullAt(2));

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_TRUE(null_counts->IsNullAt(2));
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithSchemaEvolutionRenameFields) {
    std::vector<DataField> fields0 = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::utf8())),
    };
    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields0);
    std::vector<DataField> fields1 = {
        DataField(0, arrow::field("f3", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f0", arrow::int32())),
        DataField(4, arrow::field("f5", arrow::utf8())),
    };
    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields1);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({std::string("a"), 10}, {std::string("c"), 15}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f1", "f2"}),
        /*write_cols=*/std::vector<std::string>({"f1", "f2"}));
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({20, 2}, {25, 5}, {1, 0}, pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f2", "f0"}),
        /*write_cols=*/std::vector<std::string>({"f2", "f0"}));
    auto entry2 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({std::string("bb"), std::string("cd")},
                                          {std::string("dd"), std::string("xy")}, {1, 3},
                                          pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f5", "f1"}),
        /*write_cols=*/std::vector<std::string>({"f3", "f1", "f5"}));

    ASSERT_OK_AND_ASSIGN(auto result_stats,
                         DataEvolutionFileStoreScan::EvolutionStats({entry2, entry1, entry0},
                                                                    table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 4);

    // check min/max row
    ASSERT_TRUE(min_row->IsNullAt(0));
    ASSERT_TRUE(max_row->IsNullAt(0));
    ASSERT_EQ(min_row->GetString(1).ToString(), "cd");
    ASSERT_EQ(max_row->GetString(1).ToString(), "xy");
    ASSERT_EQ(min_row->GetInt(2), 20);
    ASSERT_EQ(max_row->GetInt(2), 25);
    ASSERT_EQ(min_row->GetString(3).ToString(), "bb");
    ASSERT_EQ(max_row->GetString(3).ToString(), "dd");

    // check null count array
    ASSERT_TRUE(null_counts->IsNullAt(0));
    ASSERT_EQ(null_counts->GetLong(1), 3);
    ASSERT_EQ(null_counts->GetLong(2), 1);
    ASSERT_EQ(null_counts->GetLong(3), 1);
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithSchemaEvolutionUpdateType) {
    std::vector<DataField> fields0 = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::utf8())),
    };
    auto table_schema0 = CreateTableSchema(/*schema_id=*/0, fields0);
    std::vector<DataField> fields1 = {
        DataField(4, arrow::field("f4", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::int32())),
        DataField(2, arrow::field("f2", arrow::int32())),
        DataField(3, arrow::field("f3", arrow::int32())),
    };

    auto table_schema1 = CreateTableSchema(/*schema_id=*/1, fields1);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        if (schema_id == 0) {
            return table_schema0;
        } else if (schema_id == 1) {
            return table_schema1;
        }
        return std::shared_ptr<TableSchema>();
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({10, std::string("a"), 11, std::string("b")},
                                          {20, std::string("c"), 22, std::string("d")},
                                          {0, 1, 2, 3}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::nullopt);
    auto entry1 = CreateManifestEntry(
        /*schema_id=*/1, /*stats=*/
        BinaryRowGenerator::GenerateStats({2}, {5}, {0}, pool_.get()),
        /*value_stats_cols=*/std::vector<std::string>({"f4"}),
        /*write_cols=*/std::vector<std::string>({"f1", "f4"}));

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {entry1, entry0}, table_schema1, schema_fetcher));

    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 4);

    // check min/max values and null_counts
    ASSERT_EQ(min_row->GetInt(0), 2);
    ASSERT_EQ(max_row->GetInt(0), 5);
    ASSERT_EQ(null_counts->GetLong(0), 0);

    ASSERT_TRUE(min_row->IsNullAt(1));
    ASSERT_TRUE(max_row->IsNullAt(1));
    ASSERT_TRUE(null_counts->IsNullAt(1));

    ASSERT_EQ(min_row->GetInt(2), 11);
    ASSERT_EQ(max_row->GetInt(2), 22);
    ASSERT_EQ(null_counts->GetLong(2), 2);

    // f1 in entry0 type mismatch schema1
    ASSERT_TRUE(min_row->IsNullAt(3));
    ASSERT_TRUE(max_row->IsNullAt(3));
    ASSERT_TRUE(null_counts->IsNullAt(3));
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithBlob) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("f0", arrow::int32())),
        DataField(1, arrow::field("f1", arrow::utf8())),
        DataField(2, BlobUtils::ToArrowField("blob")),
    };
    auto table_schema = CreateTableSchema(/*schema_id=*/0, fields);

    std::function<Result<std::shared_ptr<TableSchema>>(int64_t)> schema_fetcher =
        [&](int64_t schema_id) -> Result<std::shared_ptr<TableSchema>> {
        assert(schema_id == 0);
        return table_schema;
    };

    auto entry0 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({10, std::string("aa")}, {55, std::string("zz")}, {2, 3},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0", "f1"}),
        /*sequence_number=*/1l);

    auto blob_entry0 = CreateBlobManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {0}, pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0"}),
        /*first_row_id=*/0, /*row_count=*/10l);

    auto blob_entry1 = CreateBlobManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, {0}, pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0"}),
        /*first_row_id=*/10, /*row_count=*/90l);

    auto entry1 = CreateManifestEntry(
        /*schema_id=*/0, /*stats=*/
        BinaryRowGenerator::GenerateStats({1, std::string("a")}, {5, std::string("z")}, {0, 1},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::optional<std::vector<std::string>>({"f0", "f1"}),
        /*sequence_number=*/2l);

    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {blob_entry0, blob_entry1, entry0, entry1},
                                                table_schema, schema_fetcher));

    auto result_row_count = result_stats.first;
    ASSERT_EQ(result_row_count, 100);
    auto min_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.min_values);
    ASSERT_TRUE(min_row);
    auto max_row = std::dynamic_pointer_cast<DataEvolutionRow>(result_stats.second.max_values);
    ASSERT_TRUE(max_row);
    auto null_counts =
        std::dynamic_pointer_cast<DataEvolutionArray>(result_stats.second.null_counts);
    ASSERT_TRUE(null_counts);

    // check field count and array size
    CheckFieldCountAndArraySize(min_row, max_row, null_counts, 3);

    // check min/max row
    ASSERT_EQ(min_row->GetInt(0), 1);
    ASSERT_EQ(max_row->GetInt(0), 5);
    ASSERT_EQ(min_row->GetString(1).ToString(), "a");
    ASSERT_EQ(max_row->GetString(1).ToString(), "z");
    ASSERT_TRUE(min_row->IsNullAt(2));
    ASSERT_TRUE(max_row->IsNullAt(2));

    // check null count array
    ASSERT_EQ(null_counts->GetLong(0), 0);
    ASSERT_EQ(null_counts->GetLong(1), 1);
    ASSERT_TRUE(null_counts->IsNullAt(2));
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithRolledVectorFiles) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("id", arrow::int32())),
        DataField(1, arrow::field("embedding", arrow::fixed_size_list(arrow::float32(), 3))),
        DataField(2, arrow::field("tag", arrow::utf8())),
    };
    auto table_schema = CreateTableSchema(/*schema_id=*/0, fields);
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    auto schema_fetcher = [&](int64_t) -> Result<std::shared_ptr<TableSchema>> {
        return table_schema;
    };
    constexpr int64_t kFirstRowId = 123;
    constexpr int64_t kRowCount = 10000;
    constexpr int64_t kVectorRows = 1000;
    auto main_entry = CreateManifestEntry(
        /*schema_id=*/0,
        BinaryRowGenerator::GenerateStats({0, std::string("a")}, {9999, std::string("z")},
                                          {0, 1000}, pool_.get()),
        /*value_stats_cols=*/std::nullopt,
        /*write_cols=*/std::vector<std::string>{"id", "tag"},
        /*sequence_number=*/1, "main.parquet", kRowCount, kFirstRowId);
    std::vector<ManifestEntry> entries = {main_entry};
    for (int64_t i = 0; i < kRowCount / kVectorRows; ++i) {
        entries.push_back(CreateManifestEntry(
            /*schema_id=*/0, SimpleStats::EmptyStats(),
            /*value_stats_cols=*/std::vector<std::string>{},
            /*write_cols=*/std::vector<std::string>{"embedding"},
            /*sequence_number=*/i + 2, fmt::format("data-{}.vector.vortex", i), kVectorRows,
            kFirstRowId + i * kVectorRows));
    }
    // VECTOR updates are newer than the full-range main file and cover only 1,000 rows each.
    for (bool reverse_order : {false, true}) {
        SCOPED_TRACE(reverse_order);
        if (reverse_order) {
            std::reverse(entries.begin(), entries.end());
        }
        ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                    entries, table_schema, schema_fetcher));
        ASSERT_EQ(result_stats.first, kRowCount);
        const auto& stats = result_stats.second;
        ASSERT_EQ(stats.min_values->GetString(2).ToString(), "a");
        ASSERT_EQ(stats.max_values->GetString(2).ToString(), "z");
        ASSERT_EQ(stats.null_counts->GetLong(2), kVectorRows);
        ASSERT_TRUE(stats.min_values->IsNullAt(1));
        ASSERT_TRUE(stats.max_values->IsNullAt(1));
        ASSERT_TRUE(stats.null_counts->IsNullAt(1));

        const std::vector<std::pair<std::shared_ptr<Predicate>, bool>> cases = {
            {PredicateBuilder::IsNotNull(2, "tag", FieldType::STRING), true},
            {PredicateBuilder::IsNull(2, "tag", FieldType::STRING), true},
            {PredicateBuilder::IsNull(0, "id", FieldType::INT), false},
            {PredicateBuilder::Equal(0, "id", FieldType::INT, Literal(10000)), false},
        };
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        for (const auto& [predicate, keep] : cases) {
            SCOPED_TRACE(predicate->ToString());
            auto filters = std::make_shared<ScanFilter>(
                predicate, std::vector<std::map<std::string, std::string>>{}, std::nullopt);
            ASSERT_OK_AND_ASSIGN(auto scan,
                                 DataEvolutionFileStoreScan::Create(
                                     nullptr, nullptr, nullptr, nullptr, table_schema, arrow_schema,
                                     filters, options, CreateDefaultExecutor(), pool_));
            ASSERT_OK_AND_ASSIGN(auto filtered, scan->PostFilterManifestEntries(
                                                    std::vector<ManifestEntry>(entries)));
            ASSERT_EQ(filtered.size(), keep ? entries.size() : 0);
        }
    }
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithOnlyVectorFiles) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("embedding", arrow::fixed_size_list(arrow::float32(), 3))),
    };
    auto table_schema = CreateTableSchema(/*schema_id=*/0, fields);
    auto schema_fetcher = [&](int64_t) -> Result<std::shared_ptr<TableSchema>> {
        ADD_FAILURE() << "Dedicated VECTOR files must not contribute column statistics";
        return table_schema;
    };
    // An incremental scan can contain only a contiguous subset of independently rolled files.
    auto first = CreateManifestEntry(
        /*schema_id=*/0, SimpleStats::EmptyStats(),
        /*value_stats_cols=*/std::vector<std::string>{},
        /*write_cols=*/std::vector<std::string>{"embedding"},
        /*sequence_number=*/2, "first.vector.vortex", /*row_count=*/1000, /*first_row_id=*/123);
    auto second = CreateManifestEntry(
        /*schema_id=*/0, SimpleStats::EmptyStats(),
        /*value_stats_cols=*/std::vector<std::string>{},
        /*write_cols=*/std::vector<std::string>{"embedding"},
        /*sequence_number=*/3, "second.vector.vortex", /*row_count=*/500,
        /*first_row_id=*/1123);
    ASSERT_OK_AND_ASSIGN(auto result_stats, DataEvolutionFileStoreScan::EvolutionStats(
                                                {second, first}, table_schema, schema_fetcher));
    ASSERT_EQ(result_stats.first, 1500);
    ASSERT_TRUE(result_stats.second.min_values->IsNullAt(0));
    ASSERT_TRUE(result_stats.second.max_values->IsNullAt(0));
    ASSERT_TRUE(result_stats.second.null_counts->IsNullAt(0));
}

TEST_F(DataEvolutionFileStoreScanTest, TestEvolutionStatsWithPartialRangeProviders) {
    std::vector<DataField> fields = {
        DataField(0, arrow::field("tag", arrow::utf8())),
        DataField(1, arrow::field("embedding", arrow::fixed_size_list(arrow::float32(), 3))),
    };
    auto table_schema = CreateTableSchema(/*schema_id=*/0, fields);
    auto schema_fetcher = [&](int64_t) -> Result<std::shared_ptr<TableSchema>> {
        return table_schema;
    };
    auto first = CreateManifestEntry(
        /*schema_id=*/0,
        BinaryRowGenerator::GenerateStats({std::string("a")}, {std::string("z")}, {0}, pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::vector<std::string>{"tag"},
        /*sequence_number=*/1, "first.parquet", /*row_count=*/1000, /*first_row_id=*/123);
    auto second = CreateManifestEntry(
        /*schema_id=*/0,
        BinaryRowGenerator::GenerateStats({std::string("a")}, {std::string("z")}, {500},
                                          pool_.get()),
        /*value_stats_cols=*/std::nullopt, /*write_cols=*/std::vector<std::string>{"tag"},
        /*sequence_number=*/2, "second.parquet", /*row_count=*/1000, /*first_row_id=*/1123);
    auto vector = CreateManifestEntry(
        /*schema_id=*/0, SimpleStats::EmptyStats(),
        /*value_stats_cols=*/std::vector<std::string>{},
        /*write_cols=*/std::vector<std::string>{"embedding"},
        /*sequence_number=*/3, "data.vector.vortex", /*row_count=*/2000, /*first_row_id=*/123);
    ASSERT_OK_AND_ASSIGN(auto result_stats,
                         DataEvolutionFileStoreScan::EvolutionStats({first, vector, second},
                                                                    table_schema, schema_fetcher));
    ASSERT_EQ(result_stats.first, 2000);
    // The VECTOR range joins two main files; neither file's stats describe the whole group.
    ASSERT_TRUE(result_stats.second.min_values->IsNullAt(0));
    ASSERT_TRUE(result_stats.second.max_values->IsNullAt(0));
    ASSERT_TRUE(result_stats.second.null_counts->IsNullAt(0));
}

TEST_F(DataEvolutionFileStoreScanTest, TestFilterEntryByRowRanges) {
    // row id range [100, 200)
    auto file = std::make_shared<DataFileMeta>(
        "data-0.orc", /*file_size=*/645,
        /*row_count=*/100, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/100, /*max_sequence_number=*/199, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/100, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    ManifestEntry entry(FileKind::Add(), BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/1,
                        file);
    {
        // row_ids is null
        ASSERT_OK_AND_ASSIGN(bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                                             entry, /*row_range_index=*/std::nullopt));
        ASSERT_TRUE(exist);
    }
    {
        auto file_without_first_row_id = std::make_shared<DataFileMeta>(
            "data-0.orc", /*file_size=*/645,
            /*row_count=*/100, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/100, /*max_sequence_number=*/199, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1737111915429ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
        ManifestEntry entry_without_first_row_id(FileKind::Add(), BinaryRow::EmptyRow(),
                                                 /*bucket=*/0, /*total_buckets=*/1,
                                                 file_without_first_row_id);
        ASSERT_OK_AND_ASSIGN(RowRangeIndex row_range_index,
                             RowRangeIndex::Create(std::vector<Range>({Range(0l, 0l)})));
        // first row id is null
        ASSERT_OK_AND_ASSIGN(bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(
                                             entry_without_first_row_id, row_range_index));
        ASSERT_TRUE(exist);
    }
    {
        ASSERT_OK_AND_ASSIGN(
            RowRangeIndex row_range_index,
            RowRangeIndex::Create(std::vector<Range>({Range(0l, 0l), Range(10l, 10l)})));
        ASSERT_OK_AND_ASSIGN(
            bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(entry, row_range_index));
        ASSERT_FALSE(exist);
    }
    {
        ASSERT_OK_AND_ASSIGN(
            RowRangeIndex row_range_index,
            RowRangeIndex::Create(std::vector<Range>({Range(0l, 0l), Range(101l, 101l)})));
        ASSERT_OK_AND_ASSIGN(
            bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(entry, row_range_index));
        ASSERT_TRUE(exist);
    }
    {
        ASSERT_OK_AND_ASSIGN(
            RowRangeIndex row_range_index,
            RowRangeIndex::Create(std::vector<Range>({Range(100l, 100l), Range(189l, 189l)})));
        ASSERT_OK_AND_ASSIGN(
            bool exist, DataEvolutionFileStoreScan::FilterEntryByRowRanges(entry, row_range_index));
        ASSERT_TRUE(exist);
    }
}

}  // namespace paimon::test
