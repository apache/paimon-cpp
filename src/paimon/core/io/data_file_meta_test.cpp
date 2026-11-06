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

#include "paimon/core/io/data_file_meta.h"

#include <functional>
#include <limits>

#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DataFileMetaTest, TestCopyWithoutStats) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    SimpleStats value_stats = BinaryRowGenerator::GenerateStats(
        {1, std::string("a")}, {5, std::string("z")}, {0, 1}, pool.get());
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0.orc", /*file_size=*/645, /*row_count=*/5, BinaryRow::EmptyRow(),
        BinaryRow::EmptyRow(), SimpleStats::EmptyStats(), value_stats,
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::vector<std::string>({"f0", "f1"}),
        /*external_path=*/"file:/tmp/bucket-0/data-0.orc", /*first_row_id=*/100,
        /*write_cols=*/std::vector<std::string>({"f0"}),
        /*column_max_sequence_numbers=*/std::nullopt);

    std::shared_ptr<DataFileMeta> result = file_meta->CopyWithoutStats();

    ASSERT_NE(file_meta.get(), result.get());
    DataFileMeta expected = *file_meta;
    expected.value_stats = SimpleStats::EmptyStats();
    expected.value_stats_cols = std::vector<std::string>();
    ASSERT_EQ(expected, *result);
    ASSERT_EQ(value_stats, file_meta->value_stats);
    ASSERT_EQ(std::vector<std::string>({"f0", "f1"}), file_meta->value_stats_cols.value());

    // Upgrade cannot restore stats once they have been dropped. Writer restore must therefore
    // retain full stats for metadata-only ADD entries; see the Paimon Java bug at
    // https://github.com/apache/paimon/issues/7026.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> upgraded, result->Upgrade(/*new_level=*/1));
    ASSERT_EQ(SimpleStats::EmptyStats(), upgraded->value_stats);
    ASSERT_TRUE(upgraded->value_stats_cols.has_value());
    ASSERT_TRUE(upgraded->value_stats_cols->empty());
}

TEST(DataFileMetaTest, TestCopyWithColumnMaxSequenceNumbers) {
    auto pool = GetDefaultPool();
    auto key_stats = BinaryRowGenerator::GenerateStats({1}, {5}, {0}, pool.get());
    auto value_stats = BinaryRowGenerator::GenerateStats({1, std::string("a")},
                                                         {5, std::string("z")}, {0, 1}, pool.get());
    DataFileMeta file_meta(
        "data-0.orc", /*file_size=*/645, /*row_count=*/5,
        BinaryRowGenerator::GenerateRow({1}, pool.get()),
        BinaryRowGenerator::GenerateRow({5}, pool.get()), key_stats, value_stats,
        /*min_sequence_number=*/100, /*max_sequence_number=*/104, /*schema_id=*/2, /*level=*/1,
        /*extra_files=*/{"data-0.orc.index"}, Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/2, std::make_shared<Bytes>("index", pool.get()), FileSource::Append(),
        /*value_stats_cols=*/std::vector<std::string>{"f0", "f1"},
        /*external_path=*/"external/data-0.orc", /*first_row_id=*/10,
        /*write_cols=*/std::vector<std::string>{"f0", "f1"},
        /*column_max_sequence_numbers=*/std::vector<int64_t>{103, 104});
    const DataFileMeta original = file_meta;
    const std::vector<std::optional<std::vector<int64_t>>> replacements = {
        std::vector<int64_t>{104, 104}, std::nullopt};
    for (const auto& sequences : replacements) {
        auto copied = file_meta.CopyWithColumnMaxSequenceNumbers(sequences);
        ASSERT_TRUE(copied);
        DataFileMeta expected = original;
        expected.column_max_sequence_numbers = sequences;
        ASSERT_EQ(*copied, expected);
        ASSERT_EQ(file_meta, original);
    }
}

TEST(DataFileMetaTest, TestComparisonOfOptionalMetadata) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(auto file_meta,
                         DataFileMeta::ForAppend("data-0.orc", 100, 2, SimpleStats::EmptyStats(), 0,
                                                 1, 0, FileSource::Append(), std::nullopt,
                                                 "external/data-0.orc", 10, std::nullopt));
    file_meta->extra_files = {"changelog-0.orc", std::nullopt};
    file_meta->embedded_index = std::make_shared<Bytes>("index", pool.get());
    file_meta->ToString();
    ASSERT_TRUE(file_meta->TEST_Equal(*file_meta));
    DataFileMeta renamed = *file_meta;
    renamed.file_name = "data-1.orc";
    renamed.file_size = 200;
    renamed.creation_time = Timestamp(1737111915429ll, 0);
    renamed.extra_files = {"changelog-1.orc", std::nullopt};
    renamed.external_path = "other/data-1.orc";
    ASSERT_TRUE(file_meta->TEST_Equal(renamed));
    ASSERT_TRUE(renamed.TEST_Equal(*file_meta));

    const std::vector<std::pair<const char*, std::function<void(DataFileMeta&)>>> changes = {
        {"extra file count", [](DataFileMeta& meta) { meta.extra_files.pop_back(); }},
        {"missing extra file", [](DataFileMeta& meta) { meta.extra_files[0] = std::nullopt; }},
        {"missing embedded index", [](DataFileMeta& meta) { meta.embedded_index.reset(); }},
        {"different embedded index",
         [&](DataFileMeta& meta) {
             meta.embedded_index = std::make_shared<Bytes>("other", pool.get());
         }},
        {"missing external path", [](DataFileMeta& meta) { meta.external_path.reset(); }},
    };
    for (const auto& [name, change] : changes) {
        SCOPED_TRACE(name);
        DataFileMeta other = *file_meta;
        change(other);
        ASSERT_FALSE(file_meta->TEST_Equal(other));
        ASSERT_FALSE(other.TEST_Equal(*file_meta));
    }
}

TEST(DataFileMetaTest, TestAddRowCount) {
    DataFileMeta file_meta("data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645,
                           /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
                           SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
                           /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
                           /*level=*/0, /*extra_files=*/{},
                           /*creation_time=*/Timestamp(1737111915429ll, 0),
                           /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
                           /*value_stats_cols=*/std::nullopt,
                           /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                           /*write_cols=*/std::nullopt,
                           /*column_max_sequence_numbers=*/std::nullopt);
    ASSERT_EQ(3, file_meta.AddRowCount().value());
    // test null delete row count
    file_meta.delete_row_count = std::nullopt;
    ASSERT_EQ(std::nullopt, file_meta.AddRowCount());
}

TEST(DataFileMetaTest, TestFileFormat) {
    DataFileMeta file_meta("data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645,
                           /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
                           SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
                           /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
                           /*level=*/0, /*extra_files=*/{},
                           /*creation_time=*/Timestamp(1737111915429ll, 0),
                           /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
                           /*value_stats_cols=*/std::nullopt,
                           /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                           /*write_cols=*/std::nullopt,
                           /*column_max_sequence_numbers=*/std::nullopt);
    ASSERT_OK_AND_ASSIGN(auto file_format, file_meta.FileFormat());
    ASSERT_EQ("orc", file_format);
    file_meta.file_name = "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.parquet";
    ASSERT_OK_AND_ASSIGN(file_format, file_meta.FileFormat());
    ASSERT_EQ("parquet", file_format);
    // test invalid data file name
    file_meta.file_name = "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0";
    ASSERT_NOK_WITH_MSG(file_meta.FileFormat(),
                        "cannot find format from file data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0");
}

TEST(DataFileMetaTest, TestExternalPathDir) {
    DataFileMeta file_meta(
        "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645, /*row_count=*/5,
        BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/{}, /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/"file:/tmp/bucket-0/data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc",
        /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    ASSERT_EQ("file:/tmp/bucket-0", file_meta.ExternalPathDir().value());
    file_meta.external_path = std::nullopt;
    ASSERT_EQ(std::nullopt, file_meta.ExternalPathDir());
}

TEST(DataFileMetaTest, TestGetMaxSequenceNumber) {
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-0.orc", /*file_size=*/645,
        /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-80110e15-97b5-4bcf-ac09-6ca2659a4950-1.orc", /*file_size=*/645,
        /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/5, /*max_sequence_number=*/10, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    ASSERT_EQ(4, DataFileMeta::GetMaxSequenceNumber({file_meta1}));
    ASSERT_EQ(10, DataFileMeta::GetMaxSequenceNumber({file_meta1, file_meta2}));
    ASSERT_EQ(-1, DataFileMeta::GetMaxSequenceNumber({}));
    ASSERT_EQ(file_meta1, file_meta1);
    ASSERT_NE(file_meta1, file_meta2);
}

TEST(DataFileMetaTest, TestNonNullFirstRowId) {
    {
        auto file_meta = std::make_shared<DataFileMeta>(
            "data-0.orc", /*file_size=*/645,
            /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1737111915429ll, 0),
            /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/100, /*write_cols=*/std::nullopt,
            /*column_max_sequence_numbers=*/std::nullopt);
        ASSERT_OK_AND_ASSIGN(int64_t first_row_id, file_meta->NonNullFirstRowId());
        ASSERT_EQ(100, first_row_id);
    }
    {
        auto file_meta = std::make_shared<DataFileMeta>(
            "data-1.orc", /*file_size=*/645,
            /*row_count=*/5, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1737111915429ll, 0),
            /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt, /*column_max_sequence_numbers=*/std::nullopt);
        ASSERT_NOK_WITH_MSG(file_meta->NonNullFirstRowId(),
                            "First row id of data-1.orc should not be null.");
    }
}

TEST(DataFileMetaTest, TestNonNullRowIdRange) {
    auto create_file_meta = [](int64_t row_count, const std::optional<int64_t>& first_row_id) {
        return std::make_shared<DataFileMeta>(
            "data-0.orc", /*file_size=*/645, row_count, BinaryRow::EmptyRow(),
            BinaryRow::EmptyRow(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1737111915429ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, first_row_id, /*write_cols=*/std::nullopt,
            /*column_max_sequence_numbers=*/std::nullopt);
    };
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
    ASSERT_OK_AND_ASSIGN(Range range, create_file_meta(10, 100)->NonNullRowIdRange());
    ASSERT_EQ(range, Range(100, 109));
    ASSERT_OK_AND_ASSIGN(range, create_file_meta(2, kMax - 1)->NonNullRowIdRange());
    ASSERT_EQ(range, Range(kMax - 1, kMax));
    ASSERT_NOK_WITH_MSG(create_file_meta(3, kMax - 1)->NonNullRowIdRange(),
                        "Invalid row id range of data-0.orc");
    ASSERT_NOK_WITH_MSG(create_file_meta(0, 100)->NonNullRowIdRange(),
                        "Invalid row id range of data-0.orc");
    ASSERT_NOK_WITH_MSG(create_file_meta(10, -1)->NonNullRowIdRange(),
                        "Invalid row id range of data-0.orc");
    ASSERT_NOK_WITH_MSG(create_file_meta(10, std::nullopt)->NonNullRowIdRange(),
                        "First row id of data-0.orc should not be null.");
}

TEST(DataFileMetaTest, TestToFileSelection) {
    // row id range [100, 110)
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0.orc", /*file_size=*/645,
        /*row_count=*/10, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/100, /*max_sequence_number=*/109, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/100, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);

    {
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(std::nullopt));
        ASSERT_FALSE(result);
    }
    {
        std::vector<Range> ranges = {Range(10l, 20l), Range(105l, 107l), Range(500l, 520l)};
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(ranges));
        ASSERT_TRUE(result);
        ASSERT_EQ(result.value().ToString(), "{5,6,7}");
    }
    {
        std::vector<Range> ranges = {};
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(ranges));
        ASSERT_TRUE(result);
        ASSERT_EQ(result.value().ToString(), "{}");
    }
    {
        std::vector<Range> ranges = {Range(100l, 109l)};
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> result,
                             file_meta->ToFileSelection(ranges));
        ASSERT_FALSE(result);
    }
}

TEST(DataFileMetaTest, TestUpgrade) {
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-0.orc", /*file_size=*/645,
        /*row_count=*/10, BinaryRow::EmptyRow(), BinaryRow::EmptyRow(), SimpleStats::EmptyStats(),
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/100, /*max_sequence_number=*/109, /*schema_id=*/0,
        /*level=*/5, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1737111915429ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt,
        /*external_path=*/std::nullopt, /*first_row_id=*/100, /*write_cols=*/std::nullopt,
        /*column_max_sequence_numbers=*/std::nullopt);
    // test normal upgrade
    ASSERT_OK_AND_ASSIGN(auto new_file_meta, file_meta->Upgrade(10));
    ASSERT_EQ(new_file_meta->level, 10);
    // check other members
    file_meta->level = 10;
    ASSERT_EQ(*new_file_meta, *file_meta);

    // test invalid upgrade
    ASSERT_NOK_WITH_MSG(file_meta->Upgrade(1),
                        "new level 1 should be greater than current level 10");
}
}  // namespace paimon::test
