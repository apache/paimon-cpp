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

#include "paimon/core/operation/data_evolution_split_read.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <optional>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/vector_store_utils.h"
#include "paimon/core/deletionvectors/apply_deletion_vector_batch_reader.h"
#include "paimon/core/deletionvectors/bitmap64_deletion_vector.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/mock/mock_search_file_index.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
class CountingLocalFileSystem : public LocalFileSystem {
 public:
    explicit CountingLocalFileSystem(std::string counted_path)
        : counted_path_(std::move(counted_path)) {}

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        if (path == counted_path_) {
            ++open_count_;
        }
        return LocalFileSystem::Open(path);
    }

    int32_t OpenCount() const {
        return open_count_.load();
    }

 private:
    std::string counted_path_;
    mutable std::atomic<int32_t> open_count_{0};
};
}  // namespace

class DataEvolutionSplitReadTest : public ::testing::Test {
 public:
    std::shared_ptr<DataFileMeta> CreateDataFileMeta(
        const std::string& file_name, const std::optional<int64_t>& first_row_id,
        const std::optional<FileSource>& file_source) const {
        return DataFileMeta::ForAppend(file_name, /*file_size=*/100, /*row_count=*/100,
                                       /*row_stats=*/SimpleStats::EmptyStats(),
                                       /*min_sequence_number=*/0,
                                       /*max_sequence_number=*/100, /*schema_id=*/0, file_source,
                                       /*value_stats_cols=*/std::nullopt,
                                       /*external_path=*/std::nullopt, first_row_id,
                                       /*write_cols=*/std::nullopt)
            .value();
    }

    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name,
                                                     const std::optional<int64_t>& first_row_id,
                                                     int64_t row_count, int64_t max_seq) const {
        return DataFileMeta::ForAppend(
                   file_name, /*file_size=*/100, row_count,
                   /*row_stats=*/SimpleStats::EmptyStats(),
                   /*min_sequence_number=*/0,
                   /*max_sequence_number=*/max_seq, /*schema_id=*/0, FileSource::Append(),
                   /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, first_row_id,
                   /*write_cols=*/std::nullopt)
            .value();
    }

    std::shared_ptr<DataFileMeta> CreateNormalFile(const std::string& file_name,
                                                   int64_t first_row_id, int64_t row_count,
                                                   int64_t max_sequence_number) const {
        return DataFileMeta::ForAppend(file_name, /*file_size=*/row_count,
                                       /*row_count=*/row_count,
                                       /*row_stats=*/SimpleStats::EmptyStats(),
                                       /*min_sequence_number=*/0, max_sequence_number,
                                       /*schema_id=*/0, FileSource::Append(),
                                       /*value_stats_cols=*/std::nullopt,
                                       /*external_path=*/std::nullopt, first_row_id,
                                       /*write_cols=*/std::nullopt)
            .value();
    }

    std::shared_ptr<DataFileMeta> CreateBlobFile(
        const std::string& file_name, int64_t first_row_id, int64_t row_count,
        int64_t max_sequence_number,
        const std::optional<std::vector<std::string>>& write_cols) const {
        return DataFileMeta::ForAppend(file_name + ".blob", /*file_size=*/row_count,
                                       /*row_count=*/row_count,
                                       /*row_stats=*/SimpleStats::EmptyStats(),
                                       /*min_sequence_number=*/0, max_sequence_number,
                                       /*schema_id=*/0, FileSource::Append(),
                                       /*value_stats_cols=*/std::nullopt,
                                       /*external_path=*/std::nullopt, first_row_id, write_cols)
            .value();
    }

    static Result<std::vector<int32_t>> EmbeddingFieldIds(
        const std::shared_ptr<DataFileMeta>& file) {
        if (!VectorStoreUtils::IsVectorStoreFile(file->file_name)) {
            return Status::Invalid("Only VECTOR files need their field ids resolved");
        }
        return std::vector<int32_t>{1};
    }

    void CheckVectorReadProviders(
        const std::vector<DataEvolutionSplitRead::VectorReadRange>& ranges, int64_t first_row_id,
        const std::vector<std::vector<std::shared_ptr<DataFileMeta>>>& expected) const {
        int64_t next_row_id = first_row_id;
        for (const auto& range : ranges) {
            ASSERT_EQ(range.range.from, next_row_id);
            ASSERT_GE(range.range.to, range.range.from);
            for (int64_t row_id = range.range.from; row_id <= range.range.to; ++row_id) {
                SCOPED_TRACE(row_id);
                const int64_t index = row_id - first_row_id;
                ASSERT_LT(index, expected.size());
                ASSERT_EQ(range.files.size(), expected[index].size());
                for (const auto& file : expected[index]) {
                    ASSERT_NE(std::find(range.files.begin(), range.files.end(), file),
                              range.files.end());
                }
                for (size_t i = 1; i < range.files.size(); ++i) {
                    ASSERT_GE(range.files[i - 1]->max_sequence_number,
                              range.files[i]->max_sequence_number);
                }
            }
            next_row_id = range.range.to + 1;
        }
        ASSERT_EQ(next_row_id, first_row_id + expected.size());
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(DataEvolutionSplitReadTest, TestCreatePushDownPredicate) {
    auto f0_predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT, Literal(1));
    auto f1_predicate =
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT, Literal(2));
    auto row_id_predicate = PredicateBuilder::Equal(
        /*field_index=*/2, SpecialFields::RowId().Name(), FieldType::BIGINT, Literal(int64_t{3}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({f0_predicate, f1_predicate, row_id_predicate}));

    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(
        {DataField(0, arrow::field("f0", arrow::int32())), SpecialFields::RowId()});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> push_down,
                         DataEvolutionSplitRead::CreatePushDownPredicate(predicate, read_schema));
    ASSERT_TRUE(push_down);
    ASSERT_EQ(*push_down, *f0_predicate);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({f0_predicate, f1_predicate}));
    ASSERT_OK_AND_ASSIGN(
        push_down, DataEvolutionSplitRead::CreatePushDownPredicate(or_predicate, read_schema));
    ASSERT_FALSE(push_down);
}

TEST_F(DataEvolutionSplitReadTest, TestAddSingleBlobEntry) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));

    ASSERT_EQ(blob_bunch->Files().size(), 1);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 100);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobEntryAndTail) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/100, /*row_count=*/200,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    ASSERT_OK(blob_bunch->Add(blob_tail));

    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_EQ(blob_bunch->Files()[1], blob_tail);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 300);
}

TEST_F(DataEvolutionSplitReadTest, TestAddNonBlobFileInvalid) {
    auto blob_entry =
        CreateDataFileMeta("normal1.parquet", /*first_row_id=*/0, FileSource::Append());
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_NOK_WITH_MSG(blob_bunch->Add(blob_entry),
                        "Only blob file can be added to a blob bunch.");
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobWithSameFirstRowId) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_full_tail =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_short_tail =
        CreateBlobFile("blob3", /*first_row_id=*/0, /*row_count=*/50,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // Files with the same first row id and lower sequence numbers are older layers of a
    // partial update; they are kept for the row-level placeholder fallback, whether they
    // cover the same range or only a shorter prefix of it.
    ASSERT_OK(blob_bunch->Add(blob_full_tail));
    ASSERT_OK(blob_bunch->Add(blob_short_tail));

    ASSERT_EQ(blob_bunch->Files().size(), 3);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_EQ(blob_bunch->Files()[1], blob_full_tail);
    ASSERT_EQ(blob_bunch->Files()[2], blob_short_tail);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 100);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithOverlappingRowId) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/50, /*row_count=*/150,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // Overlapping layers with different sequence numbers are kept for the fallback.
    ASSERT_OK(blob_bunch->Add(blob_tail));

    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 200);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithOverlappingRowIdAndHigherSequenceNumber) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/50, /*row_count=*/150,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    ASSERT_OK(blob_bunch->Add(blob_tail));

    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 200);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithOverlappingRowIdInSameLayer) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/50, /*row_count=*/150,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // The bunch collects ranges; the fallback reader validates overlaps within each layer.
    ASSERT_OK(blob_bunch->Add(blob_tail));
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 200);
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithNonContinuousRowId) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/200, /*row_count=*/300,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    ASSERT_OK(blob_bunch->Add(blob_tail));
    ASSERT_NOK_WITH_MSG(blob_bunch->RowCount(),
                        "Blob file bunch should always contain a contiguous row range.");
}

TEST_F(DataEvolutionSplitReadTest, TestAddBlobFileWithDifferentWriteCols) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_tail =
        CreateBlobFile("blob2", /*first_row_id=*/100, /*row_count=*/200,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"diff_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // Adding file with different write columns should return bad status
    ASSERT_NOK_WITH_MSG(blob_bunch->Add(blob_tail),
                        "All files in a blob bunch should have the same write columns.");
}

TEST_F(DataEvolutionSplitReadTest, TestBlobBunchChecksContinuityAfterCollectingAllFiles) {
    DataEvolutionSplitRead::BlobBunch bunch(std::nullopt, /*has_row_ids_selection=*/false);
    for (int64_t first_row_id : {0, 20, 10}) {
        ASSERT_OK(bunch.Add(CreateBlobFile(std::to_string(first_row_id), first_row_id, 10, 1,
                                           std::optional<std::vector<std::string>>({"blob_col"}))));
    }
    ASSERT_OK_AND_ASSIGN(int64_t row_count, bunch.RowCount());
    ASSERT_EQ(row_count, 30);
    ASSERT_OK_AND_ASSIGN(Range range, bunch.LogicalRange());
    ASSERT_EQ(range, Range(0, 29));
}

TEST_F(DataEvolutionSplitReadTest, TestBlobBunchRowCountValidatesExpectedRange) {
    for (bool has_row_ids_selection : {false, true}) {
        DataEvolutionSplitRead::BlobBunch bunch(Range(100, 109), has_row_ids_selection);
        ASSERT_OK(bunch.Add(CreateBlobFile("blob", 99, 4, 1,
                                           std::optional<std::vector<std::string>>({"blob_col"}))));
        ASSERT_NOK_WITH_MSG(bunch.RowCount(),
                            "Blob file range should be within normal file range.");
        ASSERT_OK_AND_ASSIGN(Range range, bunch.LogicalRange());
        ASSERT_EQ(range, Range(100, 109));
    }
}

TEST_F(DataEvolutionSplitReadTest, TestEmptyBlobBunchLogicalRange) {
    DataEvolutionSplitRead::BlobBunch bunch(std::nullopt, /*has_row_ids_selection=*/true);
    ASSERT_NOK_WITH_MSG(bunch.LogicalRange(), "Blob file bunch should not be empty.");
}

TEST_F(DataEvolutionSplitReadTest, TestBlobFallbackValidatesRangesBeforeOpeningFiles) {
    auto dir = UniqueTestDirectory::Create("local");
    auto schema = DataField::ConvertDataFieldsToArrowSchema(
        {DataField(0, arrow::field("id", arrow::int32()))});
    const std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                        {Options::ROW_TRACKING_ENABLED, "true"},
                                                        {Options::DATA_EVOLUTION_ENABLED, "true"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         TableSchema::Create(0, schema, {}, {}, options));
    ReadContextBuilder builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> context,
                         InternalReadContext::Create(read_context, table_schema, options));
    DataEvolutionSplitRead split_read(/*path_factory=*/nullptr, context, GetDefaultPool(),
                                      /*executor=*/nullptr);
    auto blob =
        CreateBlobFile("blob", 100, 10, 1, std::optional<std::vector<std::string>>({"blob_col"}));
    auto overlap =
        CreateBlobFile("overlap", 105, 5, 1, std::optional<std::vector<std::string>>({"blob_col"}));
    auto create_reader = [&](const std::vector<std::shared_ptr<DataFileMeta>>& files,
                             const Range& range,
                             const std::optional<std::vector<Range>>& row_ranges) {
        return split_read.CreateBlobFallbackReader(
            BinaryRow::EmptyRow(), files, range, schema, row_ranges,
            /*data_file_path_factory=*/nullptr, /*group_dv_factory=*/nullptr,
            /*group_dv=*/std::nullopt);
    };
    ASSERT_NOK_WITH_MSG(create_reader({}, Range(100, 109), std::nullopt),
                        "Blob bunch should not be empty.");
    ASSERT_NOK_WITH_MSG(create_reader({blob}, Range(101, 109), std::nullopt),
                        "Blob file range should be within logical range.");
    ASSERT_NOK_WITH_MSG(create_reader({blob}, Range(200, 209), std::vector<Range>{Range(200, 200)}),
                        "Blob file range should intersect logical range.");
    ASSERT_NOK_WITH_MSG(create_reader({overlap, blob}, Range(100, 109), std::nullopt),
                        "should not have overlapping row id ranges");
}

TEST_F(DataEvolutionSplitReadTest, TestRowIdSelectionWithOverlap) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_sub1 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/5,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_sub2 =
        CreateBlobFile("blob3", /*first_row_id=*/5, /*row_count=*/5,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/true);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // blob_sub1 was pruned by the row-ids selection in the scan process; blob_sub2 is a newer
    // layer and both files are kept for the row-level placeholder fallback.
    ASSERT_OK(blob_bunch->Add(blob_sub2));
    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_EQ(blob_bunch->Files()[1], blob_sub2);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 10);
}

TEST_F(DataEvolutionSplitReadTest, TestRowIdSelectionWithOverlap2) {
    auto blob_entry =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/10,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_sub1 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/5,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_sub2 =
        CreateBlobFile("blob3", /*first_row_id=*/5, /*row_count=*/5,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/true);
    ASSERT_OK(blob_bunch->Add(blob_entry));
    // blob_sub1 was pruned by the row-ids selection in the scan process; blob_sub2 is an older
    // layer and both files are kept for the row-level placeholder fallback.
    ASSERT_OK(blob_bunch->Add(blob_sub2));
    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry);
    ASSERT_EQ(blob_bunch->Files()[1], blob_sub2);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 10);
}

TEST_F(DataEvolutionSplitReadTest, TestRowIdSelection) {
    auto blob_entry0 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry1 =
        CreateBlobFile("blob2", /*first_row_id=*/10, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry2 =
        CreateBlobFile("blob3", /*first_row_id=*/20, /*row_count=*/10,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/true);
    ASSERT_OK(blob_bunch->Add(blob_entry0));
    // blob_entry1 will not be added, for it has been skipped by row_ids in scan process.
    ASSERT_OK(blob_bunch->Add(blob_entry2));
    ASSERT_EQ(blob_bunch->Files().size(), 2);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry0);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry2);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 20);
}

TEST_F(DataEvolutionSplitReadTest, TestComplexBlobBunchScenario) {
    auto blob_entry1 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry2 =
        CreateBlobFile("blob2", /*first_row_id=*/100, /*row_count=*/200,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry3 =
        CreateBlobFile("blob3", /*first_row_id=*/300, /*row_count=*/300,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry4 =
        CreateBlobFile("blob4", /*first_row_id=*/600, /*row_count=*/400,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_bunch = std::make_shared<DataEvolutionSplitRead::BlobBunch>(
        /*expected_row_range=*/std::nullopt, /*has_row_ids_selection=*/false);
    ASSERT_OK(blob_bunch->Add(blob_entry1));
    ASSERT_OK(blob_bunch->Add(blob_entry2));
    ASSERT_OK(blob_bunch->Add(blob_entry3));
    ASSERT_OK(blob_bunch->Add(blob_entry4));

    ASSERT_EQ(blob_bunch->Files().size(), 4);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry1);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry2);
    ASSERT_EQ(blob_bunch->Files()[2], blob_entry3);
    ASSERT_EQ(blob_bunch->Files()[3], blob_entry4);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 1000);
}

TEST_F(DataEvolutionSplitReadTest, TestComplexBlobBunchScenario2) {
    std::vector<std::shared_ptr<DataFileMeta>> waited;
    auto data = CreateNormalFile("others.parquet", /*first_row_id=*/0, /*row_count=*/1000,
                                 /*max_sequence_number=*/1);
    auto blob_entry1 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/1000,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry2 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/500,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry3 =
        CreateBlobFile("blob3", /*first_row_id=*/500, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry4 =
        CreateBlobFile("blob4", /*first_row_id=*/750, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry5 =
        CreateBlobFile("blob5", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry6 =
        CreateBlobFile("blob6", /*first_row_id=*/100, /*row_count=*/400,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry7 =
        CreateBlobFile("blob7", /*first_row_id=*/750, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    auto blob_entry8 =
        CreateBlobFile("blob8", /*first_row_id=*/850, /*row_count=*/150,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));

    auto blob_entry9 =
        CreateBlobFile("blob9", /*first_row_id=*/100, /*row_count=*/650,
                       /*max_sequence_number=*/4,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"blob_col"}));
    waited.push_back(data);
    waited.push_back(blob_entry1);
    waited.push_back(blob_entry2);
    waited.push_back(blob_entry3);
    waited.push_back(blob_entry4);
    waited.push_back(blob_entry5);
    waited.push_back(blob_entry6);
    waited.push_back(blob_entry7);
    waited.push_back(blob_entry8);
    waited.push_back(blob_entry9);

    auto input_metas = waited;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> batches,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(input_metas)));
    ASSERT_EQ(batches.size(), 1);

    std::vector<std::shared_ptr<DataFileMeta>> batch = batches[0];
    ASSERT_EQ(batch.size(), 10);
    ASSERT_EQ(batch[0], data);
    ASSERT_EQ(batch[1], blob_entry5);
    ASSERT_EQ(batch[2], blob_entry2);
    ASSERT_EQ(batch[3], blob_entry1);
    ASSERT_EQ(batch[4], blob_entry9);
    ASSERT_EQ(batch[5], blob_entry6);
    ASSERT_EQ(batch[6], blob_entry3);
    ASSERT_EQ(batch[7], blob_entry7);
    ASSERT_EQ(batch[8], blob_entry4);
    ASSERT_EQ(batch[9], blob_entry8);

    auto blob_field_to_field_id = [](const std::shared_ptr<DataFileMeta>&) -> Result<int32_t> {
        return 0;
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> bunch,
                         DataEvolutionSplitRead::SplitFieldBunches(
                             batch, blob_field_to_field_id, /*has_row_ranges_selection=*/false));

    ASSERT_EQ(bunch.size(), 2);
    auto blob_bunch = std::dynamic_pointer_cast<DataEvolutionSplitRead::BlobBunch>(bunch[1]);

    // every sequence layer is kept for the row-level placeholder fallback
    ASSERT_EQ(blob_bunch->Files().size(), 9);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry5);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry2);
    ASSERT_EQ(blob_bunch->Files()[2], blob_entry1);
    ASSERT_EQ(blob_bunch->Files()[3], blob_entry9);
    ASSERT_EQ(blob_bunch->Files()[4], blob_entry6);
    ASSERT_EQ(blob_bunch->Files()[5], blob_entry3);
    ASSERT_EQ(blob_bunch->Files()[6], blob_entry7);
    ASSERT_EQ(blob_bunch->Files()[7], blob_entry4);
    ASSERT_EQ(blob_bunch->Files()[8], blob_entry8);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 1000);
}

TEST_F(DataEvolutionSplitReadTest, TestComplexBlobBunchScenario3) {
    std::vector<std::shared_ptr<DataFileMeta>> waited;
    auto data = CreateNormalFile("others.parquet", /*first_row_id=*/0, /*row_count=*/1000,
                                 /*max_sequence_number=*/1);
    auto blob_entry1 =
        CreateBlobFile("blob1", /*first_row_id=*/0, /*row_count=*/1000,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry2 =
        CreateBlobFile("blob2", /*first_row_id=*/0, /*row_count=*/500,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry3 =
        CreateBlobFile("blob3", /*first_row_id=*/500, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry4 =
        CreateBlobFile("blob4", /*first_row_id=*/750, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry5 =
        CreateBlobFile("blob5", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry6 =
        CreateBlobFile("blob6", /*first_row_id=*/100, /*row_count=*/400,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry7 =
        CreateBlobFile("blob7", /*first_row_id=*/750, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry8 =
        CreateBlobFile("blob8", /*first_row_id=*/850, /*row_count=*/150,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));
    auto blob_entry9 =
        CreateBlobFile("blob9", /*first_row_id=*/100, /*row_count=*/650,
                       /*max_sequence_number=*/4,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"1"}));

    auto blob_entry11 =
        CreateBlobFile("blob11", /*first_row_id=*/0, /*row_count=*/1000,
                       /*max_sequence_number=*/1,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry12 =
        CreateBlobFile("blob12", /*first_row_id=*/0, /*row_count=*/500,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry13 =
        CreateBlobFile("blob13", /*first_row_id=*/500, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry14 =
        CreateBlobFile("blob14", /*first_row_id=*/750, /*row_count=*/250,
                       /*max_sequence_number=*/2,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry15 =
        CreateBlobFile("blob15", /*first_row_id=*/0, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry16 =
        CreateBlobFile("blob16", /*first_row_id=*/100, /*row_count=*/400,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry17 =
        CreateBlobFile("blob17", /*first_row_id=*/750, /*row_count=*/100,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry18 =
        CreateBlobFile("blob18", /*first_row_id=*/850, /*row_count=*/150,
                       /*max_sequence_number=*/3,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    auto blob_entry19 =
        CreateBlobFile("blob19", /*first_row_id=*/100, /*row_count=*/650,
                       /*max_sequence_number=*/4,
                       /*write_cols=*/std::optional<std::vector<std::string>>({"2"}));
    waited.push_back(data);
    waited.push_back(blob_entry1);
    waited.push_back(blob_entry2);
    waited.push_back(blob_entry3);
    waited.push_back(blob_entry4);
    waited.push_back(blob_entry5);
    waited.push_back(blob_entry6);
    waited.push_back(blob_entry7);
    waited.push_back(blob_entry8);
    waited.push_back(blob_entry9);

    waited.push_back(blob_entry11);
    waited.push_back(blob_entry12);
    waited.push_back(blob_entry13);
    waited.push_back(blob_entry14);
    waited.push_back(blob_entry15);
    waited.push_back(blob_entry16);
    waited.push_back(blob_entry17);
    waited.push_back(blob_entry18);
    waited.push_back(blob_entry19);

    auto input_metas = waited;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> batches,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(input_metas)));
    ASSERT_EQ(batches.size(), 1);

    std::vector<std::shared_ptr<DataFileMeta>> batch = batches[0];
    auto blob_field_to_field_id = [](const std::shared_ptr<DataFileMeta>& meta) -> Result<int32_t> {
        return std::stoi(meta->write_cols.value()[0]);
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> bunch,
                         DataEvolutionSplitRead::SplitFieldBunches(
                             batch, blob_field_to_field_id, /*has_row_ranges_selection=*/false));

    ASSERT_EQ(bunch.size(), 3);
    auto blob_bunch = std::dynamic_pointer_cast<DataEvolutionSplitRead::BlobBunch>(bunch[1]);
    // every sequence layer is kept for the row-level placeholder fallback
    ASSERT_EQ(blob_bunch->Files().size(), 9);
    ASSERT_EQ(blob_bunch->Files()[0], blob_entry5);
    ASSERT_EQ(blob_bunch->Files()[1], blob_entry2);
    ASSERT_EQ(blob_bunch->Files()[2], blob_entry1);
    ASSERT_EQ(blob_bunch->Files()[3], blob_entry9);
    ASSERT_EQ(blob_bunch->Files()[4], blob_entry6);
    ASSERT_EQ(blob_bunch->Files()[5], blob_entry3);
    ASSERT_EQ(blob_bunch->Files()[6], blob_entry7);
    ASSERT_EQ(blob_bunch->Files()[7], blob_entry4);
    ASSERT_EQ(blob_bunch->Files()[8], blob_entry8);
    ASSERT_OK_AND_ASSIGN(int64_t row_count, blob_bunch->RowCount());
    ASSERT_EQ(row_count, 1000);

    auto blob_bunch2 = std::dynamic_pointer_cast<DataEvolutionSplitRead::BlobBunch>(bunch[2]);
    ASSERT_EQ(blob_bunch2->Files().size(), 9);
    ASSERT_EQ(blob_bunch2->Files()[0], blob_entry15);
    ASSERT_EQ(blob_bunch2->Files()[1], blob_entry12);
    ASSERT_EQ(blob_bunch2->Files()[2], blob_entry11);
    ASSERT_EQ(blob_bunch2->Files()[3], blob_entry19);
    ASSERT_EQ(blob_bunch2->Files()[4], blob_entry16);
    ASSERT_EQ(blob_bunch2->Files()[5], blob_entry13);
    ASSERT_EQ(blob_bunch2->Files()[6], blob_entry17);
    ASSERT_EQ(blob_bunch2->Files()[7], blob_entry14);
    ASSERT_EQ(blob_bunch2->Files()[8], blob_entry18);
    ASSERT_OK_AND_ASSIGN(int64_t row_count2, blob_bunch2->RowCount());
    ASSERT_EQ(row_count2, 1000);
}

TEST_F(DataEvolutionSplitReadTest, TestDifferentRowIdRange) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateNormalFile("file0.parquet", 1l, 100, 10),
        CreateNormalFile("file1.parquet", 1l, 50, 20)};
    ASSERT_NOK_WITH_MSG(DataEvolutionSplitRead::MergeRangesAndSort(std::move(files)),
                        "Data files should be all row id ranges same.");
}

TEST_F(DataEvolutionSplitReadTest, TestDifferentNormalRowIdRangeWithVectorFiles) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateNormalFile("file0.parquet", /*first_row_id=*/100, /*row_count=*/100,
                         /*max_sequence_number=*/10),
        CreateNormalFile("file1.parquet", /*first_row_id=*/100, /*row_count=*/50,
                         /*max_sequence_number=*/20),
        CreateDataFileMeta("file2.vector.vortex", /*first_row_id=*/100, /*row_count=*/40,
                           /*max_seq=*/10),
        CreateDataFileMeta("file3.vector.vortex", /*first_row_id=*/140, /*row_count=*/60,
                           /*max_seq=*/10)};
    ASSERT_NOK_WITH_MSG(DataEvolutionSplitRead::MergeRangesAndSort(std::move(files)),
                        "Data files should be all row id ranges same.");
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithMultipleVectorFilesPerGroup) {
    auto main0 = CreateNormalFile("main0.parquet", /*first_row_id=*/100, /*row_count=*/100,
                                  /*max_sequence_number=*/10);
    auto vector0 = CreateDataFileMeta("vector0.vector.vortex", /*first_row_id=*/100,
                                      /*row_count=*/40, /*max_seq=*/10);
    auto vector1 = CreateDataFileMeta("vector1.vector.vortex", /*first_row_id=*/140,
                                      /*row_count=*/60, /*max_seq=*/10);
    auto main1 = CreateNormalFile("main1.parquet", /*first_row_id=*/200, /*row_count=*/100,
                                  /*max_sequence_number=*/20);
    auto vector2 = CreateDataFileMeta("vector2.vector.vortex", /*first_row_id=*/200,
                                      /*row_count=*/80, /*max_seq=*/20);
    auto vector3 = CreateDataFileMeta("vector3.vector.vortex", /*first_row_id=*/280,
                                      /*row_count=*/20, /*max_seq=*/20);
    std::vector<std::shared_ptr<DataFileMeta>> files = {vector1, main1, vector3,
                                                        vector0, main0, vector2};
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {main0, vector0, vector1}, {main1, vector2, vector3}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanIndependentlyRolledVectorReadRanges) {
    auto main = CreateNormalFile("main.parquet", 100, 10, 0);
    auto vector0 = CreateDataFileMeta("vector0.vector.vortex", 100, 3, 1);
    auto vector1 = CreateDataFileMeta("vector1.vector.vortex", 103, 4, 1);
    auto vector2 = CreateDataFileMeta("vector2.vector.vortex", 107, 3, 1);
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
                         DataEvolutionSplitRead::PlanVectorReadRanges(
                             {vector2, main, vector0, vector1}, {0, 1}, EmbeddingFieldIds));
    ASSERT_EQ(ranges.size(), 3);
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {main, vector1});
    for (int32_t i : {0, 1, 2}) {
        expected[i] = {main, vector0};
    }
    for (int32_t i : {7, 8, 9}) {
        expected[i] = {main, vector2};
    }
    CheckVectorReadProviders(ranges, 100, expected);
    // Planning must retain whole-file metadata so file-local row offsets and DVs remain valid.
    ASSERT_EQ(main->first_row_id, 100);
    ASSERT_EQ(main->row_count, 10);
    ASSERT_EQ(vector1->first_row_id, 103);
    ASSERT_EQ(vector1->row_count, 4);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanSequentialVectorRead) {
    auto main = CreateNormalFile("main.parquet", 100, 10, 0);
    auto vector0 = CreateDataFileMeta("vector0.vector.vortex", 100, 3, 1);
    auto vector1 = CreateDataFileMeta("vector1.vector.vortex", 103, 4, 2);
    auto vector2 = CreateDataFileMeta("vector2.vector.vortex", 107, 3, 3);
    for (const auto& file : {vector0, vector1, vector2}) {
        file->write_cols = std::vector<std::string>{"embedding"};
    }
    auto field_ids =
        [main](const std::shared_ptr<DataFileMeta>& file) -> Result<std::vector<int32_t>> {
        return file == main ? std::vector<int32_t>{0, 2} : std::vector<int32_t>{1};
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> bunches,
                         DataEvolutionSplitRead::TryPlanSequentialVectorRead(
                             {vector2, main, vector0, vector1}, field_ids));
    ASSERT_EQ(bunches.size(), 2);
    ASSERT_EQ(bunches[0]->Files(), std::vector<std::shared_ptr<DataFileMeta>>{main});
    ASSERT_EQ(bunches[1]->Files(),
              (std::vector<std::shared_ptr<DataFileMeta>>{vector0, vector1, vector2}));
    ASSERT_OK_AND_ASSIGN(int64_t row_count, bunches[0]->RowCount());
    ASSERT_EQ(row_count, 10);
    ASSERT_OK_AND_ASSIGN(int64_t row_count2, bunches[1]->RowCount());
    ASSERT_EQ(row_count2, 10);
    ASSERT_EQ(vector1->first_row_id, 103);
    ASSERT_EQ(vector1->row_count, 4);
}

TEST_F(DataEvolutionSplitReadTest, TestSequentialVectorReadFallsBackForUnsupportedLayouts) {
    auto main = CreateNormalFile("main.parquet", 100, 10, 0);
    auto vector0 = CreateDataFileMeta("vector0.vector.vortex", 100, 4, 1);
    auto vector1 = CreateDataFileMeta("vector1.vector.vortex", 104, 6, 1);
    vector0->write_cols = vector1->write_cols = std::vector<std::string>{"embedding"};
    auto field_ids =
        [main](const std::shared_ptr<DataFileMeta>& file) -> Result<std::vector<int32_t>> {
        return file == main ? std::vector<int32_t>{0, 2} : std::vector<int32_t>{1};
    };
    auto check_fallback = [&](const std::vector<std::shared_ptr<DataFileMeta>>& files) {
        ASSERT_OK_AND_ASSIGN(
            std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> bunches,
            DataEvolutionSplitRead::TryPlanSequentialVectorRead(files, field_ids));
        ASSERT_TRUE(bunches.empty());
    };
    check_fallback({vector0, vector1});
    check_fallback({main, main, vector0, vector1});
    check_fallback({main, vector0, vector1, CreateBlobFile("payload", 100, 10, 1, {{"payload"}})});
    check_fallback({main, vector1});
    vector1->first_row_id = 103;
    check_fallback({main, vector0, vector1});
    vector1->first_row_id = 105;
    check_fallback({main, vector0, vector1});
    vector1->first_row_id = 104;
    vector1->row_count = 7;
    check_fallback({main, vector0, vector1});
    vector1->row_count = 6;
    vector1->schema_id = 1;
    check_fallback({main, vector0, vector1});
    vector1->schema_id = 0;
    vector1->file_name = "vector1.vector.parquet";
    check_fallback({main, vector0, vector1});
    vector1->file_name = "vector1.vector.vortex";
    vector1->write_cols = std::vector<std::string>{"another_embedding"};
    check_fallback({main, vector0, vector1});
    vector1->write_cols = vector0->write_cols =
        std::vector<std::string>{"embedding", "another_embedding"};
    check_fallback({main, vector0, vector1});

    vector1->write_cols = vector0->write_cols = std::vector<std::string>{"embedding"};
    auto inline_field_ids =
        [main](const std::shared_ptr<DataFileMeta>& file) -> Result<std::vector<int32_t>> {
        return file == main ? std::vector<int32_t>{0, 1, 2} : std::vector<int32_t>{1};
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>> bunches,
                         DataEvolutionSplitRead::TryPlanSequentialVectorRead(
                             {main, vector0, vector1}, inline_field_ids));
    ASSERT_TRUE(bunches.empty());
}

TEST_F(DataEvolutionSplitReadTest, TestSequentialVectorReadPropagatesMetadataErrors) {
    auto main = CreateNormalFile("main.parquet", 100, 10, 0);
    auto vector = CreateDataFileMeta("vector.vector.vortex", 100, 10, 1);
    vector->write_cols = std::vector<std::string>{"embedding"};
    auto failed_schema = [](const std::shared_ptr<DataFileMeta>&) -> Result<std::vector<int32_t>> {
        return Status::IOError("schema unavailable");
    };
    auto result =
        DataEvolutionSplitRead::TryPlanSequentialVectorRead({main, vector}, failed_schema);
    ASSERT_NOK_WITH_MSG(result, "schema unavailable");
    ASSERT_TRUE(result.status().IsIOError());
}

TEST_F(DataEvolutionSplitReadTest, TestSequentialVectorReadReusesMainReader) {
    auto dir = UniqueTestDirectory::Create("local");
    auto fs = std::make_shared<LocalFileSystem>();
    auto schema = DataField::ConvertDataFieldsToArrowSchema(
        {DataField(0, arrow::field("id", arrow::int32())),
         DataField(1, arrow::field("embedding", arrow::fixed_size_list(arrow::float32(), 2)))});
    const std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                        {Options::ROW_TRACKING_ENABLED, "true"},
                                                        {Options::DATA_EVOLUTION_ENABLED, "true"},
                                                        {Options::READ_BATCH_SIZE, "2"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         TableSchema::Create(0, schema, {}, {}, options));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(dir->Str(), schema, {}, "default", "parquet", "data-",
                                     /*legacy_partition_name_enabled=*/true, {}, std::nullopt,
                                     /*index_file_in_data_file_dir=*/false, GetDefaultPool()));
    ASSERT_OK_AND_ASSIGN(std::string bucket_path,
                         path_factory->BucketPath(BinaryRow::EmptyRow(), 0));
    ASSERT_OK(fs->Mkdirs(bucket_path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileFormat> format,
                         FileFormatFactory::Get("parquet", options));
    auto write_file = [&](const std::string& file_name, int64_t first_row_id,
                          const std::shared_ptr<arrow::Array>& array,
                          int64_t sequence) -> Result<std::shared_ptr<DataFileMeta>> {
        ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*array->type(), &c_schema));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> builder,
                               format->CreateWriterBuilder(&c_schema, 2));
        const std::string path = PathUtil::JoinPath(bucket_path, file_name);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out, fs->Create(path, false));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer, builder->Build(out, "zstd"));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array));
        PAIMON_RETURN_NOT_OK(writer->Finish());
        PAIMON_RETURN_NOT_OK(out->Close());
        PAIMON_ASSIGN_OR_RAISE(FileStatus file_status, fs->GetFileStatus(path));
        std::vector<std::string> write_cols;
        for (const auto& field : array->type()->fields()) {
            write_cols.push_back(field->name());
        }
        return DataFileMeta::ForAppend(file_name, file_status.GetLen(), array->length(),
                                       SimpleStats::EmptyStats(), sequence, sequence,
                                       /*schema_id=*/0, FileSource::Append(), std::nullopt,
                                       std::nullopt, first_row_id, write_cols);
    };
    auto main_data = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({schema->field(0)}),
                                                               "[[0], [1], [2], [3], [4], [5]]")
                         .ValueOrDie();
    auto vector_type = arrow::struct_(
        {DataField(1, arrow::field("embedding", arrow::list(arrow::float32()))).ArrowField()});
    auto vector0_data =
        arrow::ipc::internal::json::ArrayFromJSON(vector_type, "[[[0, 1]], [null], [[2, 3]]]")
            .ValueOrDie();
    auto vector1_data =
        arrow::ipc::internal::json::ArrayFromJSON(vector_type, "[[[3, 4]], [[4, 5]], [[5, 6]]]")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> main,
                         write_file("main.parquet", 0, main_data, 3));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> vector0,
                         write_file("vector0.vector.parquet", 0, vector0_data, 4));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> vector1,
                         write_file("vector1.vector.parquet", 3, vector1_data, 5));
    DataSplitImpl::Builder split_builder(BinaryRow::EmptyRow(), 0, bucket_path,
                                         {vector1, main, vector0});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataSplitImpl> split,
                         split_builder.RawConvertible(false).Build());
    auto expected_type = arrow::struct_({SpecialFields::ValueKind().ArrowField(), schema->field(0),
                                         schema->field(1), SpecialFields::RowId().ArrowField(),
                                         SpecialFields::SequenceNumber().ArrowField()});
    auto expected = checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(expected_type, R"([
            [0, 0, [0, 1], 0, 3], [0, 1, null, 1, 3], [0, 2, [2, 3], 2, 3],
            [0, 3, [3, 4], 3, 3], [0, 4, [4, 5], 4, 3], [0, 5, [5, 6], 5, 3]
        ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(2));
    for (const std::vector<std::string>& projection :
         {std::vector<std::string>{"id", "embedding"},
          std::vector<std::string>{"id", "embedding", "_ROW_ID", "_SEQUENCE_NUMBER"},
          std::vector<std::string>{"id", "_ROW_ID", "_SEQUENCE_NUMBER"},
          std::vector<std::string>{"_ROW_ID", "_SEQUENCE_NUMBER"}}) {
        auto counting_fs = std::make_shared<CountingLocalFileSystem>(
            PathUtil::JoinPath(bucket_path, main->file_name));
        ReadContextBuilder builder(dir->Str());
        builder.WithFileSystem(counting_fs).EnablePrefetch(false).SetReadFieldNames(projection);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> context,
                             InternalReadContext::Create(read_context, table_schema, options));
        auto split_read = std::make_unique<DataEvolutionSplitRead>(path_factory, context,
                                                                   GetDefaultPool(), executor);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BatchReader> reader, split_read->CreateReader(split));
        split_read.reset();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadResultCollector::CollectResult(std::move(reader)));
        ASSERT_NE(result, nullptr);
        arrow::ArrayVector columns = {expected->field(0)};
        std::vector<std::string> names = {"_VALUE_KIND"};
        for (const auto& name : projection) {
            columns.push_back(expected->GetFieldByName(name));
            names.push_back(name);
        }
        auto projected = arrow::StructArray::Make(columns, names).ValueOrDie();
        ASSERT_TRUE(result->Equals(arrow::ChunkedArray(projected))) << result->ToString();
        ASSERT_EQ(counting_fs->OpenCount(), 1);
    }
}

TEST_F(DataEvolutionSplitReadTest, TestPlanVectorReadRangesPreservesOldPrefixAndSuffix) {
    auto main = CreateNormalFile("main.parquet", 0, 10, 0);
    auto old_vector = CreateDataFileMeta("old.vector.vortex", 0, 10, 1);
    auto new_vector = CreateDataFileMeta("new.vector.vortex", 3, 3, 2);
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
                         DataEvolutionSplitRead::PlanVectorReadRanges(
                             {main, old_vector, new_vector}, {1}, EmbeddingFieldIds));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {main, old_vector});
    for (int32_t i : {3, 4, 5}) {
        expected[i] = {main, new_vector};
    }
    CheckVectorReadProviders(ranges, 0, expected);
    ASSERT_EQ(old_vector->first_row_id, 0);
    ASSERT_EQ(old_vector->row_count, 10);
    ASSERT_EQ(new_vector->first_row_id, 3);
    ASSERT_EQ(new_vector->row_count, 3);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanVectorReadRangesWithInterleavedSequences) {
    auto main = CreateNormalFile("main.parquet", 0, 10, 0);
    auto vector0 = CreateDataFileMeta("vector0.vector.vortex", 0, 5, 4);
    auto vector1 = CreateDataFileMeta("vector1.vector.vortex", 2, 6, 3);
    auto vector2 = CreateDataFileMeta("vector2.vector.vortex", 5, 5, 2);
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
                         DataEvolutionSplitRead::PlanVectorReadRanges(
                             {vector2, vector1, main, vector0}, {1}, EmbeddingFieldIds));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {main, vector0});
    for (int32_t i : {5, 6, 7}) {
        expected[i] = {main, vector1};
    }
    for (int32_t i : {8, 9}) {
        expected[i] = {main, vector2};
    }
    CheckVectorReadProviders(ranges, 0, expected);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanVectorReadRangesKeepsProvidersForOtherFields) {
    auto main = CreateNormalFile("main.parquet", 0, 10, 0);
    auto old_vector = CreateDataFileMeta("old.vector.vortex", 0, 10, 1);
    auto new_vector = CreateDataFileMeta("new.vector.vortex", 3, 3, 2);
    auto file_to_field_ids =
        [&old_vector](const std::shared_ptr<DataFileMeta>& file) -> Result<std::vector<int32_t>> {
        return file == old_vector ? std::vector<int32_t>{1, 2} : std::vector<int32_t>{1};
    };
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
                         DataEvolutionSplitRead::PlanVectorReadRanges(
                             {main, old_vector, new_vector}, {1, 2}, file_to_field_ids));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {main, old_vector});
    for (int32_t i : {3, 4, 5}) {
        expected[i] = {main, old_vector, new_vector};
    }
    CheckVectorReadProviders(ranges, 0, expected);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanVectorReadRangesKeepsNormalFilesForGaps) {
    auto main = CreateNormalFile("main.parquet", 0, 10, 0);
    auto vector = CreateDataFileMeta("middle.vector.vortex", 3, 3, 1);
    ASSERT_OK_AND_ASSIGN(
        std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
        DataEvolutionSplitRead::PlanVectorReadRanges({main, vector}, {1}, EmbeddingFieldIds));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {main});
    for (int32_t i : {3, 4, 5}) {
        expected[i] = {main, vector};
    }
    CheckVectorReadProviders(ranges, 0, expected);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanVectorReadRangesForScalarProjection) {
    auto main = CreateNormalFile("main.parquet", 0, 10, 0);
    auto vector0 = CreateDataFileMeta("vector0.vector.vortex", 0, 3, 1);
    auto vector1 = CreateDataFileMeta("vector1.vector.vortex", 3, 7, 1);
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
                         DataEvolutionSplitRead::PlanVectorReadRanges({vector1, main, vector0}, {0},
                                                                      EmbeddingFieldIds));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {main});
    CheckVectorReadProviders(ranges, 0, expected);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanVectorOnlyReadRangesKeepsRowCountProviders) {
    auto other_field = CreateDataFileMeta("other.vector.vortex", 0, 10, 1);
    auto projected_field = CreateDataFileMeta("projected.vector.vortex", 3, 3, 2);
    auto file_to_field_ids =
        [&other_field](const std::shared_ptr<DataFileMeta>& file) -> Result<std::vector<int32_t>> {
        return file == other_field ? std::vector<int32_t>{2} : std::vector<int32_t>{1};
    };
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
                         DataEvolutionSplitRead::PlanVectorReadRanges(
                             {other_field, projected_field}, {1}, file_to_field_ids));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {other_field});
    for (int32_t i : {3, 4, 5}) {
        expected[i] = {projected_field};
    }
    CheckVectorReadProviders(ranges, 0, expected);
}

TEST_F(DataEvolutionSplitReadTest, TestPlanVectorReadRangesUsesFieldIdsAfterRename) {
    auto main = CreateNormalFile("main.parquet", 0, 10, 0);
    auto old_vector = CreateDataFileMeta("old.vector.vortex", 0, 10, 1);
    old_vector->write_cols = std::vector<std::string>{"embedding_before_rename"};
    old_vector->schema_id = 0;
    auto new_vector = CreateDataFileMeta("new.vector.vortex", 3, 3, 2);
    new_vector->write_cols = std::vector<std::string>{"embedding_after_rename"};
    new_vector->schema_id = 1;
    auto file_to_field_ids =
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<std::vector<int32_t>> {
        const std::map<int64_t, std::string> schema_names = {{0, "embedding_before_rename"},
                                                             {1, "embedding_after_rename"}};
        if (file->write_cols != std::vector<std::string>{schema_names.at(file->schema_id)}) {
            return Status::Invalid("VECTOR write column does not match its original schema");
        }
        return std::vector<int32_t>{1};
    };
    ASSERT_OK_AND_ASSIGN(std::vector<DataEvolutionSplitRead::VectorReadRange> ranges,
                         DataEvolutionSplitRead::PlanVectorReadRanges(
                             {main, old_vector, new_vector}, {1}, file_to_field_ids));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected(10, {main, old_vector});
    for (int32_t i : {3, 4, 5}) {
        expected[i] = {main, new_vector};
    }
    CheckVectorReadProviders(ranges, 0, expected);
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithSameFirstRowId) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {CreateDataFileMeta("file0", 1l, 1, 10),
                                                        CreateDataFileMeta("file1", 1l, 1, 20),
                                                        CreateDataFileMeta("file2", 1l, 1, 30)};
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[2], files[1], files[0]}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithMixedFirstRowId) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("file0", 1l, 1, 1), CreateDataFileMeta("file1", 2l, 1, 2),
        CreateDataFileMeta("file2", 1l, 1, 3), CreateDataFileMeta("file3", 2l, 1, 4),
        CreateDataFileMeta("file4", 3l, 1, 5),
    };
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[2], files[0]}, {files[3], files[1]}, {files[4]}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithComplexScenario) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("file0", 1l, 1, 1), CreateDataFileMeta("file1", 2l, 1, 3),
        CreateDataFileMeta("file2", 3l, 1, 5), CreateDataFileMeta("file3", 1l, 1, 2),
        CreateDataFileMeta("file4", 4l, 1, 8), CreateDataFileMeta("file5", 2l, 1, 4),
        CreateDataFileMeta("file6", 3l, 1, 6), CreateDataFileMeta("file7", 3l, 1, 7),
        CreateDataFileMeta("file8", 5l, 1, 9),
    };
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[3], files[0]},
        {files[5], files[1]},
        {files[7], files[6], files[2]},
        {files[4]},
        {files[8]}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestSplitWithMultipleBlobFilesPerGroup) {
    std::vector<std::shared_ptr<DataFileMeta>> files = {
        CreateDataFileMeta("file0.parquet", 1l, 10, 1),
        CreateDataFileMeta("file1.blob", 1l, 1, 1),
        CreateDataFileMeta("file2.blob", 2l, 9, 1),
        CreateDataFileMeta("file3.parquet", 20l, 10, 2),
        CreateDataFileMeta("file4.blob", 20l, 5, 2),
        CreateDataFileMeta("file5.blob", 25l, 5, 2),
        CreateDataFileMeta("file6.parquet", 1l, 10, 3)};
    auto tmp_files = files;
    ASSERT_OK_AND_ASSIGN(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> split_metas,
                         DataEvolutionSplitRead::MergeRangesAndSort(std::move(tmp_files)));
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> expected_metas = {
        {files[6], files[0], files[1], files[2]}, {files[3], files[4], files[5]}};
    ASSERT_EQ(expected_metas, split_metas);
}

TEST_F(DataEvolutionSplitReadTest, TestFileIndexSelectionOnlyPushesDownNormalGroups) {
    auto dir = UniqueTestDirectory::Create("local");
    auto pool = GetDefaultPool();
    auto schema = DataField::ConvertDataFieldsToArrowSchema(
        {DataField(0, arrow::field("f0", arrow::int32())),
         DataField(1, BlobUtils::ToArrowField("b0")),
         DataField(2, arrow::field("embedding", arrow::list(arrow::float32())))});
    const std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                        {Options::ROW_TRACKING_ENABLED, "true"},
                                                        {Options::DATA_EVOLUTION_ENABLED, "true"},
                                                        {Options::FILE_INDEX_READ_ENABLED, "true"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         TableSchema::Create(0, schema, {}, {}, options));
    ReadContextBuilder builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> context,
                         InternalReadContext::Create(read_context, table_schema, options));
    DataEvolutionSplitRead split_read(/*path_factory=*/nullptr, context, pool,
                                      /*executor=*/nullptr);

    // Use a real embedded bitmap index without creating any data files.
    auto index_schema = arrow::schema({schema->field(0)});
    auto data = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(index_schema->fields()),
                                                          R"([[0],[1],[2],[3]])")
                    .ValueOrDie();
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*index_schema, &c_schema).ok());
    BitmapFileIndex index({});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexWriter> writer,
                         index.CreateWriter(&c_schema, pool));
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*data, &c_array).ok());
    ASSERT_OK(writer->AddBatch(&c_array));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> payload, writer->SerializedBytes());
    FileIndexFormat::ColumnIndexes indexes;
    indexes["f0"]["bitmap"] = payload;
    auto normal = CreateNormalFile("normal.parquet", 100, 4, 20);
    normal->write_cols = std::vector<std::string>{"f0"};
    ASSERT_OK_AND_ASSIGN(normal->embedded_index, MockSearchFileIndex::Serialize(indexes, pool));

    auto blob = CreateBlobFile("partial", 101, 2, 30, std::vector<std::string>{"b0"});
    auto vector = CreateNormalFile("partial.vector.parquet", 101, 2, 30);
    vector->write_cols = std::vector<std::string>{"embedding"};
    auto same_range = CreateNormalFile("same.parquet", 100, 4, 10);
    same_range->write_cols = std::vector<std::string>{"b0"};
    auto different_range = CreateNormalFile("different.parquet", 101, 2, 10);
    different_range->write_cols = std::vector<std::string>{"b0"};
    auto match = PredicateBuilder::Equal(0, "f0", FieldType::INT, Literal(1));
    auto miss = PredicateBuilder::Equal(0, "f0", FieldType::INT, Literal(9));
    auto last_match = PredicateBuilder::Equal(0, "f0", FieldType::INT, Literal(3));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> sparse_match,
                         PredicateBuilder::Or({match, last_match}));

    for (const auto& other : {blob, vector, different_range}) {
        ASSERT_OK_AND_ASSIGN(
            std::optional<RoaringBitmap32> selection,
            split_read.SelectByFileIndex(match, {normal, other}, schema, std::nullopt,
                                         /*data_file_path_factory=*/nullptr));
        ASSERT_FALSE(selection.has_value());

        std::vector<Range> row_ranges = {Range(100, 102)};
        ASSERT_OK_AND_ASSIGN(
            selection, split_read.SelectByFileIndex(match, {normal, other}, schema, row_ranges,
                                                    /*data_file_path_factory=*/nullptr));
        ASSERT_FALSE(selection.has_value());

        // The normal file can still reject the entire group when its index has no matches.
        ASSERT_OK_AND_ASSIGN(
            selection, split_read.SelectByFileIndex(miss, {normal, other}, schema, std::nullopt,
                                                    /*data_file_path_factory=*/nullptr));
        ASSERT_TRUE(selection.has_value());
        ASSERT_TRUE(selection->IsEmpty());
    }
    for (const auto& files : {std::vector<std::shared_ptr<DataFileMeta>>{normal},
                              std::vector<std::shared_ptr<DataFileMeta>>{normal, same_range}}) {
        ASSERT_OK_AND_ASSIGN(std::optional<RoaringBitmap32> selection,
                             split_read.SelectByFileIndex(match, files, schema, std::nullopt,
                                                          /*data_file_path_factory=*/nullptr));
        ASSERT_EQ(selection, std::optional<RoaringBitmap32>(RoaringBitmap32::From({1})));

        // Row ranges use absolute IDs; the returned bitmap keeps file-local positions.
        const std::vector<std::pair<std::vector<Range>, std::vector<int32_t>>> cases = {
            {{Range(101, 101)}, {1}},
            {{Range(103, 103)}, {3}},
            {{Range(100, 100)}, {}},
            {{Range(102, 102)}, {}},
            {{Range(104, 105)}, {}},
            {{}, {}},
            {{Range(103, 103), Range(100, 101), Range(101, 102)}, {1, 3}},
            {{Range(103, 110), Range(99, 101)}, {1, 3}}};
        for (const auto& [row_ranges, expected] : cases) {
            ASSERT_OK_AND_ASSIGN(
                selection, split_read.SelectByFileIndex(sparse_match, files, schema, row_ranges,
                                                        /*data_file_path_factory=*/nullptr));
            ASSERT_EQ(selection, std::optional<RoaringBitmap32>(RoaringBitmap32::From(expected)));
        }
        ASSERT_OK_AND_ASSIGN(selection,
                             split_read.SelectByFileIndex(/*predicate=*/nullptr, files, schema,
                                                          std::vector<Range>{Range(100, 102)},
                                                          /*data_file_path_factory=*/nullptr));
        ASSERT_FALSE(selection.has_value());
        ASSERT_OK_AND_ASSIGN(selection,
                             split_read.SelectByFileIndex(/*predicate=*/nullptr, files, schema,
                                                          std::vector<Range>{Range(200, 201)},
                                                          /*data_file_path_factory=*/nullptr));
        ASSERT_TRUE(selection.has_value());
        ASSERT_TRUE(selection->IsEmpty());
    }
}

namespace {
std::shared_ptr<DeletionVector> MakeBitmapDv(const std::vector<int64_t>& deleted_positions) {
    RoaringBitmap32 bitmap;
    for (int64_t position : deleted_positions) {
        bitmap.Add(static_cast<uint32_t>(position));
    }
    return std::make_shared<BitmapDeletionVector>(bitmap);
}

DeletionVector::Factory MakeSingleFileDvFactory(const std::string& expected_file_name,
                                                const std::shared_ptr<DeletionVector>& dv) {
    return [expected_file_name,
            dv](const std::string& file_name) -> Result<std::shared_ptr<DeletionVector>> {
        if (file_name == expected_file_name) {
            return dv;
        }
        return std::shared_ptr<DeletionVector>();
    };
}
}  // namespace

TEST_F(DataEvolutionSplitReadTest, TestExcludeDeletedPositionsKeepsBitmapSelection) {
    const std::vector<std::pair<std::vector<int32_t>, std::vector<int32_t>>> cases = {
        {{0, 1, 2, 3, 4, 5, 6, 7}, {0, 2, 4, 5, 6, 7}}, {{1, 3}, {}}, {{0, 2}, {0, 2}}, {{}, {}}};
    for (bool bitmap64 : {false, true}) {
        std::shared_ptr<DeletionVector> dv =
            bitmap64 ? std::shared_ptr<DeletionVector>(std::make_shared<Bitmap64DeletionVector>())
                     : MakeBitmapDv({});
        ASSERT_OK(dv->Delete(1));
        ASSERT_OK(dv->Delete(3));
        if (bitmap64) {
            ASSERT_OK(dv->Delete(1LL << 32));
        }
        for (const auto& [selected, expected] : cases) {
            ASSERT_OK_AND_ASSIGN(RoaringBitmap32 live,
                                 DataEvolutionSplitRead::ExcludeDeletedPositions(
                                     RoaringBitmap32::From(selected), dv));
            ASSERT_EQ(live, RoaringBitmap32::From(expected));
        }
    }
}

TEST_F(DataEvolutionSplitReadTest, TestGroupBitmapCombinesWithRowRangesAndDvInFileReader) {
    auto dir = UniqueTestDirectory::Create("local");
    auto schema = DataField::ConvertDataFieldsToArrowSchema(
        {DataField(0, arrow::field("f0", arrow::int32()))});
    const std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "parquet"},
                                                        {Options::ROW_TRACKING_ENABLED, "true"},
                                                        {Options::DATA_EVOLUTION_ENABLED, "true"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         TableSchema::Create(0, schema, {}, {}, options));
    ReadContextBuilder builder(dir->Str());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ReadContext> read_context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InternalReadContext> context,
                         InternalReadContext::Create(read_context, table_schema, options));
    DataEvolutionSplitRead split_read(/*path_factory=*/nullptr, context, GetDefaultPool(),
                                      /*executor=*/nullptr);
    split_read.file_indexes_evaluated_ = true;
    split_read.group_selection_ = RoaringBitmap32::From({0, 1, 3});
    auto file = CreateNormalFile("normal.parquet", 100, 4, 20);
    auto type = arrow::struct_(schema->fields());
    auto data =
        arrow::ipc::internal::json::ArrayFromJSON(type, R"([[0],[1],[2],[3]])").ValueOrDie();
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileBatchReader> reader,
        split_read.ApplyIndexAndDvReaderIfNeeded(
            std::make_unique<MockFileBatchReader>(data, type, 2), file, schema, schema,
            /*predicate=*/nullptr, MakeSingleFileDvFactory(file->file_name, MakeBitmapDv({1})),
            std::vector<Range>{Range(101, 103)}, /*data_file_path_factory=*/nullptr,
            /*file_index_options=*/{}, /*index_scores=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                         ReadResultCollector::CollectResult(std::move(reader)));
    auto expected = arrow::ipc::internal::json::ArrayFromJSON(type, R"([[3]])").ValueOrDie();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->Equals(arrow::ChunkedArray(expected))) << result->ToString();
}

TEST_F(DataEvolutionSplitReadTest, TestReadGroupDeletionVector) {
    // the anchor is the oldest normal file; blob files never anchor the deletion vector
    std::vector<std::shared_ptr<DataFileMeta>> group = {
        CreateNormalFile("newest.parquet", /*first_row_id=*/0, /*row_count=*/100,
                         /*max_sequence_number=*/20),
        CreateNormalFile("anchor.parquet", /*first_row_id=*/0, /*row_count=*/100,
                         /*max_sequence_number=*/10),
        CreateDataFileMeta("blob0.blob", /*first_row_id=*/0, /*row_count=*/100, /*max_seq=*/5)};

    auto dv = MakeBitmapDv({3, 7});
    ASSERT_OK_AND_ASSIGN(std::optional<DataEvolutionSplitRead::GroupDeletionVector> group_dv,
                         DataEvolutionSplitRead::ReadGroupDeletionVector(
                             group, MakeSingleFileDvFactory("anchor.parquet", dv)));
    ASSERT_TRUE(group_dv.has_value());
    ASSERT_EQ(group_dv->deletion_vector, dv);
    ASSERT_EQ(group_dv->anchor_range, Range(0, 99));

    // no deletion vector on the anchor file
    ASSERT_OK_AND_ASSIGN(group_dv, DataEvolutionSplitRead::ReadGroupDeletionVector(
                                       group, MakeSingleFileDvFactory("newest.parquet", dv)));
    ASSERT_FALSE(group_dv.has_value());

    // an empty deletion vector is treated as absent
    ASSERT_OK_AND_ASSIGN(group_dv,
                         DataEvolutionSplitRead::ReadGroupDeletionVector(
                             group, MakeSingleFileDvFactory("anchor.parquet", MakeBitmapDv({}))));
    ASSERT_FALSE(group_dv.has_value());

    // a split without any deletion file hands down a null factory, which is not an error
    ASSERT_OK_AND_ASSIGN(group_dv, DataEvolutionSplitRead::ReadGroupDeletionVector(
                                       group, DeletionVector::Factory()));
    ASSERT_FALSE(group_dv.has_value());

    // Match Java: a split carrying deletion files requires a normal anchor in every read group.
    std::vector<std::shared_ptr<DataFileMeta>> anchorless_group = {
        CreateDataFileMeta("blob1.blob", /*first_row_id=*/100, /*row_count=*/100, /*max_seq=*/5),
        CreateDataFileMeta("blob2.blob", /*first_row_id=*/100, /*row_count=*/100, /*max_seq=*/6),
        CreateDataFileMeta("vector-store.vector.parquet", /*first_row_id=*/100, /*row_count=*/100,
                           /*max_seq=*/7)};
    ASSERT_NOK_WITH_MSG(DataEvolutionSplitRead::ReadGroupDeletionVector(
                            anchorless_group, MakeSingleFileDvFactory("anchor.parquet", dv)),
                        "normal anchor file");
    ASSERT_OK_AND_ASSIGN(group_dv, DataEvolutionSplitRead::ReadGroupDeletionVector(
                                       anchorless_group, DeletionVector::Factory()));
    ASSERT_FALSE(group_dv.has_value());
}

TEST_F(DataEvolutionSplitReadTest, TestCreateGroupDvFactoryShiftsPositions) {
    auto anchor = CreateNormalFile("anchor.parquet", /*first_row_id=*/100, /*row_count=*/100,
                                   /*max_sequence_number=*/10);
    // blob file covers row ids [140, 169], its offset inside the anchor range is 40
    auto blob = CreateDataFileMeta("blob0.blob", /*first_row_id=*/140, /*row_count=*/30,
                                   /*max_seq=*/20);
    std::vector<std::shared_ptr<DataFileMeta>> group = {anchor, blob};

    // deletion vector positions are anchor-relative: positions {10, 45} are row ids {110, 145}
    DataEvolutionSplitRead::GroupDeletionVector group_dv{MakeBitmapDv({10, 45}), Range(100, 199)};
    ASSERT_OK_AND_ASSIGN(
        DeletionVector::Factory factory,
        DataEvolutionSplitRead::CreateGroupDvFactory(group, group_dv, /*row_ranges=*/std::nullopt));
    ASSERT_TRUE(factory);

    // the anchor file reads the deletion vector unshifted
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVector> anchor_dv, factory("anchor.parquet"));
    ASSERT_TRUE(anchor_dv);
    ASSERT_OK_AND_ASSIGN(bool is_deleted, anchor_dv->IsDeleted(10));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, anchor_dv->IsDeleted(45));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, anchor_dv->IsDeleted(5));
    ASSERT_FALSE(is_deleted);

    // the blob file reads it shifted to its local positions: local 5 is row id 145
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVector> blob_dv, factory("blob0.blob"));
    ASSERT_TRUE(blob_dv);
    ASSERT_OK_AND_ASSIGN(is_deleted, blob_dv->IsDeleted(5));
    ASSERT_TRUE(is_deleted);
    ASSERT_OK_AND_ASSIGN(is_deleted, blob_dv->IsDeleted(10));
    ASSERT_FALSE(is_deleted);
    ASSERT_FALSE(blob_dv->IsEmpty());
    // the view is limited to the blob file's 30-row window: cardinality only counts the
    // deleted position inside the window, probing beyond the window is rejected
    ASSERT_EQ(blob_dv->GetCardinality().value(), 1);
    ASSERT_NOK_WITH_MSG(blob_dv->IsDeleted(30), "out of window");

    // the mutating and serializing halves would silently drop the shift, so they reject
    ASSERT_NOK_WITH_MSG(blob_dv->Delete(0), "read-only");
    ASSERT_NOK_WITH_MSG(blob_dv->CheckedDelete(0), "read-only");
    ASSERT_NOK_WITH_MSG(blob_dv->Merge(MakeBitmapDv({0})), "read-only");
    ASSERT_NOK_WITH_MSG(blob_dv->SerializeTo(GetDefaultPool(), /*out=*/nullptr),
                        "does not support serialization");
    ASSERT_NOK_WITH_MSG(blob_dv->SerializeToBytes(GetDefaultPool()),
                        "does not support serialization");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVector> unknown_dv, factory("unknown.parquet"));
    ASSERT_FALSE(unknown_dv);

    ASSERT_OK_AND_ASSIGN(DeletionVector::Factory null_factory,
                         DataEvolutionSplitRead::CreateGroupDvFactory(group, std::nullopt,
                                                                      /*row_ranges=*/std::nullopt));
    ASSERT_FALSE(null_factory);
}

TEST_F(DataEvolutionSplitReadTest, TestCreateGroupDvFactoryRejectsFileOutsideAnchorRange) {
    auto anchor = CreateNormalFile("anchor.parquet", /*first_row_id=*/100, /*row_count=*/100,
                                   /*max_sequence_number=*/10);
    auto blob = CreateDataFileMeta("blob0.blob", /*first_row_id=*/180, /*row_count=*/30,
                                   /*max_seq=*/20);
    std::vector<std::shared_ptr<DataFileMeta>> group = {anchor, blob};

    DataEvolutionSplitRead::GroupDeletionVector group_dv{MakeBitmapDv({10}), Range(100, 199)};
    ASSERT_NOK_WITH_MSG(
        DataEvolutionSplitRead::CreateGroupDvFactory(group, group_dv, /*row_ranges=*/std::nullopt),
        "should contain selected row id range");
}

TEST_F(DataEvolutionSplitReadTest, TestGroupDvChecksOnlySelectedRanges) {
    auto anchor = CreateNormalFile("anchor.parquet", 100, 100, 10);
    auto prefix = CreateDataFileMeta("prefix.blob", 90, 30, 20);
    auto suffix = CreateDataFileMeta("suffix.blob", 180, 30, 20);
    std::vector<std::shared_ptr<DataFileMeta>> group = {anchor, prefix, suffix};
    for (bool bitmap64 : {false, true}) {
        std::shared_ptr<DeletionVector> dv =
            bitmap64 ? std::shared_ptr<DeletionVector>(std::make_shared<Bitmap64DeletionVector>())
                     : MakeBitmapDv({});
        ASSERT_OK(dv->Delete(5));
        ASSERT_OK(dv->Delete(85));
        DataEvolutionSplitRead::GroupDeletionVector group_dv{dv, Range(100, 199)};
        ASSERT_OK_AND_ASSIGN(
            DeletionVector::Factory factory,
            DataEvolutionSplitRead::CreateGroupDvFactory(
                group, group_dv, std::vector<Range>{Range(180, 199), Range(100, 119)}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVector> prefix_dv, factory("prefix.blob"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVector> suffix_dv, factory("suffix.blob"));
        ASSERT_TRUE(prefix_dv->IsDeleted(15).value());
        ASSERT_TRUE(suffix_dv->IsDeleted(5).value());
        ASSERT_NOK_WITH_MSG(DataEvolutionSplitRead::CreateGroupDvFactory(
                                group, group_dv, std::vector<Range>{Range(99, 105)}),
                            "should contain selected row id range");
        ASSERT_NOK_WITH_MSG(DataEvolutionSplitRead::CreateGroupDvFactory(
                                group, group_dv, std::vector<Range>{Range(195, 200)}),
                            "should contain selected row id range");
        ASSERT_OK(DataEvolutionSplitRead::CreateGroupDvFactory(
            group, group_dv, std::vector<Range>{Range(300, 310)}));

        // A format can return an entire batch with only some rows selected. The DV wrapper
        // must never probe unselected prefix/suffix positions outside the anchor range.
        auto type = arrow::struct_({arrow::field("f0", arrow::int32())});
        auto data =
            arrow::ipc::internal::json::ArrayFromJSON(type, R"([[0],[1],[2],[3]])").ValueOrDie();
        auto small_file = CreateDataFileMeta("small.blob", 99, 4, 20);
        ASSERT_OK_AND_ASSIGN(factory,
                             DataEvolutionSplitRead::CreateGroupDvFactory(
                                 {small_file}, group_dv, std::vector<Range>{Range(100, 101)}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<DeletionVector> shifted_dv, factory("small.blob"));
        RoaringBitmap32 selection = RoaringBitmap32::From({1, 2});
        ApplyDeletionVectorBatchReader reader(
            std::make_unique<MockFileBatchReader>(data, type, selection, 4), shifted_dv);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> result,
                             ReadResultCollector::CollectResult(&reader));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->length(), 2);
    }
}

TEST_F(DataEvolutionSplitReadTest, TestPartialBlobBunchUsesNormalRange) {
    auto blob = CreateBlobFile("blob", 103, 2, 20, std::optional<std::vector<std::string>>({"b0"}));
    DataEvolutionSplitRead::BlobBunch bunch(Range(100, 109), /*has_row_ids_selection=*/false);
    ASSERT_OK(bunch.Add(blob));
    ASSERT_OK_AND_ASSIGN(int64_t row_count, bunch.RowCount());
    ASSERT_EQ(row_count, 10);
    ASSERT_OK_AND_ASSIGN(Range logical_range, bunch.LogicalRange());
    ASSERT_EQ(logical_range, Range(100, 109));
    auto tail = CreateBlobFile("tail", 108, 2, 20, std::optional<std::vector<std::string>>({"b0"}));
    ASSERT_OK(bunch.Add(tail));
    ASSERT_OK_AND_ASSIGN(int64_t row_count2, bunch.RowCount());
    ASSERT_EQ(row_count2, 10);
}

TEST_F(DataEvolutionSplitReadTest, TestExcludeDeletedRowIds) {
    // anchor range starts at 100, deleted anchor positions {3, 4, 7} are row ids {103, 104, 107}
    DataEvolutionSplitRead::GroupDeletionVector group_dv{MakeBitmapDv({3, 4, 7}), Range(100, 199)};

    ASSERT_OK_AND_ASSIGN(std::vector<Range> remaining,
                         DataEvolutionSplitRead::ExcludeDeletedRowIds({Range(102, 108)}, group_dv));
    std::vector<Range> expected = {Range(102, 102), Range(105, 106), Range(108, 108)};
    ASSERT_EQ(remaining, expected);

    // ranges without deleted rows are kept whole
    ASSERT_OK_AND_ASSIGN(remaining, DataEvolutionSplitRead::ExcludeDeletedRowIds(
                                        {Range(110, 115), Range(120, 121)}, group_dv));
    expected = {Range(110, 115), Range(120, 121)};
    ASSERT_EQ(remaining, expected);

    // a fully deleted range disappears
    ASSERT_OK_AND_ASSIGN(remaining,
                         DataEvolutionSplitRead::ExcludeDeletedRowIds({Range(103, 104)}, group_dv));
    ASSERT_TRUE(remaining.empty());
}

TEST_F(DataEvolutionSplitReadTest, TestExcludeDeletedRowIdsRestrictsDeletionsToSelectedGaps) {
    for (bool bitmap64 : {false, true}) {
        std::shared_ptr<DeletionVector> dv =
            bitmap64 ? std::shared_ptr<DeletionVector>(std::make_shared<Bitmap64DeletionVector>())
                     : MakeBitmapDv({});
        for (int64_t position = 0; position < 200; position += 2) {
            ASSERT_OK(dv->Delete(position));
        }
        DataEvolutionSplitRead::GroupDeletionVector group_dv{dv, Range(100, 599)};

        // Selected gaps exceed the DV cardinality, but all deletions precede them.
        ASSERT_OK_AND_ASSIGN(
            std::vector<Range> remaining,
            DataEvolutionSplitRead::ExcludeDeletedRowIds({Range(300, 499)}, group_dv));
        ASSERT_EQ(remaining, (std::vector<Range>{Range(300, 499)}));

        // Deletions between selected ranges and after them must also be ignored.
        ASSERT_OK(dv->Delete(450));
        ASSERT_OK_AND_ASSIGN(remaining, DataEvolutionSplitRead::ExcludeDeletedRowIds(
                                            {Range(100, 110), Range(300, 499)}, group_dv));
        ASSERT_EQ(remaining,
                  (std::vector<Range>{Range(101, 101), Range(103, 103), Range(105, 105),
                                      Range(107, 107), Range(109, 109), Range(300, 499)}));

        // Deleting row id 450 (anchor position 350) splits the second selected gap.
        ASSERT_OK(dv->Delete(350));
        ASSERT_OK_AND_ASSIGN(remaining, DataEvolutionSplitRead::ExcludeDeletedRowIds(
                                            {Range(100, 110), Range(400, 499)}, group_dv));
        ASSERT_EQ(remaining, (std::vector<Range>{Range(101, 101), Range(103, 103), Range(105, 105),
                                                 Range(107, 107), Range(109, 109), Range(400, 449),
                                                 Range(451, 499)}));

        // Keep both endpoints of a selected range subject to deletion filtering.
        ASSERT_OK(dv->Delete(200));
        ASSERT_OK(dv->Delete(399));
        ASSERT_OK_AND_ASSIGN(
            remaining, DataEvolutionSplitRead::ExcludeDeletedRowIds({Range(300, 499)}, group_dv));
        ASSERT_EQ(remaining, (std::vector<Range>{Range(301, 449), Range(451, 498)}));
    }
}

TEST_F(DataEvolutionSplitReadTest, Bitmap64GroupWindowWithHighOffset) {
    const int64_t offset = 1LL << 32;
    auto anchor = CreateNormalFile("anchor.parquet", 100, offset + 30, 10);
    auto blob = CreateDataFileMeta("blob0.blob", 100 + offset, 20, 20);
    auto dv = std::make_shared<Bitmap64DeletionVector>();
    for (int64_t p : {offset - 1, offset, offset + 5, offset + 20}) {
        ASSERT_OK(dv->Delete(p));
    }
    DataEvolutionSplitRead::GroupDeletionVector group_dv{dv, Range(100, 100 + offset + 29)};
    ASSERT_OK_AND_ASSIGN(auto factory, DataEvolutionSplitRead::CreateGroupDvFactory(
                                           {anchor, blob}, group_dv, /*row_ranges=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto view, factory("blob0.blob"));
    ASSERT_TRUE(view);
    ASSERT_TRUE(view->IsDeleted(0).value());
    ASSERT_TRUE(view->IsDeleted(5).value());
    ASSERT_FALSE(view->IsDeleted(6).value());
    ASSERT_EQ(view->GetCardinality().value(), 2);
    std::vector<int64_t> positions;
    ASSERT_OK(view->ForEachDeletedPosition([&](int64_t p) { positions.push_back(p); }));
    ASSERT_EQ(positions, (std::vector<int64_t>{0, 5}));
    ASSERT_OK_AND_ASSIGN(std::vector<Range> live, DataEvolutionSplitRead::ExcludeDeletedRowIds(
                                                      {group_dv.anchor_range}, group_dv));
    ASSERT_EQ(live, (std::vector<Range>{
                        Range(100, 100 + offset - 2), Range(101 + offset, 104 + offset),
                        Range(106 + offset, 119 + offset), Range(121 + offset, 129 + offset)}));
}

}  // namespace paimon::test
