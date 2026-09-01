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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/global_index/indexed_split_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using Row = std::tuple<int32_t, int32_t, int32_t, std::string>;

class TrackingLocalFileSystem : public LocalFileSystem {
 public:
    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        {
            std::scoped_lock lock(mutex_);
            opened_paths_.push_back(path);
        }
        return LocalFileSystem::Open(path);
    }

    std::set<std::string> OpenedIndexPaths() const {
        std::scoped_lock lock(mutex_);
        std::set<std::string> index_paths;
        for (const std::string& path : opened_paths_) {
            if (path.find("/index/index-") != std::string::npos) {
                index_paths.insert(path);
            }
        }
        return index_paths;
    }

 private:
    mutable std::mutex mutex_;
    mutable std::vector<std::string> opened_paths_;
};

std::vector<Row> ExpectedScoreZeroRows(int64_t snapshot_id) {
    std::vector<Row> rows;
    for (int32_t id = 10; id <= 2000; id += 10) {
        if (snapshot_id >= 4 && (id == 10 || id == 20)) {
            continue;
        }
        rows.emplace_back(-1, id, 0, "keep");
    }
    if (snapshot_id >= 3) {
        rows.emplace_back(-1, 2001, 0, "keep");
    }
    return rows;
}

std::vector<Row> ExpectedOrRowsAtSnapshot4() {
    std::vector<Row> rows;
    for (int32_t id = 1; id <= 2000; id++) {
        if (id == 10 || id == 20 || id % 2 != 0) {
            continue;
        }
        rows.emplace_back(-1, id, id % 10, "keep");
    }
    rows.emplace_back(-1, 2001, 0, "keep");
    return rows;
}

std::vector<Row> ExpectedResidualRowsAtSnapshot4() {
    std::vector<Row> rows;
    for (int32_t id = 30; id <= 1500; id += 10) {
        rows.emplace_back(-1, id, 0, "keep");
    }
    return rows;
}

std::vector<Row> ExpectedPartitionRowsAtSnapshot2() {
    std::vector<Row> rows;
    for (int32_t pt = 1; pt <= 2; pt++) {
        for (int32_t id = 1; id <= 100; id++) {
            rows.emplace_back(pt, id, id % 10, id % 2 == 0 ? "keep" : "drop");
        }
    }
    return rows;
}

std::vector<Row> ExpectedFallbackRows() {
    std::vector<Row> rows;
    for (int32_t id = 20; id <= 100; id += 10) {
        rows.emplace_back(1, id, 0, "keep");
    }
    rows.emplace_back(1, 101, 0, "keep");
    for (int32_t id = 10; id <= 100; id += 10) {
        rows.emplace_back(2, id, 0, "keep");
    }
    return rows;
}

std::pair<size_t, size_t> CountSplitKinds(const std::shared_ptr<Plan>& plan) {
    size_t indexed_count = 0;
    size_t data_count = 0;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        if (std::dynamic_pointer_cast<IndexedSplitImpl>(split) != nullptr) {
            indexed_count++;
        } else if (std::dynamic_pointer_cast<DataSplitImpl>(split) != nullptr) {
            data_count++;
        }
    }
    return {indexed_count, data_count};
}

}  // namespace

class PrimaryKeySortedIndexInteTest : public ::testing::Test,
                                      public ::testing::WithParamInterface<std::string> {
 protected:
    void SetUp() override {
        format_ = GetParam();
    }

    std::string TablePath(bool partitioned) const {
        const std::string table_name = partitioned ? "pk_btree_partitioned_e2e" : "pk_btree_e2e";
        return PathUtil::JoinPath(GetDataDir(),
                                  fmt::format("{}/{}.db/{}", format_, table_name, table_name));
    }

    std::shared_ptr<Predicate> ScoreEqual(bool partitioned, int32_t score) const {
        return PredicateBuilder::Equal(partitioned ? 2 : 1, "score", FieldType::INT,
                                       Literal(score));
    }

    std::shared_ptr<Predicate> ScoreGreaterOrEqual(bool partitioned, int32_t score) const {
        return PredicateBuilder::GreaterOrEqual(partitioned ? 2 : 1, "score", FieldType::INT,
                                                Literal(score));
    }

    std::shared_ptr<Predicate> TagEqual(bool partitioned, const std::string& tag) const {
        return PredicateBuilder::Equal(partitioned ? 3 : 2, "tag", FieldType::STRING,
                                       Literal(FieldType::STRING, tag.data(), tag.size()));
    }

    std::shared_ptr<Predicate> IdLessOrEqual(bool partitioned, int32_t id) const {
        return PredicateBuilder::LessOrEqual(partitioned ? 1 : 0, "id", FieldType::INT,
                                             Literal(id));
    }

    Result<std::shared_ptr<Plan>> Scan(
        bool partitioned, const std::shared_ptr<Predicate>& predicate,
        const std::optional<int64_t>& snapshot_id, bool index_enabled,
        const std::vector<std::map<std::string, std::string>>& partition_filters,
        const std::string& branch, const std::shared_ptr<FileSystem>& file_system) const {
        ScanContextBuilder builder(TablePath(partitioned));
        builder.SetPredicate(predicate).AddOption(Options::GLOBAL_INDEX_ENABLED,
                                                  index_enabled ? "true" : "false");
        if (snapshot_id != std::nullopt) {
            builder.AddOption(Options::SCAN_SNAPSHOT_ID, fmt::format("{}", snapshot_id.value()));
        }
        if (!partition_filters.empty()) {
            builder.SetPartitionFilter(partition_filters);
        }
        if (!branch.empty()) {
            builder.AddOption(Options::BRANCH, branch);
        }
        if (file_system != nullptr) {
            builder.WithFileSystem(file_system);
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> table_scan,
                               TableScan::Create(std::move(context)));
        return table_scan->CreatePlan();
    }

    Result<std::vector<Row>> Read(bool partitioned, const std::shared_ptr<Predicate>& predicate,
                                  const std::vector<std::shared_ptr<Split>>& splits,
                                  const std::map<std::string, std::string>& options,
                                  const std::shared_ptr<FileSystem>& file_system) const {
        ReadContextBuilder builder(TablePath(partitioned));
        if (partitioned) {
            builder.SetReadFieldNames({"pt", "id", "score", "tag"});
        } else {
            builder.SetReadFieldNames({"id", "score", "tag"});
        }
        builder.SetPredicate(predicate).EnablePredicateFilter(true);
        for (const auto& [key, value] : options) {
            builder.AddOption(key, value);
        }
        if (file_system != nullptr) {
            builder.WithFileSystem(file_system);
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                               TableRead::Create(std::move(context)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> batch_reader,
                               table_read->CreateReader(splits));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> result,
                               ReadResultCollector::CollectResult(std::move(batch_reader)));
        if (result == nullptr) {
            return std::vector<Row>();
        }

        auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(result->type());
        if (struct_type == nullptr) {
            return Status::Invalid("primary-key E2E read result is not a struct array");
        }
        int32_t pt_field = partitioned ? struct_type->GetFieldIndex("pt") : -1;
        int32_t id_field = struct_type->GetFieldIndex("id");
        int32_t score_field = struct_type->GetFieldIndex("score");
        int32_t tag_field = struct_type->GetFieldIndex("tag");
        if (id_field < 0 || score_field < 0 || tag_field < 0 || (partitioned && pt_field < 0)) {
            return Status::Invalid(fmt::format("unexpected primary-key E2E result type {}",
                                               result->type()->ToString()));
        }

        std::vector<Row> rows;
        for (const std::shared_ptr<arrow::Array>& chunk : result->chunks()) {
            auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(chunk);
            if (struct_array == nullptr) {
                return Status::Invalid("primary-key E2E result chunk is not a struct array");
            }
            auto id_array =
                std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->field(id_field));
            auto score_array =
                std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->field(score_field));
            auto tag_array =
                std::dynamic_pointer_cast<arrow::StringArray>(struct_array->field(tag_field));
            std::shared_ptr<arrow::Int32Array> pt_array;
            if (partitioned) {
                pt_array =
                    std::dynamic_pointer_cast<arrow::Int32Array>(struct_array->field(pt_field));
            }
            if (id_array == nullptr || score_array == nullptr || tag_array == nullptr ||
                (partitioned && pt_array == nullptr)) {
                return Status::Invalid("unexpected primary-key E2E result column type");
            }
            for (int64_t row = 0; row < struct_array->length(); row++) {
                if (id_array->IsNull(row) || score_array->IsNull(row) || tag_array->IsNull(row) ||
                    (partitioned && pt_array->IsNull(row))) {
                    return Status::Invalid("unexpected null in primary-key E2E result");
                }
                rows.emplace_back(partitioned ? pt_array->Value(row) : -1, id_array->Value(row),
                                  score_array->Value(row), tag_array->GetString(row));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    std::string format_;
};

TEST_P(PrimaryKeySortedIndexInteTest, HistoricalSnapshotsMixedFilesAndDisabledIndex) {
    std::shared_ptr<Predicate> predicate = ScoreEqual(/*partitioned=*/false, 0);
    const std::vector<int64_t> snapshot_ids = {2, 3, 5};
    for (int64_t snapshot_id : snapshot_ids) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<Plan> plan,
            Scan(/*partitioned=*/false, predicate, snapshot_id, /*index_enabled=*/true,
                 /*partition_filters=*/{}, /*branch=*/"", /*file_system=*/nullptr));
        ASSERT_EQ(snapshot_id, plan->SnapshotId());
        const auto [indexed_count, data_count] = CountSplitKinds(plan);
        ASSERT_GT(indexed_count, 0);
        if (snapshot_id == 3) {
            ASSERT_GT(data_count, 0);
        } else {
            ASSERT_EQ(0, data_count);
        }
        ASSERT_OK_AND_ASSIGN(std::vector<Row> rows,
                             Read(/*partitioned=*/false, predicate, plan->Splits(), /*options=*/{},
                                  /*file_system=*/nullptr));
        ASSERT_EQ(ExpectedScoreZeroRows(snapshot_id), rows);
    }

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Plan> disabled_plan,
        Scan(/*partitioned=*/false, predicate, /*snapshot_id=*/5, /*index_enabled=*/false,
             /*partition_filters=*/{}, /*branch=*/"", /*file_system=*/nullptr));
    const auto [disabled_indexed_count, disabled_data_count] = CountSplitKinds(disabled_plan);
    ASSERT_EQ(0, disabled_indexed_count);
    ASSERT_GT(disabled_data_count, 0);
    ASSERT_OK_AND_ASSIGN(
        std::vector<Row> disabled_rows,
        Read(/*partitioned=*/false, predicate, disabled_plan->Splits(), /*options=*/{},
             /*file_system=*/nullptr));
    ASSERT_EQ(ExpectedScoreZeroRows(/*snapshot_id=*/5), disabled_rows);
}

TEST_P(PrimaryKeySortedIndexInteTest, MultiSourceOrdinalsBecomeBoundedFileLocalRanges) {
    std::shared_ptr<Predicate> predicate = ScoreEqual(/*partitioned=*/false, 0);
    auto tracking_file_system = std::make_shared<TrackingLocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Plan> plan,
        Scan(/*partitioned=*/false, predicate, /*snapshot_id=*/2, /*index_enabled=*/true,
             /*partition_filters=*/{}, /*branch=*/"", tracking_file_system));

    std::set<std::string> source_files;
    int64_t selected_rows = 0;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split);
        ASSERT_TRUE(indexed_split != nullptr);
        auto inner_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
        ASSERT_TRUE(inner_split != nullptr);
        ASSERT_FALSE(inner_split->RawConvertible());
        ASSERT_EQ(1, inner_split->DataFiles().size());
        const std::shared_ptr<DataFileMeta>& file = inner_split->DataFiles()[0];
        source_files.insert(file->file_name);
        for (const Range& range : indexed_split->RowRanges()) {
            ASSERT_GE(range.from, 0);
            ASSERT_GE(range.to, range.from);
            ASSERT_LT(range.to, file->row_count);
            selected_rows += range.to - range.from + 1;
        }
    }
    ASSERT_GE(source_files.size(), 2);
    ASSERT_EQ(200, selected_rows);
    ASSERT_EQ(1, tracking_file_system->OpenedIndexPaths().size());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows,
                         Read(/*partitioned=*/false, predicate, plan->Splits(), /*options=*/{},
                              tracking_file_system));
    ASSERT_EQ(ExpectedScoreZeroRows(/*snapshot_id=*/2), rows);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> ranges_only_rows,
                         Read(/*partitioned=*/false, /*predicate=*/nullptr, plan->Splits(),
                              /*options=*/{}, tracking_file_system));
    ASSERT_EQ(ExpectedScoreZeroRows(/*snapshot_id=*/2), ranges_only_rows);
}

TEST_P(PrimaryKeySortedIndexInteTest, DeletionVectorResidualAndBooleanFallback) {
    std::shared_ptr<Predicate> score = ScoreEqual(/*partitioned=*/false, 0);
    std::shared_ptr<Predicate> id_upper_bound = IdLessOrEqual(/*partitioned=*/false, /*id=*/1500);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> and_predicate,
                         PredicateBuilder::And({score, id_upper_bound}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> and_plan,
                         Scan(/*partitioned=*/false, and_predicate, /*snapshot_id=*/4,
                              /*index_enabled=*/true, /*partition_filters=*/{}, /*branch=*/"",
                              /*file_system=*/nullptr));
    bool has_deletion_vector = false;
    for (const std::shared_ptr<Split>& split : and_plan->Splits()) {
        auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split);
        if (indexed_split == nullptr) {
            continue;
        }
        auto inner_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
        ASSERT_TRUE(inner_split != nullptr);
        for (const std::optional<DeletionFile>& deletion_file : inner_split->DeletionFiles()) {
            has_deletion_vector = has_deletion_vector || deletion_file.has_value();
        }
    }
    ASSERT_TRUE(has_deletion_vector);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> and_rows, Read(/*partitioned=*/false, and_predicate,
                                                         and_plan->Splits(), /*options=*/{},
                                                         /*file_system=*/nullptr));
    ASSERT_EQ(ExpectedResidualRowsAtSnapshot4(), and_rows);

    std::shared_ptr<Predicate> tag = TagEqual(/*partitioned=*/false, "keep");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> or_predicate,
                         PredicateBuilder::Or({score, tag}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> or_plan,
                         Scan(/*partitioned=*/false, or_predicate, /*snapshot_id=*/4,
                              /*index_enabled=*/true, /*partition_filters=*/{}, /*branch=*/"",
                              /*file_system=*/nullptr));
    const auto [or_indexed_count, or_data_count] = CountSplitKinds(or_plan);
    ASSERT_EQ(0, or_indexed_count);
    ASSERT_GT(or_data_count, 0);
    ASSERT_OK_AND_ASSIGN(std::vector<Row> or_rows, Read(/*partitioned=*/false, or_predicate,
                                                        or_plan->Splits(), /*options=*/{},
                                                        /*file_system=*/nullptr));
    ASSERT_EQ(ExpectedOrRowsAtSnapshot4(), or_rows);
}

TEST_P(PrimaryKeySortedIndexInteTest, PartitionsBucketsAndIndexPaths) {
    std::shared_ptr<Predicate> predicate = ScoreGreaterOrEqual(/*partitioned=*/true, /*score=*/0);
    auto tracking_file_system = std::make_shared<TrackingLocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Plan> plan,
        Scan(/*partitioned=*/true, predicate, /*snapshot_id=*/2, /*index_enabled=*/true,
             /*partition_filters=*/{}, /*branch=*/"", tracking_file_system));

    std::set<int32_t> partitions;
    std::set<int32_t> buckets;
    int64_t selected_rows = 0;
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(split);
        ASSERT_TRUE(indexed_split != nullptr);
        auto inner_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
        ASSERT_TRUE(inner_split != nullptr);
        ASSERT_EQ(1, inner_split->DataFiles().size());
        partitions.insert(inner_split->Partition().GetInt(0));
        buckets.insert(inner_split->Bucket());
        const int64_t row_count = inner_split->DataFiles()[0]->row_count;
        for (const Range& range : indexed_split->RowRanges()) {
            ASSERT_GE(range.from, 0);
            ASSERT_LT(range.to, row_count);
            selected_rows += range.to - range.from + 1;
        }
    }
    ASSERT_EQ((std::set<int32_t>{1, 2}), partitions);
    ASSERT_EQ((std::set<int32_t>{0, 1}), buckets);
    ASSERT_EQ(200, selected_rows);
    ASSERT_FALSE(tracking_file_system->OpenedIndexPaths().empty());

    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows,
                         Read(/*partitioned=*/true, predicate, plan->Splits(), /*options=*/{},
                              tracking_file_system));
    ASSERT_EQ(ExpectedPartitionRowsAtSnapshot2(), rows);
}

TEST_P(PrimaryKeySortedIndexInteTest, FallbackTableReadRoutesMainAndFallbackSplits) {
    std::shared_ptr<Predicate> predicate = ScoreEqual(/*partitioned=*/true, 0);
    const std::vector<std::map<std::string, std::string>> main_partition = {{{"pt", "1"}}};
    const std::vector<std::map<std::string, std::string>> fallback_partition = {{{"pt", "2"}}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Plan> main_plan,
        Scan(/*partitioned=*/true, predicate, /*snapshot_id=*/5, /*index_enabled=*/true,
             main_partition, /*branch=*/"", /*file_system=*/nullptr));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> fallback_plan,
                         Scan(/*partitioned=*/true, predicate, /*snapshot_id=*/std::nullopt,
                              /*index_enabled=*/false, fallback_partition, /*branch=*/"fallback",
                              /*file_system=*/nullptr));

    std::vector<std::shared_ptr<Split>> routed_splits;
    for (const std::shared_ptr<Split>& split : main_plan->Splits()) {
        ASSERT_TRUE(std::dynamic_pointer_cast<IndexedSplitImpl>(split) != nullptr);
        routed_splits.push_back(split);
    }
    for (const std::shared_ptr<Split>& split : fallback_plan->Splits()) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        ASSERT_TRUE(data_split != nullptr);
        routed_splits.push_back(
            std::make_shared<FallbackDataSplit>(data_split, /*is_fallback=*/true));
    }
    ASSERT_FALSE(main_plan->Splits().empty());
    ASSERT_FALSE(fallback_plan->Splits().empty());

    const std::map<std::string, std::string> read_options = {
        {Options::SCAN_FALLBACK_BRANCH, "fallback"}};
    ASSERT_OK_AND_ASSIGN(std::vector<Row> rows,
                         Read(/*partitioned=*/true, predicate, routed_splits, read_options,
                              /*file_system=*/nullptr));
    ASSERT_EQ(ExpectedFallbackRows(), rows);
}

TEST_P(PrimaryKeySortedIndexInteTest, ScoredSplitFailsBeforeForceKeepDeleteFallback) {
    std::shared_ptr<Predicate> predicate = ScoreEqual(/*partitioned=*/false, 0);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<Plan> plan,
        Scan(/*partitioned=*/false, predicate, /*snapshot_id=*/2, /*index_enabled=*/true,
             /*partition_filters=*/{}, /*branch=*/"", /*file_system=*/nullptr));
    ASSERT_FALSE(plan->Splits().empty());
    auto indexed_split = std::dynamic_pointer_cast<IndexedSplitImpl>(plan->Splits()[0]);
    ASSERT_TRUE(indexed_split != nullptr);
    auto inner_split = std::dynamic_pointer_cast<DataSplitImpl>(indexed_split->GetDataSplit());
    ASSERT_TRUE(inner_split != nullptr);
    ASSERT_FALSE(indexed_split->RowRanges().empty());
    const Range& first_range = indexed_split->RowRanges()[0];
    auto scored_split = std::make_shared<IndexedSplitImpl>(
        inner_split, std::vector<Range>{Range(first_range.from, first_range.from)},
        std::vector<float>{0.5F});

    ReadContextBuilder builder(TablePath(/*partitioned=*/false));
    builder.SetReadFieldNames({"id", "score", "tag"})
        .SetPredicate(predicate)
        .EnablePredicateFilter(true);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReadContext> context, builder.Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableRead> table_read,
                         TableRead::Create(std::move(context)));
    auto* key_value_table_read = dynamic_cast<KeyValueTableRead*>(table_read.get());
    ASSERT_TRUE(key_value_table_read != nullptr);
    key_value_table_read->ForceKeepDelete(true);
    ASSERT_NOK_WITH_MSG(key_value_table_read->CreateReader(scored_split),
                        "Primary-key reads do not support scored indexed splits yet");
}

std::vector<std::string> PrimaryKeySortedIndexFormats() {
    std::vector<std::string> formats = {"parquet"};
#ifdef PAIMON_ENABLE_ORC
    formats.emplace_back("orc");
#endif
    return formats;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, PrimaryKeySortedIndexInteTest,
                         ::testing::ValuesIn(PrimaryKeySortedIndexFormats()));

}  // namespace paimon::test
