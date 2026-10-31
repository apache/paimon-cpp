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

#include "paimon/core/table/source/data_evolution_full_text_read.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/index_full_text_search_split_impl.h"
#include "paimon/defs.h"
#include "paimon/factories/factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/table/source/full_text_search_builder.h"
#include "paimon/testing/mock/mock_global_index_reader.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {
namespace {
constexpr char kFakeIndexType[] = "fake-full-text";
constexpr char kQuery[] = R"({"match":{"query":"paimon"}})";

struct RecordedSearch {
    std::string file_name;
    std::vector<std::string> schema_fields;
    std::string field_name;
    std::string query;
    int32_t limit;
    std::optional<std::vector<int64_t>> include_row_ids;
};

class FakeIndexState {
 public:
    static FakeIndexState& Instance() {
        static FakeIndexState state;
        return state;
    }

    void Reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        scores_.clear();
        searches_.clear();
    }

    void SetScores(const std::string& file_name, std::vector<float> scores) {
        std::lock_guard<std::mutex> lock(mutex_);
        scores_[file_name] = std::move(scores);
    }

    Result<std::vector<float>> Scores(const std::string& file_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto iter = scores_.find(file_name);
        if (iter == scores_.end()) {
            return Status::Invalid("no scores for fake index file " + file_name);
        }
        return iter->second;
    }

    void Record(RecordedSearch search) {
        std::lock_guard<std::mutex> lock(mutex_);
        searches_[search.file_name] = std::move(search);
    }

    std::map<std::string, RecordedSearch> Searches() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return searches_;
    }

 private:
    mutable std::mutex mutex_;
    std::map<std::string, std::vector<float>> scores_;
    std::map<std::string, RecordedSearch> searches_;
};

class FakeFullTextReader : public MockGlobalIndexReader {
 public:
    FakeFullTextReader(const std::string& file_name, std::vector<std::string> schema_fields,
                       std::vector<float> scores)
        : file_name_(file_name),
          schema_fields_(std::move(schema_fields)),
          scores_(std::move(scores)) {}

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search) override {
        RecordedSearch recorded{file_name_,    schema_fields_, search->field_name,
                                search->query, search->limit,  std::nullopt};
        if (search->include_row_ids) {
            std::vector<int64_t> row_ids;
            for (auto iter = search->include_row_ids->Begin();
                 iter != search->include_row_ids->End(); ++iter) {
                row_ids.push_back(*iter);
            }
            recorded.include_row_ids = std::move(row_ids);
        }
        FakeIndexState::Instance().Record(std::move(recorded));

        std::vector<std::pair<int64_t, float>> rows;
        for (int64_t row = 0; row < static_cast<int64_t>(scores_.size()); ++row) {
            if (!search->include_row_ids || search->include_row_ids->Contains(row)) {
                rows.emplace_back(row, scores_[row]);
            }
        }
        std::sort(rows.begin(), rows.end(), [](const auto& left, const auto& right) {
            return left.second != right.second ? left.second > right.second
                                               : left.first < right.first;
        });
        rows.resize(std::min(rows.size(), static_cast<size_t>(search->limit)));
        std::sort(rows.begin(), rows.end());
        RoaringBitmap64 bitmap;
        std::vector<float> scores;
        for (const auto& [row, score] : rows) {
            bitmap.Add(row);
            scores.push_back(score);
        }
        return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap),
                                                               std::move(scores));
    }

 private:
    std::string file_name_;
    std::vector<std::string> schema_fields_;
    std::vector<float> scores_;
};

class FakeFullTextIndexer : public GlobalIndexer {
 public:
    Result<std::optional<std::vector<std::string>>> GetExtraFieldNames() const override {
        return std::optional<std::vector<std::string>>();
    }

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override {
        return Status::NotImplemented("fake full-text index cannot be written");
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files, int64_t total_row_count,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<MemoryPool>& pool) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                          arrow::ImportSchema(arrow_schema));
        if (files.size() != 1) {
            return Status::Invalid("fake full-text index expects one file per shard");
        }
        std::string file_name = PathUtil::GetName(files[0].file_path);
        PAIMON_ASSIGN_OR_RAISE(std::vector<float> scores,
                               FakeIndexState::Instance().Scores(file_name));
        if (static_cast<int64_t>(scores.size()) != total_row_count) {
            return Status::Invalid("fake full-text index expects a score for each row");
        }
        return std::make_shared<FakeFullTextReader>(file_name, schema->field_names(),
                                                    std::move(scores));
    }

    bool SupportsFullTextSearch() const override {
        return true;
    }
};

class FakeFullTextIndexerFactory : public GlobalIndexerFactory {
 public:
    const char* Identifier() const override {
        return "fake-full-text-global";
    }

    Result<std::unique_ptr<GlobalIndexer>> Create(
        const std::map<std::string, std::string>& options) const override {
        return std::make_unique<FakeFullTextIndexer>();
    }
};
REGISTER_PAIMON_FACTORY(FakeFullTextIndexerFactory);

using ScoredRows = std::vector<std::pair<int64_t, float>>;

ScoredRows ToRows(const ScoredGlobalIndexResult& result) {
    ScoredRows rows;
    auto iter = result.CreateScoredIterator().value();
    while (iter->HasNext()) {
        rows.push_back(iter->NextWithScore());
    }
    return rows;
}
}  // namespace

class DataEvolutionFullTextReadTest : public ::testing::Test {
 protected:
    void SetUp() override {
        FakeIndexState::Instance().Reset();
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(dir_);
        auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                     arrow::field("content", arrow::utf8()),
                                     arrow::field("title", arrow::utf8())});
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog, Catalog::Create(dir_->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
        ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema,
                                       /*partition_keys=*/{}, /*primary_keys=*/{},
                                       {{Options::FILE_FORMAT, "parquet"},
                                        {Options::FILE_SYSTEM, "local"},
                                        {Options::ROW_TRACKING_ENABLED, "true"},
                                        {Options::DATA_EVOLUTION_ENABLED, "true"}},
                                       /*ignore_if_exists=*/false));
        table_path_ = PathUtil::JoinPath(dir_->Str(), "foo.db/bar");
    }

    std::shared_ptr<FullTextSearchSplit> NewSplit(
        const std::string& file_name, const Range& range, const std::vector<float>& scores,
        const std::vector<Range>& search_row_ranges, int32_t index_field_id = 1,
        const std::optional<std::vector<int32_t>>& extra_field_ids = std::nullopt,
        int64_t snapshot_id = 1, const std::string& index_type = kFakeIndexType) const {
        FakeIndexState::Instance().SetScores(file_name, scores);
        GlobalIndexMeta meta(range.from, range.to, index_field_id, extra_field_ids,
                             /*index_meta=*/nullptr);
        auto file = std::make_shared<IndexFileMeta>(
            index_type, file_name, /*file_size=*/128, /*row_count=*/range.Count(),
            /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt, meta);
        return IndexFullTextSearchSplitImpl::Create(snapshot_id, "content", range.from, range.to,
                                                    {file}, search_row_ranges)
            .value();
    }

    std::unique_ptr<FullTextSearchBuilder> NewBuilder(int32_t limit) const {
        auto builder = std::make_unique<FullTextSearchBuilder>(table_path_);
        builder->SetQuery("content", kQuery).SetLimit(limit);
        return builder;
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::string table_path_;
};

TEST_F(DataEvolutionFullTextReadTest, TestMergesShardResultsIntoGlobalTopK) {
    std::vector<std::shared_ptr<FullTextSearchSplit>> splits = {
        NewSplit("a.index", Range(0, 4), {0.1f, 0.9f, 0.5f, 0.3f, 0.7f}, {Range(0, 4)}),
        NewSplit("b.index", Range(5, 9), {0.8f, 0.2f, 0.6f, 0.4f, 1.0f}, {Range(5, 9)})};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FullTextRead> read, NewBuilder(3)->NewFullTextRead());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result, read->Read(splits));
    ASSERT_EQ(ToRows(*result), ScoredRows({{1, 0.9f}, {5, 0.8f}, {9, 1.0f}}));

    std::map<std::string, RecordedSearch> searches = FakeIndexState::Instance().Searches();
    ASSERT_EQ(searches.size(), 2u);
    for (const auto& [file_name, search] : searches) {
        ASSERT_EQ(search.limit, 3) << file_name;
        ASSERT_EQ(search.field_name, "content") << file_name;
        ASSERT_EQ(search.query, kQuery) << file_name;
        ASSERT_EQ(search.schema_fields, std::vector<std::string>({"content"})) << file_name;
        ASSERT_FALSE(search.include_row_ids) << file_name;
    }

    FullTextScan::Plan plan(/*snapshot_id=*/1, splits);
    ASSERT_OK_AND_ASSIGN(read, NewBuilder(100)->NewFullTextRead());
    ASSERT_OK_AND_ASSIGN(result, read->Read(plan));
    ASSERT_EQ(ToRows(*result).size(), 10u);
}

TEST_F(DataEvolutionFullTextReadTest, TestRanksOnlySearchRowRanges) {
    std::vector<std::shared_ptr<FullTextSearchSplit>> splits = {
        NewSplit("a.index", Range(0, 4), {0.1f, 0.9f, 0.5f, 0.3f, 0.7f},
                 {Range(0, 0), Range(3, 4)}),
        NewSplit("b.index", Range(10, 14), {0.8f, 0.2f, 0.6f, 0.4f, 1.0f}, {Range(12, 14)})};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FullTextRead> read, NewBuilder(3)->NewFullTextRead());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result, read->Read(splits));
    ASSERT_EQ(ToRows(*result), ScoredRows({{4, 0.7f}, {12, 0.6f}, {14, 1.0f}}));

    std::map<std::string, RecordedSearch> searches = FakeIndexState::Instance().Searches();
    ASSERT_EQ(searches.size(), 2u);
    ASSERT_EQ(searches["a.index"].include_row_ids, std::vector<int64_t>({0, 3, 4}));
    ASSERT_EQ(searches["b.index"].include_row_ids, std::vector<int64_t>({2, 3, 4}));
}

TEST_F(DataEvolutionFullTextReadTest, TestIncludeRowIds) {
    auto row_ids_of = [](const std::optional<RoaringBitmap64>& bitmap) {
        std::vector<int64_t> row_ids;
        for (auto iter = bitmap->Begin(); iter != bitmap->End(); ++iter) {
            row_ids.push_back(*iter);
        }
        return row_ids;
    };
    std::vector<float> scores(10, 0.1f);
    auto split = std::dynamic_pointer_cast<IndexFullTextSearchSplit>(
        NewSplit("a.index", Range(10, 19), scores, {Range(10, 12), Range(17, 19)}));
    ASSERT_TRUE(split);
    std::optional<RoaringBitmap64> row_ids =
        DataEvolutionFullTextRead::IncludeRowIds(*split, /*live_rows=*/nullptr);
    ASSERT_TRUE(row_ids);
    ASSERT_EQ(row_ids_of(row_ids), std::vector<int64_t>({10, 11, 12, 17, 18, 19}));
    RoaringBitmap64 live_rows = RoaringBitmap64::From({0, 11, 12, 15, 18, 25});
    row_ids = DataEvolutionFullTextRead::IncludeRowIds(*split, &live_rows);
    ASSERT_TRUE(row_ids);
    ASSERT_EQ(row_ids_of(row_ids), std::vector<int64_t>({11, 12, 18}));
    RoaringBitmap64 no_live_rows_in_split = RoaringBitmap64::From({0, 15, 16});
    row_ids = DataEvolutionFullTextRead::IncludeRowIds(*split, &no_live_rows_in_split);
    ASSERT_TRUE(row_ids);
    ASSERT_TRUE(row_ids->IsEmpty());

    auto whole_split = std::dynamic_pointer_cast<IndexFullTextSearchSplit>(
        NewSplit("b.index", Range(10, 19), scores, {Range(10, 19)}));
    ASSERT_TRUE(whole_split);
    ASSERT_FALSE(DataEvolutionFullTextRead::IncludeRowIds(*whole_split, /*live_rows=*/nullptr));
    RoaringBitmap64 all_rows;
    all_rows.AddRange(0, 30);
    ASSERT_FALSE(DataEvolutionFullTextRead::IncludeRowIds(*whole_split, &all_rows));
    ASSERT_TRUE(DataEvolutionFullTextRead::IncludeRowIds(*whole_split, &live_rows));
    RoaringBitmap64 empty_live_rows;
    row_ids = DataEvolutionFullTextRead::IncludeRowIds(*whole_split, &empty_live_rows);
    ASSERT_TRUE(row_ids);
    ASSERT_TRUE(row_ids->IsEmpty());

    const int64_t max_row_id = std::numeric_limits<int64_t>::max();
    auto widest_split = std::dynamic_pointer_cast<IndexFullTextSearchSplit>(
        NewSplit("widest.index", Range(1, max_row_id), /*scores=*/{}, {Range(1, max_row_id)}));
    ASSERT_TRUE(widest_split);
    ASSERT_FALSE(DataEvolutionFullTextRead::IncludeRowIds(*widest_split, /*live_rows=*/nullptr));
    RoaringBitmap64 sparse_live_rows = RoaringBitmap64::From({0, 5, 20, max_row_id});
    row_ids = DataEvolutionFullTextRead::IncludeRowIds(*widest_split, &sparse_live_rows);
    ASSERT_TRUE(row_ids);
    ASSERT_EQ(row_ids_of(row_ids), std::vector<int64_t>({5, 20, max_row_id}));
    auto wide_split = std::dynamic_pointer_cast<IndexFullTextSearchSplit>(
        NewSplit("wide.index", Range(1, max_row_id), /*scores=*/{},
                 {Range(1, 10), Range(max_row_id - 10, max_row_id)}));
    ASSERT_TRUE(wide_split);
    row_ids = DataEvolutionFullTextRead::IncludeRowIds(*wide_split, &sparse_live_rows);
    ASSERT_TRUE(row_ids);
    ASSERT_EQ(row_ids_of(row_ids), std::vector<int64_t>({5, max_row_id}));
}

TEST_F(DataEvolutionFullTextReadTest, TestReadsEachSplitWithItsOwnIndexFields) {
    std::vector<std::shared_ptr<FullTextSearchSplit>> splits = {
        NewSplit("dedicated.index", Range(100, 102), {0.1f, 0.5f, 0.2f}, {Range(100, 102)}),
        NewSplit("multi.index", Range(200, 202), {0.3f, 0.4f, 0.6f}, {Range(200, 202)},
                 /*index_field_id=*/2, std::vector<int32_t>({1}))};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FullTextRead> read, NewBuilder(3)->NewFullTextRead());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result, read->Read(splits));
    ASSERT_EQ(ToRows(*result), ScoredRows({{101, 0.5f}, {201, 0.4f}, {202, 0.6f}}));
    std::map<std::string, RecordedSearch> searches = FakeIndexState::Instance().Searches();
    ASSERT_EQ(searches.size(), 2u);
    ASSERT_EQ(searches["dedicated.index"].schema_fields, std::vector<std::string>({"content"}));
    ASSERT_EQ(searches["multi.index"].schema_fields,
              std::vector<std::string>({"title", "content"}));
    for (const auto& [file_name, search] : searches) {
        ASSERT_EQ(search.field_name, "content") << file_name;
        ASSERT_FALSE(search.include_row_ids) << file_name;
    }
}

TEST_F(DataEvolutionFullTextReadTest, TestRejectsInvalidSplits) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FullTextRead> read, NewBuilder(3)->NewFullTextRead());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> empty, read->Read({}));
    ASSERT_TRUE(ToRows(*empty).empty());

    auto split = NewSplit("a.index", Range(0, 1), {0.1f, 0.2f}, {Range(0, 1)});
    auto other_snapshot = NewSplit("b.index", Range(2, 3), {0.1f, 0.2f}, {Range(2, 3)},
                                   /*index_field_id=*/1, std::nullopt, /*snapshot_id=*/2);
    ASSERT_NOK_WITH_MSG(read->Read({split, other_snapshot}),
                        "planned from snapshots 1 and 2 cannot be read together");
    ASSERT_NOK_WITH_MSG(read->Read({nullptr}), "cannot cast to an index split");

    FullTextSearchBuilder title_builder(table_path_);
    title_builder.SetQuery("title", kQuery).SetLimit(3);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FullTextRead> title_read, title_builder.NewFullTextRead());
    ASSERT_NOK_WITH_MSG(title_read->Read({split}),
                        "a split of column content cannot be read by a full-text search of "
                        "column title");

    auto unknown_type = NewSplit("c.index", Range(0, 1), {0.1f, 0.2f}, {Range(0, 1)},
                                 /*index_field_id=*/1, std::nullopt, /*snapshot_id=*/1,
                                 /*index_type=*/"no-such-index");
    ASSERT_NOK_WITH_MSG(read->Read({unknown_type}),
                        "no global indexer is registered for index type no-such-index; while "
                        "searching the split of column content in snapshot 1 over rows [0, 1] "
                        "with no-such-index index files [c.index]");
}

TEST_F(DataEvolutionFullTextReadTest, TestRejectsSplitOfReplacedColumn) {
    std::vector<std::shared_ptr<FullTextSearchSplit>> splits = {
        NewSplit("a.index", Range(0, 1), {0.1f, 0.2f}, {Range(0, 1)}),
        NewSplit("multi.index", Range(2, 3), {0.1f, 0.2f}, {Range(2, 3)},
                 /*index_field_id=*/2, std::vector<int32_t>({1}))};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FullTextRead> read, NewBuilder(3)->NewFullTextRead());
    ASSERT_OK(read->Read(splits));

    SchemaManager schema_manager(dir_->GetFileSystem(), table_path_);
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                         schema_manager.Latest());
    ASSERT_TRUE(latest_schema);
    const std::shared_ptr<TableSchema>& old_schema = latest_schema.value();
    auto replaced_content = arrow::schema(
        {arrow::field("id", arrow::int32()), arrow::field("old_content", arrow::utf8()),
         arrow::field("title", arrow::utf8()), arrow::field("content", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> new_schema,
        TableSchema::Create(old_schema->Id() + 1, replaced_content, old_schema->PartitionKeys(),
                            old_schema->PrimaryKeys(), old_schema->Options()));
    ASSERT_OK_AND_ASSIGN(std::string new_schema_json, new_schema->ToJsonString());
    ASSERT_OK(dir_->GetFileSystem()->AtomicStore(
        PathUtil::JoinPath(schema_manager.SchemaDirectory(),
                           "schema-" + std::to_string(new_schema->Id())),
        new_schema_json));

    ASSERT_OK_AND_ASSIGN(read, NewBuilder(3)->NewFullTextRead());
    for (const auto& split : splits) {
        ASSERT_NOK_WITH_MSG(read->Read({split}),
                            "a split of column content does not index its field id 3");
    }
}

TEST_F(DataEvolutionFullTextReadTest, TestBuilderValidatesSettings) {
    FullTextSearchBuilder no_query(table_path_);
    no_query.SetLimit(3);
    ASSERT_NOK_WITH_MSG(no_query.NewFullTextScan(), "Query must be set via SetQuery()");
    ASSERT_NOK_WITH_MSG(no_query.NewFullTextRead(), "Query must be set via SetQuery()");

    FullTextSearchBuilder no_limit(table_path_);
    no_limit.SetQuery("content", kQuery);
    ASSERT_OK(no_limit.NewFullTextScan());
    ASSERT_NOK_WITH_MSG(no_limit.NewFullTextRead(), "Limit must be positive");
    ASSERT_NOK_WITH_MSG(NewBuilder(0)->NewFullTextRead(), "Limit must be positive");
    ASSERT_NOK_WITH_MSG(NewBuilder(0)->ExecuteLocal(), "Limit must be positive");

    std::unique_ptr<FullTextSearchBuilder> no_pool = NewBuilder(3);
    no_pool->WithMemoryPool(nullptr);
    ASSERT_NOK_WITH_MSG(no_pool->NewFullTextScan(), "memory pool is null pointer");

    std::unique_ptr<FullTextSearchBuilder> full_mode = NewBuilder(3);
    full_mode->AddOption(Options::GLOBAL_INDEX_SEARCH_MODE, "full");
    ASSERT_NOK_WITH_MSG(full_mode->NewFullTextScan(), "full and detail are not supported yet");
    ASSERT_NOK_WITH_MSG(full_mode->NewFullTextRead(), "full and detail are not supported yet");

    std::unique_ptr<FullTextSearchBuilder> no_thread = NewBuilder(3);
    no_thread->AddOption(Options::GLOBAL_INDEX_THREAD_NUM, "0");
    ASSERT_NOK_WITH_MSG(no_thread->NewFullTextRead(), "invalid global index thread number 0");

    FullTextSearchBuilder missing_column(table_path_);
    missing_column.SetQuery("missing", kQuery).SetLimit(3);
    ASSERT_NOK_WITH_MSG(missing_column.NewFullTextScan(), "Text column 'missing' does not exist.");

    FullTextSearchBuilder missing_table(PathUtil::JoinPath(dir_->Str(), "foo.db/missing"));
    missing_table.SetQuery("content", kQuery).SetLimit(3);
    ASSERT_NOK_WITH_MSG(missing_table.NewFullTextScan(), "schema file not found");
}

TEST_F(DataEvolutionFullTextReadTest, TestBuilderRejectsPrimaryKeyFullTextIndex) {
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), /*nullable=*/false),
                                 arrow::field("content", arrow::utf8())});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Catalog> catalog, Catalog::Create(dir_->Str(), {}));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "pk"), &c_schema, /*partition_keys=*/{},
                                   /*primary_keys=*/{"id"},
                                   {{Options::FILE_FORMAT, "parquet"},
                                    {Options::FILE_SYSTEM, "local"},
                                    {Options::BUCKET, "1"},
                                    {Options::DELETION_VECTORS_ENABLED, "true"},
                                    {Options::PK_FULL_TEXT_INDEX_COLUMNS, "content"}},
                                   /*ignore_if_exists=*/false));
    FullTextSearchBuilder builder(PathUtil::JoinPath(dir_->Str(), "foo.db/pk"));
    builder.SetQuery("content", kQuery).SetLimit(3);
    ASSERT_NOK_WITH_MSG(builder.NewFullTextScan(),
                        "with a primary-key full-text index is not supported yet");
    ASSERT_NOK_WITH_MSG(builder.NewFullTextRead(),
                        "with a primary-key full-text index is not supported yet");
}

TEST_F(DataEvolutionFullTextReadTest, TestResolvesPlanSnapshot) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FullTextScan> scan, NewBuilder(3)->NewFullTextScan());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FullTextScan::Plan> plan, scan->CreatePlan());
    ASSERT_FALSE(plan->SnapshotId());
    ASSERT_TRUE(plan->Splits().empty());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                         NewBuilder(3)->ExecuteLocal());
    ASSERT_TRUE(ToRows(*result).empty());

    std::vector<std::tuple<std::string, std::string, std::string>> time_travel_errors = {
        {"", Options::SCAN_SNAPSHOT_ID, "snapshot-1"},
        {"", Options::SCAN_TAG_NAME, "Tag '1' doesn't exist."},
        {"", Options::SCAN_TIMESTAMP,
         "There is currently no snapshot earlier than or equal to timestamp"},
        {"", Options::SCAN_TIMESTAMP_MILLIS,
         "There is currently no snapshot earlier than or equal to timestamp [1]"},
        {"from-snapshot", Options::SCAN_TAG_NAME, "Tag '1' doesn't exist."},
        {"from-snapshot-full", Options::SCAN_SNAPSHOT_ID, "snapshot-1"},
        {"from-timestamp", Options::SCAN_TIMESTAMP_MILLIS,
         "There is currently no snapshot earlier than or equal to timestamp [1]"},
        {"latest-full", Options::SCAN_SNAPSHOT_ID,
         "scan.snapshot-id must be null when you use latest-full for scan.mode"}};
    for (const auto& [mode, key, error] : time_travel_errors) {
        SCOPED_TRACE(mode + " " + key);
        std::unique_ptr<FullTextSearchBuilder> builder = NewBuilder(3);
        if (!mode.empty()) {
            builder->AddOption(Options::SCAN_MODE, mode);
        }
        builder->AddOption(key, key == Options::SCAN_TIMESTAMP ? "2023-06-01 00:00:00" : "1");
        ASSERT_OK_AND_ASSIGN(scan, builder->NewFullTextScan());
        ASSERT_NOK_WITH_MSG(scan->CreatePlan(), error);
    }
}

}  // namespace paimon::test
