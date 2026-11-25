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

#include "paimon/table/source/hybrid_search_builder.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <limits>
#include <mutex>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/table.h"
#include "paimon/commit_context.h"
#include "paimon/common/global_index/scored_global_index_result_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/factories/factory.h"
#include "paimon/file_store_commit.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/table/source/vector_search_builder.h"
#include "paimon/testing/mock/mock_global_index_reader.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
using Rows = std::vector<std::pair<int64_t, float>>;
const std::map<std::string, std::string> kAscending = {{"order", "ascending"}};
constexpr char kRequireConcurrentShards[] = "test.require-concurrent-vector-shards";

Rows ToRows(const ScoredGlobalIndexResult& result) {
    auto iter = result.CreateScoredIterator().value();
    Rows rows;
    while (iter->HasNext()) {
        rows.push_back(iter->NextWithScore());
    }
    return rows;
}

class VectorSearchBarrier {
 public:
    Status WaitForBothShards() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++started_;
        ready_.notify_all();
        if (!ready_.wait_for(lock, std::chrono::seconds(5), [this] { return started_ >= 2; })) {
            return Status::Invalid("Expected two concurrent vector shard searches");
        }
        return Status::OK();
    }

 private:
    std::mutex mutex_;
    std::condition_variable ready_;
    int32_t started_ = 0;
};

class TestRouteReader : public MockGlobalIndexReader {
 public:
    TestRouteReader(const std::shared_ptr<Bytes>& values,
                    const std::shared_ptr<VectorSearchBarrier>& barrier)
        : values_(values), barrier_(barrier) {
        std::vector<int64_t> matches;
        for (size_t i = 0; i < values_->size(); ++i) {
            if ((*values_)[i]) {
                matches.push_back(i);
            }
        }
        SetDefaultResult(matches);
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& search) override {
        if (search->field_name != "vec" || search->query.size() != 1) {
            return Status::Invalid("Unexpected vector query");
        }
        if (barrier_) {
            PAIMON_RETURN_NOT_OK(barrier_->WaitForBothShards());
        }
        float scale = search->query.front();
        if (search->options == kAscending) {
            scale = -scale;
        }
        return Hits(search->limit, scale, search->pre_filter);
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search) override {
        if (search->field_name != "content" || search->query != "lake") {
            return Status::Invalid("Unexpected full-text query");
        }
        return Hits(search->limit, 1, [&](int64_t id) {
            return !search->include_row_ids || search->include_row_ids->Contains(id);
        });
    }

 private:
    Result<std::shared_ptr<ScoredGlobalIndexResult>> Hits(
        int32_t limit, float scale, const VectorSearch::PreFilter& include) const {
        Rows rows;
        for (int64_t i = 0; i < static_cast<int64_t>(values_->size()); ++i) {
            if (!include || include(i)) {
                rows.emplace_back(i, scale * (*values_)[i]);
            }
        }
        return ScoredGlobalIndexResultUtils::FromRows(std::move(rows), limit);
    }
    std::shared_ptr<Bytes> values_;
    std::shared_ptr<VectorSearchBarrier> barrier_;
};

class TestRouteIndexer : public GlobalIndexer {
 public:
    explicit TestRouteIndexer(bool scored, std::shared_ptr<VectorSearchBarrier> barrier = nullptr)
        : scored_(scored), barrier_(std::move(barrier)) {}
    bool SupportsFullTextSearch() const override {
        return scored_;
    }
    bool SupportsVectorSearch() const override {
        return scored_;
    }

    Result<std::optional<std::vector<std::string>>> GetExtraFieldNames() const override {
        return std::optional<std::vector<std::string>>();
    }
    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string&, ::ArrowSchema*, const std::shared_ptr<GlobalIndexFileWriter>&,
        const std::shared_ptr<MemoryPool>&) const override {
        return Status::NotImplemented("test uses index metadata");
    }
    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema*, const std::shared_ptr<GlobalIndexFileReader>&,
        const std::vector<GlobalIndexIOMeta>& files, int64_t count,
        const std::optional<std::vector<Range>>&,
        const std::shared_ptr<MemoryPool>&) const override {
        if (files.size() == 1 && !files[0].metadata) {
            return std::make_shared<MockGlobalIndexReader>();
        }
        if (files.size() != 1 || static_cast<int64_t>(files[0].metadata->size()) != count) {
            return Status::Invalid("Expected one index with a byte for every row");
        }
        return std::make_shared<TestRouteReader>(files[0].metadata, barrier_);
    }

 private:
    bool scored_;
    std::shared_ptr<VectorSearchBarrier> barrier_;
};

class TestRouteFactory : public GlobalIndexerFactory {
 public:
    const char* Identifier() const override {
        return "hybrid-test-global";
    }
    Result<std::unique_ptr<GlobalIndexer>> Create(
        const std::map<std::string, std::string>& options) const override {
        auto barrier = options.count(kRequireConcurrentShards)
                           ? std::make_shared<VectorSearchBarrier>()
                           : nullptr;
        return std::make_unique<TestRouteIndexer>(true, std::move(barrier));
    }
};
REGISTER_PAIMON_FACTORY(TestRouteFactory);

class TestRouteFilterFactory : public GlobalIndexerFactory {
 public:
    const char* Identifier() const override {
        return "hybrid-filter-test-global";
    }
    Result<std::unique_ptr<GlobalIndexer>> Create(
        const std::map<std::string, std::string>&) const override {
        return std::make_unique<TestRouteIndexer>(false);
    }
};
REGISTER_PAIMON_FACTORY(TestRouteFilterFactory);

class CommitOnManifestOpen : public LocalFileSystem {
 public:
    CommitOnManifestOpen(std::string manifest, std::function<Status()> commit)
        : manifest_(std::move(manifest)), commit_(std::move(commit)) {}
    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        if (PathUtil::GetName(path) == manifest_ && !committed_.exchange(true)) {
            PAIMON_RETURN_NOT_OK(commit_());
        }
        return LocalFileSystem::Open(path);
    }

 private:
    std::string manifest_;
    std::function<Status()> commit_;
    mutable std::atomic<bool> committed_{false};
};

class HybridSearchBuilderTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(dir_);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Catalog> catalog, Catalog::Create(dir_->Str(), {}));
        ASSERT_OK(catalog->CreateDatabase("db", {}, false));
        auto schema = arrow::schema(
            {arrow::field("p", arrow::int32()), arrow::field("vec", arrow::list(arrow::float32())),
             arrow::field("content", arrow::utf8()), arrow::field("id", arrow::int32())});
        row_type_ = arrow::struct_(schema->fields());
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        Identifier name("db", "hybrid");
        ASSERT_OK(catalog->CreateTable(name, &c_schema, {"p"}, {},
                                       {{Options::FILE_FORMAT, "parquet"},
                                        {Options::DATA_EVOLUTION_ENABLED, "true"},
                                        {Options::ROW_TRACKING_ENABLED, "true"},
                                        {Options::GLOBAL_INDEX_THREAD_NUM, "2"}},
                                       false));
        ASSERT_OK_AND_ASSIGN(table_, catalog->GetTable(name));
        path_ = table_->GetRootPath();
    }

    Status WriteRows(int32_t partition, const std::string& rows) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TestHelper> writer,
                               TestHelper::Create(path_, {}, false));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RecordBatch> batch,
                               TestHelper::MakeRecordBatch(
                                   row_type_, rows, {{"p", std::to_string(partition)}}, 0, {}));
        return writer->WriteAndCommit(std::move(batch), partition, std::nullopt).status();
    }

    Status CommitIndexes(int32_t partition, int64_t start, const std::string& vectors,
                         const std::string& text, const std::string& matches,
                         std::vector<std::shared_ptr<IndexFileMeta>> files = {}) const {
        for (const auto& [field, values] :
             std::map<int32_t, std::string>{{1, vectors}, {2, text}, {3, matches}}) {
            if (values.empty()) {
                continue;
            }
            int64_t count = values.size();
            GlobalIndexMeta meta(start, start + count - 1, field, std::nullopt,
                                 std::make_shared<Bytes>(values, GetDefaultPool().get()));
            files.push_back(
                std::make_shared<IndexFileMeta>(field == 3 ? "hybrid-filter-test" : "hybrid-test",
                                                fmt::format("shard-{}-{}.index", field, start),
                                                count, count, std::nullopt, std::nullopt, meta));
        }
        BinaryRow part = BinaryRowGenerator::GenerateRow({partition}, GetDefaultPool().get());
        auto message = std::make_shared<CommitMessageImpl>(
            part, 0, std::nullopt, DataIncrement(std::move(files)), CompactIncrement({}, {}, {}));
        CommitContextBuilder builder(path_, "hybrid-test");
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                               FileStoreCommit::Create(std::move(context)));
        return commit->Commit({message});
    }

    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<arrow::DataType> row_type_;
    std::shared_ptr<Table> table_;
    std::string path_;
};

TEST_F(HybridSearchBuilderTest, FiltersOptionsAndLimitsReachBothRoutes) {
    ASSERT_OK(
        WriteRows(1, R"([[1,[9],"lake",0],[1,[2],"lake",42],[1,[6],"lake",42],[1,[7],"lake",0]])"));
    ASSERT_OK(WriteRows(2, R"([[2,[7],"lake",42],[2,[1],"lake",42]])"));
    ASSERT_OK(CommitIndexes(1, 0, {9, 2, 6, 7}, {9, 6, 8, 4}, {0, 1, 1, 0}));
    ASSERT_OK(CommitIndexes(2, 4, {7, 1}, {12, 15}, {1, 1}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HybridSearchBuilder> search,
                         table_->NewHybridSearchBuilder());
    search->WithPartitionFilter({{{"p", "1"}}, {{"p", "2"}}})
        .WithPartitionFilter({{{"p", "1"}}})
        .WithPartitionFilter({})
        .WithFilter(PredicateBuilder::Equal(3, "id", FieldType::INT, Literal(42)))
        .AddVectorRoute("vec", {1}, 1, 2, kAscending)
        .AddFullTextRoute("content", "lake", 1)
        .WithWeightedScoreRanker()
        .SetLimit(2);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> hits, search->ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{1, 2}, {2, 1}}));
    auto partition_filter = PredicateBuilder::GreaterThan(0, "p", FieldType::INT, Literal(0));
    ASSERT_OK_AND_ASSIGN(hits, search->WithFilter(partition_filter).ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{1, 2}, {2, 1}}));
    search->SetLimit(1).WithRanker(" MRR ");
    ASSERT_OK_AND_ASSIGN(hits, search->ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{1, 2}}));
    ASSERT_OK_AND_ASSIGN(
        hits, search->WithFilter(PredicateBuilder::Equal(0, "p", FieldType::INT, Literal(2)))
                  .ExecuteLocal());
    ASSERT_TRUE(ToRows(*hits).empty());
}

TEST_F(HybridSearchBuilderTest, SkipsDeclinedVectorShardsAndReportsFailures) {
    ASSERT_OK(WriteRows(1, R"([[1,null,"lake",0],[1,[8],"sand",1],[1,[2],"sand",2]])"));
    GlobalIndexMeta meta(0, 0, 1, std::nullopt, nullptr);
    auto shard = std::make_shared<IndexFileMeta>("hybrid-test", "no-vector.index", 1, 1,
                                                 std::nullopt, std::nullopt, meta);
    ASSERT_OK(CommitIndexes(1, 0, {}, {7}, {}, {shard}));
    HybridSearchBuilder hybrid(path_);
    hybrid.AddVectorRoute("vec", {1}, 1, 2)
        .AddFullTextRoute("content", "lake", 1)
        .WithRanker("mrr")
        .SetLimit(2);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> hits, hybrid.ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{0, 1}}));

    ASSERT_OK(CommitIndexes(1, 1, {8}, {}, {}));
    ASSERT_OK_AND_ASSIGN(hits, hybrid.ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{0, 1}, {1, 2}}));
    ASSERT_NOK_WITH_MSG(
        hybrid.AddVectorRoute("vec", {1, 2}, 1).ExecuteLocal(),
        "Hybrid vector route 3 on 'vec': Vector search of 'vec' in snapshot 3 over rows [1, 1]: "
        "Unexpected vector query");

    GlobalIndexMeta invalid_meta(
        2, 2, 1, std::nullopt,
        std::make_shared<Bytes>(std::string({1, 2}), GetDefaultPool().get()));
    auto invalid_shard =
        std::make_shared<IndexFileMeta>("hybrid-test", "unreadable-vector.index", 1, 1,
                                        std::nullopt, std::nullopt, std::move(invalid_meta));
    ASSERT_OK(CommitIndexes(1, 2, {}, {}, {}, {invalid_shard}));
    VectorSearchBuilder vector(path_);
    vector.SetOptions({{Options::GLOBAL_INDEX_THREAD_NUM, "3"}}).SetQuery("vec", {1}).SetLimit(1);
    ASSERT_NOK_WITH_MSG(
        vector.ExecuteLocal(),
        "Opening vector shard for 'vec' in snapshot 4 over rows [2, 2]: Expected one index");
}

TEST_F(HybridSearchBuilderTest, SearchesVectorShardsConcurrently) {
    ASSERT_OK(WriteRows(1, R"([[1,[9],"lake",42],[1,[1],"sand",1]])"));
    ASSERT_OK(CommitIndexes(1, 0, {9}, {}, {}));
    ASSERT_OK(CommitIndexes(1, 1, {1}, {}, {}));
    VectorSearchBuilder vector(path_);
    vector.SetOptions({{kRequireConcurrentShards, "true"}}).SetQuery("vec", {1}).SetLimit(2);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> hits, vector.ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{0, 9}, {1, 1}}));
}

TEST_F(HybridSearchBuilderTest, PostFiltersVectorCandidatesWithinEachShard) {
    ASSERT_OK(WriteRows(1, R"([[1,[9],"sand",42],[1,[8],"sand",0],[1,[7],"lake",42]])"));
    ASSERT_OK(WriteRows(2, R"([[2,[6],"lake",0],[2,[5],"sand",0]])"));
    ASSERT_OK(CommitIndexes(1, 0, {9, 8, 7}, {1, 1, 1}, {1, 0, 1}));
    ASSERT_OK(CommitIndexes(2, 3, {6, 5}, {1, 1}, {0, 0}));
    auto filter = PredicateBuilder::Equal(2, "content", FieldType::STRING,
                                          Literal(FieldType::STRING, "lake", 4));
    const char* factor = Options::VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Executor> executor, CreateDefaultExecutor(1));
    VectorSearchBuilder vector(path_);
    vector.WithExecutor(executor)
        .WithPartitionFilter({{{"p", "1"}}, {{"p", "2"}}})
        .SetQuery("vec", {1})
        .SetLimit(1)
        .WithFilter(filter);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> hits, vector.ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{2, 7}}));

    ASSERT_OK_AND_ASSIGN(
        hits, vector.SetOptions({{Options::GLOBAL_INDEX_FILTER_REFINE_FROM_DATA, "true"}})
                  .ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{2, 7}}));

    ASSERT_OK_AND_ASSIGN(hits, vector.SetOptions({{factor, "1"}}).ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{3, 6}}));
    ASSERT_OK_AND_ASSIGN(hits, vector.SetQuery("vec", {1}, {{factor, "3"}}).ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{2, 7}}));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorScan> scan, vector.NewVectorScan());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VectorScan::Plan> plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<VectorRead> read,
        vector.WithFilter(PredicateBuilder::Equal(3, "id", FieldType::INT, Literal(0)))
            .SetQuery("vec", {1}, {{factor, "2147483647"}})
            .SetLimit(2)
            .NewVectorRead());
    ASSERT_OK_AND_ASSIGN(hits, read->Read(*plan));
    ASSERT_EQ(ToRows(*hits), Rows({{2, 7}, {3, 6}}));
    ASSERT_NOK_WITH_MSG(vector.SetQuery("vec", {1}, {{factor, "0"}}).ExecuteLocal(),
                        "must be positive");

    HybridSearchBuilder hybrid(path_);
    ASSERT_OK_AND_ASSIGN(hits, hybrid.WithFilter(filter)
                                   .SetOptions({{factor, "1"}})
                                   .AddVectorRoute("vec", {1}, 1, 2, {{factor, "3"}})
                                   .AddVectorRoute("vec", {1}, 1)
                                   .WithRanker("mrr")
                                   .SetLimit(2)
                                   .ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{2, 2}, {3, 1}}));

    auto id_filter = PredicateBuilder::Equal(3, "id", FieldType::INT, Literal(42));
    const char* refine_from_data = Options::GLOBAL_INDEX_FILTER_REFINE_FROM_DATA;
    VectorSearchBuilder partial(path_);
    partial.SetQuery("vec", {1}).SetLimit(1).WithFilter(filter).WithFilter(id_filter);
    ASSERT_OK_AND_ASSIGN(hits, partial.ExecuteLocal());
    ASSERT_TRUE(ToRows(*hits).empty());
    ASSERT_OK_AND_ASSIGN(scan, partial.NewVectorScan());
    ASSERT_OK_AND_ASSIGN(plan, scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(
        read, partial.SetQuery("vec", {1}, {{refine_from_data, "true"}}).NewVectorRead());
    ASSERT_OK_AND_ASSIGN(hits, read->Read(*plan));
    ASSERT_EQ(ToRows(*hits), Rows({{2, 7}}));
    ASSERT_OK_AND_ASSIGN(
        hits, partial.SetQuery("vec", {1}).SetOptions({{refine_from_data, "true"}}).ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{2, 7}}));

    HybridSearchBuilder mixed(path_);
    ASSERT_OK_AND_ASSIGN(hits, mixed.WithFilter(filter)
                                   .WithFilter(id_filter)
                                   .SetOptions({{refine_from_data, "true"}})
                                   .AddVectorRoute("vec", {1}, 1, 2, {{refine_from_data, "false"}})
                                   .AddFullTextRoute("content", "lake", 1)
                                   .WithRanker("mrr")
                                   .SetLimit(1)
                                   .ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{2, 1}}));
}

TEST_F(HybridSearchBuilderTest, UsesSingleFieldScalarIndexesOverlappingVectorShards) {
    ASSERT_OK(WriteRows(1, R"([[1,[9],"sand",0],[1,[8],"lake",42]])"));
    ASSERT_OK(WriteRows(2, R"([[2,[1],"sand",0]])"));
    ASSERT_OK(CommitIndexes(1, 0, {9, 8}, {}, {}));
    ASSERT_OK(CommitIndexes(2, 2, {}, {}, {0}));
    VectorSearchBuilder vector(path_);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ScoredGlobalIndexResult> hits,
        vector.SetQuery("vec", {1})
            .SetLimit(1)
            .WithFilter(PredicateBuilder::Equal(3, "id", FieldType::INT, Literal(42)))
            .ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{1, 8}}));

    GlobalIndexMeta compound(0, 1, 3, std::vector<int32_t>{2},
                             std::make_shared<Bytes>(std::string({1, 0}), GetDefaultPool().get()));
    auto file = std::make_shared<IndexFileMeta>("hybrid-filter-test", "compound.index", 2, 2,
                                                std::nullopt, std::nullopt, compound);
    ASSERT_OK(CommitIndexes(1, 0, {}, {}, {0}, {file}));
    ASSERT_OK_AND_ASSIGN(hits, vector.ExecuteLocal());
    ASSERT_TRUE(ToRows(*hits).empty());

    ASSERT_OK(CommitIndexes(1, 1, {}, {}, {1}));
    ASSERT_OK_AND_ASSIGN(
        hits,
        vector.SetQuery("vec", {1}, {{Options::VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR, "1"}})
            .ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{1, 8}}));
}

TEST_F(HybridSearchBuilderTest, TimeTravelUsesHistoricalFieldIds) {
    ASSERT_OK(WriteRows(1, R"([[1,[9],"lake",42]])"));
    ASSERT_OK(CommitIndexes(1, 0, {9}, {7}, {1}));
    auto fs = dir_->GetFileSystem();
    SnapshotManager snapshots(fs, path_);
    TagManager tags(fs, path_);
    ASSERT_OK_AND_ASSIGN(Snapshot indexed, snapshots.LoadSnapshot(2));
    ASSERT_OK_AND_ASSIGN(std::string tag_json, indexed.ToJsonString());
    ASSERT_OK(WriteRows(2, R"([[2,[4],"sand",0]])"));
    SchemaManager schemas(fs, path_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> original,
                         schemas.ReadSchema(indexed.SchemaId()));
    auto vector_type = arrow::list(arrow::float32());
    auto replaced = arrow::schema(
        {arrow::field("p", arrow::int32()), arrow::field("old_vec", vector_type),
         arrow::field("old_content", arrow::utf8()), arrow::field("id", arrow::int32()),
         arrow::field("vec", vector_type), arrow::field("content", arrow::binary())});
    auto current_options = original->Options();
    current_options[Options::BLOB_FIELD] = "content";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TableSchema> next,
        TableSchema::Create(1, replaced, original->PartitionKeys(), {}, current_options));
    ASSERT_OK_AND_ASSIGN(std::string json, next->ToJsonString());
    const std::string latest_schema_path =
        PathUtil::JoinPath(schemas.SchemaDirectory(), "schema-1");
    ASSERT_OK(fs->AtomicStore(latest_schema_path, json));

    const std::map<std::string, std::string> tag = {{Options::SCAN_TAG_NAME, "indexed"}};
    VectorSearchBuilder vector(path_);
    vector.SetOptions(tag).SetQuery("vec", {1}).SetLimit(1);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorScan> vector_scan, vector.NewVectorScan());
    ASSERT_NOK(vector_scan->CreatePlan());
    ASSERT_OK(fs->WriteFile(tags.TagPath("indexed"), tag_json, false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VectorScan::Plan> vector_plan, vector_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorRead> vector_read, vector.NewVectorRead());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> hits,
                         vector_read->Read(*vector_plan));
    ASSERT_EQ(ToRows(*hits), Rows({{0, 9}}));
    VectorSearchBuilder current(path_);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorRead> current_read,
                         current.SetQuery("vec", {1}).SetLimit(1).NewVectorRead());
    ASSERT_NOK_WITH_MSG(current_read->Read(*vector_plan), "different table, branch or field");
    for (const auto& [root, branch] : std::vector<std::pair<std::string, std::string>>{
             {PathUtil::JoinPath(dir_->Str(), "other"), "main"}, {path_, "dev"}}) {
        SchemaManager other_schemas(fs, root, branch);
        ASSERT_OK(other_schemas.CreateTable(arrow::schema(row_type_->fields()),
                                            original->PartitionKeys(), {}, original->Options()));
        VectorSearchBuilder other(root);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorRead> other_read,
                             other.SetOptions({{Options::BRANCH, branch}})
                                 .SetQuery("vec", {1})
                                 .SetLimit(1)
                                 .NewVectorRead());
        ASSERT_NOK_WITH_MSG(other_read->Read(*vector_plan), "different table, branch or field");
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Catalog> catalog, Catalog::Create(dir_->Str(), {}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> latest,
                         catalog->GetTable(Identifier("db", "hybrid")));
    ASSERT_EQ(latest->GetSchema()->FieldNames(), next->FieldNames());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HybridSearchBuilder> bound,
                         latest->NewHybridSearchBuilder());
    HybridSearchBuilder by_path(path_);
    for (auto* builder : {bound.get(), &by_path}) {
        builder->AddVectorRoute("vec", {1}, 1)
            .AddFullTextRoute("content", "lake", 1)
            .WithRanker("mrr")
            .SetLimit(1);
        ASSERT_OK_AND_ASSIGN(hits, builder->ExecuteLocal());
        ASSERT_TRUE(ToRows(*hits).empty());
        for (const auto& option : std::map<std::string, std::string>{
                 {Options::SCAN_SNAPSHOT_ID, std::to_string(indexed.Id())},
                 {Options::SCAN_TAG_NAME, "indexed"},
                 {Options::SCAN_TIMESTAMP_MILLIS, std::to_string(indexed.TimeMillis())}}) {
            ASSERT_OK_AND_ASSIGN(hits, builder->SetOptions({option}).ExecuteLocal());
            ASSERT_EQ(ToRows(*hits), Rows({{0, 2}}));
        }
    }
    vector.WithFilter(PredicateBuilder::Equal(2, "content", FieldType::STRING,
                                              Literal(FieldType::STRING, "lake", 4)));
    ASSERT_OK_AND_ASSIGN(vector_scan, vector.NewVectorScan());
    ASSERT_OK_AND_ASSIGN(vector_plan, vector_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(vector_read, vector.NewVectorRead());
    ASSERT_OK_AND_ASSIGN(hits, vector_read->Read(*vector_plan));
    ASSERT_EQ(ToRows(*hits), Rows({{0, 9}}));

    ASSERT_OK_AND_ASSIGN(Snapshot newer, snapshots.LoadSnapshot(indexed.Id() + 1));
    ASSERT_OK_AND_ASSIGN(tag_json, newer.ToJsonString());
    ASSERT_OK(fs->WriteFile(tags.TagPath("indexed"), tag_json, true));
    ASSERT_OK(fs->Delete(snapshots.SnapshotPath(indexed.Id())));
    const std::string retained_schemas = PathUtil::JoinPath(path_, "retained-schemas");
    ASSERT_OK(fs->Rename(schemas.SchemaDirectory(), retained_schemas));
    ASSERT_OK_AND_ASSIGN(hits, vector_read->Read(*vector_plan));
    ASSERT_EQ(ToRows(*hits), Rows({{0, 9}}));
    ASSERT_OK(fs->Rename(retained_schemas, schemas.SchemaDirectory()));

    ASSERT_OK_AND_ASSIGN(vector_plan, vector_scan->CreatePlan());
    ASSERT_EQ(vector_plan->SnapshotId(), newer.Id());

    current_options[Options::QUERY_AUTH_ENABLED] = "true";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableSchema> protected_schema,
                         next->Copy(current_options));
    ASSERT_OK_AND_ASSIGN(std::string protected_json, protected_schema->ToJsonString());
    ASSERT_OK(fs->WriteFile(latest_schema_path, protected_json, true));
    ASSERT_NOK_WITH_MSG(bound->SetOptions(tag).ExecuteLocal(), "row filters and column masks");
}

TEST_F(HybridSearchBuilderTest, PinsSnapshotAcrossRoutesAndSplitScanRead) {
    VectorSearchBuilder future(path_);
    future.SetOptions({{Options::SCAN_SNAPSHOT_ID, "2"}}).SetQuery("vec", {1});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorScan> future_scan, future.NewVectorScan());
    ASSERT_NOK(future_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorSearchBuilder> vector,
                         table_->NewVectorSearchBuilder());
    vector->SetQuery("vec", {1}).SetLimit(1);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorScan> scan, vector->NewVectorScan());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VectorScan::Plan> empty_plan, scan->CreatePlan());
    ASSERT_EQ(empty_plan->SnapshotId(), std::nullopt);

    ASSERT_OK(WriteRows(
        1, R"([[1,[2],"lake",42],[1,[8],"sand",42],[1,[9],"sand",42],[1,[10],"lake",42]])"));
    ASSERT_OK(CommitIndexes(1, 0, {2, 8}, {1, 2}, {1, 1}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VectorScan::Plan> previously_missing,
                         future_scan->CreatePlan());
    ASSERT_EQ(previously_missing->SnapshotId(), 2);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<VectorScan::Plan> plan, scan->CreatePlan());
    ASSERT_EQ(plan->SnapshotId(), 2);

    SnapshotManager snapshots(dir_->GetFileSystem(), path_);
    ASSERT_OK_AND_ASSIGN(Snapshot first, snapshots.LoadSnapshot(*plan->SnapshotId()));
    ASSERT_TRUE(first.IndexManifest());
    const std::string snapshot_path = snapshots.SnapshotPath(first.Id());
    auto fs = std::make_shared<CommitOnManifestOpen>(*first.IndexManifest(), [this, snapshot_path] {
        PAIMON_RETURN_NOT_OK(CommitIndexes(1, 2, {9, 10}, {12, 40}, {1, 1}));
        return dir_->GetFileSystem()->Delete(snapshot_path);
    });
    HybridSearchBuilder search(path_);
    search.WithFileSystem(fs)
        .SetOptions({{Options::DELETION_VECTORS_ENABLED, "true"},
                     {Options::GLOBAL_INDEX_FILTER_REFINE_FROM_DATA, "true"}})
        .WithFilter(PredicateBuilder::Equal(3, "id", FieldType::INT, Literal(42)))
        .WithFilter(PredicateBuilder::Equal(2, "content", FieldType::STRING,
                                            Literal(FieldType::STRING, "lake", 4)))
        .AddVectorRoute("vec", {1}, 1, 1, {{Options::SCAN_SNAPSHOT_ID, "1"}})
        .AddFullTextRoute("content", "lake", 1)
        .SetLimit(1);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> hits, search.ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{0, 2.0f / 61}}));
    ASSERT_OK_AND_ASSIGN(std::string snapshot_json, first.ToJsonString());
    ASSERT_OK(dir_->GetFileSystem()->WriteFile(snapshot_path, snapshot_json, false));
    ASSERT_OK(snapshots.LoadSnapshot(first.Id() + 1));
    ASSERT_OK_AND_ASSIGN(hits, search.WithRrfRanker().ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{3, 2.0f / 61}}));
    ASSERT_OK_AND_ASSIGN(hits,
                         search
                             .SetOptions({{Options::SCAN_SNAPSHOT_ID, std::to_string(first.Id())},
                                          {Options::GLOBAL_INDEX_FILTER_REFINE_FROM_DATA, "true"}})
                             .ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{0, 2.0f / 61}}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorRead> read, vector->NewVectorRead());
    ASSERT_OK_AND_ASSIGN(hits, read->Read(*empty_plan));
    ASSERT_TRUE(ToRows(*hits).empty());
    ASSERT_OK_AND_ASSIGN(hits, read->Read(*plan));
    ASSERT_EQ(ToRows(*hits), Rows({{1, 8}}));
    ASSERT_OK_AND_ASSIGN(hits, vector->ExecuteLocal());
    ASSERT_EQ(ToRows(*hits), Rows({{3, 10}}));
    const std::map<std::string, std::string> pinned = {
        {Options::SCAN_SNAPSHOT_ID, std::to_string(first.Id())}};
    ASSERT_OK(dir_->GetFileSystem()->Delete(snapshots.SnapshotPath(first.Id())));
    ASSERT_OK_AND_ASSIGN(read, vector->SetOptions(pinned).SetQuery("vec", {-1}).NewVectorRead());
    ASSERT_OK_AND_ASSIGN(hits, read->Read(*plan));
    ASSERT_EQ(ToRows(*hits), Rows({{0, -2}}));
}

TEST_F(HybridSearchBuilderTest, EmptyTableAndValidation) {
    HybridSearchBuilder empty(path_);
    ASSERT_NOK(empty.SetLimit(1).ExecuteLocal());
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ScoredGlobalIndexResult> hits,
        empty.AddVectorRoute("vec", {1}, 2).AddFullTextRoute("content", "lake", 2).ExecuteLocal());
    ASSERT_TRUE(ToRows(*hits).empty());
    ASSERT_NOK(empty.SetLimit(0).ExecuteLocal());
    ASSERT_NOK_WITH_MSG(empty.SetLimit(1).WithRanker("sum").ExecuteLocal(),
                        "Unsupported hybrid ranker");
    for (float weight : {0.0f, -1.0f, std::numeric_limits<float>::quiet_NaN(),
                         std::numeric_limits<float>::infinity()}) {
        HybridSearchBuilder builder(path_);
        ASSERT_NOK_WITH_MSG(
            builder.SetLimit(1)
                .AddFullTextRoute("content", "lake", 1)
                .AddFullTextRoute("content", "lake", 1, weight)
                .ExecuteLocal(),
            "Hybrid full-text route 2 on 'content': Search route weight must be finite and "
            "positive");
    }
    ASSERT_NOK(HybridSearchBuilder(path_).SetLimit(1).AddVectorRoute("", {1}, 1).ExecuteLocal());
    ASSERT_NOK(HybridSearchBuilder(path_).SetLimit(1).AddVectorRoute("vec", {}, 1).ExecuteLocal());
    ASSERT_NOK(HybridSearchBuilder(path_).SetLimit(1).AddVectorRoute("vec", {1}, 0).ExecuteLocal());
    ASSERT_NOK_WITH_MSG(HybridSearchBuilder(path_)
                            .SetLimit(1)
                            .AddFullTextRoute("content", "lake", 1, 1, kAscending)
                            .ExecuteLocal(),
                        "options");
    ASSERT_NOK_WITH_MSG(
        HybridSearchBuilder(path_).SetLimit(1).AddVectorRoute("missing", {1}, 1).ExecuteLocal(),
        "does not exist");
    ASSERT_NOK_WITH_MSG(
        empty.WithRrfRanker().SetOptions({{Options::QUERY_AUTH_ENABLED, "true"}}).ExecuteLocal(),
        "row filters and column masks");
    HybridSearchBuilder route_mode(path_);
    ASSERT_NOK_WITH_MSG(
        route_mode.SetLimit(1)
            .AddVectorRoute("vec", {1}, 1, 1, {{Options::VECTOR_INDEX_SEARCH_MODE, "full"}})
            .ExecuteLocal(),
        "full and detail are not supported");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<VectorSearchBuilder> vector,
                         table_->NewVectorSearchBuilder());
    vector->SetQuery("vec", {1}).SetLimit(1);
    ASSERT_NOK_WITH_MSG(vector->SetOptions({{Options::TYPE, "format-table"}}).ExecuteLocal(),
                        "type contradicts the loaded table");
    ASSERT_OK(vector->SetOptions({{Options::VECTOR_INDEX_SEARCH_MODE, "detail"}})
                  .SetQuery("vec", {1}, {{Options::VECTOR_INDEX_SEARCH_MODE, "fast"}})
                  .ExecuteLocal());
    ASSERT_NOK_WITH_MSG(
        vector->SetOptions({{Options::SCAN_MODE, "latest-full"}, {Options::SCAN_SNAPSHOT_ID, "1"}})
            .NewVectorRead(),
        "scan.snapshot-id must be null when you use latest-full for scan.mode");
}

TEST_F(HybridSearchBuilderTest, RejectsIndexedAppendOnlyTableWithoutDataEvolution) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Catalog> catalog, Catalog::Create(dir_->Str(), {}));
    const Identifier name("db", "tracked_append");
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema(row_type_->fields()), &c_schema).ok());
    ASSERT_OK(catalog->CreateTable(
        name, &c_schema, {"p"}, {},
        {{Options::FILE_FORMAT, "parquet"}, {Options::ROW_TRACKING_ENABLED, "true"}}, false));
    ASSERT_OK_AND_ASSIGN(table_, catalog->GetTable(name));
    path_ = table_->GetRootPath();
    ASSERT_OK(WriteRows(1, R"([[1,[9],"lake",42],[1,[1],"sand",1]])"));
    ASSERT_OK(CommitIndexes(1, 0, {9, 1}, {7, 2}, {}));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HybridSearchBuilder> hybrid,
                         table_->NewHybridSearchBuilder());
    ASSERT_NOK_WITH_MSG(hybrid->AddVectorRoute("vec", {1}, 1)
                            .AddFullTextRoute("content", "lake", 1)
                            .SetLimit(1)
                            .ExecuteLocal(),
                        "Hybrid search requires a data-evolution table");

    VectorSearchBuilder vector(path_);
    vector.SetQuery("vec", {1}).SetLimit(1);
    ASSERT_NOK_WITH_MSG(vector.NewVectorScan(), "Vector search requires a data-evolution table");
    ASSERT_NOK_WITH_MSG(vector.NewVectorRead(), "Vector search requires a data-evolution table");
    ASSERT_NOK_WITH_MSG(vector.ExecuteLocal(), "Vector search requires a data-evolution table");
    ASSERT_NOK_WITH_MSG(
        vector.SetOptions({{Options::DATA_EVOLUTION_ENABLED, "true"}}).ExecuteLocal(),
        "Vector search requires a data-evolution table");
}

TEST_F(HybridSearchBuilderTest, RejectsPrimaryKeyTable) {
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                 arrow::field("vec", arrow::list(arrow::float32())),
                                 arrow::field("content", arrow::utf8())});
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Catalog> catalog, Catalog::Create(dir_->Str(), {}));
    Identifier name("db", "pk");
    ASSERT_OK(catalog->CreateTable(name, &c_schema, {}, {"id"},
                                   {{Options::FILE_FORMAT, "parquet"},
                                    {Options::BUCKET, "1"},
                                    {Options::DELETION_VECTORS_ENABLED, "true"},
                                    {Options::PK_VECTOR_INDEX_COLUMNS, "vec"},
                                    {"fields.vec.pk-vector.index.type", "lumina"}},
                                   false));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table, catalog->GetTable(name));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<HybridSearchBuilder> search,
                         table->NewHybridSearchBuilder());
    ASSERT_NOK_WITH_MSG(search->AddFullTextRoute("content", "lake", 1)
                            .AddVectorRoute("vec", {1}, 1)
                            .SetLimit(1)
                            .ExecuteLocal(),
                        "Hybrid search requires a data-evolution table");
}
}  // namespace
}  // namespace paimon::test
