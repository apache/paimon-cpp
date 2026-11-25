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

#include "paimon/table/source/vector_search_builder.h"

#include <algorithm>
#include <future>
#include <limits>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/global_index/scored_global_index_result_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/source/filtered_row_id_reader.h"
#include "paimon/core/table/source/table_search_context.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/predicate/vector_search.h"

namespace paimon {
namespace {
class VectorIndexPlan : public VectorScan::Plan {
 public:
    VectorIndexPlan(const TableSearchContext& context, const DataField& field)
        : context(context), field(field) {}
    std::optional<int64_t> SnapshotId() const override {
        return snapshot ? std::optional<int64_t>(snapshot->Id()) : std::nullopt;
    }

    TableSearchContext context;
    DataField field;
    std::optional<Snapshot> snapshot;
    std::unique_ptr<GlobalIndexScanImpl> scan;
};

Result<std::shared_ptr<VectorIndexPlan>> PlanVector(const TableSearchContext& base_context,
                                                    const DataField& field,
                                                    const std::optional<Snapshot>& snapshot) {
    auto plan = std::make_shared<VectorIndexPlan>(base_context, field);
    if (snapshot) {
        auto& context = plan->context;
        plan->snapshot = snapshot;
        PAIMON_ASSIGN_OR_RAISE(context.executor, GlobalIndexScanImpl::GetOrCreateExecutor(
                                                     context.executor, context.options));
        PAIMON_ASSIGN_OR_RAISE(plan->scan, GlobalIndexScanImpl::Create(
                                               context.root_path, context.table_schema, *snapshot,
                                               context.partition_filter, context.options,
                                               context.executor, context.pool));
    }
    return plan;
}

class DataEvolutionVectorScan : public VectorScan {
 public:
    DataEvolutionVectorScan(const TableSearchContext& context, DataField field)
        : context_(context), field_(std::move(field)) {}
    Result<std::shared_ptr<Plan>> CreatePlan() override {
        PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, context_.ResolveSnapshot());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, PlanVector(context_, field_, snapshot));
        return plan;
    }

 private:
    TableSearchContext context_;
    DataField field_;
};

class TimeTravelVectorScan : public VectorScan {
 public:
    TimeTravelVectorScan(const TableSearchSettings& settings, const std::string& field_name,
                         const std::map<std::string, std::string>& query_options)
        : settings_(settings), field_name_(field_name), query_options_(query_options) {}

    Result<std::shared_ptr<Plan>> CreatePlan() override {
        bool retry_time_travel = false;
        PAIMON_ASSIGN_OR_RAISE(
            TableSearchSettings::ResolvedColumn column,
            settings_.ResolveVectorColumn(field_name_, query_options_, &retry_time_travel));
        DataEvolutionVectorScan scan(column.context, std::move(column.field));
        return scan.CreatePlan();
    }

 private:
    TableSearchSettings settings_;
    std::string field_name_;
    std::map<std::string, std::string> query_options_;
};

struct VectorReadOptions {
    int32_t candidates_factor;
    bool refine_from_data;
};

Result<VectorReadOptions> ResolveReadOptions(const CoreOptions& options,
                                             const VectorSearch& query) {
    const char* key = Options::VECTOR_INDEX_POST_FILTER_CANDIDATES_FACTOR;
    const auto& effective = query.options.count(key) ? query.options : options.ToMap();
    PAIMON_ASSIGN_OR_RAISE(int32_t factor,
                           OptionsUtils::GetValueFromMap<int32_t>(effective, key, 10));
    if (factor <= 0) {
        return Status::Invalid(fmt::format("{} must be positive, got {}", key, factor));
    }
    return VectorReadOptions{factor, options.GlobalIndexFilterRefineFromData()};
}

// An absent answer requires post-filtering. An evaluated empty bitmap is a known empty match set.
Result<std::optional<RoaringBitmap64>> EvaluateScalarPreFilter(
    const VectorIndexPlan& plan, const std::vector<GlobalIndexScanImpl::VectorIndexShard>& shards,
    const FilteredRowIdReader& row_reader, bool refine_from_data) {
    std::vector<Range> ranges;
    ranges.reserve(shards.size());
    for (const auto& shard : shards) {
        ranges.push_back(shard.range);
    }
    PAIMON_ASSIGN_OR_RAISE(RowRangeIndex indexed_ranges, RowRangeIndex::Create(ranges));
    const auto& context = plan.context;
    PAIMON_ASSIGN_OR_RAISE(std::optional<GlobalIndexEvaluatorImpl::Evaluation> evaluation,
                           plan.scan->ScanWithCoverage(context.filter, indexed_ranges));
    if (!evaluation) {
        return std::optional<RoaringBitmap64>();
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 candidates,
                           GlobalIndexUtils::ToBitmap(evaluation->result));
    PAIMON_ASSIGN_OR_RAISE(bool exact,
                           FilteredRowIdReader::IsExact(*context.table_schema, context.filter,
                                                        evaluation->contributing_field_ids));
    if (exact || candidates.IsEmpty()) {
        return std::optional<RoaringBitmap64>(std::move(candidates));
    }
    candidates = GlobalIndexUtils::RowIdsWithin(candidates, indexed_ranges.Ranges());
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 matched,
                           row_reader.RefineCandidates(std::move(candidates), refine_from_data));
    return std::optional<RoaringBitmap64>(std::move(matched));
}

void KeepShardsWithRows(const RoaringBitmap64& rows,
                        std::vector<GlobalIndexScanImpl::VectorIndexShard>* shards) {
    shards->erase(std::remove_if(shards->begin(), shards->end(),
                                 [&rows](const auto& shard) {
                                     auto first = rows.EqualOrLarger(shard.range.from);
                                     return first == rows.End() || *first > shard.range.to;
                                 }),
                  shards->end());
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> PostFilter(const ScoredGlobalIndexResult& result,
                                                            const Range& range,
                                                            const FilteredRowIdReader& row_reader,
                                                            int32_t limit) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> iter,
                           result.CreateScoredIterator());
    RoaringBitmap64 candidates;
    std::vector<std::pair<int64_t, float>> rows;
    while (iter->HasNext()) {
        auto hit = iter->NextWithScore();
        if (hit.first < range.from || hit.first > range.to) {
            return Status::Invalid("Vector candidate lies outside its source shard");
        }
        candidates.Add(hit.first);
        rows.push_back(hit);
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 matched,
                           row_reader.MatchingRowIds(std::move(candidates)));
    rows.erase(std::remove_if(rows.begin(), rows.end(),
                              [&matched](const auto& hit) { return !matched.Contains(hit.first); }),
               rows.end());
    return ScoredGlobalIndexResultUtils::FromRows(std::move(rows), limit);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> SearchVectorShards(
    const VectorIndexPlan& plan, const std::vector<GlobalIndexScanImpl::VectorIndexShard>& shards,
    const std::shared_ptr<VectorSearch>& search,
    const LiveRowGlobalIndexReader::LiveRowsSupplier& load_live_rows,
    const std::optional<FilteredRowIdReader>& post_filter, int32_t candidates_factor) {
    using ShardResult = Result<std::shared_ptr<ScoredGlobalIndexResult>>;
    std::vector<std::shared_ptr<ScoredGlobalIndexResult>> results;
    results.reserve(shards.size());
    const auto& executor = plan.context.executor;
    const size_t max_in_flight =
        std::min(shards.size(), std::max<size_t>(1, executor->GetThreadNum()));
    std::vector<std::future<ShardResult>> searches;
    searches.reserve(max_in_flight);
    // Drain submitted searches when opening a later shard or submitting its query fails.
    ScopeGuard wait_for_searches([&]() {
        for (auto& pending : searches) {
            if (pending.valid()) {
                pending.wait();
            }
        }
    });

    auto append_result = [&](ShardResult shard_result, const Range& range) -> Status {
        if (!shard_result.ok()) {
            return shard_result.status().WithMessage(fmt::format(
                "Vector search of '{}' in snapshot {} over rows [{}, {}]: {}", search->field_name,
                plan.snapshot->Id(), range.from, range.to, shard_result.status().message()));
        }
        std::shared_ptr<ScoredGlobalIndexResult> result = std::move(shard_result).value();
        if (result) {
            if (post_filter) {
                PAIMON_ASSIGN_OR_RAISE(result,
                                       PostFilter(*result, range, *post_filter, search->limit));
            }
            results.push_back(std::move(result));
        }
        return Status::OK();
    };
    size_t next_shard = 0;
    auto collect_searches = [&]() -> Status {
        // Post-filter on the caller thread because data reads can use the same executor.
        for (auto& result : CollectAll(searches)) {
            PAIMON_RETURN_NOT_OK(append_result(std::move(result), shards[next_shard].range));
            ++next_shard;
        }
        searches.clear();
        return Status::OK();
    };

    for (const auto& shard : shards) {
        auto request = search;
        if (post_filter) {
            request = std::make_shared<VectorSearch>(*search);
            const int64_t candidates = static_cast<int64_t>(search->limit) * candidates_factor;
            request->limit = static_cast<int32_t>(
                std::min({candidates, shard.range.Count(),
                          static_cast<int64_t>(std::numeric_limits<int32_t>::max())}));
        }
        auto opened = plan.scan->OpenVectorReader(plan.field, shard, load_live_rows);
        if (!opened.ok()) {
            return opened.status().WithMessage(
                fmt::format("Opening vector shard for '{}' in snapshot {} over rows [{}, {}]: {}",
                            search->field_name, plan.snapshot->Id(), shard.range.from,
                            shard.range.to, opened.status().message()));
        }
        std::shared_ptr<GlobalIndexReader> reader = std::move(opened).value();
        if (shards.size() == 1) {
            PAIMON_RETURN_NOT_OK(append_result(reader->VisitVectorSearch(request), shard.range));
            break;
        }
        searches.push_back(Via(executor.get(), [reader = std::move(reader), request] {
            return reader->VisitVectorSearch(request);
        }));
        if (searches.size() == max_in_flight) {
            PAIMON_RETURN_NOT_OK(collect_searches());
        }
    }
    PAIMON_RETURN_NOT_OK(collect_searches());
    return ScoredGlobalIndexResultUtils::MergeTopK(results, search->limit);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> ReadVector(const VectorIndexPlan& plan,
                                                            const VectorSearch& query,
                                                            const VectorReadOptions& options) {
    if (!plan.scan) {
        return ScoredGlobalIndexResultUtils::FromRows({});
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexScanImpl::VectorIndexShard> shards,
                           plan.scan->PlanVectorShards(plan.field));
    if (shards.empty()) {
        return ScoredGlobalIndexResultUtils::FromRows({});
    }
    const auto& context = plan.context;
    auto search = std::make_shared<VectorSearch>(query);
    search->pool = context.pool;
    search->return_similarity_score = true;
    std::optional<FilteredRowIdReader> post_filter;
    std::optional<RoaringBitmap64> scalar_matches;
    if (context.filter) {
        post_filter.emplace(context.root_path, context.table_schema, context.options,
                            context.partition_filter, context.filter, plan.snapshot->Id(),
                            context.executor, context.pool, plan.snapshot);
        PAIMON_ASSIGN_OR_RAISE(scalar_matches, EvaluateScalarPreFilter(plan, shards, *post_filter,
                                                                       options.refine_from_data));
        if (scalar_matches) {
            KeepShardsWithRows(*scalar_matches, &shards);
            if (shards.empty()) {
                return ScoredGlobalIndexResultUtils::FromRows({});
            }
            post_filter.reset();
        }
    }
    std::vector<Range> shard_ranges;
    shard_ranges.reserve(shards.size());
    for (const auto& shard : shards) {
        shard_ranges.push_back(shard.range);
    }
    PAIMON_ASSIGN_OR_RAISE(LiveRowGlobalIndexReader::LiveRowsSupplier load_live_rows,
                           plan.scan->CreateLiveRowsSupplier(shard_ranges));
    if (load_live_rows) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const RoaringBitmap64> live_rows, load_live_rows());
        if (live_rows) {
            if (scalar_matches) {
                *scalar_matches &= *live_rows;
                load_live_rows = {};
            }
            KeepShardsWithRows(scalar_matches ? *scalar_matches : *live_rows, &shards);
        }
    }
    if (scalar_matches) {
        auto matching_rows = std::make_shared<const RoaringBitmap64>(std::move(*scalar_matches));
        search->pre_filter = [matching_rows](int64_t id) { return matching_rows->Contains(id); };
    }
    return SearchVectorShards(plan, shards, search, load_live_rows, post_filter,
                              options.candidates_factor);
}

class DataEvolutionVectorRead : public VectorRead {
 public:
    DataEvolutionVectorRead(const TableSearchContext& context, const DataField& field,
                            VectorSearch query, VectorReadOptions options)
        : path_(context.root_path),
          branch_(BranchManager::NormalizeBranch(context.options.GetBranch())),
          field_id_(field.Id()),
          query_(std::move(query)),
          options_(options) {}
    Result<std::shared_ptr<ScoredGlobalIndexResult>> Read(
        const VectorScan::Plan& plan) const override {
        const auto* index_plan = dynamic_cast<const VectorIndexPlan*>(&plan);
        if (!index_plan || index_plan->field.Id() != field_id_ ||
            index_plan->field.Name() != query_.field_name ||
            index_plan->context.root_path != path_ ||
            BranchManager::NormalizeBranch(index_plan->context.options.GetBranch()) != branch_) {
            return Status::Invalid("Vector plan belongs to a different table, branch or field");
        }
        return ReadVector(*index_plan, query_, options_);
    }

 private:
    std::string path_;
    std::string branch_;
    int32_t field_id_;
    VectorSearch query_;
    VectorReadOptions options_;
};
}  // namespace

Result<std::shared_ptr<ScoredGlobalIndexResult>> TableSearchContext::SearchVector(
    const DataField& field, const std::optional<Snapshot>& snapshot,
    const VectorSearch& query) const {
    PAIMON_ASSIGN_OR_RAISE(VectorReadOptions read_options, ResolveReadOptions(options, query));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<VectorIndexPlan> plan,
                           PlanVector(*this, field, snapshot));
    return ReadVector(*plan, query, read_options);
}

class VectorSearchBuilder::Impl {
 public:
    explicit Impl(const std::string& path) : settings(path) {}
    TableSearchSettings settings;
    std::string field_name;
    std::vector<float> query_vector;
    std::map<std::string, std::string> query_options;
    int32_t limit = 0;

    Result<VectorSearch> CreateQuery() const {
        if (field_name.empty() || query_vector.empty() || limit <= 0) {
            return Status::Invalid(
                "Vector search requires a column, a non-empty vector and a positive limit");
        }
        return VectorSearch(field_name, limit, query_vector, nullptr, nullptr, std::nullopt,
                            query_options);
    }
};

VectorSearchBuilder::VectorSearchBuilder(const std::string& table_path)
    : impl_(std::make_unique<Impl>(table_path)) {}
VectorSearchBuilder::~VectorSearchBuilder() = default;

Result<std::unique_ptr<VectorSearchBuilder>> FileStoreTable::NewVectorSearchBuilder() const {
    auto builder = std::make_unique<VectorSearchBuilder>(root_path_);
    builder->impl_->settings = TableSearchSettings(*this);
    return builder;
}

VectorSearchBuilder& VectorSearchBuilder::WithPartitionFilter(
    const std::vector<std::map<std::string, std::string>>& partitions) {
    impl_->settings.partition_filters_.push_back(partitions);
    return *this;
}

VectorSearchBuilder& VectorSearchBuilder::WithFilter(const std::shared_ptr<Predicate>& predicate) {
    if (predicate) {
        impl_->settings.filters_.push_back(predicate);
    }
    return *this;
}

VectorSearchBuilder& VectorSearchBuilder::SetLimit(int32_t limit) {
    impl_->limit = limit;
    return *this;
}

VectorSearchBuilder& VectorSearchBuilder::SetOptions(
    const std::map<std::string, std::string>& options) {
    impl_->settings.options_ = options;
    return *this;
}

VectorSearchBuilder& VectorSearchBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->settings.file_system_ = file_system;
    return *this;
}

VectorSearchBuilder& VectorSearchBuilder::WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) {
    impl_->settings.memory_pool_ = pool;
    return *this;
}

VectorSearchBuilder& VectorSearchBuilder::WithExecutor(const std::shared_ptr<Executor>& executor) {
    impl_->settings.executor_ = executor;
    return *this;
}

VectorSearchBuilder& VectorSearchBuilder::SetQuery(
    const std::string& field, const std::vector<float>& vector,
    const std::map<std::string, std::string>& options) {
    impl_->field_name = field;
    impl_->query_vector = vector;
    impl_->query_options = options;
    return *this;
}

Result<std::unique_ptr<VectorScan>> VectorSearchBuilder::NewVectorScan() const {
    bool retry_time_travel = false;
    Result<TableSearchSettings::ResolvedColumn> column = impl_->settings.ResolveVectorColumn(
        impl_->field_name, impl_->query_options, &retry_time_travel);
    if (!retry_time_travel) {
        PAIMON_ASSIGN_OR_RAISE(TableSearchSettings::ResolvedColumn resolved, std::move(column));
        if (!resolved.context.time_travel.value()) {
            return std::make_unique<DataEvolutionVectorScan>(resolved.context,
                                                             std::move(resolved.field));
        }
    }
    // Resolve time travel per plan because tags and snapshot availability can change.
    return std::make_unique<TimeTravelVectorScan>(impl_->settings, impl_->field_name,
                                                  impl_->query_options);
}

Result<std::unique_ptr<VectorRead>> VectorSearchBuilder::NewVectorRead() const {
    PAIMON_ASSIGN_OR_RAISE(VectorSearch query, impl_->CreateQuery());
    PAIMON_ASSIGN_OR_RAISE(TableSearchSettings::ResolvedColumn column,
                           impl_->settings.ResolveVectorColumn(query.field_name, query.options));
    PAIMON_ASSIGN_OR_RAISE(VectorReadOptions options,
                           ResolveReadOptions(column.context.options, query));
    return std::make_unique<DataEvolutionVectorRead>(column.context, column.field, std::move(query),
                                                     options);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> VectorSearchBuilder::ExecuteLocal() const {
    PAIMON_ASSIGN_OR_RAISE(VectorSearch query, impl_->CreateQuery());
    PAIMON_ASSIGN_OR_RAISE(TableSearchSettings::ResolvedColumn column,
                           impl_->settings.ResolveVectorColumn(query.field_name, query.options));
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, column.context.ResolveSnapshot());
    return column.context.SearchVector(column.field, snapshot, query);
}

}  // namespace paimon
