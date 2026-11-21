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

#include "paimon/table/source/hybrid_search_builder.h"

#include <cmath>
#include <cstddef>
#include <optional>
#include <utility>
#include <variant>

#include "fmt/format.h"
#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/source/table_search_context.h"
#include "paimon/global_index/hybrid_search_ranker.h"
#include "paimon/predicate/vector_search.h"

namespace paimon {
namespace {
struct HybridSearchRoute {
    std::string field_name;
    std::variant<std::vector<float>, std::string> query;
    int32_t route_limit;
    float weight;
    std::map<std::string, std::string> query_options;

    SearchKind Kind() const {
        return std::holds_alternative<std::string>(query) ? SearchKind::FULL_TEXT
                                                          : SearchKind::VECTOR;
    }

    Status Validate() const {
        if (field_name.empty() || route_limit <= 0) {
            return Status::Invalid("A search route requires a field name and a positive limit");
        }
        if (!std::isfinite(weight) || weight <= 0) {
            return Status::Invalid("Search route weight must be finite and positive");
        }
        if (Kind() == SearchKind::FULL_TEXT && !query_options.empty()) {
            return Status::Invalid("Full-text hybrid route options are not supported");
        }
        if (Kind() == SearchKind::VECTOR && std::get<std::vector<float>>(query).empty()) {
            return Status::Invalid("Vector query must not be empty");
        }
        return Status::OK();
    }

    Status WithErrorContext(const Status& status, size_t route_index) const {
        const char* kind = Kind() == SearchKind::VECTOR ? "vector" : "full-text";
        return status.WithMessage(fmt::format("Hybrid {} route {} on '{}': {}", kind,
                                              route_index + 1, field_name, status.message()));
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> Execute(
        const TableSearchContext& base_context, const std::optional<Snapshot>& snapshot) const {
        std::optional<TableSearchContext> route_context;
        if (!query_options.empty()) {
            route_context.emplace(base_context);
            PAIMON_RETURN_NOT_OK(route_context->ApplyVectorSearchOptions(query_options));
        }
        const TableSearchContext& context = route_context ? *route_context : base_context;
        const SearchKind kind = Kind();
        PAIMON_ASSIGN_OR_RAISE(DataField field, context.ResolveField(field_name, kind));
        if (kind == SearchKind::VECTOR) {
            VectorSearch search(field_name, route_limit, std::get<std::vector<float>>(query),
                                nullptr, nullptr, std::nullopt, query_options);
            return context.SearchVector(field, snapshot, search);
        }
        return context.SearchFullText(field, snapshot, std::get<std::string>(query), route_limit);
    }
};
}  // namespace

class HybridSearchBuilder::Impl {
 public:
    explicit Impl(const std::string& path) : settings_(path) {}
    TableSearchSettings settings_;
    std::vector<HybridSearchRoute> routes_;
    int32_t final_limit_ = 0;
    std::string ranker_;
};

HybridSearchBuilder::HybridSearchBuilder(const std::string& table_path)
    : impl_(std::make_unique<Impl>(table_path)) {}
HybridSearchBuilder::~HybridSearchBuilder() = default;

Result<std::unique_ptr<HybridSearchBuilder>> FileStoreTable::NewHybridSearchBuilder() const {
    auto builder = std::make_unique<HybridSearchBuilder>(root_path_);
    builder->impl_->settings_ = TableSearchSettings(*this);
    return builder;
}

HybridSearchBuilder& HybridSearchBuilder::WithPartitionFilter(
    const std::vector<std::map<std::string, std::string>>& partitions) {
    impl_->settings_.partition_filters_.push_back(partitions);
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::WithFilter(const std::shared_ptr<Predicate>& predicate) {
    if (predicate) {
        impl_->settings_.filters_.push_back(predicate);
    }
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::SetLimit(int32_t limit) {
    impl_->final_limit_ = limit;
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::SetOptions(
    const std::map<std::string, std::string>& options) {
    impl_->settings_.options_ = options;
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->settings_.file_system_ = file_system;
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) {
    impl_->settings_.memory_pool_ = pool;
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::WithExecutor(const std::shared_ptr<Executor>& executor) {
    impl_->settings_.executor_ = executor;
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::AddVectorRoute(
    const std::string& column, const std::vector<float>& vector, int32_t limit, float weight,
    const std::map<std::string, std::string>& options) {
    impl_->routes_.push_back({column, vector, limit, weight, options});
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::AddFullTextRoute(
    const std::string& field, const std::string& query, int32_t limit, float weight,
    const std::map<std::string, std::string>& options) {
    impl_->routes_.push_back({field, query, limit, weight, options});
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::WithRanker(const std::string& ranker) {
    impl_->ranker_ = ranker;
    return *this;
}

HybridSearchBuilder& HybridSearchBuilder::WithRrfRanker() {
    return WithRanker("rrf");
}

HybridSearchBuilder& HybridSearchBuilder::WithWeightedScoreRanker() {
    return WithRanker("weighted_score");
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> HybridSearchBuilder::ExecuteLocal() const {
    if (impl_->routes_.empty() || impl_->final_limit_ <= 0) {
        return Status::Invalid(
            "Hybrid search requires at least one route and a positive final limit");
    }
    PAIMON_ASSIGN_OR_RAISE(HybridSearchRanker::Type ranker,
                           HybridSearchRanker::Parse(impl_->ranker_));
    for (size_t route_index = 0; route_index < impl_->routes_.size(); ++route_index) {
        const auto& route = impl_->routes_[route_index];
        const Status validation = route.Validate();
        if (!validation.ok()) {
            return route.WithErrorContext(validation, route_index);
        }
    }
    PAIMON_ASSIGN_OR_RAISE(TableSearchContext context, impl_->settings_.Resolve());
    PAIMON_RETURN_NOT_OK(context.RequireDataEvolution("Hybrid"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot, context.ResolveSnapshot());
    if (snapshot) {
        PAIMON_ASSIGN_OR_RAISE(context.executor, GlobalIndexScanImpl::GetOrCreateExecutor(
                                                     context.executor, context.options));
    }
    std::vector<HybridSearchRanker::WeightedResult> results;
    results.reserve(impl_->routes_.size());
    for (size_t route_index = 0; route_index < impl_->routes_.size(); ++route_index) {
        const auto& route = impl_->routes_[route_index];
        Result<std::shared_ptr<ScoredGlobalIndexResult>> result = route.Execute(context, snapshot);
        if (!result.ok()) {
            return route.WithErrorContext(result.status(), route_index);
        }
        results.push_back({std::move(result).value(), route.weight});
    }
    return HybridSearchRanker::Rank(ranker, results, impl_->final_limit_);
}

}  // namespace paimon
