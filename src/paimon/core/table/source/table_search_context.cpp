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

#include "paimon/core/table/source/table_search_context.h"

#include <algorithm>
#include <iterator>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/global_index/scored_global_index_result_utils.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store_table.h"
#include "paimon/core/table/source/data_evolution_full_text_read.h"
#include "paimon/core/table/source/data_evolution_full_text_scan.h"
#include "paimon/core/tag/tag.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/core/utils/tag_manager.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/status.h"

namespace paimon {

namespace {
/// Projects a necessary partition condition from a row filter. AND keeps each projectable child.
/// OR needs a partition condition from every child.
Result<std::shared_ptr<Predicate>> ImpliedPartitionPredicate(
    const std::shared_ptr<Predicate>& predicate,
    const std::map<std::string, int32_t>& partition_key_to_idx) {
    auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (!compound) {
        return PredicateUtils::CreatePickedFieldFilter(predicate, partition_key_to_idx);
    }
    bool is_and = compound->GetFunction().GetType() == Function::Type::AND;
    std::vector<std::shared_ptr<Predicate>> implied_children;
    for (const auto& child : compound->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> implied,
                               ImpliedPartitionPredicate(child, partition_key_to_idx));
        if (implied) {
            implied_children.push_back(std::move(implied));
        } else if (!is_and) {
            return std::shared_ptr<Predicate>();
        }
    }
    if (implied_children.empty()) {
        return std::shared_ptr<Predicate>();
    }
    return is_and ? PredicateBuilder::And(implied_children)
                  : PredicateBuilder::Or(implied_children);
}

struct SearchFilters {
    /// Field indexes in this predicate refer to the partition keys.
    std::shared_ptr<Predicate> partition_predicate;
    std::shared_ptr<Predicate> data_filter;
};

Result<SearchFilters> ResolveFilters(const std::vector<std::shared_ptr<Predicate>>& filters,
                                     const arrow::Schema& schema,
                                     const std::vector<std::string>& partition_keys) {
    if (filters.empty()) {
        return SearchFilters();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> filter, PredicateBuilder::And(filters));
    if (PredicateUtils::ContainsFullTextMatch(filter)) {
        return Status::NotImplemented("FullTextMatch predicate execution is not implemented yet");
    }
    PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
        schema, filter, /*validate_field_idx=*/false));
    std::map<std::string, int32_t> partition_key_to_idx =
        ObjectUtils::CreateIdentifierToIndexMap(partition_keys);
    std::vector<std::shared_ptr<Predicate>> partition_conjuncts;
    std::vector<std::shared_ptr<Predicate>> data_conjuncts;
    for (const auto& conjunct : PredicateUtils::SplitAnd(filter)) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Predicate> partition_conjunct,
            PredicateUtils::CreatePickedFieldFilter(conjunct, partition_key_to_idx));
        if (partition_conjunct) {
            partition_conjuncts.push_back(std::move(partition_conjunct));
            continue;
        }
        data_conjuncts.push_back(conjunct);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> implied_partitions,
                               ImpliedPartitionPredicate(conjunct, partition_key_to_idx));
        if (implied_partitions) {
            partition_conjuncts.push_back(std::move(implied_partitions));
        }
    }
    SearchFilters search_filters;
    if (!partition_conjuncts.empty()) {
        PAIMON_ASSIGN_OR_RAISE(search_filters.partition_predicate,
                               PredicateBuilder::And(partition_conjuncts));
    }
    if (!data_conjuncts.empty()) {
        PAIMON_ASSIGN_OR_RAISE(search_filters.data_filter, PredicateBuilder::And(data_conjuncts));
    }
    return search_filters;
}

Result<std::optional<Snapshot>> ResolveTimeTravel(const CoreOptions& options,
                                                  const std::string& root_path);
}  // namespace

TableSearchSettings::TableSearchSettings(const FileStoreTable& table)
    : table_path_(table.GetRootPath()),
      file_system_(table.GetFileSystem()),
      bound_schema_(checked_pointer_cast<TableSchema>(table.GetSchema())),
      bound_options_(table.options_),
      bound_file_system_(file_system_) {
    bound_options_[Options::BRANCH] = table.CurrentBranch();
}

Result<TableSearchSettings::ResolvedColumn> TableSearchSettings::ResolveVectorColumn(
    const std::string& name, const std::map<std::string, std::string>& vector_options,
    bool* retry_time_travel) const {
    PAIMON_ASSIGN_OR_RAISE(TableSearchContext context, Resolve(retry_time_travel));
    PAIMON_RETURN_NOT_OK(context.ApplyVectorSearchOptions(vector_options));
    PAIMON_ASSIGN_OR_RAISE(DataField field, context.ResolveField(name, SearchKind::VECTOR));
    return ResolvedColumn{std::move(context), std::move(field)};
}

Result<TableSearchContext> TableSearchSettings::Resolve(bool* retry_time_travel) const {
    if (memory_pool_ == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string root_path, PathUtil::NormalizePath(table_path_));
    if (root_path.empty()) {
        return Status::Invalid("cannot search with empty table path");
    }
    std::shared_ptr<TableSchema> table_schema = bound_schema_;
    if (!table_schema) {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options,
                               CoreOptions::FromMap(options_, file_system_));
        std::string branch = BranchManager::NormalizeBranch(tmp_options.GetBranch());
        SchemaManager schema_manager(tmp_options.GetFileSystem(), root_path, branch);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                               schema_manager.Latest());
        if (!latest) {
            return Status::Invalid(
                fmt::format("schema file not found in path {}, branch {}", root_path, branch));
        }
        table_schema = *latest;
    }
    PAIMON_ASSIGN_OR_RAISE(TableType table_type,
                           TableTypeDefine::FromOptions(table_schema->Options()));
    if (table_type != TableType::TABLE && table_type != TableType::MATERIALIZED_TABLE) {
        return Status::NotImplemented(
            fmt::format("Table search is not supported for {} with type '{}'", root_path,
                        table_schema->Options().at(Options::TYPE)));
    }
    auto merged_options = bound_schema_ ? bound_options_ : table_schema->Options();
    for (const auto& [key, value] : options_) {
        merged_options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(TableType requested_type, TableTypeDefine::FromOptions(merged_options));
    if (requested_type != table_type) {
        return Status::Invalid("search type contradicts the loaded table");
    }
    if (bound_schema_) {
        if (bound_file_system_ != file_system_) {
            return Status::Invalid("search file system contradicts the loaded table");
        }
        if (options_.count(Options::BRANCH) &&
            BranchManager::NormalizeBranch(options_.at(Options::BRANCH)) !=
                BranchManager::NormalizeBranch(bound_options_.at(Options::BRANCH))) {
            return Status::Invalid("search branch contradicts the loaded table");
        }
        PAIMON_RETURN_NOT_OK(
            SchemaValidation::RejectQueryAuth(*table_schema, bound_options_, "Search"));
        PAIMON_ASSIGN_OR_RAISE(merged_options,
                               CatalogUtils::ResolveQueryAuthReadOptions(
                                   /*catalog=*/nullptr, /*identifier=*/std::nullopt, merged_options,
                                   bound_options_.at(Options::BRANCH), file_system_, root_path,
                                   /*has_specific_table_schema=*/true, "search"));
    }
    PAIMON_RETURN_NOT_OK(
        SchemaValidation::RejectQueryAuth(*table_schema, merged_options, "Search"));
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(merged_options, file_system_));
    const Status startup_status = SchemaValidation::ValidateStartupMode(options);
    if (!startup_status.ok()) {
        if (retry_time_travel) {
            *retry_time_travel = true;
        }
        return startup_status;
    }
    Result<std::optional<Snapshot>> time_travel = ResolveTimeTravel(options, root_path);
    if (!time_travel.ok() && retry_time_travel) {
        *retry_time_travel = true;
        return time_travel.status();
    }
    if (time_travel.ok() && time_travel.value() &&
        time_travel.value()->SchemaId() != table_schema->Id()) {
        SchemaManager manager(options.GetFileSystem(), root_path,
                              BranchManager::NormalizeBranch(options.GetBranch()));
        PAIMON_ASSIGN_OR_RAISE(table_schema, manager.ReadSchema(time_travel.value()->SchemaId()));
        PAIMON_RETURN_NOT_OK(DataEvolutionFullTextRead::BindSchemaOptions(*table_schema, options_,
                                                                          &options, "Search"));
    }
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(SearchFilters search_filters,
                           ResolveFilters(filters_, *arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> external_path_weights,
                           options.GetExternalPathWeights());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> global_index_external_paths,
                           options.CreateGlobalIndexExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            root_path, arrow_schema, table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_paths, options.IndexFileInDataFileDir(), external_path_weights,
            memory_pool_));
    std::vector<std::shared_ptr<Predicate>> conjuncts;
    if (search_filters.partition_predicate) {
        conjuncts.push_back(search_filters.partition_predicate);
    }
    for (const auto& specs : partition_filters_) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<PredicateFilter> predicate,
                               FileStoreScan::CreatePartitionPredicate(
                                   table_schema->PartitionKeys(), options.GetPartitionDefaultName(),
                                   arrow_schema, specs));
        if (predicate) {
            conjuncts.push_back(std::move(predicate));
        }
    }
    std::shared_ptr<PredicateFilter> partition_filter;
    if (!conjuncts.empty()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> combined,
                               PredicateBuilder::And(conjuncts));
        partition_filter = std::dynamic_pointer_cast<PredicateFilter>(combined);
        if (!partition_filter) {
            return Status::Invalid("invalid partition predicate, cannot cast to predicate filter");
        }
    }
    return TableSearchContext{root_path,    table_schema,     std::move(options),
                              path_factory, partition_filter, search_filters.data_filter,
                              memory_pool_, executor_,        std::move(time_travel),
                              options_};
}

Status TableSearchContext::RequireDataEvolution(const char* search) const {
    PAIMON_ASSIGN_OR_RAISE(bool table_has_data_evolution,
                           OptionsUtils::GetValueFromMap<bool>(
                               table_schema->Options(), Options::DATA_EVOLUTION_ENABLED, false));
    if (!table_has_data_evolution || !options.DataEvolutionEnabled() ||
        !options.RowTrackingEnabled() || !table_schema->PrimaryKeys().empty()) {
        return Status::NotImplemented(
            fmt::format("{} search requires a data-evolution table with row tracking", search));
    }
    return Status::OK();
}

Status TableSearchContext::ApplyVectorSearchOptions(
    const std::map<std::string, std::string>& query_options) {
    if (query_options.empty()) {
        return Status::OK();
    }
    // Limit route overrides to search modes and refinement so all routes share one table view.
    std::map<std::string, std::string> merged;
    for (const char* key :
         {Options::GLOBAL_INDEX_SEARCH_MODE, Options::VECTOR_INDEX_SEARCH_MODE,
          Options::SCALAR_INDEX_SEARCH_MODE, Options::GLOBAL_INDEX_FILTER_REFINE_FROM_DATA}) {
        auto it = query_options.find(key);
        if (it != query_options.end()) {
            merged.insert(*it);
        }
    }
    if (!merged.empty()) {
        const auto& table_options = options.ToMap();
        merged.insert(table_options.begin(), table_options.end());
        PAIMON_ASSIGN_OR_RAISE(options, CoreOptions::FromMap(merged, options.GetFileSystem()));
    }
    return Status::OK();
}

Result<DataField> TableSearchContext::ResolveField(const std::string& name, SearchKind kind) const {
    const bool vector = kind == SearchKind::VECTOR;
    if (vector) {
        PAIMON_RETURN_NOT_OK(RequireDataEvolution("Vector"));
        if (options.GetVectorIndexSearchMode() != GlobalIndexSearchMode::FAST) {
            return Status::NotImplemented(fmt::format(
                "vector search only supports the fast {} (or {}); full and detail are not "
                "supported yet",
                Options::VECTOR_INDEX_SEARCH_MODE, Options::GLOBAL_INDEX_SEARCH_MODE));
        }
        if (filter && options.GetScalarIndexSearchMode() != GlobalIndexSearchMode::FAST) {
            return Status::NotImplemented(fmt::format(
                "a vector search with a row filter only supports the fast {} (or {}); full and "
                "detail are not supported yet",
                Options::SCALAR_INDEX_SEARCH_MODE, Options::GLOBAL_INDEX_SEARCH_MODE));
        }
    }
    Result<DataField> field = table_schema->GetField(name);
    if (!field.ok()) {
        return Status::Invalid(
            fmt::format("{} column '{}' does not exist.", vector ? "Vector" : "Text", name));
    }
    return field;
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> TableSearchContext::SearchFullText(
    const DataField& field, const std::optional<Snapshot>& snapshot, const std::string& query,
    int32_t limit) const {
    if (!snapshot) {
        return ScoredGlobalIndexResultUtils::FromRows({});
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DataEvolutionFullTextRead> read,
                           DataEvolutionFullTextRead::Create(
                               root_path, table_schema, options, option_overrides, path_factory,
                               partition_filter, filter, field, query, limit, executor, pool));
    DataEvolutionFullTextScan scan(root_path, table_schema, snapshot, options, path_factory,
                                   partition_filter, filter, field, executor, pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FullTextScan::Plan> plan, scan.CreatePlan());
    return read->Read(plan->Splits(), snapshot);
}

namespace {
Result<std::optional<Snapshot>> ResolveTimeTravel(const CoreOptions& options,
                                                  const std::string& root_path) {
    const char* const time_travel_keys[] = {Options::SCAN_SNAPSHOT_ID, Options::SCAN_TAG_NAME,
                                            Options::SCAN_TIMESTAMP,
                                            Options::SCAN_TIMESTAMP_MILLIS};
    const std::map<std::string, std::string>& raw_options = options.ToMap();
    auto key_iter =
        std::find_if(std::begin(time_travel_keys), std::end(time_travel_keys),
                     [&raw_options](const char* key) { return raw_options.count(key) > 0; });
    if (key_iter == std::end(time_travel_keys)) {
        return std::optional<Snapshot>();
    }
    const std::string branch = BranchManager::NormalizeBranch(options.GetBranch());
    SnapshotManager snapshot_manager(options.GetFileSystem(), root_path, branch);
    const std::string key = *key_iter;
    if (key == Options::SCAN_SNAPSHOT_ID) {
        const std::optional<int64_t>& snapshot_id = options.GetScanSnapshotId();
        if (!snapshot_id) {
            return Status::Invalid(fmt::format("invalid {}", key));
        }
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot,
                               snapshot_manager.LoadSnapshot(snapshot_id.value()));
        return std::optional<Snapshot>(std::move(snapshot));
    }
    if (key == Options::SCAN_TAG_NAME) {
        std::optional<std::string> tag_name = options.GetScanTagName();
        if (!tag_name) {
            return Status::Invalid(fmt::format("invalid {}", key));
        }
        TagManager tag_manager(options.GetFileSystem(), root_path, branch);
        PAIMON_ASSIGN_OR_RAISE(Tag tag, tag_manager.GetOrThrow(tag_name.value()));
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, tag.TrimToSnapshot());
        return std::optional<Snapshot>(std::move(snapshot));
    }
    std::optional<int64_t> timestamp_millis = options.GetScanTimestampMillis();
    if (!timestamp_millis) {
        return Status::Invalid(fmt::format("invalid {}", key));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> snapshot,
                           snapshot_manager.EarlierOrEqualTimeMillis(timestamp_millis.value()));
    if (!snapshot) {
        return Status::Invalid(
            fmt::format("There is currently no snapshot earlier than or equal to timestamp [{}]",
                        timestamp_millis.value()));
    }
    return snapshot;
}
}  // namespace

Result<std::optional<Snapshot>> TableSearchContext::ResolveSnapshot() const {
    if (!time_travel.ok() || time_travel.value()) {
        return time_travel;
    }
    SnapshotManager manager(options.GetFileSystem(), root_path,
                            BranchManager::NormalizeBranch(options.GetBranch()));
    return manager.LatestSnapshot();
}

}  // namespace paimon
