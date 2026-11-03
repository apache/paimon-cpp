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

#include "paimon/table/source/full_text_search_builder.h"

#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_evolution_full_text_read.h"
#include "paimon/core/table/source/data_evolution_full_text_scan.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/executor.h"
#include "paimon/format/file_format.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {

class FullTextSearchBuilder::Impl {
 public:
    /// What a scan and a read of the search are created from.
    struct ResolvedSearch {
        std::string root_path;
        std::shared_ptr<TableSchema> table_schema;
        CoreOptions options;
        std::shared_ptr<FileStorePathFactory> path_factory;
        std::shared_ptr<PredicateFilter> partition_filter;
        DataField text_field;
    };

    /// Loads the table and checks the settings shared by scans and reads.
    Result<ResolvedSearch> Resolve() const;

    Status CheckLimit() const;

    std::unique_ptr<FullTextScan> CreateScan(const ResolvedSearch& search) const;

    /// Creates a read of `search`; the limit must have passed `CheckLimit()`.
    Result<std::unique_ptr<FullTextRead>> CreateRead(const ResolvedSearch& search) const;

    std::string table_path_;
    std::vector<std::map<std::string, std::string>> partition_filters_;
    std::optional<int32_t> limit_;
    std::optional<std::string> field_name_;
    std::string query_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> file_system_;
};

namespace {
/// Rejects a column with a primary-key full-text index, which Java searches with
/// `PrimaryKeyFullTextScan` and `PrimaryKeyFullTextRead` instead.
Status CheckNoPrimaryKeyFullTextIndex(const TableSchema& table_schema, const CoreOptions& options,
                                      const DataField& text_field) {
    if (options.DataEvolutionEnabled()) {
        return Status::OK();
    }
    const auto& schema_options = table_schema.Options();
    auto columns = schema_options.find(Options::PK_FULL_TEXT_INDEX_COLUMNS);
    if (columns == schema_options.end() || StringUtils::IsNullOrWhitespaceOnly(columns->second)) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexDefinitions definitions,
                           PrimaryKeyIndexDefinitions::Create(table_schema));
    for (const auto& definition : definitions.Definitions()) {
        if (definition.GetFamily() == PrimaryKeyIndexDefinition::Family::FULL_TEXT &&
            definition.FieldId() == text_field.Id()) {
            return Status::NotImplemented(fmt::format(
                "full-text search of column {} with a primary-key full-text index is not "
                "supported yet",
                text_field.Name()));
        }
    }
    return Status::OK();
}
}  // namespace

Result<FullTextSearchBuilder::Impl::ResolvedSearch> FullTextSearchBuilder::Impl::Resolve() const {
    if (!field_name_) {
        return Status::Invalid("Query must be set via SetQuery()");
    }
    if (memory_pool_ == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    PAIMON_ASSIGN_OR_RAISE(std::string root_path, PathUtil::NormalizePath(table_path_));
    if (root_path.empty()) {
        return Status::Invalid("cannot search with empty table path");
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_options, CoreOptions::FromMap(options_, file_system_));
    std::string branch = BranchManager::NormalizeBranch(tmp_options.GetBranch());
    SchemaManager schema_manager(tmp_options.GetFileSystem(), root_path, branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager.Latest());
    if (!latest_schema) {
        return Status::Invalid(
            fmt::format("schema file not found in path {}, branch {}", root_path, branch));
    }
    std::shared_ptr<TableSchema> table_schema = latest_schema.value();
    std::map<std::string, std::string> merged_options = table_schema->Options();
    for (const auto& [key, value] : options_) {
        merged_options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(merged_options, file_system_));
    if (options.GetFullTextIndexSearchMode() != GlobalIndexSearchMode::FAST) {
        return Status::NotImplemented(fmt::format(
            "full-text search only supports the fast {} (or {}), which searches only the rows "
            "that an index covers; full and detail are not supported yet",
            Options::FULL_TEXT_INDEX_SEARCH_MODE, Options::GLOBAL_INDEX_SEARCH_MODE));
    }

    Result<DataField> text_field = table_schema->GetField(field_name_.value());
    if (!text_field.ok()) {
        return Status::Invalid(
            fmt::format("Text column '{}' does not exist.", field_name_.value()));
    }
    PAIMON_RETURN_NOT_OK(
        CheckNoPrimaryKeyFullTextIndex(*table_schema, options, text_field.value()));

    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths, options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           options.CreateGlobalIndexExternalPath());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            root_path, arrow_schema, table_schema->PartitionKeys(),
            options.GetPartitionDefaultName(), options.GetFileFormat()->Identifier(),
            options.DataFilePrefix(), options.LegacyPartitionNameEnabled(), external_paths,
            global_index_external_path, options.IndexFileInDataFileDir(), memory_pool_));
    std::shared_ptr<PredicateFilter> partition_filter;
    if (!partition_filters_.empty()) {
        PAIMON_ASSIGN_OR_RAISE(partition_filter,
                               FileStoreScan::CreatePartitionPredicate(
                                   table_schema->PartitionKeys(), options.GetPartitionDefaultName(),
                                   arrow_schema, partition_filters_));
    }
    return ResolvedSearch{root_path,    table_schema,     std::move(options),
                          path_factory, partition_filter, std::move(text_field).value()};
}

Status FullTextSearchBuilder::Impl::CheckLimit() const {
    if (!limit_ || limit_.value() <= 0) {
        return Status::Invalid("Limit must be positive, set via SetLimit()");
    }
    return Status::OK();
}

std::unique_ptr<FullTextScan> FullTextSearchBuilder::Impl::CreateScan(
    const ResolvedSearch& search) const {
    return std::make_unique<DataEvolutionFullTextScan>(
        search.root_path, search.table_schema, search.options, search.path_factory,
        search.partition_filter, search.text_field, memory_pool_);
}

Result<std::unique_ptr<FullTextRead>> FullTextSearchBuilder::Impl::CreateRead(
    const ResolvedSearch& search) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DataEvolutionFullTextRead> read,
                           DataEvolutionFullTextRead::Create(
                               search.root_path, search.table_schema, search.options,
                               search.path_factory, search.partition_filter, search.text_field,
                               query_, limit_.value(), executor_, memory_pool_));
    return std::unique_ptr<FullTextRead>(std::move(read));
}

FullTextSearchBuilder::FullTextSearchBuilder(const std::string& table_path)
    : impl_(std::make_unique<FullTextSearchBuilder::Impl>()) {
    impl_->table_path_ = table_path;
}

FullTextSearchBuilder::~FullTextSearchBuilder() = default;

FullTextSearchBuilder& FullTextSearchBuilder::SetPartitionFilter(
    const std::vector<std::map<std::string, std::string>>& partition_filters) {
    impl_->partition_filters_ = partition_filters;
    return *this;
}

FullTextSearchBuilder& FullTextSearchBuilder::SetLimit(int32_t limit) {
    impl_->limit_ = limit;
    return *this;
}

FullTextSearchBuilder& FullTextSearchBuilder::SetQuery(const std::string& field_name,
                                                       const std::string& query) {
    impl_->field_name_ = field_name;
    impl_->query_ = query;
    return *this;
}

FullTextSearchBuilder& FullTextSearchBuilder::AddOption(const std::string& key,
                                                        const std::string& value) {
    impl_->options_[key] = value;
    return *this;
}

FullTextSearchBuilder& FullTextSearchBuilder::SetOptions(
    const std::map<std::string, std::string>& options) {
    impl_->options_ = options;
    return *this;
}

FullTextSearchBuilder& FullTextSearchBuilder::WithMemoryPool(
    const std::shared_ptr<MemoryPool>& memory_pool) {
    impl_->memory_pool_ = memory_pool;
    return *this;
}

FullTextSearchBuilder& FullTextSearchBuilder::WithExecutor(
    const std::shared_ptr<Executor>& executor) {
    impl_->executor_ = executor;
    return *this;
}

FullTextSearchBuilder& FullTextSearchBuilder::WithFileSystem(
    const std::shared_ptr<FileSystem>& file_system) {
    impl_->file_system_ = file_system;
    return *this;
}

Result<std::unique_ptr<FullTextScan>> FullTextSearchBuilder::NewFullTextScan() const {
    PAIMON_ASSIGN_OR_RAISE(Impl::ResolvedSearch search, impl_->Resolve());
    return impl_->CreateScan(search);
}

Result<std::unique_ptr<FullTextRead>> FullTextSearchBuilder::NewFullTextRead() const {
    PAIMON_RETURN_NOT_OK(impl_->CheckLimit());
    PAIMON_ASSIGN_OR_RAISE(Impl::ResolvedSearch search, impl_->Resolve());
    return impl_->CreateRead(search);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> FullTextSearchBuilder::ExecuteLocal() const {
    PAIMON_RETURN_NOT_OK(impl_->CheckLimit());
    // The scan and the read share one load of the table, so they see the same schema.
    PAIMON_ASSIGN_OR_RAISE(Impl::ResolvedSearch search, impl_->Resolve());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FullTextRead> read, impl_->CreateRead(search));
    std::unique_ptr<FullTextScan> scan = impl_->CreateScan(search);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FullTextScan::Plan> plan, scan->CreatePlan());
    return read->Read(*plan);
}

}  // namespace paimon
