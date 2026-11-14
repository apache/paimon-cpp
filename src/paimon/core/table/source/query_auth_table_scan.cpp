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

#include "paimon/core/table/source/query_auth_table_scan.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <optional>
#include <utility>
#include <vector>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/query_auth_catalog.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/query_auth_split.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"

namespace paimon {
namespace {

std::set<std::string> Intersect(const std::set<std::string>& left,
                                const std::set<std::string>& right) {
    std::set<std::string> result;
    for (const std::string& value : left) {
        if (right.count(value) > 0) {
            result.insert(value);
        }
    }
    return result;
}

Status CheckNoMaskedPartitionFilter(const std::set<std::string>& partition_filter_fields,
                                    const std::set<std::string>& masked_fields) {
    const std::set<std::string> masked = Intersect(partition_filter_fields, masked_fields);
    if (!masked.empty()) {
        return Status::NotImplemented(fmt::format(
            "A partition filter cannot be enforced on masked partition key '{}': it is matched "
            "against the raw partition value. Filter on the column through the predicate instead.",
            *masked.begin()));
    }
    return Status::OK();
}

const DataField* FindField(const std::vector<DataField>& fields, const std::string& name) {
    auto iter = std::find_if(fields.begin(), fields.end(),
                             [&](const DataField& field) { return field.Name() == name; });
    return iter != fields.end() ? &*iter : nullptr;
}

std::string FieldIdOrAbsent(const DataField* field) {
    return field != nullptr ? std::to_string(field->Id()) : std::string("absent");
}

}  // namespace

Result<std::unique_ptr<TableScan>> QueryAuthTableScan::Create(
    const std::shared_ptr<ScanContext>& context, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<FileSystem>& file_system, const std::string& branch,
    const std::shared_ptr<Catalog>& catalog, const Identifier& identifier,
    const std::string& table_path, InnerScanFactory create_inner) {
    if (catalog == nullptr) {
        return Status::NotImplemented(fmt::format(
            "Cannot read a table with '{}=true' without the catalog that serves its row filters "
            "and column masks: load the table through its catalog.",
            Options::QUERY_AUTH_ENABLED));
    }
    const QueryAuthCatalog* auth_catalog = AsQueryAuthCatalog(catalog);
    if (auth_catalog == nullptr) {
        return Status::NotImplemented(fmt::format(
            "Cannot read a table with '{}=true': its catalog does not support the query auth "
            "interface.",
            Options::QUERY_AUTH_ENABLED));
    }
    if (context->GetGlobalIndexResult() != nullptr) {
        return Status::NotImplemented(
            "Search is not supported on a query-auth table: the index ranks raw values, which a "
            "column mask invalidates.");
    }
    if (context->GetRealtimeContext() != nullptr) {
        return Status::NotImplemented("Real-time reads are not supported on a query-auth table.");
    }
    // The rules are asked for the branch planned, even when only the options name it.
    PAIMON_ASSIGN_OR_RAISE(std::string resolved_branch,
                           BranchManager::ResolveBranch(identifier, {}, branch, "scan"));
    PAIMON_ASSIGN_OR_RAISE(Identifier auth_identifier,
                           CatalogUtils::ResolvedBranchIdentifier(identifier, resolved_branch));
    PAIMON_RETURN_NOT_OK(CatalogUtils::CheckPathIsTableLocation(
        auth_identifier, catalog->GetTableLocation(auth_identifier), table_path, "scan"));
    std::set<std::string> user_filter_fields;
    std::set<std::string> partition_filter_fields;
    const std::shared_ptr<ScanFilter>& scan_filter = context->GetScanFilters();
    if (scan_filter != nullptr) {
        if (scan_filter->GetPredicate() != nullptr) {
            PAIMON_RETURN_NOT_OK(
                PredicateUtils::GetAllNames(scan_filter->GetPredicate(), &user_filter_fields));
        }
        for (const auto& partition : scan_filter->GetPartitionFilters()) {
            for (const auto& [key, value] : partition) {
                partition_filter_fields.insert(key);
            }
        }
    }
    // As in Java, the operands of the filters are authorized along with the projected columns.
    const std::optional<std::vector<std::string>>& read_field_names = context->GetReadFieldNames();
    std::vector<std::string> requested = read_field_names.value_or(std::vector<std::string>());
    requested.insert(requested.end(), user_filter_fields.begin(), user_filter_fields.end());
    requested.insert(requested.end(), partition_filter_fields.begin(),
                     partition_filter_fields.end());
    std::vector<std::string> columns;
    std::set<std::string> seen;
    for (const std::string& name : requested) {
        if (!table_schema->GetField(name).ok()) {
            return Status::NotImplemented(fmt::format(
                "Column '{}' is not a column of the table schema, which is all a query-auth scan "
                "can authorize.",
                name));
        }
        if (seen.insert(name).second) {
            columns.push_back(name);
        }
    }
    std::optional<std::vector<std::string>> authorization_columns;
    if (read_field_names) {
        authorization_columns = std::move(columns);
    }
    return std::unique_ptr<TableScan>(
        new QueryAuthTableScan(context, auth_catalog, std::move(auth_identifier), table_schema,
                               file_system, resolved_branch, catalog, table_path,
                               std::move(create_inner), std::move(authorization_columns),
                               std::move(user_filter_fields), std::move(partition_filter_fields)));
}

QueryAuthTableScan::QueryAuthTableScan(
    const std::shared_ptr<ScanContext>& context, const QueryAuthCatalog* auth_catalog,
    Identifier auth_identifier, const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<FileSystem>& file_system, const std::string& branch,
    const std::shared_ptr<Catalog>& catalog, const std::string& table_path,
    InnerScanFactory create_inner, std::optional<std::vector<std::string>> authorization_columns,
    std::set<std::string> user_filter_fields, std::set<std::string> partition_filter_fields)
    : context_(context),
      catalog_(catalog),
      table_path_(table_path),
      auth_catalog_(auth_catalog),
      auth_identifier_(std::move(auth_identifier)),
      table_schema_(table_schema),
      file_system_(file_system),
      branch_(branch),
      create_inner_(std::move(create_inner)),
      authorization_columns_(std::move(authorization_columns)),
      user_filter_fields_(std::move(user_filter_fields)),
      partition_filter_fields_(std::move(partition_filter_fields)) {}

std::shared_ptr<Metrics> QueryAuthTableScan::GetMetrics() const {
    return inner_ != nullptr ? inner_->GetMetrics() : TableScan::GetMetrics();
}

Status QueryAuthTableScan::AddErrorContext(const Status& status) const {
    return status.WithMessage(fmt::format("Query auth of {} (branch {}, schema {}): {}",
                                          auth_identifier_.ToString(), branch_, table_schema_->Id(),
                                          status.message()));
}

Status QueryAuthTableScan::ValidateAgainstCurrentSchemas(const TableQueryAuthResult& rules) const {
    SchemaManager schema_manager(file_system_, table_path_, branch_);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                           schema_manager.Latest());
    const std::shared_ptr<TableSchema>& latest_schema = latest ? latest.value() : table_schema_;
    const std::vector<DataField>& planned_fields = table_schema_->Fields();
    const std::vector<DataField>& latest_fields = latest_schema->Fields();
    // The catalog authorizes the columns by their names in the latest schema, so each must still
    // be the field the scan plans under that name, whether or not the rules mention it.
    std::vector<std::string> columns;
    if (authorization_columns_) {
        columns = authorization_columns_.value();
    } else {
        for (const DataField& field : planned_fields) {
            columns.push_back(field.Name());
        }
    }
    auto validate_columns = [&](const std::vector<DataField>& fields, int64_t schema_id,
                                const char* source) -> Status {
        for (const std::string& column : columns) {
            const DataField* planned = FindField(planned_fields, column);
            const DataField* current = FindField(fields, column);
            if (planned == nullptr || current == nullptr || planned->Id() != current->Id()) {
                return Status::Invalid(fmt::format(
                    "Column '{}' is field {} in schema {} the scan plans with, but field {} in {} "
                    "{}, under which the catalog authorized it. Create the scan again.",
                    column, FieldIdOrAbsent(planned), table_schema_->Id(), FieldIdOrAbsent(current),
                    source, schema_id));
            }
        }
        return Status::OK();
    };
    PAIMON_RETURN_NOT_OK(validate_columns(latest_fields, latest_schema->Id(), "the latest schema"));
    PAIMON_RETURN_NOT_OK(rules.ValidateAgainstSchema(latest_fields));
    PAIMON_RETURN_NOT_OK(rules.ValidateFieldIds(rules.BindFieldIds(latest_fields), planned_fields));
    // A rule column the planned schema does not hold would leave every split unreadable.
    PAIMON_RETURN_NOT_OK(
        rules.ValidateFieldIds(rules.BindFieldIds(planned_fields), planned_fields));

    // The catalog can advance its schema before publishing it under the table path. It
    // authorizes current names, so binding those names only to local field ids could expose an
    // old column that the catalog has dropped and replaced with another of the same name.
    std::string location;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> catalog_schema,
                           auth_catalog_->LoadTableSchemaAndLocation(auth_identifier_, &location));
    // The identifier may have been bound to another table since the scan was created.
    PAIMON_RETURN_NOT_OK(
        CatalogUtils::CheckPathIsTableLocation(auth_identifier_, location, table_path_, "scan"));
    auto catalog_data_schema = std::dynamic_pointer_cast<DataSchema>(catalog_schema);
    if (catalog_data_schema == nullptr) {
        return Status::Invalid("Query auth requires a data table schema from the catalog.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_schema,
                           catalog_data_schema->GetArrowSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_schema.get()));
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> catalog_fields,
                           DataField::ConvertArrowSchemaToDataFields(arrow_schema));
    PAIMON_RETURN_NOT_OK(
        validate_columns(catalog_fields, catalog_data_schema->Id(), "catalog schema"));
    PAIMON_RETURN_NOT_OK(rules.ValidateAgainstSchema(catalog_fields));
    return rules.ValidateFieldIds(rules.BindFieldIds(catalog_fields), planned_fields);
}

Result<std::shared_ptr<TableQueryAuthResult>> QueryAuthTableScan::LoadAndValidateRules() const {
    const auto start = std::chrono::steady_clock::now();
    Result<std::shared_ptr<TableQueryAuthResult>> rules =
        auth_catalog_->AuthTableQuery(auth_identifier_, authorization_columns_);
    Status status = rules.status();
    const char* failed_step = "catalog request";
    if (status.ok()) {
        status = ValidateAgainstCurrentSchemas(*rules.value());
        failed_step = "validation";
    }
    if (logger_->IsLevelEnabled(PAIMON_LOG_LEVEL_DEBUG)) {
        const std::string outcome =
            status.ok() ? fmt::format("{} filters, {} masks", rules.value()->Filter().size(),
                                      rules.value()->ColumnMasking().size())
                        : fmt::format("{} failed with {}", failed_step, status.CodeAsString());
        const int64_t elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                       std::chrono::steady_clock::now() - start)
                                       .count();
        const std::string message = fmt::format(
            "query auth of {} (branch {}, schema {}) took {} us: {}", auth_identifier_.ToString(),
            branch_, table_schema_->Id(), elapsed_us, outcome);
        PAIMON_LOG_DEBUG(logger_, "%s", message.c_str());
    }
    if (!status.ok()) {
        return AddErrorContext(status);
    }
    return rules;
}

Result<std::unique_ptr<TableScan>> QueryAuthTableScan::CreateInnerScan(
    const TableQueryAuthResult& rules) const {
    const std::set<std::string> masked_fields = rules.MaskedFields();
    PAIMON_RETURN_NOT_OK(CheckNoMaskedPartitionFilter(partition_filter_fields_, masked_fields));
    const std::shared_ptr<ScanFilter>& scan_filter = context_->GetScanFilters();
    std::vector<std::shared_ptr<Predicate>> pushed;
    // Raw statistics of a masked column must not prune: the read evaluates those conjuncts on the
    // masked values, so they drop rows only then and a pushed limit could stop short of them.
    const bool masked_conjuncts_excluded = !Intersect(user_filter_fields_, masked_fields).empty();
    if (scan_filter != nullptr && scan_filter->GetPredicate() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<Predicate> unmasked,
            PredicateUtils::ExcludePredicateWithFields(scan_filter->GetPredicate(), masked_fields));
        if (unmasked != nullptr) {
            pushed.push_back(unmasked);
        }
    }
    // The row filter only removes what the read would drop anyway, so it can prune.
    if (rules.RowFilter() != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> row_filter,
                               PredicateUtils::CreatePickedFieldFilter(
                                   rules.RowFilter(), ObjectUtils::CreateIdentifierToIndexMap(
                                                          table_schema_->FieldNames())));
        if (row_filter != nullptr) {
            pushed.push_back(row_filter);
        }
    }
    std::shared_ptr<Predicate> pushed_predicate;
    if (!pushed.empty()) {
        PAIMON_ASSIGN_OR_RAISE(pushed_predicate, PredicateBuilder::And(pushed));
    }
    auto pushed_filter = std::make_shared<ScanFilter>(
        pushed_predicate,
        scan_filter != nullptr ? scan_filter->GetPartitionFilters()
                               : std::vector<std::map<std::string, std::string>>(),
        scan_filter != nullptr ? scan_filter->GetBucketFilter() : std::nullopt);
    // As in Java, a row filter on data columns turns the limit off too: it is pushed down as a
    // predicate, which the inner scan does not push a limit past.
    auto inner_context = std::make_shared<ScanContext>(
        context_->IsStreamingMode(),
        masked_conjuncts_excluded ? std::optional<int32_t>() : context_->GetLimit(), pushed_filter,
        /*global_index_result=*/nullptr, /*realtime_context=*/nullptr, context_->GetMemoryPool(),
        context_->GetExecutor(), context_->GetCache(), context_->GetReadFieldNames());
    return create_inner_(inner_context);
}

Status QueryAuthTableScan::ValidatePushdownCompatibility(const TableQueryAuthResult& rules) const {
    const std::set<std::string> masked_fields = rules.MaskedFields();
    PAIMON_RETURN_NOT_OK(CheckNoMaskedPartitionFilter(partition_filter_fields_, masked_fields));
    // A pushed row filter that changed would prune the rows the new one lets through, and a
    // column masked since the predicate on it was pushed would have pruned by its raw values.
    if (!rules.SameFilter(*pushed_rules_) ||
        !ObjectUtils::ContainsAll(pushed_rules_->MaskedFields(),
                                  Intersect(user_filter_fields_, masked_fields))) {
        return Status::Invalid(
            "Query auth rules changed since the scan pushed down its filters. Recreate the scan "
            "to apply the new rules.");
    }
    return Status::OK();
}

Result<std::shared_ptr<Plan>> QueryAuthTableScan::CreatePlan() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableQueryAuthResult> rules, LoadAndValidateRules());
    if (inner_ == nullptr) {
        Result<std::unique_ptr<TableScan>> inner = CreateInnerScan(*rules);
        if (!inner.ok()) {
            return AddErrorContext(inner.status());
        }
        inner_ = std::move(inner).value();
        pushed_rules_ = rules;
    } else {
        Status compatible = ValidatePushdownCompatibility(*rules);
        if (!compatible.ok()) {
            return AddErrorContext(compatible);
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, inner_->CreatePlan());
    // Bound to the schema planned, so the read can tell a column since renamed or replaced, and
    // one added since, from the columns the rules were authorized for.
    QueryAuthScope scope;
    scope.table_path = table_path_;
    scope.branch = branch_;
    for (const DataField& field : table_schema_->Fields()) {
        if (!authorization_columns_ ||
            ObjectUtils::Contains(authorization_columns_.value(), field.Name())) {
            scope.authorized_field_ids.emplace(field.Name(), field.Id());
        }
    }
    scope.rule_field_ids = rules->BindFieldIds(table_schema_->Fields());
    // Every split carries the rules, none or not: the read refuses a split planned without them.
    std::vector<std::shared_ptr<Split>> splits;
    splits.reserve(plan->Splits().size());
    for (const std::shared_ptr<Split>& split : plan->Splits()) {
        splits.push_back(std::make_shared<QueryAuthSplit>(split, rules, scope));
    }
    return std::shared_ptr<Plan>(std::make_shared<PlanImpl>(plan->SnapshotId(), splits));
}

}  // namespace paimon
