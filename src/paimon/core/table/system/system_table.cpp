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

#include "paimon/core/table/system/system_table.h"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/audit_log_system_table.h"
#include "paimon/core/table/system/binlog_system_table.h"
#include "paimon/core/table/system/global_system_tables.h"
#include "paimon/core/table/system/metadata_system_tables.h"
#include "paimon/core/table/system/read_optimized_system_table.h"
#include "paimon/core/table/system/system_table_schema.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/write_context.h"

namespace paimon {
namespace {

using SystemTableFactory = std::function<Result<std::shared_ptr<SystemTable>>(
    const std::shared_ptr<FileSystem>&, const std::string&, const std::shared_ptr<TableSchema>&,
    const std::map<std::string, std::string>&, const std::string&)>;

struct SystemTableRegistryEntry {
    std::string name;
    SystemTableFactory factory;
};

std::map<std::string, std::string> MergeOptions(
    const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& dynamic_options) {
    auto options = table_schema->Options();
    for (const auto& [key, value] : dynamic_options) {
        options[key] = value;
    }
    return options;
}

std::string LoadBranch(const std::map<std::string, std::string>& options) {
    auto branch_iter = options.find(Options::BRANCH);
    return branch_iter == options.end() ? BranchManager::DEFAULT_MAIN_BRANCH : branch_iter->second;
}

const std::vector<SystemTableRegistryEntry>& SystemTableRegistry() {
    static const std::vector<SystemTableRegistryEntry> registry = {
        {OptionsSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& /*dynamic_options*/,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<OptionsSystemTable>(fs, table_path, table_schema);
         }},
        {AuditLogSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& data_table_name) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<AuditLogSystemTable>(
                 fs, table_path, table_schema, data_table_name,
                 MergeOptions(table_schema, dynamic_options));
         }},
        {BinlogSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& data_table_name) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<BinlogSystemTable>(
                 fs, table_path, table_schema, data_table_name,
                 MergeOptions(table_schema, dynamic_options));
         }},
        {ReadOptimizedSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& data_table_name) -> Result<std::shared_ptr<SystemTable>> {
             return std::make_shared<ReadOptimizedSystemTable>(
                 fs, table_path, table_schema, data_table_name,
                 MergeOptions(table_schema, dynamic_options));
         }},
        {SnapshotsSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<SnapshotsSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {SchemasSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<SchemasSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {TagsSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<TagsSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {BranchesSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<BranchesSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {ConsumersSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             return std::make_shared<ConsumersSystemTable>(fs, table_path, LoadBranch(options));
         }},
        {ManifestsSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             auto branch = LoadBranch(options);
             return std::make_shared<ManifestsSystemTable>(fs, table_path, std::move(branch),
                                                           table_schema, std::move(options));
         }},
        {FilesSystemTable::kName,
         [](const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
            const std::shared_ptr<TableSchema>& table_schema,
            const std::map<std::string, std::string>& dynamic_options,
            const std::string& /*data_table_name*/) -> Result<std::shared_ptr<SystemTable>> {
             auto options = MergeOptions(table_schema, dynamic_options);
             auto branch = LoadBranch(options);
             return std::make_shared<FilesSystemTable>(fs, table_path, std::move(branch),
                                                       table_schema, std::move(options));
         }},
    };
    return registry;
}

std::optional<SystemTableFactory> FindSystemTableFactory(const std::string& system_table_name) {
    std::string normalized_name = StringUtils::ToLowerCase(system_table_name);
    for (const auto& entry : SystemTableRegistry()) {
        if (entry.name == normalized_name) {
            return entry.factory;
        }
    }
    return std::nullopt;
}

}  // namespace

SystemTable::SystemTable(const std::shared_ptr<FileSystem>& fs, const std::string& table_path,
                         std::string name)
    : Table(fs, table_path, /*schema=*/nullptr,
            Identifier(Identifier::kUnknownDatabase, "system_table"), /*uuid=*/""),
      name_(std::move(name)) {}

std::string SystemTable::ComposeName(const std::string& data_table_name, const std::string& kind) {
    return data_table_name + Catalog::SYSTEM_TABLE_SPLITTER + kind;
}

Status SystemTable::CheckNoSearch(const std::shared_ptr<ReadContext>& context) {
    if (context->HasVectorOrFullTextSearch()) {
        return Status::NotImplemented(
            "VectorSearch or FullTextSearch does not support system-table reads yet");
    }
    if (PredicateUtils::ContainsFullTextMatch(context->GetPredicate())) {
        return Status::NotImplemented("FullTextMatch predicate execution is not implemented yet");
    }
    return Status::OK();
}

Status SystemTable::AdoptOwnSchema() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> arrow_schema, ArrowSchema());
    schema_ = std::make_shared<SystemTableSchema>(std::move(arrow_schema));
    return Status::OK();
}

Result<std::shared_ptr<Table>> SystemTable::Copy(
    const std::map<std::string, std::string>& dynamic_options) const {
    if (!origin_) {
        // A table no loader built - one of the schema-only handles `LoadForSchema` hands out - has
        // no recorded inputs to rebuild from, the same defensive case the refused write and
        // commit below guard.
        return Status::Invalid(Name(),
                               " was not loaded by a system table loader: copying it is "
                               "not supported");
    }
    if (origin_->context) {
        // Java Paimon's global system tables copy over the options they already carry and ignore
        // the dynamic ones (`CatalogOptionsTable#copy`), so the copy re-enters the same loader.
        return GlobalSystemTableLoader::Load(origin_->kind, *origin_->context);
    }
    const std::shared_ptr<TableSchema>& loaded_schema = origin_->table_schema;
    // `type` is immutable the way `FileStoreTable::Copy` holds it: the kind of a copy is named by
    // the primary keys of the schema it serves, never by this option. A value naming another
    // type is refused, and one naming the same type is dropped.
    auto type_option = dynamic_options.find(Options::TYPE);
    if (type_option != dynamic_options.end()) {
        PAIMON_ASSIGN_OR_RAISE(TableType schema_type,
                               TableTypeDefine::FromOptions(loaded_schema->Options()));
        std::map<std::string, std::string> named{{Options::TYPE, type_option->second}};
        PAIMON_ASSIGN_OR_RAISE(TableType named_type, TableTypeDefine::FromOptions(named));
        if (named_type != schema_type) {
            return Status::Invalid(fmt::format("Change '{}' is not supported yet.", Options::TYPE));
        }
    }
    // `branch` names the branch the copy serves, the way it routes a load: a copy on the branch
    // this view serves keeps the data table's schema, while one on another branch reads that
    // branch's own latest schema and refuses a branch that holds none.
    std::string current_branch = LoadBranch(MergeOptions(loaded_schema, origin_->dynamic_options));
    std::string target_branch = current_branch;
    auto branch_option = dynamic_options.find(Options::BRANCH);
    if (branch_option != dynamic_options.end()) {
        target_branch = BranchManager::NormalizeBranch(branch_option->second);
    }
    std::map<std::string, std::string> accumulated = origin_->dynamic_options;
    accumulated.erase(Options::TYPE);
    accumulated.erase(Options::BRANCH);
    for (const auto& [key, value] : dynamic_options) {
        if (key != Options::TYPE && key != Options::BRANCH) {
            accumulated.insert_or_assign(key, value);
        }
    }
    std::shared_ptr<TableSchema> base_schema = loaded_schema;
    std::string data_table_name = origin_->data_table_name;
    if (target_branch != current_branch) {
        // The branch is read through the file system this table serves, and then served through
        // the schema that branch holds, the same read `FileStoreTable::Copy` makes.
        SchemaManager schema_manager(GetFileSystem(), GetRootPath(), target_branch);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                               schema_manager.Latest());
        if (!latest) {
            return Status::Invalid(fmt::format("branch {} does not exist", target_branch));
        }
        base_schema = *latest;
        // The copy answers with the logical name of the branch it now serves, composed the way
        // `LoadFromPath` composes one: the data table's name with the branch it addresses.
        const std::string& recorded_name =
            data_table_name.empty() ? PathUtil::GetName(GetRootPath()) : data_table_name;
        PAIMON_ASSIGN_OR_RAISE(
            std::string data_table_base,
            Identifier(Identifier::kUnknownDatabase, recorded_name).GetDataTableName());
        data_table_name = Identifier(Identifier::kUnknownDatabase, data_table_base, target_branch)
                              .GetObjectName();
    }
    std::map<std::string, std::string> schema_overridable = accumulated;
    if (target_branch != BranchManager::DEFAULT_MAIN_BRANCH) {
        schema_overridable[Options::BRANCH] = target_branch;
    }
    PAIMON_RETURN_NOT_OK(SchemaValidation::CheckImmutableOptions(*base_schema, accumulated));
    return SystemTableLoader::Load(origin_->kind, GetFileSystem(), GetRootPath(), base_schema,
                                   schema_overridable, data_table_name);
}

Result<std::unique_ptr<FileStoreWrite>> SystemTable::NewWrite(
    std::unique_ptr<WriteContext> /*context*/) const {
    return Status::NotImplemented(Name(), " system table is read-only: writing is not supported");
}

Result<std::unique_ptr<FileStoreCommit>> SystemTable::NewCommit(
    std::unique_ptr<CommitContext> /*context*/) const {
    return Status::NotImplemented(Name(),
                                  " system table is read-only: committing for it is not supported");
}

bool SystemTableLoader::IsSupported(const std::string& system_table_name) {
    return FindSystemTableFactory(system_table_name).has_value();
}

Result<std::shared_ptr<SystemTable>> SystemTableLoader::Load(
    const std::string& system_table_name, const std::shared_ptr<FileSystem>& fs,
    const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& dynamic_options, const std::string& data_table_name) {
    std::optional<SystemTableFactory> factory = FindSystemTableFactory(system_table_name);
    if (factory) {
        // A caller that knows no logical name for the data table - a load from a bare path with no
        // branch - leaves the views to answer with the directory name, as before.
        const std::string effective_name =
            data_table_name.empty() ? PathUtil::GetName(table_path) : data_table_name;
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<SystemTable> system_table,
            factory.value()(fs, table_path, table_schema, dynamic_options, effective_name));
        // The inputs this table was built from are recorded for `Copy`, which merges new dynamic
        // options into them and rebuilds through this same loader, the way Java Paimon's system
        // tables re-wrap `dataTable.copy(dynamicOptions)`.
        SystemTable::Origin origin;
        origin.kind = StringUtils::ToLowerCase(system_table_name);
        origin.table_schema = table_schema;
        origin.dynamic_options = dynamic_options;
        origin.data_table_name = data_table_name;
        system_table->origin_.emplace(std::move(origin));
        PAIMON_RETURN_NOT_OK(system_table->AdoptOwnSchema());
        return system_table;
    }
    return Status::NotImplemented("unsupported system table: ", system_table_name);
}

Result<std::optional<SystemTablePath>> SystemTableLoader::TryParsePath(const std::string& path) {
    std::string table_name = PathUtil::GetName(path);
    std::string parent = PathUtil::GetParentDirPath(path);
    std::string parent_name = PathUtil::GetName(parent);

    // Detect global system table paths: <warehouse>/sys/<table_name>
    if (parent_name == Catalog::SYSTEM_DATABASE_NAME) {
        SystemTablePath system_table_path;
        system_table_path.is_global = true;
        system_table_path.system_table_name = table_name;
        return std::optional<SystemTablePath>(std::move(system_table_path));
    }

    // The database is unknown on a plain table path; only the object name is looked at below.
    Identifier identifier(Identifier::kUnknownDatabase, table_name);
    PAIMON_ASSIGN_OR_RAISE(bool is_system_table, identifier.IsSystemTable());
    if (!is_system_table) {
        return std::optional<SystemTablePath>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::string data_table_name, identifier.GetDataTableName());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> branch, identifier.GetBranchName());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> system_table_name,
                           identifier.GetSystemTableName());
    SystemTablePath system_table_path;
    system_table_path.table_path = PathUtil::JoinPath(parent, data_table_name);
    system_table_path.branch = std::move(branch);
    system_table_path.system_table_name = system_table_name.value();
    return std::optional<SystemTablePath>(std::move(system_table_path));
}

Result<std::shared_ptr<SystemTable>> SystemTableLoader::LoadFromPath(
    const std::shared_ptr<FileSystem>& fs, const std::string& path,
    const std::map<std::string, std::string>& dynamic_options) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path, TryParsePath(path));
    if (!system_table_path) {
        return Status::Invalid("path is not a system table path: ", path);
    }
    const auto& parsed = system_table_path.value();

    // Handle global system tables (under sys/ directory)
    if (parsed.is_global) {
        GlobalSystemTableContext context;
        context.fs = fs;
        // The warehouse is the grandparent of the sys/<table> path
        context.warehouse = PathUtil::GetParentDirPath(PathUtil::GetParentDirPath(path));
        context.catalog_options = dynamic_options;
        // Global tables loaded from path do not have a Catalog reference and
        // cannot enumerate databases/tables. Only tables that don't require
        // catalog enumeration (e.g. catalog_options) will work in this path.
        return GlobalSystemTableLoader::Load(parsed.system_table_name, context);
    }

    SchemaManager schema_manager(fs, parsed.table_path,
                                 parsed.branch.value_or(BranchManager::DEFAULT_MAIN_BRANCH));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager.Latest());
    if (!latest_schema) {
        return Status::NotExist("base table schema not found for system table path: ", path);
    }
    auto options = dynamic_options;
    if (parsed.branch) {
        options[Options::BRANCH] = parsed.branch.value();
    }
    // The logical name of the data table this path addresses, branch included: a `T$branch_dev$ro`
    // path names the view `T$branch_dev$ro`, not the `T$ro` the bare directory name would give
    // it. The branch constructor folds a spelling of the main branch into the bare name.
    const Identifier data_identifier(Identifier::kUnknownDatabase,
                                     PathUtil::GetName(parsed.table_path),
                                     parsed.branch.value_or(std::string()));
    return Load(parsed.system_table_name, fs, parsed.table_path, latest_schema.value(), options,
                data_identifier.GetObjectName());
}

}  // namespace paimon
