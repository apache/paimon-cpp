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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/core/table/file_store_table.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/sensitive_config_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/catalog/catalog_utils.h"
#include "paimon/core/catalog/commit_lock.h"
#include "paimon/core/catalog/renaming_snapshot_commit.h"
#include "paimon/core/catalog/snapshot_commit.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/pk/primary_key_index_definitions.h"
#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/operation/expire_snapshots.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/append_only_file_store_table.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/table/primary_key_file_store_table.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/data_evolution_batch_scan.h"
#include "paimon/core/table/source/data_table_batch_scan.h"
#include "paimon/core/table/source/data_table_stream_scan.h"
#include "paimon/core/table/source/fallback_table_read.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/core/table/source/primary_key_index_batch_scan.h"
#include "paimon/core/table/source/read_optimized_scan_options.h"
#include "paimon/core/table/source/realtime_table_scan.h"
#include "paimon/core/table/source/snapshot/snapshot_reader.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/context_utils.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/read_context.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/write_context.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class Executor;
class MemoryPool;

FileStoreTable::FileStoreTable(const std::shared_ptr<FileStore>& store,
                               const Identifier& identifier, const std::string& uuid)
    : Table(store->GetFileSystem(), store->GetRootPath(), store->GetSchema(), identifier, uuid),
      store_(store) {}

Result<std::shared_ptr<FileStoreTable>> FileStoreTable::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
    const std::shared_ptr<Schema>& schema, const Identifier& identifier, const std::string& uuid,
    const std::string& branch, const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::map<std::string, std::string>& fs_scheme_map,
    const std::map<std::string, std::string>& dynamic_options) {
    // The kind the schema's primary keys name, which no schema change can add or drop, so the kind
    // the table is built as is the kind every schema its root path ever held describes. A caller
    // without a schema gets the append-only kind, whose operators report that none was found; a
    // schema that is not a table schema - a system table's read-only one - is refused, not cast.
    std::shared_ptr<TableSchema> table_schema;
    if (schema != nullptr) {
        table_schema = std::dynamic_pointer_cast<TableSchema>(schema);
        if (table_schema == nullptr) {
            return Status::Invalid(
                fmt::format("{} is not a managed table schema", identifier.ToString()));
        }
    }
    // The store parses the schema's options, so one that does not parse is refused here.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStore> store,
                           FileStore::Create(file_system, root_path, table_schema, branch,
                                             schema_manager, snapshot_manager, fs_scheme_map));
    std::shared_ptr<FileStoreTable> table;
    if (table_schema != nullptr && !table_schema->PrimaryKeys().empty()) {
        table = std::make_shared<PrimaryKeyFileStoreTable>(store, identifier, uuid);
    } else {
        table = std::make_shared<AppendOnlyFileStoreTable>(store, identifier, uuid);
    }
    table->dynamic_options_ = dynamic_options;
    return table;
}

Result<std::shared_ptr<Table>> FileStoreTable::Copy(
    const std::map<std::string, std::string>& dynamic_options) const {
    std::shared_ptr<TableSchema> loaded_schema = std::static_pointer_cast<TableSchema>(schema_);
    if (loaded_schema == nullptr) {
        return Status::Invalid(fmt::format("not found latest schema of {}", FullName()));
    }
    // `type` is structural and read from the schema alone, the promise `Copy` makes: an option
    // given at the call must not restyle the table. `branch` is the same promise - the copy serves
    // the branch this table does, and a caller that means to move has `SwitchToBranch`. Both are
    // dropped before the merge, so the copy's options never speak of a branch it does not serve.
    std::map<std::string, std::string> overridable = dynamic_options;
    overridable.erase(Options::TYPE);
    overridable.erase(Options::BRANCH);
    // The physical layout the data on the path was written by is as immovable as the style or the
    // branch: the copy serves the same data.
    PAIMON_RETURN_NOT_OK(SchemaValidation::CheckImmutableOptions(*loaded_schema, overridable));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> merged_schema,
                           loaded_schema->MergeOptions(overridable));
    // The managers are handed over, not rebuilt, when the copy resolves the same file system this
    // table does: its operators then read the schema cache this table already warmed. The copy
    // resolves from the file system the caller opened this table through, not the one it resolved
    // to, so a file-system option it was given speaks; when one does change the resolution, fresh
    // managers are built over it instead. The scheme map travels the same way.
    const bool fs_resolution_may_change =
        FileStore::FileSystemResolutionMayChange(*loaded_schema, overridable);
    // The dynamic options the copy was given join the ones this table was loaded with - which a
    // copy of a copy carries the same way - so a read's fallback reader serves what the copy's own
    // reader does however many copies deep. The branch was dropped above, so none accumulates, and
    // an option given again replaces the one carried, as the merge above does.
    std::map<std::string, std::string> accumulated = dynamic_options_;
    for (const auto& [key, value] : overridable) {
        accumulated.insert_or_assign(key, value);
    }
    return FileStoreTable::Create(store_->GetSpecifiedFileSystem(), root_path_,
                                  std::shared_ptr<TableSchema>(std::move(merged_schema)),
                                  identifier_, CatalogUuid().value_or(""), CurrentBranch(),
                                  fs_resolution_may_change ? nullptr : GetSchemaManager(),
                                  fs_resolution_may_change ? nullptr : GetSnapshotManager(),
                                  store_->GetFsSchemeMap(), accumulated);
}

Result<std::shared_ptr<Table>> FileStoreTable::SwitchToBranch(const std::string& branch) const {
    const std::string target_branch = BranchManager::NormalizeBranch(branch);
    if (target_branch == CurrentBranch()) {
        // The table already serves this branch, so a copy of it is the same table to the caller.
        return FileStoreTable::Create(store_->GetSpecifiedFileSystem(), root_path_, schema_,
                                      identifier_, CatalogUuid().value_or(""), CurrentBranch(),
                                      GetSchemaManager(), GetSnapshotManager(),
                                      store_->GetFsSchemeMap(), dynamic_options_);
    }
    // The schema of the branch switched to, read from its own directory.
    auto schema_manager = std::make_shared<SchemaManager>(file_system_, root_path_, target_branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                           schema_manager->Latest());
    if (!latest) {
        return Status::Invalid(fmt::format("branch {} does not exist", target_branch));
    }
    // A branch other than main is named in the options the schema carries - the way a read context
    // names it - so what reads through them serves the branch the managers point at.
    std::map<std::string, std::string> overridable;
    if (target_branch != BranchManager::DEFAULT_MAIN_BRANCH) {
        overridable[Options::BRANCH] = target_branch;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableSchema> branch_schema,
                           (*latest)->MergeOptions(overridable));
    // The branch's schema may name another file system than this table's options resolved - a
    // recreation that changed the access configuration - and a manager kept through that
    // disagreement would read the branch's metadata through the backend this table left, so the
    // branch builds its own. The schema is in hand already, so only the cache is lost.
    const std::shared_ptr<SchemaManager> branch_schema_manager =
        (store_->GetSpecifiedFileSystem() == nullptr &&
         (!store_->GetFsSchemeMap().empty() ||
          !FileStore::FileSystemResolutionAgrees(
              branch_schema->Options(), std::static_pointer_cast<TableSchema>(schema_)->Options())))
            ? nullptr
            : schema_manager;
    // The identifier names the branch switched to, as Java's `newCatalogEnvironment` rebuilds it,
    // so `Name()` and `FullName()` answer for the table served rather than the one left behind.
    PAIMON_ASSIGN_OR_RAISE(Identifier branch_identifier,
                           CatalogUtils::BranchIdentifier(identifier_, target_branch));
    return FileStoreTable::Create(store_->GetSpecifiedFileSystem(), root_path_,
                                  std::shared_ptr<TableSchema>(std::move(branch_schema)),
                                  branch_identifier, CatalogUuid().value_or(""), target_branch,
                                  branch_schema_manager, /*snapshot_manager=*/nullptr,
                                  store_->GetFsSchemeMap(), /*dynamic_options=*/{});
}

Status FileStoreTable::RestoreRealtimeCommittedProgress(
    const std::shared_ptr<RealtimeContext>& realtime_context,
    const std::shared_ptr<SnapshotManager>& snapshot_manager, const CoreOptions& options) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                           RealtimeContextImpl::Cast(realtime_context));
    PAIMON_RETURN_NOT_OK(realtime_context_impl->CheckUsable());
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                           snapshot_manager->LatestSnapshot());
    if (latest_snapshot) {
        PAIMON_ASSIGN_OR_RAISE(
            RealtimeOffsetMap realtime_committed_offsets,
            RealtimeCommitProperties::ReadOffsets(latest_snapshot, options.GetFileSystem()));
        PAIMON_RETURN_NOT_OK(realtime_context_impl->AdvanceCommittedProgress(
            latest_snapshot->Id(), realtime_committed_offsets, /*committed_data_files=*/{}));
    }
    return Status::OK();
}

Status FileStoreTable::CheckOptionsAgreeWithTable(
    const TableSchema& table_schema, const std::map<std::string, std::string>& context_options) {
    const std::map<std::string, std::string>& table_options = table_schema.Options();
    for (const auto& [key, value] : context_options) {
        auto table_option = table_options.find(key);
        if (table_option != table_options.end() && table_option->second != value) {
            // A value may be a credential and the error travels into logs, so it is redacted.
            return Status::Invalid(
                fmt::format("option '{}' is '{}' in the context but '{}' on the table", key,
                            SensitiveConfigUtils::RedactValue(key, value),
                            SensitiveConfigUtils::RedactValue(key, table_option->second)));
        }
    }
    return Status::OK();
}

std::unique_ptr<FileStoreScan> FileStoreTable::WithTablePath(std::unique_ptr<FileStoreScan>&& scan,
                                                             const std::string& root_path) {
    scan->WithTablePath(root_path);
    return std::move(scan);
}

Result<std::unique_ptr<FileStoreWrite>> FileStoreTable::NewWrite(
    std::unique_ptr<WriteContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "write"));
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    // A catalog write names its table; the file system comes from the table's own credentials.
    if (specific_fs == nullptr && ctx->GetFileSystemSchemeToIdentifierMap().empty() &&
        ctx->GetCatalog() != nullptr && !ctx->GetIdentifier()) {
        return Status::Invalid("a catalog write requires a table identifier");
    }
    // The write goes to the branch the table serves, not the context's own.
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           CatalogUtils::BranchIdentifier(ctx->GetIdentifier(), CurrentBranch()));
    const std::shared_ptr<TableSchema>& schema = store_->GetSchema();
    if (schema == nullptr) {
        return Status::Invalid(
            fmt::format("not found latest schema in branch {}", CurrentBranch()));
    }
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*schema, ctx->GetOptions()));
    CoreOptions options = store_->GetOptions();
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*schema, options));
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateVectorStore(*schema, options));
    if (options.GetVectorFileFormat() && !ctx->GetWriteSchema().empty()) {
        return Status::NotImplemented(
            "Dedicated VECTOR storage does not yet support custom write schemas or partial writes");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> file_store_path_factory,
        store_->NewPathFactory(arrow_schema, options,
                               options.GetWriteFileFormat(/*level=*/0)->Identifier(),
                               ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SnapshotManager> snapshot_manager,
        store_->NewSnapshotManager(options, ctx->GetCatalog(), catalog_identifier));
    std::shared_ptr<IOManager> io_manager;
    const auto& io_temp_dir = ctx->GetTempDirectory();
    if (!io_temp_dir.empty()) {
        io_manager = std::make_shared<IOManager>(io_temp_dir, options.GetFileSystem());
    }

    bool ignore_previous_files = ctx->IgnorePreviousFiles();
    if (ctx->GetRealtimeContext() && !options.RealtimeEnabled()) {
        return Status::Invalid("real-time write requires realtime.enabled=true");
    }
    if (ctx->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context_impl,
                               RealtimeContextImpl::Cast(ctx->GetRealtimeContext()));
        PAIMON_RETURN_NOT_OK(realtime_context_impl->CheckUsable());
    }
    return CreateWrite(*ctx, arrow_schema, partition_schema, options, file_store_path_factory,
                       snapshot_manager, io_manager, ignore_previous_files);
}

Result<std::unique_ptr<TableScan>> FileStoreTable::NewScan(
    std::unique_ptr<ScanContext> context) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(context, "scan"));
    const std::shared_ptr<TableSchema>& table_schema = store_->GetSchema();
    if (table_schema == nullptr) {
        return Status::Invalid("not found latest schema");
    }
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*table_schema, context->GetOptions()));
    PAIMON_ASSIGN_OR_RAISE(bool read_optimized,
                           OptionsUtils::GetValueFromMap<bool>(context->GetOptions(),
                                                               kReadOptimizedScanOption, false));
    // The branch in the store's options is the table's, so one the context carries is not honoured.
    CoreOptions core_options = store_->GetOptions();
    // The cache is a runtime setting of this scan, not of the table.
    core_options.WithCache(context->GetCache());

    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*table_schema, core_options));

    PAIMON_RETURN_NOT_OK(
        RealtimeTableScan::ValidateScan(*table_schema, core_options, *context, read_optimized));
    // validate options
    if (core_options.GetBucket() == -1) {
        if (!table_schema->PrimaryKeys().empty()) {
            return Status::NotImplemented(fmt::format(
                "do not support pk table bucket={} in scan process", core_options.GetBucket()));
        }
    } else if (core_options.GetBucket() < 1 &&
               !SchemaValidation::IsPostponeBucketTable(*table_schema, core_options.GetBucket())) {
        return Status::Invalid(
            fmt::format("do not support bucket={} in scan process", core_options.GetBucket()));
    }

    // validate schema and scan filter
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    if (context->GetScanFilters() && context->GetScanFilters()->GetPredicate()) {
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *arrow_schema, context->GetScanFilters()->GetPredicate(),
            /*validate_field_idx=*/false));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                           store_->NewPathFactory(arrow_schema, core_options,
                                                  core_options.GetFileFormat()->Identifier(),
                                                  context->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        store_->NewManifestList(core_options, path_factory, context->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Schema> partition_schema,
        FieldMapping::GetPartitionSchema(arrow_schema, table_schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestFile> manifest_file,
                           store_->NewManifestFile(core_options, path_factory, partition_schema,
                                                   context->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStoreScan> file_store_scan,
        NewFileStoreScan(manifest_list, manifest_file, arrow_schema, core_options,
                         context->GetExecutor(), context->GetMemoryPool(), context.get()));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SplitGenerator> split_generator,
                           NewSplitGenerator(core_options));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<IndexFileHandler> index_file_handler,
        store_->NewIndexFileHandler(core_options, path_factory, context->GetMemoryPool()));
    auto snapshot_reader = std::make_shared<SnapshotReader>(
        file_store_scan, path_factory, std::move(split_generator), std::move(index_file_handler));
    const bool pk_table = !table_schema->PrimaryKeys().empty();
    if (read_optimized && pk_table && context->IsStreamingMode()) {
        return Status::NotImplemented(
            "read-optimized system table does not support streaming scan for primary key table");
    }
    if (context->IsStreamingMode()) {
        return std::make_unique<DataTableStreamScan>(core_options, snapshot_reader);
    }
    auto batch_scan = std::make_unique<DataTableBatchScan>(
        /*pk_table=*/pk_table, core_options, snapshot_reader, read_optimized, context->GetLimit(),
        /*realtime_pk_scan=*/pk_table && context->GetRealtimeContext() != nullptr);
    if (context->GetRealtimeContext()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContextImpl> realtime_context,
                               RealtimeContextImpl::Cast(context->GetRealtimeContext()));
        return std::make_unique<RealtimeTableScan>(
            std::move(batch_scan), pk_table, realtime_context, path_factory,
            snapshot_reader->GetSnapshotManager(), core_options.GetFileSystem(),
            context->GetScanFilters(), core_options.GetRealtimeReadViewTtlMillis());
    }
    if (core_options.DataEvolutionEnabled()) {
        return std::make_unique<DataEvolutionBatchScan>(
            root_path_, snapshot_reader, std::move(batch_scan), table_schema,
            context->GetGlobalIndexResult(), core_options, context->GetMemoryPool(),
            context->GetExecutor());
    }
    if (pk_table && !read_optimized && core_options.GlobalIndexEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexDefinitions definitions,
                               PrimaryKeyIndexDefinitions::Create(*table_schema));
        if (!definitions.ScalarDefinitions().empty()) {
            return PrimaryKeyIndexBatchScan::Create(snapshot_reader, std::move(batch_scan),
                                                    table_schema, path_factory, core_options,
                                                    context->GetMemoryPool());
        }
    }
    return batch_scan;
}

Result<std::unique_ptr<TableRead>> FileStoreTable::CreateTableRead(
    const std::shared_ptr<InternalReadContext>& internal_context,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<Executor>& executor) const {
    if (PredicateUtils::ContainsFullTextMatch(internal_context->GetPredicate())) {
        return Status::NotImplemented("FullTextMatch predicate execution is not implemented yet");
    }
    if (!internal_context->GetIndexScoreBindings().empty()) {
        return Status::NotImplemented("Per-field index score output is not implemented yet");
    }
    const auto& core_options = internal_context->GetCoreOptions();
    const auto& table_schema = internal_context->GetTableSchema();
    if (internal_context->GetRealtimeContext() && !core_options.RealtimeEnabled()) {
        return Status::Invalid("real-time read requires realtime.enabled=true");
    }
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*table_schema, core_options));
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        store_->NewPathFactory(arrow_schema, core_options,
                               core_options.GetFileFormat()->Identifier(), memory_pool));
    // The reader is selected by the schema the read serves, not by the table's kind: an
    // append-only table can have a primary-key fallback branch and vice versa, so a reader typed
    // after the main table would fail the fallback's splits.
    if (internal_context->GetPrimaryKeys().empty()) {
        return std::make_unique<AppendOnlyTableRead>(path_factory, internal_context, memory_pool,
                                                     executor);
    }
    return KeyValueTableRead::Create(path_factory, internal_context, memory_pool, executor);
}

Result<std::unique_ptr<TableRead>> FileStoreTable::NewRead(
    std::unique_ptr<ReadContext> context) const {
    std::shared_ptr<ReadContext> read_context = std::move(context);
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(read_context, "read"));
    const std::shared_ptr<TableSchema>& loaded_schema = store_->GetSchema();
    if (loaded_schema == nullptr) {
        return Status::Invalid("not found latest schema");
    }
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*loaded_schema, read_context->GetOptions()));
    // The read reads through the branch the table serves, not the context's own.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal_context,
                           InternalReadContext::CreateForTable(read_context, loaded_schema,
                                                               store_->GetOptions(), root_path_));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> table_read,
                           CreateTableRead(internal_context, read_context->GetMemoryPool(),
                                           read_context->GetExecutor()));

    std::optional<std::string> scan_fallback_branch =
        internal_context->GetCoreOptions().GetScanFallbackBranch();
    if (!scan_fallback_branch ||
        StringUtils::IsNullOrWhitespaceOnly(scan_fallback_branch.value())) {
        return std::move(table_read);
    }

    // The fallback branch is not the table's, so its schema is read through its own manager.
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> fallback_context,
                           InternalReadContext::CreateForFallbackBranch(
                               read_context, /*branch=*/scan_fallback_branch.value(), file_system_,
                               root_path_, dynamic_options_));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> fallback_table_read,
                           CreateTableRead(fallback_context, read_context->GetMemoryPool(),
                                           read_context->GetExecutor()));
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool = internal_context->GetArrowMemoryPool();
    return std::make_unique<FallbackTableRead>(std::move(table_read),
                                               std::move(fallback_table_read), arrow_pool);
}

Result<std::unique_ptr<TableRead>> TableRead::Create(std::unique_ptr<ReadContext> ctx) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "read"));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, FileStoreTableFactory::Create(*ctx));
    return table->NewRead(std::move(ctx));
}

namespace {

bool IsCatalogCommit(const CommitContext& ctx) {
    return AsVersionManaged(ctx.GetCatalog()) != nullptr;
}

Result<std::shared_ptr<SnapshotCommit>> NewSnapshotCommit(
    const CommitContext& ctx, const std::optional<Identifier>& catalog_identifier,
    const CoreOptions& options, const std::shared_ptr<SnapshotManager>& snapshot_manager) {
    if (IsCatalogCommit(ctx)) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitLock> lock,
                               AsVersionManaged(ctx.GetCatalog())
                                   ->CreateCommitLock(catalog_identifier.value(), ctx.GetTableId(),
                                                      ctx.GetCommitUser(), options));
        // Reuse the caller's UUID so table recreation cannot redirect this commit.
        return std::shared_ptr<SnapshotCommit>(std::make_shared<CatalogSnapshotCommit>(
            ctx.GetCatalog(), catalog_identifier.value(), ctx.GetTableId(), std::move(lock)));
    }
    if (ctx.UseRESTCatalogCommit()) {
        if (options.RestCommitAcquireLock()) {
            return Status::Invalid(fmt::format(
                "'{}' needs a commit through a REST catalog with WithCatalog(); a commit table "
                "request built for the caller to send cannot hold a commit lease",
                Options::REST_COMMIT_ACQUIRE_LOCK));
        }
        return std::shared_ptr<SnapshotCommit>(
            std::make_shared<CatalogSnapshotCommit>(ctx.GetTableId()));
    }
    // Catalogs without version management may still supply a table ID.
    if (ctx.GetTableId() && ctx.GetCatalog() == nullptr) {
        return Status::Invalid(
            "a commit which publishes the snapshot by writing it to the table directory sends no "
            "commit table request, so there is nowhere to put a table id; commit through a "
            "catalog with WithCatalog() or ask for the request with UseRESTCatalogCommit()");
    }
    return std::shared_ptr<SnapshotCommit>(
        std::make_shared<RenamingSnapshotCommit>(options.GetFileSystem(), snapshot_manager));
}

}  // namespace

Result<std::unique_ptr<FileStoreCommit>> FileStoreTable::NewCommit(
    std::unique_ptr<CommitContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "commit"));
    PAIMON_RETURN_NOT_OK(CheckVersionManagementImplemented(ctx->GetCatalog()));
    std::shared_ptr<FileSystem> specific_fs = ctx->GetSpecificFileSystem();
    // A catalog commit names its table; the file system comes from the table's own credentials.
    if (specific_fs == nullptr && ctx->GetCatalog() != nullptr && !ctx->GetIdentifier()) {
        return Status::Invalid("a catalog commit requires a table identifier");
    }
    // A commit commits to the root path and branch the table was loaded with, not the context's.
    PAIMON_ASSIGN_OR_RAISE(std::optional<Identifier> catalog_identifier,
                           CatalogUtils::BranchIdentifier(ctx->GetIdentifier(), CurrentBranch()));
    // The id a snapshot records the catalog's current schema by, read through the loader below
    // rather than trusted from a schema this commit will not re-read.
    FileStoreCommitImpl::SchemaIdLoader schema_id_loader;
    if (ctx->GetCatalog() != nullptr) {
        if (!catalog_identifier) {
            return Status::Invalid("a catalog commit requires a table identifier");
        }
        std::shared_ptr<Catalog> catalog = ctx->GetCatalog();
        Identifier identifier = catalog_identifier.value();
        schema_id_loader = [catalog, identifier]() -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema,
                                   catalog->LoadTableSchema(identifier));
            std::shared_ptr<DataSchema> data_schema = std::dynamic_pointer_cast<DataSchema>(schema);
            if (data_schema == nullptr) {
                return Status::Invalid(
                    "the catalog did not hand back a data table schema, which has the id a "
                    "snapshot records");
            }
            return data_schema->Id();
        };
    }
    const std::shared_ptr<TableSchema>& schema = store_->GetSchema();
    if (schema == nullptr) {
        return Status::Invalid(
            fmt::format("not found latest schema in branch {}", CurrentBranch()));
    }
    std::shared_ptr<arrow::Schema> arrow_schema =
        DataField::ConvertDataFieldsToArrowSchema(schema->Fields());
    PAIMON_RETURN_NOT_OK(CheckOptionsAgreeWithTable(*schema, ctx->GetOptions()));
    CoreOptions options = store_->GetOptions();
    PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateRealtime(*schema, options));
    assert(options.GetFileSystem());
    assert(options.GetFileFormat());
    PAIMON_RETURN_NOT_OK(FileStoreCommitImpl::ValidateCommitOptions(options));
    PAIMON_ASSIGN_OR_RAISE(bool is_object_store, FileSystem::IsObjectStore(root_path_));
    // Catalog commits do not require atomic rename on the object store. The inte-test marker is
    // not a table option, so it is read from the context the commit was handed.
    if (is_object_store && !ctx->UseRESTCatalogCommit() &&
        AsVersionManaged(ctx->GetCatalog()) == nullptr &&
        ctx->GetOptions().find("enable-object-store-commit-in-inte-test") ==
            ctx->GetOptions().end()) {
        return Status::NotImplemented(
            "commit operation does not support object store file system for now");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
        store_->NewPartitionComputer(arrow_schema, options, ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        store_->NewPathFactory(arrow_schema, options, options.GetFileFormat()->Identifier(),
                               ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SnapshotManager> snapshot_manager,
        store_->NewSnapshotManager(options, ctx->GetCatalog(), catalog_identifier));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ManifestList> manifest_list,
                           store_->NewManifestList(options, path_factory, ctx->GetMemoryPool()));

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> partition_schema,
                           FieldMapping::GetPartitionSchema(arrow_schema, schema->PartitionKeys()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        store_->NewManifestFile(options, path_factory, partition_schema, ctx->GetMemoryPool()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<IndexManifestFile> index_manifest_file,
        store_->NewIndexManifestFile(options, path_factory, ctx->GetMemoryPool()));

    auto expire_snapshots = std::make_shared<ExpireSnapshots>(
        snapshot_manager, path_factory, manifest_list, manifest_file, index_manifest_file,
        options.GetFileSystem(), options.GetExpireConfig(), options.RealtimeEnabled(),
        ctx->GetExecutor());

    CommitScanner::ScanSupplier scan_supplier =
        NewCommitScanSupplier(snapshot_manager, manifest_list, manifest_file, arrow_schema, options,
                              ctx->GetExecutor(), ctx->GetMemoryPool());

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SnapshotCommit> snapshot_commit,
                           SnapshotCommit::Create(ctx->GetCatalog(), catalog_identifier, options,
                                                  ctx->GetTableId(), ctx->UseRESTCatalogCommit(),
                                                  options.GetFileSystem(), snapshot_manager));

    return std::make_unique<FileStoreCommitImpl>(
        ctx->GetMemoryPool(), ctx->GetExecutor(), arrow_schema, root_path_, ctx->GetCommitUser(),
        options, path_factory, std::move(partition_computer), snapshot_manager, snapshot_commit,
        ctx->IgnoreEmptyCommit(), ctx->AppendCommitCheckConflict(), schema,
        std::move(schema_id_loader), manifest_file, manifest_list, index_manifest_file,
        expire_snapshots, GetSchemaManager(), std::move(scan_supplier));
}

}  // namespace paimon
