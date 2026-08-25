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

#include "paimon/table/source/table_read.h"

#include <cassert>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/reader/concat_batch_reader.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/format/format_table_loader.h"
#include "paimon/core/table/format/format_table_read.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/fallback_table_read.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/core/table/system/system_table.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"

namespace paimon {
class DataSplit;
class Executor;
class MemoryPool;

namespace {

/// @param loaded_schema The schema of `branch`, when the caller already read it, or null when it
///        has to be read here. `TableRead::Create()` reads one to dispatch on its table type, and
///        reading it again for the branch it came from would cost a second listing and read.
Result<std::unique_ptr<InternalReadContext>> CreateInternalReadContext(
    const std::shared_ptr<ReadContext>& context, const std::string& branch,
    const std::shared_ptr<TableSchema>& loaded_schema) {
    std::map<std::string, std::string> tmp_options = context->GetOptions();
    std::shared_ptr<TableSchema> table_schema;
    const auto& specific_table_schema = context->GetSpecificTableSchema();
    if (loaded_schema != nullptr) {
        table_schema = loaded_schema;
    } else if (branch == BranchManager::DEFAULT_MAIN_BRANCH && specific_table_schema) {
        PAIMON_ASSIGN_OR_RAISE(table_schema,
                               TableSchema::CreateFromJson(specific_table_schema.value()));
    } else {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions tmp_core_options,
                               CoreOptions::FromMap(tmp_options, context->GetSpecificFileSystem(),
                                                    context->GetFileSystemSchemeToIdentifierMap()));
        SchemaManager schema_manager(tmp_core_options.GetFileSystem(), context->GetPath(), branch);
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                               schema_manager.Latest());
        if (!latest_schema) {
            return Status::Invalid(fmt::format("schema file not found in path {}, branch {}",
                                               context->GetPath(), branch));
        }
        table_schema = latest_schema.value();
    }
    assert(table_schema);

    // merge options
    auto options = table_schema->Options();
    for (const auto& [key, value] : tmp_options) {
        options[key] = value;
    }
    if (branch != BranchManager::DEFAULT_MAIN_BRANCH) {
        options[Options::BRANCH] = branch;
    }
    return InternalReadContext::Create(context, table_schema, options);
}

Result<std::unique_ptr<TableRead>> CreateTableRead(
    const std::shared_ptr<InternalReadContext>& internal_context,
    const std::shared_ptr<MemoryPool>& memory_pool, const std::shared_ptr<Executor>& executor) {
    const auto& core_options = internal_context->GetCoreOptions();
    const auto& table_schema = internal_context->GetTableSchema();
    if (internal_context->GetRealtimeContext() && !core_options.RealtimeEnabled()) {
        return Status::Invalid("real-time read requires realtime.enabled=true");
    }
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                           core_options.CreateExternalPaths());
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                           core_options.CreateGlobalIndexExternalPath());

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileStorePathFactory> path_factory,
        FileStorePathFactory::Create(
            internal_context->GetPath(), arrow_schema, table_schema->PartitionKeys(),
            core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
            core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
            external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
            memory_pool));

    if (internal_context->GetPrimaryKeys().empty()) {
        return std::make_unique<AppendOnlyTableRead>(path_factory, internal_context, memory_pool,
                                                     executor);
    }
    return KeyValueTableRead::Create(path_factory, internal_context, memory_pool, executor);
}

Result<std::unique_ptr<TableRead>> NewDataTableRead(
    const std::shared_ptr<ReadContext>& context,
    const std::shared_ptr<TableSchema>& loaded_schema) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InternalReadContext> internal_context,
                           CreateInternalReadContext(context, context->GetBranch(), loaded_schema));

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableRead> table_read,
        CreateTableRead(internal_context, context->GetMemoryPool(), context->GetExecutor()));

    std::optional<std::string> scan_fallback_branch =
        internal_context->GetCoreOptions().GetScanFallbackBranch();
    if (!scan_fallback_branch ||
        StringUtils::IsNullOrWhitespaceOnly(scan_fallback_branch.value())) {
        return std::move(table_read);
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> fallback_context,
        CreateInternalReadContext(context, /*branch=*/scan_fallback_branch.value(),
                                  /*loaded_schema=*/nullptr));

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableRead> fallback_table_read,
        CreateTableRead(fallback_context, context->GetMemoryPool(), context->GetExecutor()));
    return std::make_unique<FallbackTableRead>(
        std::move(table_read), std::move(fallback_table_read), context->GetMemoryPool());
}

}  // namespace

TableRead::TableRead(const std::shared_ptr<MemoryPool>& memory_pool) : pool_(memory_pool) {}

namespace {

/// Maps a `ReadContext` onto `FormatTableRead`, which reads everything it needs out of the context
/// itself: the columns, the predicate, the pool and executor, and what a file is opened with. It
/// refuses by name what a format table cannot honour.
Result<std::unique_ptr<TableRead>> NewFormatTableRead(const std::shared_ptr<FormatTable>& table,
                                                      const std::shared_ptr<ReadContext>& context) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableRead> read,
                           FormatTableRead::Create(table, context));
    return std::unique_ptr<TableRead>(std::move(read));
}

}  // namespace

Result<std::unique_ptr<TableRead>> TableRead::Create(std::unique_ptr<ReadContext> ctx) {
    std::shared_ptr<ReadContext> context = std::move(ctx);
    if (context == nullptr) {
        return Status::Invalid("read context is null pointer");
    }
    if (context->GetMemoryPool() == nullptr) {
        return Status::Invalid("memory pool is null pointer");
    }
    if (context->GetExecutor() == nullptr) {
        return Status::Invalid("executor is null pointer");
    }
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_core_options,
        CoreOptions::FromMap(context->GetOptions(), context->GetSpecificFileSystem(),
                             context->GetFileSystemSchemeToIdentifierMap()));
    PAIMON_ASSIGN_OR_RAISE(std::optional<SystemTablePath> system_table_path,
                           SystemTableLoader::TryParsePath(context->GetPath()));
    if (system_table_path) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<SystemTable> system_table,
            SystemTableLoader::LoadFromPath(tmp_core_options.GetFileSystem(), context->GetPath(),
                                            context->GetOptions()));
        return system_table->NewRead(context);
    }

    // A format table has no manifest to read its files through, and a file another engine wrote
    // carries no field ids, so which files there are and how a row is put back together is its
    // own. How a file is opened is not: both paths go through `DataFileReaderFactory`, so prefetch
    // and the cache apply either way. One `TableRead` interface serves both, as Java Paimon serves
    // both through one `ReadBuilder`.
    std::shared_ptr<TableSchema> latest_schema;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FormatTable> format_table,
        FormatTableLoader::TryLoad(tmp_core_options.GetFileSystem(), context->GetPath(),
                                   context->GetBranch(), context->GetOptions(),
                                   context->GetSpecificTableSchema(),
                                   /*schema_manager=*/nullptr, &latest_schema));
    if (format_table != nullptr) {
        return NewFormatTableRead(format_table, context);
    }

    // With the schema the dispatch already read, so the managed path does not read it again.
    return NewDataTableRead(context, latest_schema);
}

Result<std::unique_ptr<BatchReader>> TableRead::CreateReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    std::vector<std::unique_ptr<BatchReader>> batch_readers;
    batch_readers.reserve(splits.size());
    for (const auto& split : splits) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, CreateReader(split));
        batch_readers.emplace_back(std::move(reader));
    }
    return std::make_unique<ConcatBatchReader>(std::move(batch_readers), pool_);
}

Result<std::unique_ptr<CountReader>> TableRead::CreateCountReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    (void)splits;
    return Status::NotImplemented("CreateCountReader is not implemented for this table type");
}

}  // namespace paimon
