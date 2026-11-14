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

#include "paimon/table/format/format_table.h"

#include <optional>

#include "fmt/format.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/format/format_table_file_store_commit.h"
#include "paimon/core/table/format/format_table_file_store_write.h"
#include "paimon/core/table/format/format_table_read.h"
#include "paimon/core/table/format/format_table_scan.h"
#include "paimon/core/utils/context_utils.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/write_context.h"

namespace paimon {

namespace {
constexpr char kDefaultFileFormat[] = "parquet";
}  // namespace

FormatTable::FormatTable(const std::shared_ptr<FileSystem>& file_system,
                         const std::string& location, const Identifier& identifier,
                         const std::shared_ptr<DataSchema>& schema,
                         const std::map<std::string, std::string>& options, Format format,
                         const std::string& file_compression,
                         const std::string& partition_default_name,
                         bool partition_only_value_in_path, bool location_carries_paimon_metadata)
    : Table(file_system, location, schema, identifier, ""),
      options_(options),
      format_(format),
      file_compression_(file_compression),
      partition_default_name_(partition_default_name),
      partition_only_value_in_path_(partition_only_value_in_path),
      location_carries_paimon_metadata_(location_carries_paimon_metadata) {}

FormatTable::~FormatTable() = default;

Result<FormatTable::Format> FormatTable::ParseFormat(const std::string& file_format) {
    std::string normalized = StringUtils::ToLowerCase(file_format);
    if (normalized == "parquet") {
        return Format::PARQUET;
    }
    if (normalized == "orc") {
        return Format::ORC;
    }
    if (normalized == "csv" || normalized == "text" || normalized == "json" ||
        normalized == "mosaic") {
        return Status::NotImplemented(
            fmt::format("format table file format '{}' is not supported by paimon-cpp yet. "
                        "Supported formats: parquet, orc",
                        file_format));
    }
    return Status::Invalid(fmt::format(
        "format table unsupported file format: {}. Supported formats: parquet, orc", file_format));
}

std::string FormatTable::FormatToString(Format format) {
    switch (format) {
        case Format::PARQUET:
            return "parquet";
        case Format::ORC:
            return "orc";
    }
    return "unknown";
}

Result<std::shared_ptr<FormatTable>> FormatTable::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::string& table_path,
    const Identifier& identifier, const std::map<std::string, std::string>& dynamic_options) {
    if (file_system == nullptr) {
        return Status::Invalid("format table requires a file system");
    }
    if (table_path.empty()) {
        return Status::Invalid(
            fmt::format("format table {} requires a location", identifier.GetFullName()));
    }
    PAIMON_ASSIGN_OR_RAISE(bool exist, file_system->Exists(table_path));
    if (!exist) {
        return Status::NotExist(fmt::format("{} not exist", identifier.ToString()));
    }
    SchemaManager schema_manager(file_system, table_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                           schema_manager.Latest());
    if (!latest_schema) {
        return Status::NotExist(
            fmt::format("load table schema for {} failed", identifier.ToString()));
    }
    // The schema was just read from under the table path, so that is where the metadata lives.
    return Create(file_system, table_path, identifier,
                  checked_pointer_cast<DataSchema>(*latest_schema),
                  /*location_carries_paimon_metadata=*/true, dynamic_options);
}

Result<std::shared_ptr<FormatTable>> FormatTable::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::string& location,
    const Identifier& identifier, const std::shared_ptr<DataSchema>& schema,
    bool location_carries_paimon_metadata,
    const std::map<std::string, std::string>& dynamic_options) {
    if (file_system == nullptr) {
        return Status::Invalid("format table requires a file system");
    }
    if (location.empty()) {
        // Every path is checked against the location, and an empty one is a prefix of nothing.
        return Status::Invalid(
            fmt::format("format table {} requires a location", identifier.GetFullName()));
    }
    if (schema == nullptr) {
        return Status::Invalid("format table requires a schema");
    }
    // From the schema alone: `type` is structural, so an option given at the call must not let
    // one read open a managed table as a format table, or hide one.
    PAIMON_ASSIGN_OR_RAISE(bool is_format_table, TableTypeDefine::IsFormatTable(schema->Options()));
    if (!is_format_table) {
        return Status::Invalid(
            fmt::format("table {} is not a format table, its '{}' option is not "
                        "'{}'",
                        identifier.GetFullName(), Options::TYPE, TableTypeDefine::kFormatTable));
    }
    // What the schema stored with anything given at the call on top, the precedence every
    // context builder promises. `type` was read above and is not one of them.
    std::map<std::string, std::string> options = schema->Options();
    for (const auto& [key, value] : dynamic_options) {
        if (key == Options::TYPE) {
            continue;
        }
        options[key] = value;
    }

    // The checks table creation would have run, again: this schema may have come from a rest
    // catalog or straight from a caller. Run against the merged options, so that an option this
    // refuses is refused wherever it came from.
    Status valid = SchemaValidation::ValidateGenericDataSchema(*schema);
    if (valid.ok()) {
        valid = SchemaValidation::ValidateFormatTableSchema(*schema, options, file_system);
    }
    if (!valid.ok()) {
        return Status(valid.code(), fmt::format("cannot open format table {}: {}",
                                                identifier.GetFullName(), valid.message()));
    }

    // Before `CoreOptions`, which resolves `file.format` through the factories and would fail with
    // a missing-factory error where this reports the format by name.
    PAIMON_ASSIGN_OR_RAISE(std::string file_format,
                           OptionsUtils::GetValueFromMap<std::string>(options, Options::FILE_FORMAT,
                                                                      kDefaultFileFormat));
    PAIMON_ASSIGN_OR_RAISE(Format format, ParseFormat(file_format));

    // Everything else through `CoreOptions`, so an option means what it does elsewhere.
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options, file_system));

    return std::shared_ptr<FormatTable>(new FormatTable(
        file_system, location, identifier, schema, options, format,
        core_options.FormatTableFileCompression(), core_options.GetPartitionDefaultName(),
        core_options.FormatTablePartitionOnlyValueInPath(), location_carries_paimon_metadata));
}

Result<std::shared_ptr<FormatTable>> FormatTable::Copy(
    const std::shared_ptr<FormatTable>& table,
    const std::map<std::string, std::string>& dynamic_options) {
    if (table == nullptr) {
        return Status::Invalid("format table requires a table to copy");
    }
    if (dynamic_options.empty()) {
        return table;
    }
    // The options this table already carries go back in as dynamic ones, so an option an earlier
    // call set is not lost to the schema's own value underneath it.
    std::map<std::string, std::string> options = table->options_;
    for (const auto& [key, value] : dynamic_options) {
        options[key] = value;
    }
    return Create(table->file_system_, table->root_path_, table->identifier_, table->GetSchema(),
                  table->location_carries_paimon_metadata_, options);
}

const std::vector<std::string>& FormatTable::PartitionKeys() const {
    return GetSchema()->PartitionKeys();
}

std::string FormatTable::FullName() const {
    return identifier_.GetFullName();
}

Result<std::unique_ptr<::ArrowSchema>> FormatTable::GetArrowSchema() const {
    return GetSchema()->GetArrowSchema();
}

namespace {
Result<std::unique_ptr<FileStoreWrite>> NewFormatTableWrite(
    const std::shared_ptr<FormatTable>& table, const WriteContext& ctx) {
    if (ctx.IsStreamingMode()) {
        return Status::NotImplemented(
            "a format table has no snapshots, so there is nothing a streaming write could "
            "commit against");
    }
    if (ctx.GetRealtimeContext() != nullptr) {
        return Status::NotImplemented("a format table has no real-time store to write into");
    }
    if (!ctx.GetWriteSchema().empty()) {
        return Status::NotImplemented(
            "a format table write takes the table's own columns; a write schema naming a "
            "subset of them is not supported yet");
    }
    // A write id prefixes a postpone-bucket writer's files so that one compaction reader can
    // put them back in order. A format table has no buckets, so it would identify nothing.
    if (ctx.GetWriteId().has_value()) {
        return Status::NotImplemented(
            "a format table has no buckets, so a write id would name nothing");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableFileStoreWrite> format_write,
                           FormatTableFileStoreWrite::Create(table, ctx.GetMemoryPool()));
    return std::unique_ptr<FileStoreWrite>(std::move(format_write));
}

Result<std::unique_ptr<TableScan>> NewFormatTableScan(const std::shared_ptr<FormatTable>& table,
                                                      const std::shared_ptr<ScanContext>& context) {
    // Anything a `ScanContext` carries that a format table cannot honour is refused rather than
    // silently dropped.
    if (context->IsStreamingMode()) {
        return Status::NotImplemented(
            "a format table has no snapshots, so there is nothing for a streaming scan to follow");
    }
    if (context->GetRealtimeContext() != nullptr) {
        return Status::NotImplemented(
            "a format table has no real-time store to union with what is on disk");
    }
    if (context->GetGlobalIndexResult() != nullptr) {
        return Status::NotImplemented("a format table carries no global index");
    }
    std::map<std::string, std::string> partition_filter;
    std::shared_ptr<ScanFilter> filters = context->GetScanFilters();
    if (filters != nullptr) {
        if (filters->GetPredicate() != nullptr) {
            return Status::NotImplemented(
                "a format table scan does not take a predicate: its files carry no statistics to "
                "skip by, and pruning by the partition columns a predicate names is not "
                "implemented yet; give the partition values as a partition filter instead");
        }
        if (filters->GetBucketFilter()) {
            return Status::NotImplemented("a format table has no buckets to filter by");
        }
        const std::vector<std::map<std::string, std::string>>& partition_filters =
            filters->GetPartitionFilters();
        if (partition_filters.size() > 1) {
            return Status::NotImplemented("a format table scan takes at most one partition filter");
        }
        if (partition_filters.size() == 1) {
            partition_filter = partition_filters.front();
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableScan> scan,
                           FormatTableScan::Create(table, partition_filter, context->GetLimit()));
    return std::unique_ptr<TableScan>(std::move(scan));
}

Result<std::unique_ptr<TableRead>> NewFormatTableRead(const std::shared_ptr<FormatTable>& table,
                                                      const std::shared_ptr<ReadContext>& context) {
    if (context->EnablePreciseFileIndexEvaluate()) {
        return Status::NotImplemented(
            "Precise file-index evaluation is not supported for format tables");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableRead> read,
                           FormatTableRead::Create(table, context));
    return std::unique_ptr<TableRead>(std::move(read));
}

Result<std::unique_ptr<FileStoreCommit>> NewFormatTableCommit(
    const std::shared_ptr<FormatTable>& table, const CommitContext& ctx) {
    if (!ctx.IgnoreEmptyCommit()) {
        return Status::NotImplemented(
            "a format table cannot record an empty commit: keeping one means writing a "
            "snapshot that adds no files, and there are no snapshots here");
    }
    if (ctx.UseRESTCatalogCommit()) {
        return Status::NotImplemented(
            "a format table commits by renaming files into place, not by sending a snapshot "
            "to a rest catalog");
    }
    if (ctx.AppendCommitCheckConflict()) {
        return Status::NotImplemented(
            "a format table has no manifests to check a concurrent commit against");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatTableFileStoreCommit> format_commit,
                           FormatTableFileStoreCommit::Create(table));
    return std::unique_ptr<FileStoreCommit>(std::move(format_commit));
}
}  // namespace
Result<std::shared_ptr<Table>> FormatTable::CopyWithoutTimeTravel(
    const std::map<std::string, std::string>& options) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatTable> table,
                           Copy(std::make_shared<FormatTable>(*this), options));
    return std::shared_ptr<Table>(std::move(table));
}
Result<std::unique_ptr<TableScan>> FormatTable::NewScan(std::unique_ptr<ScanContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "scan"));
    auto table = std::make_shared<FormatTable>(*this);
    std::shared_ptr<ScanContext> context = std::move(ctx);
    return NewFormatTableScan(table, context);
}
Result<std::unique_ptr<TableRead>> FormatTable::NewRead(std::unique_ptr<ReadContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "read"));
    auto table = std::make_shared<FormatTable>(*this);
    std::shared_ptr<ReadContext> context = std::move(ctx);
    return NewFormatTableRead(table, context);
}
Result<std::unique_ptr<FileStoreWrite>> FormatTable::NewWrite(
    std::unique_ptr<WriteContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "write"));
    auto table = std::make_shared<FormatTable>(*this);
    std::shared_ptr<WriteContext> context = std::move(ctx);
    return NewFormatTableWrite(table, *context);
}
Result<std::unique_ptr<FileStoreCommit>> FormatTable::NewCommit(
    std::unique_ptr<CommitContext> ctx) const {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "commit"));
    auto table = std::make_shared<FormatTable>(*this);
    std::shared_ptr<CommitContext> context = std::move(ctx);
    return NewFormatTableCommit(table, *context);
}

}  // namespace paimon
