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
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"

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
    : file_system_(file_system),
      location_(location),
      identifier_(identifier),
      schema_(schema),
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
    // The table type comes from the schema alone. `type` is structural: letting an option given
    // at the call override it would let one read or write open a managed table as a format table,
    // or hide a format table.
    PAIMON_ASSIGN_OR_RAISE(bool is_format_table, TableTypeDefine::IsFormatTable(schema->Options()));
    if (!is_format_table) {
        return Status::Invalid(
            fmt::format("table {} is not a format table, its '{}' option is not "
                        "'{}'",
                        identifier.GetFullName(), Options::TYPE, TableTypeDefine::kFormatTable));
    }
    // The runtime options: what the schema stored, with anything given at the call on top, the
    // precedence every context builder promises. `type` was read above and is not one of them.
    std::map<std::string, std::string> options = schema->Options();
    for (const auto& [key, value] : dynamic_options) {
        if (key == Options::TYPE) {
            continue;
        }
        options[key] = value;
    }

    // The checks creation would have run, again: this schema may have come from a rest catalog or
    // straight from a caller. The message names the table and keeps the original status code.
    //
    // Against the merged options rather than the schema's own, so that an option this refuses is
    // refused wherever it comes from instead of being dropped in silence.
    Status valid = SchemaValidation::ValidateGenericDataSchema(*schema);
    if (valid.ok()) {
        valid = SchemaValidation::ValidateFormatTableSchema(*schema, options, file_system);
    }
    if (!valid.ok()) {
        return Status(valid.code(), fmt::format("cannot open format table {}: {}",
                                                identifier.GetFullName(), valid.message()));
    }

    // Before `CoreOptions`, which resolves `file.format` through the factories and would fail
    // with a missing-factory error where this names the format.
    PAIMON_ASSIGN_OR_RAISE(std::string file_format,
                           OptionsUtils::GetValueFromMap<std::string>(options, Options::FILE_FORMAT,
                                                                      kDefaultFileFormat));
    PAIMON_ASSIGN_OR_RAISE(Format format, ParseFormat(file_format));

    // Everything else through `CoreOptions`, so an option means what it does on the managed
    // table path and a default lives in one place.
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options, file_system));

    return std::shared_ptr<FormatTable>(new FormatTable(
        file_system, location, identifier, schema, options, format,
        core_options.FormatTableFileCompression(), core_options.GetPartitionDefaultName(),
        core_options.FormatTablePartitionOnlyValueInPath(), location_carries_paimon_metadata));
}

const std::vector<std::string>& FormatTable::PartitionKeys() const {
    return schema_->PartitionKeys();
}

std::string FormatTable::FullName() const {
    return identifier_.GetFullName();
}

Result<std::unique_ptr<::ArrowSchema>> FormatTable::GetArrowSchema() const {
    return schema_->GetArrowSchema();
}

}  // namespace paimon
