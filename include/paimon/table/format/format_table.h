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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class FileSystem;

/// A directory of data files in one format, without snapshots or manifests.
/// Partitions use key=value directories, or bare values with
/// format-table.partition-path-only-value. The type option must be format-table; file.format
/// defaults to parquet.
///
/// Writes accept inserts only, and reads expose _VALUE_KIND as INSERT. Load through
/// Catalog::GetTable() and create operators through the Table interface. Instances must be owned by
/// shared_ptr.
///
/// See docs/source/user_guide/format_table.rst for supported features.
class PAIMON_EXPORT FormatTable : public Table, public std::enable_shared_from_this<FormatTable> {
 public:
    /// Formats a format table's files can be in.
    enum class Format {
        PARQUET,
        ORC,
    };

    /// Parses the `file.format` option, case-insensitively. A format this library has no reader
    /// for is rejected by name, instead of failing later with a missing-format-factory error.
    static Result<Format> ParseFormat(const std::string& file_format);

    /// The identifier of a format, as it appears in `file.format` and as a file extension.
    static std::string FormatToString(Format format);

    /// Loads a format table from a schema stored under its data directory.
    /// Use Catalog::GetTable() for tables whose schema is stored in a metastore.
    ///
    /// @param file_system File system holding the table directory.
    /// @param table_path Table directory and data location.
    /// @param identifier Logical table identifier.
    /// @param dynamic_options Overrides for the stored schema options.
    /// @return The table, or an error status.
    static Result<std::shared_ptr<FormatTable>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& table_path,
        const Identifier& identifier, const std::map<std::string, std::string>& dynamic_options);

    /// Builds a format table from a schema that is already loaded, for a caller that has one in
    /// hand, such as a catalog that just created the table.
    ///
    /// @param location Directory the data files live in. It may not be empty: every path this
    ///        table reads or writes is checked against it, and an empty one is a prefix of
    ///        nothing. A trailing separator names the same directory as none.
    /// @param location_carries_paimon_metadata See `LocationCarriesPaimonMetadata()`. Only the
    ///        caller knows: a file system catalog puts metadata there, a REST or Hive catalog
    ///        keeps it in the metastore.
    /// @param dynamic_options Options given at the call, which win over the ones stored in the
    ///        schema. Empty when the caller has none.
    /// @return A result containing the format table, or an error status.
    static Result<std::shared_ptr<FormatTable>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& location,
        const Identifier& identifier, const std::shared_ptr<DataSchema>& schema,
        bool location_carries_paimon_metadata,
        const std::map<std::string, std::string>& dynamic_options);

    ~FormatTable() override;

    std::string Name() const override {
        return identifier_.GetObjectName();
    }

    std::string FullName() const override {
        return identifier_.GetFullName();
    }

    /// Returns the same instance when dynamic_options is empty.
    Result<std::shared_ptr<Table>> Copy(
        const std::map<std::string, std::string>& dynamic_options) const override;

    /// Directory the data files live in.
    const std::string& Location() const {
        return GetRootPath();
    }

    /// Format of every data file in the directory.
    Format GetFormat() const {
        return format_;
    }

    /// Fields the table is partitioned by, in the order their directories nest.
    const std::vector<std::string>& PartitionKeys() const;

    /// Compression new data files are written with. It is resolved from `file.compression`, then
    /// `format-table.file.compression`, then the bare `compression` key an engine's own writer
    /// reads, then what the table's format writes by default.
    const std::string& FileCompression() const {
        return file_compression_;
    }

    /// Directory name standing for a null partition value, from `partition.default-name`.
    const std::string& PartitionDefaultName() const {
        return partition_default_name_;
    }

    /// Whether a partition directory is named by its value alone (`2025/01/`) instead of
    /// `key=value` (`year=2025/month=01/`), from `format-table.partition-path-only-value`. The
    /// value-only layout carries no field names, so the nesting order of the table's partition
    /// keys alone says which key a directory belongs to.
    bool PartitionOnlyValueInPath() const {
        return partition_only_value_in_path_;
    }

    /// Table options: the ones stored in the schema, with any given at the call on top.
    const std::map<std::string, std::string>& Options() const {
        return options_;
    }

    /// Returns the data schema, including partition columns.
    std::shared_ptr<DataSchema> LatestSchema() const {
        return std::static_pointer_cast<DataSchema>(schema_);
    }

    /// Schema of the table as an arrow schema, including its partition fields.
    Result<std::unique_ptr<::ArrowSchema>> GetArrowSchema() const;

    /// Whether this table's own metadata lives under its location, as told by whoever loaded it.
    ///
    /// Only then are the `schema` and `branch` directories below the location table metadata
    /// rather than table content. For a table whose schema lives in a metastore they are data,
    /// and are read and written like any other directory.
    bool LocationCarriesPaimonMetadata() const {
        return location_carries_paimon_metadata_;
    }

    /// Creates operators using this table's options and the supplied operation context.
    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const override;

    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;

 private:
    FormatTable(const std::shared_ptr<FileSystem>& file_system, const std::string& location,
                const Identifier& identifier, const std::shared_ptr<DataSchema>& schema,
                const std::map<std::string, std::string>& options, Format format,
                const std::string& file_compression, const std::string& partition_default_name,
                bool partition_only_value_in_path, bool location_carries_paimon_metadata);

    /// Returns a mutable shared_ptr from this const instance; the table itself is not modified.
    std::shared_ptr<FormatTable> SharedSelf() const {
        return std::const_pointer_cast<FormatTable>(shared_from_this());
    }

    /// Typed implementation of Copy() for internal callers.
    Result<std::shared_ptr<FormatTable>> CopySelf(
        const std::map<std::string, std::string>& dynamic_options) const;

    std::map<std::string, std::string> options_;
    Format format_;
    std::string file_compression_;
    std::string partition_default_name_;
    bool partition_only_value_in_path_ = false;
    bool location_carries_paimon_metadata_ = false;
};

}  // namespace paimon
