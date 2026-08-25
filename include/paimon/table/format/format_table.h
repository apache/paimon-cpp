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
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {

class FileSystem;

/// A table that is a directory of data files of one format, laid out like a standard Hive table.
///
/// It carries no snapshots and no manifests: the files in the directory are the table, and a
/// partitioned table's partitions are the `key=value` directories below its location, or the
/// bare-value ones when `format-table.partition-path-only-value` asks for that layout. A table is
/// a format table when its `type` option is `format-table`; `file.format` then names the format of
/// every file in it, defaulting to `parquet`.
///
/// Reads return the table's own columns, and writes only insert: there is nowhere to record that a
/// row was updated or deleted, so the `_VALUE_KIND` field every `BatchReader` carries is filled
/// with inserts throughout.
///
/// This is the only format table type in the public API. Reading and writing go through the entry
/// points every other table uses - `TableScan`, `TableRead`, `FileStoreWrite` and
/// `FileStoreCommit` - each of which recognises a format table from the schema under the table
/// path, so a plan comes back as a `Plan`, a split as a `Split` and a commit message as a
/// `CommitMessage`.
///
/// `docs/source/user_guide/format_table.rst` lists what is not supported yet, notably the `csv`,
/// `json`, `text` and `mosaic` file formats and `metastore.partitioned-table`.
class PAIMON_EXPORT FormatTable {
 public:
    /// Formats a format table's files can be in.
    enum class Format {
        PARQUET,
        ORC,
    };

    /// Parses the `file.format` option, case-insensitively. A format this library has no reader
    /// for is rejected by name, rather than failing later with a missing-format-factory error.
    static Result<Format> ParseFormat(const std::string& file_format);

    /// The identifier of a format, as it appears in `file.format` and as a file extension.
    static std::string FormatToString(Format format);

    /// Loads a format table from its directory, reading the schema stored under it.
    ///
    /// This needs a schema file under the table directory, which is what a table created through
    /// `SchemaManager` or a file system catalog has. A table whose schema lives in a metastore
    /// has none, and is loaded through `Catalog::GetFormatTable()` instead.
    ///
    /// @param file_system File system holding the table directory.
    /// @param table_path Root path of the table, which is also its data location.
    /// @param identifier Logical table identifier, used for naming and error messages.
    /// @param dynamic_options Options given at the call, which win over the ones stored in the
    ///        schema, as they do for every other table type.
    static Result<std::shared_ptr<FormatTable>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& table_path,
        const Identifier& identifier,
        const std::map<std::string, std::string>& dynamic_options = {});

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
    ///        schema.
    static Result<std::shared_ptr<FormatTable>> Create(
        const std::shared_ptr<FileSystem>& file_system, const std::string& location,
        const Identifier& identifier, const std::shared_ptr<DataSchema>& schema,
        bool location_carries_paimon_metadata = false,
        const std::map<std::string, std::string>& dynamic_options = {});

    ~FormatTable();

    /// Directory the data files live in.
    const std::string& Location() const {
        return location_;
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
    /// value-only layout carries no field names, so only the nesting order of the table's
    /// partition keys says which key a directory belongs to.
    bool PartitionOnlyValueInPath() const {
        return partition_only_value_in_path_;
    }

    /// Table options: the ones stored in the schema, with any given at the call on top.
    const std::map<std::string, std::string>& Options() const {
        return options_;
    }

    /// A name to identify this table.
    std::string Name() const {
        return identifier_.GetTableName();
    }

    /// Full name of the table, database.tableName.
    std::string FullName() const;

    /// Schema of the table, including its partition fields.
    std::shared_ptr<DataSchema> LatestSchema() const {
        return schema_;
    }

    /// Schema of the table as an arrow schema, including its partition fields.
    Result<std::unique_ptr<::ArrowSchema>> GetArrowSchema() const;

    /// File system holding the table directory.
    std::shared_ptr<FileSystem> GetFileSystem() const {
        return file_system_;
    }

    /// Whether this table's own metadata lives under its location, as told by whoever loaded it.
    ///
    /// Only then are the `schema` and `branch` directories below the location table metadata
    /// rather than table content. For a table whose schema lives in a metastore they are data,
    /// and are read and written like any other directory.
    bool LocationCarriesPaimonMetadata() const {
        return location_carries_paimon_metadata_;
    }

 private:
    FormatTable(const std::shared_ptr<FileSystem>& file_system, const std::string& location,
                const Identifier& identifier, const std::shared_ptr<DataSchema>& schema,
                const std::map<std::string, std::string>& options, Format format,
                const std::string& file_compression, const std::string& partition_default_name,
                bool partition_only_value_in_path, bool location_carries_paimon_metadata);

    std::shared_ptr<FileSystem> file_system_;
    std::string location_;
    Identifier identifier_;
    std::shared_ptr<DataSchema> schema_;
    std::map<std::string, std::string> options_;
    Format format_;
    std::string file_compression_;
    std::string partition_default_name_;
    bool partition_only_value_in_path_ = false;
    bool location_carries_paimon_metadata_ = false;
};

}  // namespace paimon
