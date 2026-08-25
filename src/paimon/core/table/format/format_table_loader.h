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
#include <optional>
#include <string>

#include "paimon/result.h"

namespace paimon {

class FileSystem;
class FormatTable;
class SchemaManager;
class TableSchema;

/// Recognises a format table behind a table path.
///
/// The scan, read, write and commit entry points all take a path and have to answer the same
/// question before they can dispatch, so one place answers it and they cannot disagree.
class FormatTableLoader {
 public:
    FormatTableLoader() = delete;
    ~FormatTableLoader() = delete;

    /// Loads `table_path` as a format table, or returns null when it is not one.
    ///
    /// Null is also the answer when there is no schema under the path at all: what to say about a
    /// table that is not there is the managed path's to decide, and it says it in more detail.
    ///
    /// The table type is read from the schema, never from `options`: `type` is structural, and one
    /// call's options must not decide what kind of table this is.
    ///
    /// @param branch Branch the schema is read from. Take it from the same place the managed path
    ///        in the same entry point takes it, or the two dispatch on different schemas.
    /// @param options Options given at the call, which win over the ones the schema stored.
    /// @param specific_table_schema A schema the caller already holds, as json. It saves reading
    ///        one from under the path and changes nothing else: the table still keeps its metadata
    ///        there. Used only on the main branch, as on the managed read path.
    /// @param schema_manager The manager to read the schema through, or null to read it through
    ///        one of this call's own. A caller that goes on to use a `SchemaManager` itself must
    ///        pass that one: a manager caches the schemas it read, so reading through a second
    ///        one both costs an extra read and leaves the caller's cache cold.
    /// @param table_schema_out Set to the schema this read, or to null when there is none under
    ///        the path. Every caller dispatches on that schema and then needs it again for the
    ///        managed table it turned out to be, so it is handed back rather than read twice.
    static Result<std::shared_ptr<FormatTable>> TryLoad(
        const std::shared_ptr<FileSystem>& file_system, const std::string& table_path,
        const std::string& branch, const std::map<std::string, std::string>& options,
        const std::optional<std::string>& specific_table_schema,
        const SchemaManager* schema_manager, std::shared_ptr<TableSchema>* table_schema_out);
};

}  // namespace paimon
