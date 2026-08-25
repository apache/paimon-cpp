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

#include "paimon/core/table/format/format_table_loader.h"

#include <cassert>
#include <optional>

#include "paimon/catalog/identifier.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/schema/schema.h"
#include "paimon/table/format/format_table.h"

namespace paimon {

Result<std::shared_ptr<FormatTable>> FormatTableLoader::TryLoad(
    const std::shared_ptr<FileSystem>& file_system, const std::string& table_path,
    const std::string& branch, const std::map<std::string, std::string>& options,
    const std::optional<std::string>& specific_table_schema, const SchemaManager* schema_manager,
    std::shared_ptr<TableSchema>* table_schema_out) {
    assert(table_schema_out != nullptr);
    table_schema_out->reset();

    std::shared_ptr<TableSchema> table_schema;
    if (branch == BranchManager::DEFAULT_MAIN_BRANCH && specific_table_schema) {
        // Handing the schema over only saves reading it; it says nothing about where the table
        // keeps its metadata. A context always names a table path and this library writes a
        // table's metadata under it, so `schema` and `branch` below it stay metadata either way.
        PAIMON_ASSIGN_OR_RAISE(table_schema,
                               TableSchema::CreateFromJson(specific_table_schema.value()));
    } else {
        // Through the caller's manager when it has one, so that the read warms the cache it goes
        // on to use rather than a cache that dies with this call.
        SchemaManager own_schema_manager(file_system, table_path, branch);
        const SchemaManager& reader =
            schema_manager != nullptr ? *schema_manager : own_schema_manager;
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema,
                               reader.Latest());
        if (!latest_schema) {
            return std::shared_ptr<FormatTable>();
        }
        table_schema = *latest_schema;
    }
    *table_schema_out = table_schema;

    // From the schema alone: a `type` given at the call must not decide what kind of table this
    // is, not even for the length of that call.
    PAIMON_ASSIGN_OR_RAISE(bool is_format_table,
                           TableTypeDefine::IsFormatTable(table_schema->Options()));
    if (!is_format_table) {
        return std::shared_ptr<FormatTable>();
    }
    // A context names a path, never a catalog identifier, so the table is known by the directory
    // it sits in.
    return FormatTable::Create(file_system, table_path, Identifier(PathUtil::GetName(table_path)),
                               checked_pointer_cast<DataSchema>(table_schema),
                               /*location_carries_paimon_metadata=*/true, options);
}

}  // namespace paimon
