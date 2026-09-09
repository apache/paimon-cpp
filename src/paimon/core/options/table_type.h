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

#pragma once

#include <map>
#include <string>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Type of a table, carried by the `type` table option.
///
/// Every type paimon defines is named here, even the ones this library cannot open: telling such
/// a table from a managed one means knowing its name.
enum class TableType {
    /// A managed paimon table with snapshots and manifests.
    TABLE = 1,
    /// A directory of data files laid out like a standard Hive table, without paimon metadata.
    FORMAT_TABLE = 2,
    /// A managed paimon table that also carries the SQL it materializes.
    MATERIALIZED_TABLE = 3,
    /// A managed paimon table over the objects of a location.
    OBJECT_TABLE = 4,
    /// A lance table, see 'https://lancedb.github.io/lance/'.
    LANCE_TABLE = 5,
    /// An iceberg table, see 'https://iceberg.apache.org/'.
    ICEBERG_TABLE = 6,
};

/// Identifiers of `TableType` as they appear in the `type` table option.
struct TableTypeDefine {
    static constexpr char kTable[] = "table";
    static constexpr char kFormatTable[] = "format-table";
    static constexpr char kMaterializedTable[] = "materialized-table";
    static constexpr char kObjectTable[] = "object-table";
    static constexpr char kLanceTable[] = "lance-table";
    static constexpr char kIcebergTable[] = "iceberg-table";

    /// Reads the table type out of a table option map.
    ///
    /// An absent `type` is `TableType::TABLE`, and the value is matched without regard to case.
    /// A value that names no table type is rejected rather than read as a managed table.
    static Result<TableType> FromOptions(const std::map<std::string, std::string>& options) {
        auto iter = options.find(Options::TYPE);
        if (iter == options.end()) {
            return TableType::TABLE;
        }
        std::string value = StringUtils::ToLowerCase(iter->second);
        if (value == kTable) {
            return TableType::TABLE;
        }
        if (value == kFormatTable) {
            return TableType::FORMAT_TABLE;
        }
        if (value == kMaterializedTable) {
            return TableType::MATERIALIZED_TABLE;
        }
        if (value == kObjectTable) {
            return TableType::OBJECT_TABLE;
        }
        if (value == kLanceTable) {
            return TableType::LANCE_TABLE;
        }
        if (value == kIcebergTable) {
            return TableType::ICEBERG_TABLE;
        }
        return Status::Invalid(fmt::format("unknown table type: {}", iter->second));
    }

    static Result<bool> IsFormatTable(const std::map<std::string, std::string>& options) {
        PAIMON_ASSIGN_OR_RAISE(TableType table_type, FromOptions(options));
        return table_type == TableType::FORMAT_TABLE;
    }
};

}  // namespace paimon
