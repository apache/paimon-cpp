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
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/data/generic_row.h"
#include "paimon/core/table/system/system_table.h"

namespace paimon {

class FileSystem;

/// Base class for system tables whose result can be materialized as a single in-memory
/// RecordBatch.
///
/// It provides the common singleton split scan and one-shot batch reader.
class InMemorySystemTable : public SystemTable {
 public:
    InMemorySystemTable(const std::shared_ptr<FileSystem>& fs, std::string table_path,
                        std::string name);

    Result<std::unique_ptr<TableScan>> NewScan(
        std::unique_ptr<ScanContext> /*context*/,
        const std::map<std::string, std::string>& dynamic_options = {}) const override;
    Result<std::unique_ptr<TableRead>> NewRead(
        std::unique_ptr<ReadContext> context,
        const std::map<std::string, std::string>& dynamic_options = {}) const override;
    virtual Result<std::vector<GenericRow>> BuildRows() const = 0;

 protected:
    /// Creates an independent read view when dynamic options affect materialized rows.
    virtual Result<std::shared_ptr<const InMemorySystemTable>> ForRead(
        const std::map<std::string, std::string>& options) const;

    const std::string& TablePath() const {
        return GetRootPath();
    }
};

}  // namespace paimon
