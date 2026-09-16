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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/generic_lru_cache.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/stats/simple_stats_evolutions.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/table/source/table_scan_resources.h"

namespace paimon {

struct ScanSchemaResources {
    std::shared_ptr<arrow::Schema> arrow_schema;
    std::shared_ptr<arrow::Schema> partition_schema;
    std::vector<DataField> primary_key_fields;
    std::shared_ptr<SimpleStatsEvolutions> stats_evolutions;
};

class TableScanResources::Impl {
 public:
    Impl(const std::string& path, const std::string& branch,
         const std::shared_ptr<FileSystem>& file_system,
         const std::shared_ptr<MemoryPool>& memory_pool);

    Status Validate(const std::string& scan_path, const std::string& scan_branch,
                    const std::shared_ptr<FileSystem>& specific_file_system) const;

    Result<std::shared_ptr<const ScanSchemaResources>> GetSchemaResources(
        const std::shared_ptr<TableSchema>& table_schema);

    const std::string path_;
    const std::string branch_;
    const std::shared_ptr<FileSystem> file_system_;
    const std::shared_ptr<MemoryPool> memory_pool_;
    const std::shared_ptr<SchemaManager> schema_manager_;
    const std::shared_ptr<SnapshotManager> snapshot_manager_;

 private:
    static constexpr int64_t kSchemaCacheCapacity = 64;
    using SchemaCache = GenericLruCache<int64_t, std::shared_ptr<const ScanSchemaResources>>;
    SchemaCache schemas_{SchemaCache::Options{/*max_weight=*/kSchemaCacheCapacity}};
};

/// Keeps implementation types out of the public resource interface.
class TableScanResourcesAccess {
 public:
    TableScanResourcesAccess() = delete;
    ~TableScanResourcesAccess() = delete;

    static TableScanResources::Impl& Get(const TableScanResources& resources) {
        return *resources.impl_;
    }
};
}  // namespace paimon
