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
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/executor.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {
class FileStoreTable;
class ScoredGlobalIndexResult;
struct VectorSearch;
enum class SearchKind { VECTOR, FULL_TEXT };

struct TableSearchContext {
    std::string root_path;
    std::shared_ptr<TableSchema> table_schema;
    CoreOptions options;
    std::shared_ptr<FileStorePathFactory> path_factory;
    std::shared_ptr<PredicateFilter> partition_filter;
    std::shared_ptr<Predicate> filter;
    std::shared_ptr<MemoryPool> pool;
    std::shared_ptr<Executor> executor;

    // Defer a snapshot lookup failure so an existing plan can still be read with the loaded schema.
    Result<std::optional<Snapshot>> time_travel;
    std::map<std::string, std::string> option_overrides;

    Status RequireDataEvolution(const char* search) const;
    Status ApplyVectorSearchOptions(const std::map<std::string, std::string>& query_options);
    Result<DataField> ResolveField(const std::string& name, SearchKind kind) const;
    Result<std::optional<Snapshot>> ResolveSnapshot() const;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> SearchVector(
        const DataField& field, const std::optional<Snapshot>& snapshot,
        const VectorSearch& query) const;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> SearchFullText(
        const DataField& field, const std::optional<Snapshot>& snapshot, const std::string& query,
        int32_t limit) const;
};

struct TableSearchSettings {
    struct ResolvedColumn {
        TableSearchContext context;
        DataField field;
    };

    explicit TableSearchSettings(const std::string& path) : table_path_(path) {}
    explicit TableSearchSettings(const FileStoreTable& table);
    Result<TableSearchContext> Resolve() const {
        return Resolve(nullptr);
    }

    Result<ResolvedColumn> ResolveVectorColumn(
        const std::string& name, const std::map<std::string, std::string>& vector_options,
        bool* retry_time_travel = nullptr) const;

    std::string table_path_;
    using PartitionSpecs = std::vector<std::map<std::string, std::string>>;
    std::vector<PartitionSpecs> partition_filters_;
    std::vector<std::shared_ptr<Predicate>> filters_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> memory_pool_ = GetDefaultPool();
    std::shared_ptr<Executor> executor_;
    std::shared_ptr<FileSystem> file_system_;

 private:
    Result<TableSearchContext> Resolve(bool* retry_time_travel) const;
    std::shared_ptr<TableSchema> bound_schema_;
    std::map<std::string, std::string> bound_options_;
    std::shared_ptr<FileSystem> bound_file_system_;
};
}  // namespace paimon
