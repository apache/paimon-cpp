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

#include "paimon/core/schema/schema_manager.h"

#include <algorithm>
#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

SchemaManager::SchemaManager(const std::shared_ptr<FileSystem>& file_system,
                             const std::string& table_root)
    : SchemaManager(file_system, table_root, BranchManager::DEFAULT_MAIN_BRANCH) {}

SchemaManager::SchemaManager(const std::shared_ptr<FileSystem>& file_system,
                             const std::string& table_root, const std::string& branch)
    : file_system_(file_system),
      table_root_(table_root),
      branch_(BranchManager::NormalizeBranch(branch)) {}

std::string SchemaManager::BranchPath() const {
    return BranchManager::BranchPath(table_root_, branch_);
}

std::string SchemaManager::ToSchemaPath(int64_t schema_id) const {
    return PathUtil::JoinPath(BranchPath(),
                              "/schema/" + std::string(SCHEMA_PREFIX) + std::to_string(schema_id));
}
Result<std::optional<std::shared_ptr<TableSchema>>> SchemaManager::Latest() const {
    std::vector<int64_t> versions;
    PAIMON_RETURN_NOT_OK(FileUtils::ListVersionedFiles(file_system_, SchemaDirectory(),
                                                       std::string(SCHEMA_PREFIX), &versions));
    if (versions.empty()) {
        return std::optional<std::shared_ptr<TableSchema>>();
    }
    int64_t max_schema_id = versions[0];
    for (const auto& version : versions) {
        max_schema_id = std::max(max_schema_id, version);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema, ReadSchema(max_schema_id));
    return std::optional<std::shared_ptr<TableSchema>>(schema);
}

Result<std::shared_ptr<TableSchema>> SchemaManager::ReadSchema(int64_t schema_id) const {
    auto iter = schema_cache_.find(schema_id);
    if (iter != schema_cache_.end()) {
        return iter->second;
    }
    auto path = ToSchemaPath(schema_id);
    std::string content;
    PAIMON_RETURN_NOT_OK(file_system_->ReadFile(path, &content));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<TableSchema> schema,
                           TableSchema::CreateFromJson(content));
    schema_cache_[schema_id] = schema;
    return schema;
}

std::string SchemaManager::SchemaDirectory() const {
    return PathUtil::JoinPath(BranchPath(), "/schema");
}

Result<bool> SchemaManager::SchemaExists(int64_t id) const {
    std::string schema_path = ToSchemaPath(id);
    return file_system_->Exists(schema_path);
}

Result<std::vector<int64_t>> SchemaManager::ListAllIds() const {
    std::vector<int64_t> versions;
    PAIMON_RETURN_NOT_OK(FileUtils::ListVersionedFiles(file_system_, SchemaDirectory(),
                                                       std::string(SCHEMA_PREFIX), &versions));
    return versions;
}

Result<std::unique_ptr<TableSchema>> SchemaManager::CreateTable(
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::string>& partition_keys,
    const std::vector<std::string>& primary_keys,
    const std::map<std::string, std::string>& options) {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest_schema, Latest());
        if (latest_schema) {
            return Status::Invalid("Schema in filesystem exists, creation is not allowed.");
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        PAIMON_RETURN_NOT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
        std::string schema_path = ToSchemaPath(0);
        PAIMON_ASSIGN_OR_RAISE(std::string content, table_schema->ToJsonString());
        auto status = file_system_->AtomicStore(schema_path, content);
        if (status.ok()) {
            return table_schema;
        }
    }
    return Status::Invalid("create table failed, should not be here");
}

}  // namespace paimon
