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

#include "paimon/core/operation/append_dv_context.h"

#include <optional>

#include "paimon/common/types/data_field.h"
#include "paimon/core/options/table_type.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/schema/schema_validation.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/file_store.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"

namespace paimon {

Result<AppendDvContext> AppendDvContext::Create(const std::string& table_path,
                                                const std::string& branch,
                                                const std::map<std::string, std::string>& options,
                                                const std::shared_ptr<FileSystem>& file_system,
                                                const std::shared_ptr<MemoryPool>& pool) {
    if (table_path.empty() || !file_system || !pool) {
        return Status::Invalid("Append DV requires a path, file system and memory pool");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::string resolved_branch,
        BranchManager::ResolveBranch(std::nullopt, options, branch, "append DV"));
    AppendDvContext context;
    context.schema_manager =
        std::make_shared<SchemaManager>(file_system, table_path, resolved_branch);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchema>> latest,
                           context.schema_manager->Latest());
    if (!latest) {
        return Status::Invalid("Append DV table schema not found");
    }
    context.schema = *latest;
    auto merged_options = context.schema->Options();
    for (const auto& [key, value] : options) {
        merged_options[key] = value;
    }
    merged_options[Options::BRANCH] = resolved_branch;
    PAIMON_ASSIGN_OR_RAISE(context.options, CoreOptions::FromMap(merged_options, file_system, {}));
    PAIMON_ASSIGN_OR_RAISE(TableType type, TableTypeDefine::FromOptions(context.schema->Options()));
    if (type != TableType::TABLE || !context.schema->PrimaryKeys().empty() ||
        context.options.DataEvolutionEnabled() || context.options.RealtimeEnabled() ||
        (context.options.GetBucket() != -1 && context.options.GetBucket() <= 0)) {
        return Status::NotImplemented("Append DV requires an ordinary managed append table");
    }
    PAIMON_RETURN_NOT_OK(
        SchemaValidation::RejectQueryAuth(*context.schema, merged_options, "append DV"));
    if (context.options.DeletionVectorsBitmap64()) {
        return Status::NotImplemented("Append DV does not support bitmap64 deletion vectors");
    }
    context.arrow_schema = DataField::ConvertDataFieldsToArrowSchema(context.schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(
        context.path_factory,
        FileStore(table_path, context.options)
            .NewPathFactory(context.arrow_schema, context.schema->PartitionKeys(),
                            context.options.GetFileFormat()->Identifier(), pool));
    return context;
}

}  // namespace paimon
