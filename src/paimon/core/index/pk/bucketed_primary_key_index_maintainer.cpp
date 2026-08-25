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

#include "paimon/core/index/pk/bucketed_primary_key_index_maintainer.h"

#include <algorithm>
#include <map>
#include <set>
#include <unordered_set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/global_index/btree/btree_defs.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/pk/primary_key_index_source_policy.h"
#include "paimon/core/index/pksorted/pk_sorted_bucket_index_state.h"
#include "paimon/core/index/pksorted/pk_sorted_index_builder.h"
#include "paimon/core/index/pksorted/pk_sorted_index_group.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/logging.h"

namespace paimon {
namespace {

Logger* GetLogger() {
    static std::unique_ptr<Logger> logger = Logger::GetLogger("BucketedPrimaryKeyIndexMaintainer");
    return logger.get();
}

void RemoveDataFiles(const std::vector<std::shared_ptr<DataFileMeta>>& files,
                     std::map<std::string, std::shared_ptr<DataFileMeta>>* active) {
    for (const std::shared_ptr<DataFileMeta>& file : files) {
        if (file != nullptr) {
            active->erase(file->file_name);
        }
    }
}

Status AddSourceFiles(const std::vector<std::shared_ptr<DataFileMeta>>& files,
                      std::map<std::string, std::shared_ptr<DataFileMeta>>* active) {
    for (const std::shared_ptr<DataFileMeta>& file : files) {
        if (file == nullptr) {
            return Status::Invalid("Primary-key index data increment contains a null file.");
        }
        if (PrimaryKeyIndexSourcePolicy::ShouldRead(*file)) {
            (*active)[file->file_name] = file;
        }
    }
    return Status::OK();
}

Status ValidateAppendFiles(const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    for (const std::shared_ptr<DataFileMeta>& file : files) {
        if (file == nullptr) {
            return Status::Invalid("Primary-key index append increment contains a null file.");
        }
        if (PrimaryKeyIndexSourcePolicy::ShouldRead(*file)) {
            return Status::Invalid(fmt::format(
                "Append file {} must not be a primary-key sorted-index source.", file->file_name));
        }
    }
    return Status::OK();
}

std::string PayloadIdentity(const std::shared_ptr<IndexFileMeta>& payload) {
    if (payload == nullptr) {
        return std::string();
    }
    return payload->ExternalPath().value_or(payload->FileName());
}

void AddUniquePayload(const std::shared_ptr<IndexFileMeta>& payload,
                      std::unordered_set<std::string>* identities,
                      std::vector<std::shared_ptr<IndexFileMeta>>* payloads) {
    std::string identity = PayloadIdentity(payload);
    if (!identity.empty() && identities->insert(identity).second) {
        payloads->push_back(payload);
    }
}

bool IsPrimaryKeyBTreePayload(const std::shared_ptr<IndexFileMeta>& payload) {
    return payload != nullptr && payload->IndexType() == BtreeDefs::kIdentifier &&
           IndexFileHandler::IsPrimaryKeySourceIndex(*payload);
}

}  // namespace

Result<std::shared_ptr<BucketedPrimaryKeyIndexMaintainer::Factory>>
BucketedPrimaryKeyIndexMaintainer::Factory::Create(
    const std::string& root_path, const std::string& branch,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::vector<PrimaryKeyIndexDefinition>& definitions,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<IndexFileHandler>& index_file_handler, const CoreOptions& options,
    const std::shared_ptr<IOManager>& io_manager, bool enable_multi_thread_spill,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    std::vector<PrimaryKeyIndexDefinition> btree_definitions;
    for (const PrimaryKeyIndexDefinition& definition : definitions) {
        if (definition.GetFamily() == PrimaryKeyIndexDefinition::Family::BTREE) {
            btree_definitions.push_back(definition);
        }
    }
    std::sort(btree_definitions.begin(), btree_definitions.end(),
              [](const PrimaryKeyIndexDefinition& left, const PrimaryKeyIndexDefinition& right) {
                  return left.FieldId() < right.FieldId();
              });
    return std::shared_ptr<Factory>(new Factory(
        root_path, branch, table_schema, std::move(btree_definitions), path_factory,
        index_file_handler, options, io_manager, enable_multi_thread_spill, executor, pool));
}

Result<std::shared_ptr<BucketedPrimaryKeyIndexMaintainer>>
BucketedPrimaryKeyIndexMaintainer::Factory::CreateMaintainer(
    const BinaryRow& partition, int32_t bucket,
    const std::vector<std::shared_ptr<DataFileMeta>>& restored_data_files,
    const std::vector<std::shared_ptr<IndexFileMeta>>& restored_payloads) const {
    std::map<std::string, std::shared_ptr<DataFileMeta>> active_data_files;
    PAIMON_RETURN_NOT_OK(AddSourceFiles(restored_data_files, &active_data_files));
    std::vector<FieldMaintainer> fields;
    fields.reserve(definitions_.size());
    for (const PrimaryKeyIndexDefinition& definition : definitions_) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PkSortedIndexBuilder> builder,
            PkSortedIndexBuilder::Create(root_path_, branch_, partition, bucket, table_schema_,
                                         definition, path_factory_, options_, io_manager_,
                                         enable_multi_thread_spill_, executor_, pool_));
        fields.push_back(
            FieldMaintainer{definition, std::shared_ptr<PkSortedIndexBuilder>(std::move(builder))});
    }
    return std::shared_ptr<BucketedPrimaryKeyIndexMaintainer>(new BucketedPrimaryKeyIndexMaintainer(
        std::move(fields), std::move(active_data_files), restored_payloads));
}

Status BucketedPrimaryKeyIndexMaintainer::PrepareCommit(CommitIncrement* increment) {
    if (increment == nullptr) {
        return Status::Invalid("Primary-key index commit increment is null.");
    }
    auto previous_data_files = active_data_files_;
    const DataIncrement& data_increment = increment->GetNewFilesIncrement();
    const CompactIncrement& compact_increment = increment->GetCompactIncrement();
    PAIMON_RETURN_NOT_OK(ValidateAppendFiles(data_increment.NewFiles()));
    RemoveDataFiles(compact_increment.CompactBefore(), &active_data_files_);
    Status update_status = AddSourceFiles(compact_increment.CompactAfter(), &active_data_files_);
    if (!update_status.ok()) {
        active_data_files_ = std::move(previous_data_files);
        return update_status;
    }

    std::vector<std::shared_ptr<DataFileMeta>> active_data;
    active_data.reserve(active_data_files_.size());
    for (const auto& file : active_data_files_) {
        active_data.push_back(file.second);
    }

    std::vector<std::shared_ptr<IndexFileMeta>> deleted_payloads;
    std::vector<std::shared_ptr<IndexFileMeta>> new_payloads;
    std::unordered_set<std::string> deleted_identities;
    std::unordered_set<std::string> new_identities;

    std::set<int32_t> owned_btree_field_ids;
    for (const FieldMaintainer& field : fields_) {
        owned_btree_field_ids.insert(field.definition.FieldId());
    }
    for (const std::shared_ptr<IndexFileMeta>& payload : active_payloads_) {
        if (!IsPrimaryKeyBTreePayload(payload)) {
            continue;
        }
        const std::optional<GlobalIndexMeta>& meta = payload->GetGlobalIndexMeta();
        if (meta != std::nullopt && meta->source_meta != nullptr &&
            owned_btree_field_ids.count(meta->index_field_id) == 0) {
            AddUniquePayload(payload, &deleted_identities, &deleted_payloads);
        }
    }

    for (const FieldMaintainer& field : fields_) {
        std::vector<std::shared_ptr<IndexFileMeta>> field_payloads;
        for (const std::shared_ptr<IndexFileMeta>& payload : active_payloads_) {
            if (!IsPrimaryKeyBTreePayload(payload) ||
                payload->IndexType() != field.definition.IndexType()) {
                continue;
            }
            const std::optional<GlobalIndexMeta>& meta = payload->GetGlobalIndexMeta();
            if (meta != std::nullopt && meta->index_field_id == field.definition.FieldId()) {
                field_payloads.push_back(payload);
            }
        }
        PkSortedBucketIndexState state = PkSortedBucketIndexState::FromActiveDataFiles(
            field.definition.FieldId(), field.definition.IndexType(), active_data, field_payloads);
        std::set<int32_t> current_levels;
        for (const std::shared_ptr<PkSortedIndexGroup>& group : state.Groups()) {
            current_levels.insert(group->DataLevel());
        }
        for (const std::shared_ptr<IndexFileMeta>& rejected : state.RejectedPayloads()) {
            AddUniquePayload(rejected, &deleted_identities, &deleted_payloads);
        }

        std::map<int32_t, std::vector<std::shared_ptr<DataFileMeta>>> desired_by_level;
        for (const std::shared_ptr<DataFileMeta>& file : active_data) {
            if (file != nullptr && PrimaryKeyIndexSourcePolicy::ShouldRead(*file)) {
                desired_by_level[file->level].push_back(file);
            }
        }
        for (auto& level_files : desired_by_level) {
            std::sort(level_files.second.begin(), level_files.second.end(),
                      [](const std::shared_ptr<DataFileMeta>& left,
                         const std::shared_ptr<DataFileMeta>& right) {
                          return left->file_name < right->file_name;
                      });
            if (current_levels.count(level_files.first) > 0) {
                continue;
            }
            Result<std::shared_ptr<IndexFileMeta>> build_result =
                field.builder->Build(level_files.second);
            if (!build_result.ok()) {
                PAIMON_LOG_WARN(
                    GetLogger(),
                    "Failed to build primary-key BTree index for column %s at data level %d; "
                    "committing that level without an index payload. %s",
                    field.definition.Column().c_str(), level_files.first,
                    build_result.status().ToString().c_str());
                continue;
            }
            AddUniquePayload(std::move(build_result).value(), &new_identities, &new_payloads);
        }
    }

    std::vector<std::shared_ptr<IndexFileMeta>> next_payloads;
    next_payloads.reserve(active_payloads_.size() + new_payloads.size());
    for (const std::shared_ptr<IndexFileMeta>& payload : active_payloads_) {
        if (deleted_identities.count(PayloadIdentity(payload)) == 0) {
            next_payloads.push_back(payload);
        }
    }
    next_payloads.insert(next_payloads.end(), new_payloads.begin(), new_payloads.end());
    active_payloads_ = std::move(next_payloads);

    bool has_compaction_transition =
        !compact_increment.CompactBefore().empty() || !compact_increment.CompactAfter().empty();
    if (has_compaction_transition) {
        increment->GetCompactIncrement().AddNewIndexFiles(std::move(new_payloads));
        increment->GetCompactIncrement().AddDeletedIndexFiles(std::move(deleted_payloads));
    } else {
        increment->GetNewFilesIncrement().AddNewIndexFiles(std::move(new_payloads));
        increment->GetNewFilesIncrement().AddDeletedIndexFiles(std::move(deleted_payloads));
    }
    return Status::OK();
}

}  // namespace paimon
