/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/index/pksorted/pk_sorted_bucket_index_state.h"

#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <utility>

#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_policy.h"

namespace paimon {
PkSortedBucketIndexState PkSortedBucketIndexState::FromActiveDataFiles(
    int32_t field_id, const std::string& index_type,
    const std::vector<std::shared_ptr<DataFileMeta>>& active_data_files,
    const std::vector<std::shared_ptr<IndexFileMeta>>& active_payloads) {
    std::map<int32_t, std::vector<PrimaryKeyIndexSourceFile>> sources_by_level;
    for (const std::shared_ptr<DataFileMeta>& data_file : active_data_files) {
        if (data_file != nullptr && PrimaryKeyIndexSourcePolicy::ShouldRead(*data_file)) {
            sources_by_level[data_file->level].emplace_back(data_file->file_name,
                                                            data_file->row_count);
        }
    }
    for (auto& level_sources : sources_by_level) {
        std::sort(
            level_sources.second.begin(), level_sources.second.end(),
            [](const PrimaryKeyIndexSourceFile& left, const PrimaryKeyIndexSourceFile& right) {
                return left.file_name < right.file_name;
            });
    }

    // Match payloads against the expected level sources; anything that does not decode or
    // does not exactly cover its level is rejected.
    std::map<int32_t, std::vector<std::shared_ptr<IndexFileMeta>>> payloads_by_level;
    std::map<int32_t, std::vector<PrimaryKeyIndexSourceMeta>> payload_metas_by_level;
    std::vector<std::shared_ptr<IndexFileMeta>> rejected;
    for (const std::shared_ptr<IndexFileMeta>& payload : active_payloads) {
        if (payload == nullptr) {
            continue;
        }
        const std::optional<GlobalIndexMeta>& global_index_meta = payload->GetGlobalIndexMeta();
        if (payload->IndexType() != index_type || global_index_meta == std::nullopt ||
            global_index_meta->index_field_id != field_id) {
            rejected.push_back(payload);
            continue;
        }
        Result<PrimaryKeyIndexSourceMeta> source_meta_result =
            PrimaryKeyIndexSourceMeta::FromIndexFile(*payload);
        if (!source_meta_result.ok()) {
            rejected.push_back(payload);
            continue;
        }
        PrimaryKeyIndexSourceMeta source_meta = std::move(source_meta_result).value();
        auto desired = sources_by_level.find(source_meta.DataLevel());
        if (desired == sources_by_level.end() || desired->second != source_meta.SourceFiles()) {
            rejected.push_back(payload);
            continue;
        }
        payloads_by_level[source_meta.DataLevel()].push_back(payload);
        payload_metas_by_level[source_meta.DataLevel()].push_back(std::move(source_meta));
    }

    std::vector<PkSortedIndexGroup> groups;
    std::set<int32_t> covered_levels;
    for (const auto& level_payloads : payloads_by_level) {
        int32_t level = level_payloads.first;
        std::optional<PkSortedIndexGroup> group;
        if (level_payloads.second.size() == 1) {
            group = PkSortedIndexGroup::Create(field_id, index_type, sources_by_level[level],
                                               level_payloads.second[0],
                                               payload_metas_by_level[level][0]);
        }
        if (group != std::nullopt) {
            groups.push_back(std::move(group).value());
            covered_levels.insert(level);
        } else {
            rejected.insert(rejected.end(), level_payloads.second.begin(),
                            level_payloads.second.end());
        }
    }

    std::vector<PrimaryKeyIndexSourceFile> covered;
    std::vector<PrimaryKeyIndexSourceFile> uncovered;
    for (const auto& level_sources : sources_by_level) {
        auto& target = covered_levels.count(level_sources.first) > 0 ? covered : uncovered;
        target.insert(target.end(), level_sources.second.begin(), level_sources.second.end());
    }
    return PkSortedBucketIndexState(std::move(groups), std::move(covered), std::move(uncovered),
                                    std::move(rejected));
}

}  // namespace paimon
