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

#include "paimon/core/index/pksorted/pk_sorted_bucket_index_state.h"

#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_policy.h"

namespace paimon {
namespace {
struct PayloadCandidate {
    std::shared_ptr<IndexFileMeta> payload;
    std::shared_ptr<PkSortedIndexGroup> group;
    std::vector<PrimaryKeyIndexSourceFile> active_sources;
    bool conflicted = false;
};
}  // namespace

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

    std::map<std::string, int64_t> active_sources;
    std::set<std::string> ambiguous_active_source_names;
    for (const auto& level_sources : sources_by_level) {
        for (const PrimaryKeyIndexSourceFile& source : level_sources.second) {
            if (!active_sources.emplace(source.file_name, source.row_count).second) {
                ambiguous_active_source_names.insert(source.file_name);
            }
        }
    }

    // Keep the complete immutable source group for ordinal localization, but only claim the
    // sources which are still active in this snapshot. Several disjoint groups may coexist
    // after an update; no active source file may be claimed by two groups.
    std::vector<PayloadCandidate> candidates;
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
        const std::vector<PrimaryKeyIndexSourceFile>& payload_sources = source_meta.SourceFiles();
        bool valid_candidate = !payload_sources.empty();
        std::vector<PrimaryKeyIndexSourceFile> active_intersection;
        for (size_t i = 0; valid_candidate && i < payload_sources.size(); i++) {
            const PrimaryKeyIndexSourceFile& source = payload_sources[i];
            if (i > 0 && payload_sources[i - 1].file_name >= source.file_name) {
                valid_candidate = false;
                break;
            }
            auto active_source = active_sources.find(source.file_name);
            if (active_source == active_sources.end()) {
                continue;
            }
            if (active_source->second != source.row_count ||
                ambiguous_active_source_names.count(source.file_name) > 0) {
                valid_candidate = false;
                break;
            }
            active_intersection.push_back(source);
        }
        if (!valid_candidate || active_intersection.empty()) {
            rejected.push_back(payload);
            continue;
        }
        std::shared_ptr<PkSortedIndexGroup> group =
            PkSortedIndexGroup::Create(field_id, index_type, payload_sources, payload, source_meta);
        if (group == nullptr) {
            rejected.push_back(payload);
            continue;
        }
        candidates.push_back(
            {payload, std::move(group), std::move(active_intersection), /*conflicted=*/false});
    }

    std::map<std::pair<std::string, int64_t>, std::vector<size_t>> candidates_by_source;
    for (size_t i = 0; i < candidates.size(); i++) {
        const PayloadCandidate& candidate = candidates[i];
        for (const PrimaryKeyIndexSourceFile& source : candidate.active_sources) {
            candidates_by_source[{source.file_name, source.row_count}].push_back(i);
        }
    }
    for (const auto& source_candidates : candidates_by_source) {
        if (source_candidates.second.size() > 1) {
            for (size_t candidate_index : source_candidates.second) {
                candidates[candidate_index].conflicted = true;
            }
        }
    }

    std::vector<std::shared_ptr<PkSortedIndexGroup>> groups;
    std::set<std::pair<std::string, int64_t>> covered_sources;
    for (PayloadCandidate& candidate : candidates) {
        if (candidate.conflicted) {
            rejected.push_back(std::move(candidate.payload));
            continue;
        }
        for (const PrimaryKeyIndexSourceFile& source : candidate.active_sources) {
            covered_sources.emplace(source.file_name, source.row_count);
        }
        groups.push_back(std::move(candidate.group));
    }
    std::sort(groups.begin(), groups.end(),
              [](const std::shared_ptr<PkSortedIndexGroup>& left,
                 const std::shared_ptr<PkSortedIndexGroup>& right) {
                  if (left->DataLevel() != right->DataLevel()) {
                      return left->DataLevel() < right->DataLevel();
                  }
                  return left->SourceFiles().front().file_name <
                         right->SourceFiles().front().file_name;
              });

    std::vector<PrimaryKeyIndexSourceFile> covered;
    std::vector<PrimaryKeyIndexSourceFile> uncovered;
    for (const auto& level_sources : sources_by_level) {
        for (const PrimaryKeyIndexSourceFile& source : level_sources.second) {
            auto& target = covered_sources.count({source.file_name, source.row_count}) > 0
                               ? covered
                               : uncovered;
            target.push_back(source);
        }
    }
    return PkSortedBucketIndexState(std::move(groups), std::move(covered), std::move(uncovered),
                                    std::move(rejected));
}

}  // namespace paimon
