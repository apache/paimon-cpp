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

#include "paimon/core/operation/manifest_file_merger.h"

#include <cstddef>
#include <functional>
#include <list>
#include <string>
#include <utility>

#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/status.h"

namespace paimon {

std::shared_ptr<Logger> ManifestFileMerger::GetLogger() {
    static std::shared_ptr<Logger> logger = Logger::GetLogger("ManifestFileMerger");
    return logger;
}

Result<std::vector<ManifestFileMeta>> ManifestFileMerger::Merge(
    const std::vector<ManifestFileMeta>& all, int64_t target_file_size, int32_t merge_min_count,
    int64_t full_compaction_file_size, ManifestFile* manifest_file) {
    if (manifest_file == nullptr) {
        return Status::Invalid("manifest_file is null pointer");
    }

    // these are the newly created manifest files, clean them up if exception occurs
    std::vector<ManifestFileMeta> new_metas_for_abort;
    ScopeGuard guard([&]() {
        for (const auto& meta : new_metas_for_abort) {
            manifest_file->DeleteQuietly(meta.FileName());
        }
    });
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::vector<ManifestFileMeta>> full_compacted,
                           TryFullCompaction(all, target_file_size, full_compaction_file_size,
                                             manifest_file, &new_metas_for_abort));
    std::vector<ManifestFileMeta> results;
    if (full_compacted != std::nullopt) {
        results = full_compacted.value();
    } else {
        PAIMON_ASSIGN_OR_RAISE(results, TryMinorCompaction(all, target_file_size, merge_min_count,
                                                           manifest_file, &new_metas_for_abort));
    }
    guard.Release();
    return results;
}

// TryFullCompaction aims to perform a full compaction of manifest files. It consolidates
// manifest files into two categories, base and delta, and applies a "full compaction" condition
// if certain thresholds are met.
Result<std::optional<std::vector<ManifestFileMeta>>> ManifestFileMerger::TryFullCompaction(
    const std::vector<ManifestFileMeta>& all, int64_t target_file_size,
    int64_t full_compaction_file_size, ManifestFile* manifest_file,
    std::vector<ManifestFileMeta>* new_metas_for_abort) {
    // 1. Splitting base and delta sets
    std::vector<ManifestFileMeta> base;
    int64_t total_manifest_size = 0;
    size_t i = 0;
    for (; i < all.size(); i++) {
        ManifestFileMeta file = all[i];
        if (file.NumDeletedFiles() == 0 && file.FileSize() >= target_file_size) {
            base.push_back(file);
            total_manifest_size += file.FileSize();
        } else {
            break;
        }
    }
    std::vector<ManifestFileMeta> delta;
    int64_t delta_delete_file_num = 0;
    int64_t total_delta_file_size = 0;
    for (; i < all.size(); i++) {
        const ManifestFileMeta& file = all[i];
        delta.push_back(file);
        total_manifest_size += file.FileSize();
        total_delta_file_size += file.FileSize();
        delta_delete_file_num += file.NumDeletedFiles();
    }
    // 2. Determining Full Compaction Requirement
    if (total_delta_file_size < full_compaction_file_size) {
        return std::optional<std::vector<ManifestFileMeta>>();
    }

    // 3. Merging Delta Files
    PAIMON_LOG_DEBUG(GetLogger(),
                     "Start Manifest File Full Compaction, pick the number of delete file: %ld, "
                     "total manifest file size: %ld",
                     delta_delete_file_num, total_manifest_size);

    if (delta.size() <= 1) {
        return std::optional<std::vector<ManifestFileMeta>>();
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestFileMeta> merged_delta,
                           MergeEntries(delta, manifest_file));

    // 4. Result Construction and Return
    std::vector<ManifestFileMeta> result;
    result.insert(result.end(), base.begin(), base.end());
    result.insert(result.end(), merged_delta.begin(), merged_delta.end());
    new_metas_for_abort->insert(new_metas_for_abort->end(), merged_delta.begin(),
                                merged_delta.end());
    return std::optional<std::vector<ManifestFileMeta>>(result);
}

// TryMinorCompaction focuses on performing a "minor compaction" of manifest files, especially
// those that are smaller than the suggested meta file size.
Result<std::vector<ManifestFileMeta>> ManifestFileMerger::TryMinorCompaction(
    const std::vector<ManifestFileMeta>& input, int64_t suggested_meta_size,
    int32_t suggested_min_meta_count, ManifestFile* manifest_file,
    std::vector<ManifestFileMeta>* new_metas_for_abort) {
    std::vector<ManifestFileMeta> result;
    std::vector<ManifestFileMeta> candidates;
    int64_t total_size = 0;
    // merge existing small manifest files
    for (const ManifestFileMeta& manifest : input) {
        candidates.push_back(manifest);
        total_size += manifest.FileSize();
        if (total_size >= suggested_meta_size) {
            if (candidates.size() == 1) {
                result.push_back(candidates[0]);
            } else {
                // reach suggested file size, perform merging and produce new file
                PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestFileMeta> merged,
                                       MergeEntries(candidates, manifest_file));
                result.insert(result.end(), merged.begin(), merged.end());
                new_metas_for_abort->insert(new_metas_for_abort->end(), merged.begin(),
                                            merged.end());
            }
            candidates.clear();
            total_size = 0;
        }
    }

    // merge the last bit of manifests if there are too many
    if (candidates.size() >= static_cast<uint32_t>(suggested_min_meta_count)) {
        if (candidates.size() == 1) {
            result.push_back(candidates[0]);
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::vector<ManifestFileMeta> merged,
                                   MergeEntries(candidates, manifest_file));
            result.insert(result.end(), merged.begin(), merged.end());
            new_metas_for_abort->insert(new_metas_for_abort->end(), merged.begin(), merged.end());
        }
    } else {
        result.insert(result.end(), candidates.begin(), candidates.end());
    }
    return result;
}

Result<std::vector<ManifestFileMeta>> ManifestFileMerger::MergeEntries(
    const std::vector<ManifestFileMeta>& metas, ManifestFile* manifest_file) {
    if (metas.size() == 1) {
        return std::vector<ManifestFileMeta>({metas[0]});
    }
    std::vector<ManifestEntry> entries;
    for (const auto& meta : metas) {
        PAIMON_RETURN_NOT_OK(manifest_file->Read(meta.FileName(), /*filter=*/nullptr, &entries));
    }
    std::vector<ManifestEntry> result;
    PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries<ManifestEntry>(entries, &result));
    return manifest_file->Write(result);
}

}  // namespace paimon
