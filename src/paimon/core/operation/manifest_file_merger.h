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
#include <optional>
#include <unordered_map>
#include <vector>

#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/logging.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

class ManifestFile;

/// This file includes several `ManifestFileMeta`, representing all data of the whole
/// table at the corresponding snapshot.
class ManifestFileMerger {
 public:
    /// Merge several `ManifestFileMeta`s.
    ///
    /// @note This method is atomic.
    static Result<std::vector<ManifestFileMeta>> Merge(const std::vector<ManifestFileMeta>& all,
                                                       int64_t manifest_target_file_size,
                                                       int32_t merge_min_count,
                                                       int64_t full_compaction_file_size,
                                                       ManifestFile* manifest_file);

 private:
    static Result<std::optional<std::vector<ManifestFileMeta>>> TryFullCompaction(
        const std::vector<ManifestFileMeta>& all, int64_t manifest_target_file_size,
        int64_t full_compaction_file_size, ManifestFile* manifest_file,
        std::vector<ManifestFileMeta>* new_metas);

    static Result<std::vector<ManifestFileMeta>> TryMinorCompaction(
        const std::vector<ManifestFileMeta>& all, int64_t manifest_target_file_size,
        int32_t merge_min_count, ManifestFile* manifest_file,
        std::vector<ManifestFileMeta>* new_metas);

    static Result<std::vector<ManifestFileMeta>> MergeEntries(
        const std::vector<ManifestFileMeta>& metas, ManifestFile* manifest_file);

    static std::shared_ptr<Logger> GetLogger();
};

}  // namespace paimon
