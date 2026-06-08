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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/options/expire_config.h"
#include "paimon/logging.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class Snapshot;
class SnapshotManager;
class FileStorePathFactory;
class FileSystem;
class ManifestEntry;
class ManifestList;
class ManifestFile;
class Executor;
class BinaryRow;

class ExpireSnapshots {
 public:
    ExpireSnapshots(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                    const std::shared_ptr<FileStorePathFactory>& path_factory,
                    const std::shared_ptr<ManifestList>& manifest_list,
                    const std::shared_ptr<ManifestFile>& manifest_file,
                    const std::shared_ptr<FileSystem>& fs, const ExpireConfig& config,
                    const std::shared_ptr<Executor>& executor);

    Result<int32_t> Expire();

 private:
    Result<int32_t> ExpireUntil(int64_t earliest_snapshot_id, int64_t end_exclusive_id);

    Status CleanUnusedDataFiles(const std::string& manifest_list_name);
    Status CleanUnusedManifests(const std::string& manifest_list_name,
                                const std::set<std::string>& skipping_sets);
    Status CleanEmptyDirectories();
    Status GetDataFilesToDelete(const std::vector<ManifestEntry>& data_file_entries,
                                std::map<std::string, ManifestEntry>* data_files_to_delete) const;
    Status GetManifestSkippingSet(const std::vector<Snapshot>& retained_snapshots,
                                  std::set<std::string>* skipping_manifest_set) const;
    bool TryDeleteEmptyDirectory(const std::string& path) const;

    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<ManifestList> manifest_list_;
    std::shared_ptr<ManifestFile> manifest_file_;
    std::shared_ptr<FileSystem> fs_;
    ExpireConfig config_;
    std::shared_ptr<Executor> executor_;
    std::unordered_map<BinaryRow, std::set<std::int32_t>> deletion_buckets_;

    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
