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

#include "paimon/core/operation/expire_snapshots.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/manifest/file_entry.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/commit/realtime_commit_properties.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/fs/file_system.h"

namespace paimon {

ExpireSnapshots::ExpireSnapshots(const std::shared_ptr<SnapshotManager>& snapshot_manager,
                                 const std::shared_ptr<FileStorePathFactory>& path_factory,
                                 const std::shared_ptr<ManifestList>& manifest_list,
                                 const std::shared_ptr<ManifestFile>& manifest_file,
                                 const std::shared_ptr<FileSystem>& fs, const ExpireConfig& config,
                                 bool realtime_enabled, const std::shared_ptr<Executor>& executor)
    : snapshot_manager_(snapshot_manager),
      path_factory_(path_factory),
      manifest_list_(manifest_list),
      manifest_file_(manifest_file),
      fs_(fs),
      config_(config),
      realtime_enabled_(realtime_enabled),
      executor_(executor),
      logger_(Logger::GetLogger("ExpireSnapshots")) {}

Result<int32_t> ExpireSnapshots::Expire() {
    int32_t retain_min = config_.GetSnapshotRetainMin();
    if (retain_min < 1) {
        return Status::Invalid(
            fmt::format("Expire failed: snapshot retain minimum '{}' is less than 1", retain_min));
    }

    int32_t retain_max = config_.GetSnapshotRetainMax();
    if (retain_max < retain_min) {
        return Status::Invalid(fmt::format(
            "Expire failed: snapshot retain maximum '{}' must be greater or equal than retain "
            "minimum '{}'",
            retain_max, retain_min));
    }
    int32_t max_deletes = config_.GetSnapshotMaxDeletes();
    if (max_deletes < 0) {
        return Status::Invalid(fmt::format(
            "Expire failed: snapshot max delete num '{}' must be greater than 0", max_deletes));
    }
    if (snapshot_manager_ == nullptr) {
        return Status::Invalid("Expire failed: snapshot manager is nullptr");
    }
    // All branches of a table share its data files, while expiration only reads the retained
    // snapshots of its own branch, so it could delete a file another branch still refers to.
    if (!BranchManager::IsMainBranch(snapshot_manager_->Branch())) {
        return Status::NotImplemented(fmt::format(
            "Expire failed: expiring snapshots of table {} on branch '{}' is not supported, "
            "because the branches of a table share its data files",
            snapshot_manager_->RootPath(), snapshot_manager_->Branch()));
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> branches,
                           BranchManager::ListBranches(fs_, snapshot_manager_->RootPath()));
    branches.erase(
        std::remove(branches.begin(), branches.end(), BranchManager::DEFAULT_MAIN_BRANCH),
        branches.end());
    if (!branches.empty()) {
        return Status::NotImplemented(fmt::format(
            "Expire failed: expiring snapshots of table {} is not supported, because the table "
            "has branches other than main ({}), which share its data files",
            snapshot_manager_->RootPath(), fmt::join(branches, ", ")));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> latest_snapshot_id,
                           snapshot_manager_->LatestSnapshotIdFromFileSystem());
    if (latest_snapshot_id == std::nullopt) {
        // no snapshot, nothing to expire
        return 0;
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<int64_t> earliest_snapshot_id,
                           snapshot_manager_->EarliestSnapshotId());
    if (earliest_snapshot_id == std::nullopt) {
        // no snapshot, nothing to expire
        return 0;
    }

    // TODO(jinli.zjw): why not only use earliest snapshot id
    int64_t min =
        std::max(latest_snapshot_id.value() - retain_max + 1, earliest_snapshot_id.value());
    int64_t max = latest_snapshot_id.value() - retain_min + 1;
    max = std::min(max, earliest_snapshot_id.value() + max_deletes);
    // TODO(jinli.zjw): support consumer manager
    int64_t older_than_ms =
        DateTimeUtils::GetCurrentUTCTimeUs() / 1000 - config_.GetSnapshotTimeRetainMs();
    for (int64_t snapshot_id = min; snapshot_id < max; snapshot_id++) {
        PAIMON_ASSIGN_OR_RAISE(bool exist, snapshot_manager_->SnapshotExists(snapshot_id));
        if (exist) {
            PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager_->LoadSnapshot(snapshot_id));
            if (older_than_ms <= snapshot.TimeMillis()) {
                return ExpireUntil(earliest_snapshot_id.value(), snapshot_id,
                                   latest_snapshot_id.value());
            }
        }
    }
    return ExpireUntil(earliest_snapshot_id.value(), max, latest_snapshot_id.value());
}

Result<int32_t> ExpireSnapshots::ExpireUntil(int64_t earliest_snapshot_id, int64_t end_exclusive_id,
                                             int64_t latest_snapshot_id) {
    if (end_exclusive_id <= earliest_snapshot_id) {
        // TODO(jinli.zjw): write earliest hint
        return 0;
    }
    // Read the retained boundary before deleting files referenced by expired snapshots.
    PAIMON_ASSIGN_OR_RAISE(bool exist, snapshot_manager_->SnapshotExists(end_exclusive_id));
    if (!exist) {
        PAIMON_LOG_DEBUG(logger_, "Skip expiration of table %s: retained snapshot #%ld is missing.",
                         snapshot_manager_->RootPath().c_str(), end_exclusive_id);
        return 0;
    }

    // An unpublished catalog rollback can reference files deleted by older snapshots.
    // Require the current snapshot and its retained history to be visible before deleting files.
    PAIMON_ASSIGN_OR_RAISE(SnapshotManager::LatestSnapshotResult latest,
                           snapshot_manager_->LatestSnapshotWithSource());
    if (snapshot_manager_->HasSnapshotLoader() && !latest.from_catalog) {
        return Status::NotImplemented(
            "snapshot expiration through a catalog requires loading its current snapshot");
    }
    if (!latest.snapshot || latest.snapshot->Id() != latest_snapshot_id) {
        PAIMON_LOG_DEBUG(logger_,
                         "Skip expiration of table %s: the current snapshot differs from the "
                         "latest published snapshot #%ld.",
                         snapshot_manager_->RootPath().c_str(), latest_snapshot_id);
        return 0;
    }
    std::vector<Snapshot> retained_snapshots;
    for (int64_t id = end_exclusive_id; id <= latest_snapshot_id; ++id) {
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager_->LoadSnapshot(id));
        retained_snapshots.push_back(std::move(snapshot));
    }
    if (latest.from_catalog && !(retained_snapshots.back() == latest.snapshot.value())) {
        PAIMON_LOG_DEBUG(logger_,
                         "Skip expiration of table %s: snapshot #%ld differs between the catalog "
                         "and file system.",
                         snapshot_manager_->RootPath().c_str(), latest_snapshot_id);
        return 0;
    }
    std::set<std::string> retained_offset_files;
    if (realtime_enabled_) {
        for (const Snapshot& retained_snapshot : retained_snapshots) {
            std::optional<std::string> offsets_path =
                RealtimeCommitProperties::GetOffsetsPath(retained_snapshot);
            if (offsets_path) {
                retained_offset_files.insert(offsets_path.value());
            }
        }
    }
    std::set<std::string> skipping_sets;
    PAIMON_RETURN_NOT_OK(GetManifestSkippingSet(retained_snapshots, &skipping_sets));
    DataFilePathFactoryCache data_file_path_factory_cache;
    std::set<std::string> skipping_data_files;
    PAIMON_RETURN_NOT_OK(GetDataFileSkippingSet(retained_snapshots, &data_file_path_factory_cache,
                                                &skipping_data_files));

    int64_t begin_inclusive_id = earliest_snapshot_id;
    for (int64_t id = end_exclusive_id - 1; id >= earliest_snapshot_id; id--) {
        PAIMON_ASSIGN_OR_RAISE(bool exist, snapshot_manager_->SnapshotExists(id));
        if (!exist) {
            begin_inclusive_id = id + 1;
            break;
        }
    }
    PAIMON_LOG_DEBUG(logger_, "Snapshot expire range of table %s is [%ld, %ld]",
                     snapshot_manager_->RootPath().c_str(), begin_inclusive_id, end_exclusive_id);

    // Since the data file deletion information for each snapshot is recorded in the delta part of
    // the next snapshot, it is necessary to check the next snapshot. Otherwise, its data files will
    // not be deleted in this round.
    for (int64_t id = begin_inclusive_id + 1; id <= end_exclusive_id; id++) {
        PAIMON_ASSIGN_OR_RAISE(bool exist, snapshot_manager_->SnapshotExists(id));
        if (!exist) {
            begin_inclusive_id++;
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager_->LoadSnapshot(id));
        PAIMON_RETURN_NOT_OK(CleanUnusedDataFiles(snapshot.DeltaManifestList(), skipping_data_files,
                                                  &data_file_path_factory_cache));
    }
    // TODO(jinli.zjw): support delete changelog files

    // data files in bucket directories has been deleted
    // then delete changed bucket directories if they are empty
    PAIMON_RETURN_NOT_OK(CleanEmptyDirectories());

    std::set<std::string> expired_offset_files;
    for (int64_t id = begin_inclusive_id; id < end_exclusive_id; id++) {
        PAIMON_LOG_DEBUG(logger_, "Ready to delete manifests in snapshot #%ld", id);
        PAIMON_ASSIGN_OR_RAISE(bool exist, snapshot_manager_->SnapshotExists(id));
        if (!exist) {
            begin_inclusive_id++;
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(Snapshot snapshot, snapshot_manager_->LoadSnapshot(id));
        PAIMON_RETURN_NOT_OK(CleanUnusedManifests(snapshot.BaseManifestList(), skipping_sets));
        PAIMON_RETURN_NOT_OK(CleanUnusedManifests(snapshot.DeltaManifestList(), skipping_sets));
        if (realtime_enabled_) {
            std::optional<std::string> offsets_path =
                RealtimeCommitProperties::GetOffsetsPath(snapshot);
            if (offsets_path) {
                expired_offset_files.insert(offsets_path.value());
            }
        }
        auto status = fs_->Delete(snapshot_manager_->SnapshotPath(id));
        // delete quietly will ignore any status error
        (void)status;
    }
    for (const std::string& offsets_path : expired_offset_files) {
        if (retained_offset_files.count(offsets_path) == 0) {
            auto status = fs_->Delete(offsets_path);
            // Orphan cleanup can retry offset files that fail to delete here.
            (void)status;
        }
    }
    PAIMON_RETURN_NOT_OK(snapshot_manager_->CommitEarliestHint(end_exclusive_id));
    return end_exclusive_id - begin_inclusive_id;
}

/// Try to delete data directories that may be empty after data file deletion.
Status ExpireSnapshots::CleanEmptyDirectories() {
    if (!config_.CleanEmptyDirectories() || deletion_buckets_.empty()) {
        return Status::OK();
    }

    // All directory paths are deduplicated and sorted by hierarchy level
    std::map<int32_t, std::set<std::string>> deduplicate;
    for (const auto& [partition, buckets] : deletion_buckets_) {
        std::vector<std::string> to_delete_empty_directories;
        // try to delete bucket directories
        for (const auto& bucket : buckets) {
            PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                                   path_factory_->BucketPath(partition, bucket));
            to_delete_empty_directories.push_back(bucket_path);
        }
        std::vector<std::future<void>> futures;
        for (const auto& empty_directory : to_delete_empty_directories) {
            futures.push_back(Via(executor_.get(), [this, &empty_directory] {
                auto ret = TryDeleteEmptyDirectory(empty_directory);
                (void)ret;
            }));
        }
        Wait(futures);

        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> hierarchical_paths,
                               path_factory_->GetHierarchicalPartitionPath(partition));
        size_t hierarchies = hierarchical_paths.size();
        if (hierarchies == 0) {
            continue;
        }

        if (TryDeleteEmptyDirectory(hierarchical_paths[hierarchies - 1])) {
            // deduplicate high level partition directories
            for (size_t hierarchy = 0; hierarchy < hierarchies - 1; hierarchy++) {
                deduplicate[hierarchy].insert(hierarchical_paths[hierarchy]);
            }
        }
    }

    for (int32_t hierarchy = deduplicate.size() - 1; hierarchy >= 0; hierarchy--) {
        auto iter = deduplicate.find(hierarchy);
        if (iter == deduplicate.end()) {
            continue;
        }
        for (const auto& path : iter->second) {
            TryDeleteEmptyDirectory(path);
        }
    }

    deletion_buckets_.clear();
    return Status::OK();
}

bool ExpireSnapshots::TryDeleteEmptyDirectory(const std::string& path) const {
    auto s = fs_->Delete(path, /*recursive=*/false);
    if (s.ok()) {
        return true;
    }
    return false;
}

Status ExpireSnapshots::CleanUnusedManifests(const std::string& manifest_list_name,
                                             const std::set<std::string>& skipping_sets) {
    std::vector<ManifestFileMeta> manifest_file_metas;
    auto status = manifest_list_->Read(manifest_list_name, nullptr, /*file_size=*/std::nullopt,
                                       &manifest_file_metas);
    if (status.ok()) {
        std::vector<std::string> to_delete_manifests;
        // TODO(jinli.zjw): optimize for async
        for (const auto& manifest_file_meta : manifest_file_metas) {
            if (skipping_sets.count(manifest_file_meta.FileName()) == 0) {
                manifest_file_->DeleteQuietly(manifest_file_meta.FileName());
            }
        }
        if (skipping_sets.count(manifest_list_name) == 0) {
            manifest_list_->DeleteQuietly(manifest_list_name);
        }
    }
    return Status::OK();
}

Status ExpireSnapshots::CleanUnusedDataFiles(
    const std::string& manifest_list_name, const std::set<std::string>& skipping_data_files,
    DataFilePathFactoryCache* data_file_path_factory_cache) {
    std::vector<ManifestFileMeta> manifest_file_metas;
    auto status = manifest_list_->Read(manifest_list_name, nullptr, /*file_size=*/std::nullopt,
                                       &manifest_file_metas);
    if (status.ok()) {
        std::map<std::string, ManifestEntry> data_files_to_delete;
        for (const auto& manifest_file_meta : manifest_file_metas) {
            std::vector<ManifestEntry> manifest_entries;
            auto status = manifest_file_->Read(manifest_file_meta.FileName(), nullptr,
                                               /*file_size=*/std::nullopt, &manifest_entries);
            if (!status.ok()) {
                // cancel deletion if any exception occurs
                PAIMON_LOG_WARN(logger_, "Failed to read some manifest files. Cancel deletion. %s",
                                status.ToString().c_str());
                return Status::OK();
            } else {
                PAIMON_RETURN_NOT_OK(GetDataFilesToDelete(manifest_entries, &data_files_to_delete));
            }
        }

        std::vector<std::future<void>> futures;
        ScopeGuard guard([&futures]() { Wait(futures); });
        for (const auto& [_, entry] : data_files_to_delete) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                                   GetDataFilePathFactory(entry, data_file_path_factory_cache));
            for (const std::string& delete_file_path :
                 data_file_path_factory->CollectFiles(entry.File())) {
                if (skipping_data_files.count(delete_file_path) != 0) {
                    continue;
                }
                futures.push_back(Via(executor_.get(), [this, delete_file_path]() {
                    auto status = fs_->Delete(delete_file_path);
                    // delete quietly will ignore any status error
                    (void)status;
                }));
            }
            deletion_buckets_[entry.Partition()].insert(entry.Bucket());
        }
    }
    return Status::OK();
}

Result<std::shared_ptr<DataFilePathFactory>> ExpireSnapshots::GetDataFilePathFactory(
    const ManifestEntry& entry, DataFilePathFactoryCache* data_file_path_factory_cache) const {
    std::unordered_map<int32_t, std::shared_ptr<DataFilePathFactory>>& bucket_factories =
        (*data_file_path_factory_cache)[entry.Partition()];
    auto factory_iter = bucket_factories.find(entry.Bucket());
    if (factory_iter == bucket_factories.end()) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFilePathFactory> factory,
            path_factory_->CreateDataFilePathFactory(entry.Partition(), entry.Bucket()));
        factory_iter = bucket_factories.emplace(entry.Bucket(), std::move(factory)).first;
    }
    return factory_iter->second;
}

Status ExpireSnapshots::GetDataFileSkippingSet(
    const std::vector<Snapshot>& retained_snapshots,
    DataFilePathFactoryCache* data_file_path_factory_cache,
    std::set<std::string>* skipping_data_files) const {
    auto retain_files = [&](const ManifestEntry& entry) -> Status {
        if (entry.Kind() == FileKind::Add()) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> factory,
                                   GetDataFilePathFactory(entry, data_file_path_factory_cache));
            for (const std::string& path : factory->CollectFiles(entry.File())) {
                skipping_data_files->insert(path);
            }
        }
        return Status::OK();
    };

    // Merge the boundary snapshot once, then protect additions from each later snapshot.
    // A rollback can add an old file again, including its extra files and external location.
    for (size_t i = 0; i < retained_snapshots.size(); ++i) {
        std::vector<ManifestFileMeta> manifests;
        if (i == 0) {
            PAIMON_RETURN_NOT_OK(
                manifest_list_->ReadDataManifests(retained_snapshots[i], &manifests));
        } else {
            PAIMON_RETURN_NOT_OK(
                manifest_list_->ReadDeltaManifests(retained_snapshots[i], &manifests));
        }
        LinkedHashMap<FileEntry::Identifier, ManifestEntry> boundary_entries;
        for (const ManifestFileMeta& manifest : manifests) {
            std::vector<ManifestEntry> entries;
            PAIMON_RETURN_NOT_OK(manifest_file_->Read(manifest.FileName(), nullptr,
                                                      /*file_size=*/std::nullopt, &entries));
            if (i == 0) {
                PAIMON_RETURN_NOT_OK(FileEntry::MergeEntries(entries, &boundary_entries));
            } else {
                for (const ManifestEntry& entry : entries) {
                    PAIMON_RETURN_NOT_OK(retain_files(entry));
                }
            }
        }
        for (const auto& [_, entry] : boundary_entries) {
            PAIMON_RETURN_NOT_OK(retain_files(entry));
        }
    }
    return Status::OK();
}

Status ExpireSnapshots::GetDataFilesToDelete(
    const std::vector<ManifestEntry>& data_file_entries,
    std::map<std::string, ManifestEntry>* data_files_to_delete) const {
    for (const auto& entry : data_file_entries) {
        PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                               path_factory_->BucketPath(entry.Partition(), entry.Bucket()));
        std::string data_file_path = PathUtil::JoinPath(bucket_path, entry.FileName());
        if (entry.Kind() == FileKind::Add()) {
            data_files_to_delete->erase(data_file_path);
        } else if (entry.Kind() == FileKind::Delete()) {
            data_files_to_delete->insert({data_file_path, entry});
        } else {
            return Status::Invalid(
                fmt::format("Unknown value kind {}", entry.Kind().ToByteValue()));
        }
    }
    return Status::OK();
}

Status ExpireSnapshots::GetManifestSkippingSet(const std::vector<Snapshot>& retained_snapshots,
                                               std::set<std::string>* skipping_manifest_set) const {
    for (const auto& snapshot : retained_snapshots) {
        skipping_manifest_set->insert(snapshot.BaseManifestList());
        skipping_manifest_set->insert(snapshot.DeltaManifestList());
        std::vector<ManifestFileMeta> manifests;
        PAIMON_RETURN_NOT_OK(manifest_list_->ReadDataManifests(snapshot, &manifests));
        for (const auto& manifest : manifests) {
            skipping_manifest_set->insert(manifest.FileName());
        }
        // TODO(jinli.zjw): skip index manifests
        if (snapshot.IndexManifest() && snapshot.IndexManifest().value() != "") {
            return Status::NotImplemented("do not support expire snapshot with index manifest");
        }
        if (snapshot.Statistics()) {
            skipping_manifest_set->insert(snapshot.Statistics().value());
        }
    }
    return Status::OK();
}

}  // namespace paimon
