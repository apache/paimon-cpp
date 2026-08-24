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

#include "paimon/core/append/data_evolution_compact_planner.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/common/utils/vector_store_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/logging.h"

namespace paimon {

namespace {

/// A bin that packs evolved field groups of one contiguous row id run into one compact task.
class CompactBin {
 public:
    Status Add(std::vector<std::shared_ptr<DataFileMeta>>&& file_group, int64_t weight) {
        if (weight_ > std::numeric_limits<int64_t>::max() - weight) {
            return Status::Invalid("Data evolution compaction bin weight overflows.");
        }
        files_.insert(files_.end(), std::make_move_iterator(file_group.begin()),
                      std::make_move_iterator(file_group.end()));
        weight_ += weight;
        return Status::OK();
    }

    int64_t Weight() const {
        return weight_;
    }

    std::vector<std::shared_ptr<DataFileMeta>> Drain() {
        std::vector<std::shared_ptr<DataFileMeta>> result = std::move(files_);
        files_.clear();
        weight_ = 0;
        return result;
    }

 private:
    std::vector<std::shared_ptr<DataFileMeta>> files_;
    int64_t weight_ = 0;
};

/// Emits a task for the bin's files when there are enough of them; too few files are simply
/// left as they are (they will be reconsidered by a later coordinator run once neighbors
/// appear).
Status TriggerTask(std::vector<std::shared_ptr<DataFileMeta>>&& files, const BinaryRow& partition,
                   int32_t compact_min_file_num,
                   std::vector<DataEvolutionNormalCompactTask>* tasks) {
    if (static_cast<int32_t>(files.size()) < compact_min_file_num) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(DataEvolutionNormalCompactTask task,
                           DataEvolutionNormalCompactTask::Create(partition, files));
    tasks->push_back(std::move(task));
    return Status::OK();
}

int64_t FileWeight(const std::shared_ptr<DataFileMeta>& file, int64_t open_file_cost) {
    return std::max(file->file_size, open_file_cost);
}

Status PlanPartition(const BinaryRow& partition,
                     const std::vector<std::shared_ptr<DataFileMeta>>& files,
                     int64_t target_file_size, int64_t open_file_cost, int32_t compact_min_file_num,
                     std::vector<DataEvolutionNormalCompactTask>* tasks) {
    // Blob files are dedicated storage: they are not rewritten and never enter a task, and
    // their row ranges stay covered by the rewritten data files. Vector-store files were
    // already rejected by PlanCompactTasks, so everything else is a normal file.
    std::vector<std::shared_ptr<DataFileMeta>> data_files;
    data_files.reserve(files.size());
    for (const auto& file : files) {
        if (BlobUtils::IsBlobFile(file->file_name)) {
            continue;
        }
        data_files.push_back(file);
    }
    if (data_files.empty()) {
        return Status::OK();
    }

    // Contiguous runs extend each file's range to [first, first + row_count], one past its
    // last row: adjacent files share an endpoint and overlap, so a run only breaks on a real
    // row id gap.
    RangeHelper<std::shared_ptr<DataFileMeta>> adjacency_helper(
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            return file->NonNullFirstRowId();
        },
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
            return first_row_id + file->row_count;
        });
    // Field groups use the closed range [first, first + row_count - 1]: only files holding the
    // same rows overlap here, and a group must cover the exact same range in every file.
    RangeHelper<std::shared_ptr<DataFileMeta>> field_group_helper(
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            return file->NonNullFirstRowId();
        },
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
            return first_row_id + file->row_count - 1;
        });

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> components,
                           adjacency_helper.MergeOverlappingRanges(std::move(data_files)));
    for (auto& component : components) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::vector<std::shared_ptr<DataFileMeta>>> field_groups,
                               field_group_helper.MergeOverlappingRanges(std::move(component)));
        CompactBin bin;
        for (auto& field_group : field_groups) {
            PAIMON_ASSIGN_OR_RAISE(bool same_range,
                                   field_group_helper.AreAllRangesSame(field_group));
            if (!same_range) {
                std::vector<std::string> file_names;
                file_names.reserve(field_group.size());
                for (const auto& file : field_group) {
                    file_names.push_back(file->file_name);
                }
                return Status::Invalid(fmt::format(
                    "Files of one data evolution field group should share the same row range, "
                    "but got [{}].",
                    fmt::join(file_names, ", ")));
            }
            int64_t weight = 0;
            for (const auto& file : field_group) {
                int64_t file_weight = FileWeight(file, open_file_cost);
                if (weight > std::numeric_limits<int64_t>::max() - file_weight) {
                    return Status::Invalid(
                        "Data evolution compaction field group weight overflows.");
                }
                weight += file_weight;
            }
            if (weight > target_file_size) {
                // A heavy group cuts the current bin and is considered on its own (still
                // subject to the min-file-num gate): merging its files is worthwhile, but
                // nothing else is packed on top of it.
                PAIMON_RETURN_NOT_OK(
                    TriggerTask(bin.Drain(), partition, compact_min_file_num, tasks));
                CompactBin single_group_bin;
                PAIMON_RETURN_NOT_OK(single_group_bin.Add(std::move(field_group), weight));
                PAIMON_RETURN_NOT_OK(
                    TriggerTask(single_group_bin.Drain(), partition, compact_min_file_num, tasks));
                continue;
            }
            PAIMON_RETURN_NOT_OK(bin.Add(std::move(field_group), weight));
            if (bin.Weight() > target_file_size) {
                PAIMON_RETURN_NOT_OK(
                    TriggerTask(bin.Drain(), partition, compact_min_file_num, tasks));
            }
        }
        // A row id gap follows: whatever is left in the bin cannot be packed any further.
        PAIMON_RETURN_NOT_OK(TriggerTask(bin.Drain(), partition, compact_min_file_num, tasks));
    }
    return Status::OK();
}

}  // namespace

Result<std::vector<DataEvolutionNormalCompactTask>> DataEvolutionCompactPlanner::PlanCompactTasks(
    const LinkedHashMap<BinaryRow, std::vector<std::shared_ptr<DataFileMeta>>>& partition_files,
    const CoreOptions& options) {
    int64_t target_file_size = options.GetTargetFileSize(/*has_primary_key=*/false);
    int64_t open_file_cost = options.GetSourceSplitOpenFileCost();
    int32_t compact_min_file_num = options.GetCompactionMinFileNum();
    if (target_file_size <= 0 || open_file_cost < 0 || compact_min_file_num <= 0) {
        return Status::Invalid(fmt::format(
            "Invalid data-evolution compaction options: target-file-size {} must be positive, "
            "open-file-cost {} must be non-negative, min-file-num {} must be positive.",
            target_file_size, open_file_cost, compact_min_file_num));
    }

    std::vector<DataEvolutionNormalCompactTask> tasks;
    for (const auto& [partition, files] : partition_files) {
        for (const auto& file : files) {
            // PlanCompactTasks is a public entry, so the input is validated here before any
            // planning arithmetic runs on it.
            if (file == nullptr) {
                return Status::Invalid("Data evolution compaction files must not be null.");
            }
            // Vector-store tables are rejected outright: without a VECTOR schema type the
            // rewrite cannot exclude the vector columns, and a normal file claiming them as
            // NULL would shadow the dedicated files. Checked before the row id validation
            // below so an unsupported table says so, rather than reporting whichever metadata
            // of a vector file happens to look invalid.
            if (VectorStoreUtils::IsVectorStoreFile(file->file_name)) {
                return Status::NotImplemented(
                    "Data-evolution compaction does not support tables with vector-store files "
                    "yet.");
            }
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
            if (file->row_count <= 0 || file->file_size < 0 || first_row_id < 0 ||
                first_row_id > std::numeric_limits<int64_t>::max() - file->row_count) {
                return Status::Invalid(fmt::format(
                    "Invalid data-evolution compaction input {}: file size {}, first row id "
                    "{} and row count {} must form a valid file and row id range.",
                    file->file_name, file->file_size, first_row_id, file->row_count));
            }
        }
        PAIMON_RETURN_NOT_OK(PlanPartition(partition, files, target_file_size, open_file_cost,
                                           compact_min_file_num, &tasks));
    }
    return tasks;
}

Result<std::vector<Range>> DataEvolutionCompactPlanner::PlanRowIdWindows(
    const std::shared_ptr<ManifestList>& manifest_list, const Snapshot& snapshot,
    const std::shared_ptr<PredicateFilter>& partition_filter,
    const std::shared_ptr<arrow::Schema>& partition_schema, int64_t candidate_files_per_round,
    Logger* logger) {
    std::vector<ManifestFileMeta> manifests;
    PAIMON_RETURN_NOT_OK(manifest_list->ReadDataManifests(snapshot, &manifests));

    struct ManifestBound {
        int64_t min_row_id;
        int64_t max_row_id;
        int64_t num_files;
    };
    std::vector<ManifestBound> bounds;
    bounds.reserve(manifests.size());
    for (const auto& manifest : manifests) {
        // Skipping a manifest the scan would drop anyway keeps its row ids from padding the
        // windows. It cannot hide a straddling file either: the gap test below only ever
        // looks at manifests that are kept.
        if (partition_filter != nullptr) {
            SimpleStats partition_stats = manifest.PartitionStats();
            PAIMON_ASSIGN_OR_RAISE(
                bool matches,
                partition_filter->Test(partition_schema,
                                       manifest.NumAddedFiles() + manifest.NumDeletedFiles(),
                                       partition_stats.MinValues(), partition_stats.MaxValues(),
                                       partition_stats.NullCounts()));
            if (!matches) {
                continue;
            }
        }
        // A manifest holding no ADD entry contributes no candidate file, so it can neither
        // pad a window nor hide a file that straddles a cut. Skipped before the statistics
        // check so a delete-only manifest cannot disable the split; the trade-off is that
        // such a manifest, if it also lacks row id statistics, is read by every round.
        if (manifest.NumAddedFiles() <= 0) {
            continue;
        }
        // A single manifest without usable row id statistics would leave a hole in the
        // windows, so the whole batching is abandoned rather than silently skipping its files.
        if (!manifest.MinRowId().has_value() || !manifest.MaxRowId().has_value() ||
            manifest.MinRowId().value() > manifest.MaxRowId().value()) {
            PAIMON_LOG_INFO(
                logger,
                "Compacting in a single round: manifest %s carries no usable row id statistics, "
                "so the row id space cannot be split.",
                manifest.FileName().c_str());
            return std::vector<Range>{};
        }
        bounds.push_back(ManifestBound{manifest.MinRowId().value(), manifest.MaxRowId().value(),
                                       manifest.NumAddedFiles()});
    }
    if (bounds.empty()) {
        return std::vector<Range>{};
    }
    std::sort(bounds.begin(), bounds.end(),
              [](const ManifestBound& left, const ManifestBound& right) {
                  return left.min_row_id != right.min_row_id ? left.min_row_id < right.min_row_id
                                                             : left.max_row_id < right.max_row_id;
              });

    std::vector<Range> windows;
    int64_t window_from = 0;
    int64_t covered_to = bounds.front().max_row_id;
    int64_t window_files = 0;
    for (size_t i = 0; i < bounds.size(); i++) {
        covered_to = std::max(covered_to, bounds[i].max_row_id);
        window_files += bounds[i].num_files;
        bool has_next = i + 1 < bounds.size();
        if (has_next && window_files >= candidate_files_per_round &&
            covered_to < bounds[i + 1].min_row_id) {
            windows.emplace_back(window_from, covered_to);
            window_from = covered_to + 1;
            window_files = 0;
            // Start the next window at the manifest that opened the gap instead of carrying
            // this window's reach forward.
            covered_to = bounds[i + 1].max_row_id;
        }
    }
    // The tail stays open ended so rows appended after the manifests were read still belong
    // to a window rather than falling outside every one of them.
    windows.emplace_back(window_from, std::numeric_limits<int64_t>::max());
    return windows;
}

}  // namespace paimon
