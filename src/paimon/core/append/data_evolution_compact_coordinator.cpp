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

#include "paimon/core/append/data_evolution_compact_coordinator.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/common/utils/vector_store_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/utils/data_evolution_utils.h"

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
    // their row ranges stay covered by the rewritten data files. Vector-store tables are
    // rejected outright: without a VECTOR schema type the rewrite cannot exclude the vector
    // columns the way Java does, and a normal file claiming them as NULL would shadow the
    // dedicated files.
    std::vector<std::shared_ptr<DataFileMeta>> data_files;
    data_files.reserve(files.size());
    for (const auto& file : files) {
        if (VectorStoreUtils::IsVectorStoreFile(file->file_name)) {
            return Status::NotImplemented(
                "Data-evolution compaction does not support tables with vector-store files "
                "yet.");
        }
        if (DataEvolutionUtils::IsNormalFile(file->file_name)) {
            data_files.push_back(file);
        }
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

Result<std::vector<DataEvolutionNormalCompactTask>>
DataEvolutionCompactCoordinator::PlanCompactTasks(
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

}  // namespace paimon
