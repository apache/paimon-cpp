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

#include "paimon/core/table/format/format_table_scan.h"

#include <algorithm>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/bin_packing.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/core_options.h"
#include "paimon/core/table/format/format_data_split.h"
#include "paimon/core/table/format/format_file_listing.h"
#include "paimon/core/table/format/format_path_validation.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/utils/partition_path_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"

namespace paimon {

namespace {
Logger* ScanLogger() {
    static std::unique_ptr<Logger> logger = Logger::GetLogger("FormatTableScan");
    return logger.get();
}
}  // namespace

FormatTableScan::FormatTableScan(const std::shared_ptr<FormatTable>& table,
                                 const std::map<std::string, std::string>& partition_filter,
                                 const std::optional<int32_t>& limit, int64_t target_split_size,
                                 int64_t open_file_cost)
    : table_(table),
      partition_filter_(partition_filter),
      limit_(limit),
      target_split_size_(target_split_size),
      open_file_cost_(open_file_cost) {}

FormatTableScan::~FormatTableScan() = default;

Result<std::unique_ptr<FormatTableScan>> FormatTableScan::Create(
    const std::shared_ptr<FormatTable>& table,
    const std::map<std::string, std::string>& partition_filter,
    const std::optional<int32_t>& limit) {
    if (table == nullptr) {
        return Status::Invalid("format table scan requires a table");
    }
    const std::vector<std::string>& partition_keys = table->PartitionKeys();
    for (const auto& filter : partition_filter) {
        const std::string& key = filter.first;
        if (std::find(partition_keys.begin(), partition_keys.end(), key) == partition_keys.end()) {
            return Status::Invalid(
                fmt::format("partition filter field '{}' is not a partition key of table {}", key,
                            table->FullName()));
        }
    }
    // Through `CoreOptions`, so `"128 mb"` means what it does elsewhere and a default lives in
    // one place.
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(table->Options(), table->GetFileSystem()));
    return std::unique_ptr<FormatTableScan>(
        new FormatTableScan(table, partition_filter, limit, core_options.GetSourceSplitTargetSize(),
                            core_options.GetSourceSplitOpenFileCost()));
}

Result<std::vector<FormatTableScan::PartitionAndPath>> FormatTableScan::FindPartitions() const {
    const std::vector<std::string>& partition_keys = table_->PartitionKeys();
    // Partitions are discovered by listing one directory level at a time, so the filter applied
    // and the number of partitions that survived it are the only way to explain an empty plan.
    PAIMON_LOG_DEBUG(ScanLogger(), "Finding partitions for format table %s, partition filter: %s",
                     table_->FullName().c_str(), fmt::format("{}", partition_filter_).c_str());
    std::shared_ptr<FileSystem> file_system = table_->GetFileSystem();

    const bool only_value = table_->PartitionOnlyValueInPath();
    // The one hidden name that is table content: a null partition's directory in the value-only
    // layout.
    const std::string& default_partition_name = table_->PartitionDefaultName();

    // One partition level at a time, keeping each partition paired with its directory.
    std::vector<PartitionAndPath> level = {
        {std::map<std::string, std::string>(), table_->Location()}};
    bool at_table_location = true;
    for (const std::string& partition_key : partition_keys) {
        std::vector<PartitionAndPath> next;
        for (const auto& [partition, directory] : level) {
            std::vector<BasicFileStatus> children;
            Status status = file_system->ListDir(directory, &children);
            if (status.IsNotExist()) {
                // Gone since it was listed, or a table directory not created yet; either way
                // the rest of the listing stands.
                continue;
            }
            PAIMON_RETURN_NOT_OK(status);
            for (const BasicFileStatus& child : children) {
                if (!child.IsDir()) {
                    continue;
                }
                std::string name = PathUtil::GetName(child.GetPath());
                if (PartitionPathUtils::IsHiddenName(name) &&
                    !(only_value && name == default_partition_name)) {
                    continue;
                }
                if (at_table_location && table_->LocationCarriesPaimonMetadata() &&
                    FormatFileListing::IsReservedDirectory(name)) {
                    continue;
                }
                std::string value;
                if (only_value) {
                    // Nothing but the level says which key a directory belongs to, so every
                    // directory here is one.
                    value = PartitionPathUtils::UnescapePathName(name);
                } else {
                    std::optional<std::pair<std::string, std::string>> key_value =
                        PartitionPathUtils::ExtractPartitionKeyValue(name);
                    if (!key_value || key_value->first != partition_key) {
                        // Something else lives here: another layout, or a nested table.
                        continue;
                    }
                    value = std::move(key_value->second);
                }
                auto filter_iter = partition_filter_.find(partition_key);
                if (filter_iter != partition_filter_.end() && filter_iter->second != value) {
                    continue;
                }
                std::map<std::string, std::string> child_partition = partition;
                child_partition[partition_key] = std::move(value);
                next.emplace_back(std::move(child_partition), child.GetPath());
            }
        }
        level = std::move(next);
        at_table_location = false;
    }
    PAIMON_LOG_DEBUG(ScanLogger(), "Found %zu partitions of format table %s", level.size(),
                     table_->FullName().c_str());
    return level;
}

Result<std::vector<std::shared_ptr<Split>>> FormatTableScan::CreateSplits(
    const std::string& directory, const std::map<std::string, std::string>& partition) const {
    // `directory` is a complete partition, or the table itself when unpartitioned, so no partition
    // directory is left below it and `partition_levels` stays 0.
    FormatDataFileListingOptions listing;
    // Only right at the table's location are `schema` and `branch` its metadata.
    PAIMON_ASSIGN_OR_RAISE(bool at_location,
                           FormatPathValidation::IsTableLocation(table_, directory));
    listing.skip_reserved_directories = at_location && table_->LocationCarriesPaimonMetadata();
    std::vector<FormatDataSplit::FileMeta> files;
    PAIMON_RETURN_NOT_OK(
        FormatFileListing::ListDataFiles(table_->GetFileSystem(), directory, listing, &files));
    std::vector<std::shared_ptr<Split>> splits;
    if (files.empty()) {
        return splits;
    }
    // The file system promises no listing order, and a scan's output should be reproducible.
    std::sort(files.begin(), files.end(),
              [](const FormatDataSplit::FileMeta& left, const FormatDataSplit::FileMeta& right) {
                  return left.file_path < right.file_path;
              });
    // A file is never cut up: parquet and orc each record where their own row groups and stripes
    // begin. An entry costs at least the open-file cost, so a split cannot gather so many small
    // files that opening them outweighs reading them.
    const int64_t open_file_cost = open_file_cost_;
    std::vector<std::vector<FormatDataSplit::FileMeta>> bins =
        BinPacking::PackForOrdered<FormatDataSplit::FileMeta>(
            std::move(files),
            [open_file_cost](const FormatDataSplit::FileMeta& file) {
                return std::max(file.file_size, open_file_cost);
            },
            target_split_size_);
    splits.reserve(bins.size());
    for (const std::vector<FormatDataSplit::FileMeta>& bin : bins) {
        splits.push_back(std::make_shared<FormatDataSplit>(bin, partition));
    }
    return splits;
}

Result<std::shared_ptr<Plan>> FormatTableScan::CreatePlan() {
    std::vector<std::shared_ptr<Split>> splits;
    // A non-positive limit asks for no rows at all, so there is nothing to read.
    if (limit_ && *limit_ <= 0) {
        return std::make_shared<PlanImpl>(std::nullopt, splits);
    }

    if (table_->PartitionKeys().empty()) {
        PAIMON_ASSIGN_OR_RAISE(splits, CreateSplits(table_->Location(), {}));
        return std::make_shared<PlanImpl>(std::nullopt, splits);
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<PartitionAndPath> partitions, FindPartitions());
    // A stable order, like the files within a partition.
    std::sort(partitions.begin(), partitions.end(),
              [](const auto& left, const auto& right) { return left.second < right.second; });
    for (const auto& [partition, directory] : partitions) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Split>> partition_splits,
                               CreateSplits(directory, partition));
        splits.insert(splits.end(), std::make_move_iterator(partition_splits.begin()),
                      std::make_move_iterator(partition_splits.end()));
    }
    return std::make_shared<PlanImpl>(std::nullopt, splits);
}

Result<std::vector<std::map<std::string, std::string>>> FormatTableScan::ListPartitions() const {
    std::vector<std::map<std::string, std::string>> result;
    if (table_->PartitionKeys().empty()) {
        return result;
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<PartitionAndPath> partitions, FindPartitions());
    // A stable order, like a plan's splits.
    std::sort(partitions.begin(), partitions.end(),
              [](const auto& left, const auto& right) { return left.second < right.second; });
    result.reserve(partitions.size());
    for (PartitionAndPath& partition_and_path : partitions) {
        result.push_back(std::move(partition_and_path.first));
    }
    return result;
}

}  // namespace paimon
