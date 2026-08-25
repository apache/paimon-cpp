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

#include "paimon/core/table/format/format_file_listing.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/utils/partition_path_utils.h"
#include "paimon/fs/file_system.h"

namespace paimon {

bool FormatFileListing::IsReservedDirectory(const std::string& name) {
    return name == "schema" || name == "branch";
}

Status FormatFileListing::ListDataFiles(const std::shared_ptr<FileSystem>& file_system,
                                        const std::string& root,
                                        const FormatDataFileListingOptions& options,
                                        std::vector<FormatDataSplit::FileMeta>* files) {
    // Checked explicitly: the file systems here report a missing directory as an empty listing
    // rather than as an error.
    PAIMON_ASSIGN_OR_RAISE(FileStatus root_status, file_system->GetFileStatus(root));
    if (!root_status.IsDir()) {
        return Status::Invalid(
            fmt::format("{} is not a directory, so it holds no table data", root));
    }

    std::vector<std::string> level = {root};
    // Depth of the directories in `level`, counted from the listed root.
    int32_t depth = 0;
    while (!level.empty()) {
        // The default partition name holds table content only at a partition level; anywhere else
        // a hidden name is a staging tree.
        const bool children_are_partitions = options.partition_levels >= depth + 1;
        const bool exempt_default_part_name = children_are_partitions &&
                                              options.only_value_in_path &&
                                              !options.default_part_name.empty();
        std::vector<std::string> next;
        for (const std::string& directory : level) {
            std::vector<FileStatus> children;
            Status status = file_system->ListFileStatus(directory, &children);
            if (status.IsNotExist()) {
                // Gone since its parent listed it; the rest still stands. Not the root, which
                // was checked above.
                continue;
            }
            PAIMON_RETURN_NOT_OK(status);
            for (const FileStatus& child : children) {
                const std::string name = PathUtil::GetName(child.GetPath());
                const bool hidden = PartitionPathUtils::IsHiddenName(name);
                if (child.IsDir()) {
                    const bool is_default_part_dir =
                        exempt_default_part_name && name == options.default_part_name;
                    if (hidden && !is_default_part_dir) {
                        continue;
                    }
                    if (depth == 0 && options.skip_reserved_directories &&
                        IsReservedDirectory(name)) {
                        continue;
                    }
                    next.push_back(child.GetPath());
                } else if (!hidden) {
                    files->emplace_back(child.GetPath(), child.GetLen());
                }
            }
        }
        level = std::move(next);
        depth++;
    }
    return Status::OK();
}

}  // namespace paimon
