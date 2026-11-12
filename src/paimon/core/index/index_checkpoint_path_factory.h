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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Path factory for global index checkpoints scoped to one task.
class IndexCheckpointPathFactory {
 public:
    virtual ~IndexCheckpointPathFactory() = default;

    /// Creates the path for the specified checkpoint id, without storage I/O.
    virtual std::string NewPath(int64_t checkpoint_id) const = 0;

    /// Returns the directories containing the checkpoint files.
    virtual const std::vector<std::string>& GetDirectoryPaths() const = 0;

    /// Returns the checkpoint id when the file name belongs to this factory.
    virtual std::optional<int64_t> GetCheckpointId(const std::string& file_name) const = 0;

    /// Returns the task's checkpoint directories under the configured global index directories.
    static Result<std::vector<std::string>> GetTaskDirectories(
        const std::string& table_path, const std::string& task_id,
        const std::vector<std::string>& global_index_external_paths) {
        PAIMON_RETURN_NOT_OK(PathUtil::CheckSinglePathComponent("checkpoint task id", task_id));
        std::vector<std::string> roots = {PathUtil::JoinPath(table_path, "index")};
        if (!global_index_external_paths.empty()) {
            roots = global_index_external_paths;
        }
        std::vector<std::string> directories;
        for (const auto& root : roots) {
            directories.push_back(
                PathUtil::JoinPath(PathUtil::JoinPath(root, "checkpoint"), task_id));
        }
        return directories;
    }
};

}  // namespace paimon
