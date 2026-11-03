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
    virtual std::string ToPath(const std::string& file_name) const = 0;

    /// Returns the directory containing the checkpoint files.
    virtual const std::string& GetDirectoryPath() const = 0;

    /// Returns the checkpoint id when the file name belongs to this factory.
    virtual std::optional<int64_t> GetCheckpointId(const std::string& file_name) const = 0;

    /// Returns the task's checkpoint directory under the configured global index directory.
    static Result<std::string> GetTaskDirectory(
        const std::string& table_path, const std::string& task_id,
        const std::optional<std::string>& global_index_external_path) {
        PAIMON_RETURN_NOT_OK(PathUtil::CheckSinglePathComponent("checkpoint task id", task_id));
        std::string index_directory =
            global_index_external_path.value_or(PathUtil::JoinPath(table_path, "index"));
        return PathUtil::JoinPath(PathUtil::JoinPath(index_directory, "checkpoint"), task_id);
    }
};

}  // namespace paimon
