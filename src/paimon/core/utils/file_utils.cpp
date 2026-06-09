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

#include "paimon/core/utils/file_utils.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

Status FileUtils::ListVersionedFiles(const std::shared_ptr<FileSystem>& fs, const std::string& dir,
                                     const std::string& prefix, std::vector<int64_t>* files) {
    std::vector<std::string> file_strs;
    PAIMON_RETURN_NOT_OK(ListOriginalVersionedFiles(fs, dir, prefix, &file_strs));
    for (const auto& file_str : file_strs) {
        std::optional<int64_t> file_number = StringUtils::StringToValue<int64_t>(file_str);
        if (file_number == std::nullopt) {
            return Status::Invalid(fmt::format("fail to convert {} to number", file_str));
        }
        files->emplace_back(file_number.value());
    }
    return Status::OK();
}

Status FileUtils::ListOriginalVersionedFiles(const std::shared_ptr<FileSystem>& fs,
                                             const std::string& dir, const std::string& prefix,
                                             std::vector<std::string>* files) {
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    PAIMON_RETURN_NOT_OK(ListVersionedFileStatus(fs, dir, prefix, &file_status_list));
    for (auto& file_status : file_status_list) {
        std::string file_name = PathUtil::GetName(file_status->GetPath());
        files->emplace_back(file_name.substr(prefix.size()));
    }
    return Status::OK();
}

Status FileUtils::ListVersionedFileStatus(
    const std::shared_ptr<FileSystem>& fs, const std::string& dir, const std::string& prefix,
    std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list) {
    PAIMON_ASSIGN_OR_RAISE(bool exist, fs->Exists(dir));
    if (exist) {
        std::vector<std::unique_ptr<BasicFileStatus>> file_statuses;
        PAIMON_RETURN_NOT_OK(fs->ListDir(dir, &file_statuses));
        for (auto& file_status : file_statuses) {
            std::string file_name = PathUtil::GetName(file_status->GetPath());
            if (StringUtils::StartsWith(file_name, prefix)) {
                file_status_list->emplace_back(std::move(file_status));
            }
        }
    }
    return Status::OK();
}

}  // namespace paimon
