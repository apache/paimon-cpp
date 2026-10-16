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
#include "paimon/common/utils/scan_cancellation.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

Status FileUtils::ListVersionedFiles(const std::shared_ptr<FileSystem>& fs, const std::string& dir,
                                     const std::string& prefix, std::vector<int64_t>* files,
                                     const ScanCancellationToken* token) {
    PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
    std::vector<std::string> file_strs;
    PAIMON_RETURN_NOT_OK(ListOriginalVersionedFiles(fs, dir, prefix, &file_strs, token));
    for (const auto& file_str : file_strs) {
        PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
        std::optional<int64_t> file_number = StringUtils::StringToValue<int64_t>(file_str);
        if (file_number == std::nullopt) {
            return Status::Invalid(fmt::format("fail to convert {} to number", file_str));
        }
        files->emplace_back(file_number.value());
    }
    return CheckScanCancellation(token);
}

Status FileUtils::ListOriginalVersionedFiles(const std::shared_ptr<FileSystem>& fs,
                                             const std::string& dir, const std::string& prefix,
                                             std::vector<std::string>* files,
                                             const ScanCancellationToken* token) {
    PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
    std::vector<BasicFileStatus> file_status_list;
    PAIMON_RETURN_NOT_OK(ListVersionedFileStatus(fs, dir, prefix, &file_status_list, token));
    for (auto& file_status : file_status_list) {
        PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
        std::string file_name = PathUtil::GetName(file_status.GetPath());
        files->emplace_back(file_name.substr(prefix.size()));
    }
    return CheckScanCancellation(token);
}

Status FileUtils::ListVersionedFileStatus(const std::shared_ptr<FileSystem>& fs,
                                          const std::string& dir, const std::string& prefix,
                                          std::vector<BasicFileStatus>* file_status_list,
                                          const ScanCancellationToken* token) {
    PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
    // No Exists() probe in front of the listing. Every FileSystem lists a directory that is not
    // there as an empty result rather than an error, so the probe only decided whether to make a
    // call that answers the same question. On a remote store it costs a round trip of its own,
    // which is what a schema or snapshot lookup pays before it can read anything.
    std::vector<BasicFileStatus> file_statuses;
    PAIMON_RETURN_NOT_OK(fs->ListDir(dir, &file_statuses));
    PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
    for (auto& file_status : file_statuses) {
        PAIMON_RETURN_NOT_OK(CheckScanCancellation(token));
        std::string file_name = PathUtil::GetName(file_status.GetPath());
        if (StringUtils::StartsWith(file_name, prefix)) {
            file_status_list->emplace_back(std::move(file_status));
        }
    }
    return CheckScanCancellation(token);
}

}  // namespace paimon
