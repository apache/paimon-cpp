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

#include "paimon/core/io/data_file_path_factory.h"

#include <optional>
#include <utility>

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/macros.h"
#include "paimon/status.h"

namespace paimon {

const char DataFilePathFactory::CHANGELOG_FILE_PREFIX[] = "changelog-";
const char DataFilePathFactory::INDEX_PATH_SUFFIX[] = ".index";

Status DataFilePathFactory::Init(const std::string& parent, const std::string& format_identifier,
                                 const std::string& data_file_prefix,
                                 std::unique_ptr<ExternalPathProvider>&& external_path_provider) {
    if (PAIMON_UNLIKELY(!UUID::Generate(&uuid_))) {
        return Status::Invalid("fail to generate uuid for data file path factory");
    }
    parent_ = parent;
    path_count_.store(0);
    format_identifier_ = format_identifier;
    data_file_prefix_ = data_file_prefix;
    external_path_provider_ = std::move(external_path_provider);
    return Status::OK();
}

std::string DataFilePathFactory::NewPath(const std::string& prefix) const {
    return NewPathFromName(NewFileName(prefix));
}

std::string DataFilePathFactory::ToPath(const std::string& file_name) const {
    return PathUtil::JoinPath(parent_, file_name);
}

std::string DataFilePathFactory::ToPath(const std::shared_ptr<DataFileMeta>& file_meta) const {
    if (file_meta->external_path) {
        return file_meta->external_path.value();
    }
    return PathUtil::JoinPath(parent_, file_meta->file_name);
}

std::string DataFilePathFactory::ToFileIndexPath(const std::string& file_path) const {
    std::string parent = PathUtil::GetParentDirPath(file_path);
    return PathUtil::JoinPath(parent, PathUtil::GetName(file_path) + INDEX_PATH_SUFFIX);
}

std::string DataFilePathFactory::ToAlignedPath(const std::string& file_name,
                                               const std::shared_ptr<DataFileMeta>& aligned) const {
    auto external_path = aligned->ExternalPathDir();
    return PathUtil::JoinPath(external_path ? external_path.value() : parent_, file_name);
}

std::vector<std::string> DataFilePathFactory::CollectFiles(
    const std::shared_ptr<DataFileMeta>& file_meta) const {
    std::vector<std::string> paths;
    paths.push_back(ToPath(file_meta));
    for (const auto& extra_file : file_meta->extra_files) {
        if (extra_file) {
            paths.push_back(ToAlignedPath(extra_file.value(), file_meta));
        }
    }
    return paths;
}
}  // namespace paimon
