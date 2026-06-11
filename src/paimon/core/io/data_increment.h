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

#pragma once
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/data_file_meta.h"

namespace paimon {
// Increment of data files, changelog files and index files.
class DataIncrement {
 public:
    explicit DataIncrement(std::vector<std::shared_ptr<IndexFileMeta>>&& new_index_files)
        : DataIncrement({}, {}, {}, std::move(new_index_files), {}) {}

    DataIncrement(std::vector<std::shared_ptr<DataFileMeta>>&& new_files,
                  std::vector<std::shared_ptr<DataFileMeta>>&& deleted_files,
                  std::vector<std::shared_ptr<DataFileMeta>>&& changelog_files)
        : DataIncrement(std::move(new_files), std::move(deleted_files), std::move(changelog_files),
                        {}, {}) {}

    DataIncrement(std::vector<std::shared_ptr<DataFileMeta>>&& new_files,
                  std::vector<std::shared_ptr<DataFileMeta>>&& deleted_files,
                  std::vector<std::shared_ptr<DataFileMeta>>&& changelog_files,
                  std::vector<std::shared_ptr<IndexFileMeta>>&& new_index_files,
                  std::vector<std::shared_ptr<IndexFileMeta>>&& deleted_index_files)
        : new_files_(std::move(new_files)),
          deleted_files_(std::move(deleted_files)),
          changelog_files_(std::move(changelog_files)),
          new_index_files_(std::move(new_index_files)),
          deleted_index_files_(std::move(deleted_index_files)) {}

    const std::vector<std::shared_ptr<DataFileMeta>>& NewFiles() const {
        return new_files_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& DeletedFiles() const {
        return deleted_files_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& ChangelogFiles() const {
        return changelog_files_;
    }

    const std::vector<std::shared_ptr<IndexFileMeta>>& NewIndexFiles() const {
        return new_index_files_;
    }

    const std::vector<std::shared_ptr<IndexFileMeta>>& DeletedIndexFiles() const {
        return deleted_index_files_;
    }

    void AddNewIndexFiles(std::vector<std::shared_ptr<IndexFileMeta>>&& new_index_files) {
        new_index_files_.insert(new_index_files_.end(),
                                std::make_move_iterator(new_index_files.begin()),
                                std::make_move_iterator(new_index_files.end()));
    }

    void AddDeletedIndexFiles(std::vector<std::shared_ptr<IndexFileMeta>>&& deleted_index_files) {
        deleted_index_files_.insert(deleted_index_files_.end(),
                                    std::make_move_iterator(deleted_index_files.begin()),
                                    std::make_move_iterator(deleted_index_files.end()));
    }

    bool IsEmpty() const {
        return new_files_.empty() && deleted_files_.empty() && changelog_files_.empty() &&
               new_index_files_.empty() && deleted_index_files_.empty();
    }

    bool operator==(const DataIncrement& other) const {
        if (this == &other) {
            return true;
        }
        return ObjectUtils::Equal(new_files_, other.new_files_) &&
               ObjectUtils::Equal(deleted_files_, other.deleted_files_) &&
               ObjectUtils::Equal(changelog_files_, other.changelog_files_) &&
               ObjectUtils::Equal(new_index_files_, other.new_index_files_) &&
               ObjectUtils::Equal(deleted_index_files_, other.deleted_index_files_);
    }

    bool TEST_Equal(const DataIncrement& other) const {
        if (this == &other) {
            return true;
        }
        return ObjectUtils::TEST_Equal(new_files_, other.new_files_) &&
               ObjectUtils::TEST_Equal(deleted_files_, other.deleted_files_) &&
               ObjectUtils::TEST_Equal(changelog_files_, other.changelog_files_) &&
               ObjectUtils::TEST_Equal(new_index_files_, other.new_index_files_) &&
               ObjectUtils::TEST_Equal(deleted_index_files_, other.deleted_index_files_);
    }

    std::string ToString() const {
        std::vector<std::string> new_files_names;
        new_files_names.reserve(new_files_.size());
        for (const auto& new_file : new_files_) {
            new_files_names.emplace_back(new_file->file_name);
        }
        std::vector<std::string> deleted_files_names;
        deleted_files_names.reserve(deleted_files_.size());
        for (const auto& deleted_file : deleted_files_) {
            deleted_files_names.emplace_back(deleted_file->file_name);
        }
        std::vector<std::string> changelog_files_names;
        changelog_files_names.reserve(changelog_files_.size());
        for (const auto& changelog_file : changelog_files_) {
            changelog_files_names.emplace_back(changelog_file->file_name);
        }
        std::vector<std::string> new_index_names;
        new_index_names.reserve(new_index_files_.size());
        for (const auto& new_index_file : new_index_files_) {
            new_index_names.emplace_back(new_index_file->FileName());
        }
        std::vector<std::string> deleted_index_names;
        deleted_index_names.reserve(deleted_index_files_.size());
        for (const auto& deleted_index_file : deleted_index_files_) {
            deleted_index_names.emplace_back(deleted_index_file->FileName());
        }

        return fmt::format(
            "DataIncrement {{newFiles = {}, deletedFiles = {}, changelogFiles = {}, newIndexFiles "
            "= {}, deletedIndexFiles = {}}}",
            fmt::join(new_files_names, ", "), fmt::join(deleted_files_names, ", "),
            fmt::join(changelog_files_names, ", "), fmt::join(new_index_names, ", "),
            fmt::join(deleted_index_names, ", "));
    }

 private:
    std::vector<std::shared_ptr<DataFileMeta>> new_files_;
    std::vector<std::shared_ptr<DataFileMeta>> deleted_files_;
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files_;
    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files_;
    std::vector<std::shared_ptr<IndexFileMeta>> deleted_index_files_;
};

}  // namespace paimon
