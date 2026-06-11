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
// Files changed before and after compaction, with changelog produced during compaction.
class CompactIncrement {
 public:
    CompactIncrement(std::vector<std::shared_ptr<DataFileMeta>>&& compact_before,
                     std::vector<std::shared_ptr<DataFileMeta>>&& compact_after,
                     std::vector<std::shared_ptr<DataFileMeta>>&& changelog_files)
        : CompactIncrement(std::move(compact_before), std::move(compact_after),
                           std::move(changelog_files), {}, {}) {}

    CompactIncrement(std::vector<std::shared_ptr<DataFileMeta>>&& compact_before,
                     std::vector<std::shared_ptr<DataFileMeta>>&& compact_after,
                     std::vector<std::shared_ptr<DataFileMeta>>&& changelog_files,
                     std::vector<std::shared_ptr<IndexFileMeta>>&& new_index_files,
                     std::vector<std::shared_ptr<IndexFileMeta>>&& deleted_index_files)
        : compact_before_(std::move(compact_before)),
          compact_after_(std::move(compact_after)),
          changelog_files_(std::move(changelog_files)),
          new_index_files_(std::move(new_index_files)),
          deleted_index_files_(std::move(deleted_index_files)) {}

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactBefore() const {
        return compact_before_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& CompactAfter() const {
        return compact_after_;
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
        return compact_before_.empty() && compact_after_.empty() && changelog_files_.empty() &&
               new_index_files_.empty() && deleted_index_files_.empty();
    }

    bool operator==(const CompactIncrement& other) const {
        if (this == &other) {
            return true;
        }
        return ObjectUtils::Equal(compact_before_, other.compact_before_) &&
               ObjectUtils::Equal(compact_after_, other.compact_after_) &&
               ObjectUtils::Equal(changelog_files_, other.changelog_files_) &&
               ObjectUtils::Equal(new_index_files_, other.new_index_files_) &&
               ObjectUtils::Equal(deleted_index_files_, other.deleted_index_files_);
    }

    bool TEST_Equal(const CompactIncrement& other) const {
        if (this == &other) {
            return true;
        }
        return ObjectUtils::TEST_Equal(compact_before_, other.compact_before_) &&
               ObjectUtils::TEST_Equal(compact_after_, other.compact_after_) &&
               ObjectUtils::TEST_Equal(changelog_files_, other.changelog_files_) &&
               ObjectUtils::TEST_Equal(new_index_files_, other.new_index_files_) &&
               ObjectUtils::TEST_Equal(deleted_index_files_, other.deleted_index_files_);
    }

    std::string ToString() const {
        std::vector<std::string> compact_before_names;
        compact_before_names.reserve(compact_before_.size());
        for (const auto& file : compact_before_) {
            compact_before_names.emplace_back(file->file_name);
        }
        std::vector<std::string> compact_after_names;
        compact_after_names.reserve(compact_after_.size());
        for (const auto& file : compact_after_) {
            compact_after_names.emplace_back(file->file_name);
        }
        std::vector<std::string> changelog_files_names;
        changelog_files_names.reserve(changelog_files_.size());
        for (const auto& file : changelog_files_) {
            changelog_files_names.emplace_back(file->file_name);
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
            "CompactIncrement {{compactBefore = {}, compactAfter = {}, changelogFiles = {}, "
            "newIndexFiles = {}, deletedIndexFiles = {}}}",
            fmt::join(compact_before_names, ", "), fmt::join(compact_after_names, ", "),
            fmt::join(changelog_files_names, ", "), fmt::join(new_index_names, ", "),
            fmt::join(deleted_index_names, ", "));
    }

 private:
    std::vector<std::shared_ptr<DataFileMeta>> compact_before_;
    std::vector<std::shared_ptr<DataFileMeta>> compact_after_;
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files_;
    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files_;
    std::vector<std::shared_ptr<IndexFileMeta>> deleted_index_files_;
};

}  // namespace paimon
