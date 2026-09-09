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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/table/format/format_data_split.h"
#include "paimon/status.h"

namespace paimon {

class FileSystem;

/// What the listing has to know about the directory tree below its root.
struct FormatDataFileListingOptions {
    /// Zero when the root is already a complete partition.
    int32_t partition_levels = 0;
    /// The partition keys of those levels, outermost first, so a directory at a partition level
    /// can be told from one that is not a partition of this table at all. Empty skips that check,
    /// which is what a `key=value` layout needs when the caller does not know the keys.
    std::vector<std::string> partition_keys;
    /// Whether a partition directory is named by its value alone instead of `key=value`.
    bool only_value_in_path = false;
    /// The null partition value's name: in the value-only layout, the one hidden name that is
    /// table content.
    std::string default_part_name;
    /// False when the schema lives in a metastore, where either name is data.
    bool skip_reserved_directories = false;
};

/// Finds the data files of a format table in the directory tree it is laid out in.
class FormatFileListing {
 public:
    FormatFileListing() = delete;
    ~FormatFileListing() = delete;

    /// Whether `name` is metadata this library keeps under a table location. A table whose
    /// schema lives in a catalog has none.
    static bool IsReservedDirectory(const std::string& name);

    /// Collects every committed data file under `root`, at any depth.
    ///
    /// A hidden `_` / `.` name is skipped and never descended into, since an uncommitted job
    /// stages output there under ordinary file names; `default_part_name` at a partition level is
    /// the exception. A missing root is an error - that is not the same as a table with no rows -
    /// while a directory that disappears mid-listing is skipped.
    static Status ListDataFiles(const std::shared_ptr<FileSystem>& file_system,
                                const std::string& root,
                                const FormatDataFileListingOptions& options,
                                std::vector<FormatDataSplit::FileMeta>* files);
};

}  // namespace paimon
