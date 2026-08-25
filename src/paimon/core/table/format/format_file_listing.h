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
    /// Levels below the root holding partition directories rather than table content. Zero when
    /// the root is already a complete partition.
    int32_t partition_levels = 0;
    /// Whether a partition directory is named by its value alone instead of `key=value`.
    bool only_value_in_path = false;
    /// The name standing for a null partition value: in the value-only layout, the one hidden
    /// name that holds table content.
    std::string default_part_name;
    /// Whether `schema` and `branch` below the root are this table's metadata. False when the
    /// schema lives in a metastore, where either name is data.
    bool skip_reserved_directories = false;
};

/// Finds the data files of a format table in the directory tree it is laid out in.
class FormatFileListing {
 public:
    FormatFileListing() = delete;
    ~FormatFileListing() = delete;

    /// Whether `name` is a directory this library keeps under a format table's location as
    /// metadata. A table whose schema lives in a catalog has none.
    static bool IsReservedDirectory(const std::string& name);

    /// Collects every committed data file under `root`, at any depth.
    ///
    /// A hidden `_` / `.` name is skipped and never descended into: an uncommitted job stages
    /// output there under ordinary data file names, so only the directory above tells them apart.
    /// The one exception is `default_part_name`, and only at a partition level.
    ///
    /// A root that does not exist is an error: the location is wrong or the data is gone, which is
    /// not the same as a table with no rows. A directory that disappears mid-listing is skipped.
    static Status ListDataFiles(const std::shared_ptr<FileSystem>& file_system,
                                const std::string& root,
                                const FormatDataFileListingOptions& options,
                                std::vector<FormatDataSplit::FileMeta>* files);
};

}  // namespace paimon
