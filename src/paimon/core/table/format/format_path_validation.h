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

#include <map>
#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/format/format_table.h"

namespace paimon {

/// Checks that a path a caller handed over really names something of this format table.
///
/// A split and a commit message both come back through an interface that takes the base type, so
/// what arrives may belong to another plan, another table or a plan made before the files moved,
/// and nothing further down re-checks the paths they name.
class FormatPathValidation {
 public:
    FormatPathValidation() = delete;
    ~FormatPathValidation() = delete;

    /// Fails when `path` is not a file inside `location`. By path component, not by string
    /// prefix: `<location>/../victim` starts with the location and still resolves outside it.
    /// Only the path text is checked, so a symbolic link pointing out of the table is not caught.
    static Status ValidatePathUnderLocation(const std::string& path, const std::string& location,
                                            const std::string& what);

    /// The directory a partition's files belong in. Only for building a path to write or clear;
    /// to check one that already exists use `ValidateFileInPartition()`, since another engine may
    /// spell the same value differently.
    static Result<std::string> BuildPartitionDirectory(
        const std::shared_ptr<FormatTable>& table,
        const std::map<std::string, std::string>& partition);

    /// Fails when `file_path` does not sit in the directory `partition` names, which would
    /// publish rows under a partition they never had or clear the wrong one on an overwrite.
    ///
    /// Compared on the values the directory names spell out, not on the directory string: a value
    /// can be escaped more than one way, and `100%` and `100%25` name the same one. Levels below
    /// the partition are allowed.
    static Status ValidateFileInPartition(const std::shared_ptr<FormatTable>& table,
                                          const std::string& file_path,
                                          const std::map<std::string, std::string>& partition,
                                          const std::string& what);

    /// Fails when `file_path` is a file a scan of this table would never return: a hidden `_` /
    /// `.` name, or this table's own `schema` or `branch` directory. A split that did not come
    /// from this scan never went through that listing, so the same rules run here.
    static Status ValidateFileIsVisible(const std::shared_ptr<FormatTable>& table,
                                        const std::string& file_path, const std::string& what);

    /// Whether `directory` is the table's own location, whichever way either was written. It
    /// decides whether `schema` and `branch` below it are metadata or data, so a trailing
    /// separator must not make the two compare different.
    static Result<bool> IsTableLocation(const std::shared_ptr<FormatTable>& table,
                                        const std::string& directory);

    /// `ValidateFileIsVisible()` for a path ending in a directory. The last component is what
    /// makes it a separate call: in the value-only layout a partition named `schema` would be
    /// written where a file system catalog keeps the table's own metadata.
    static Status ValidateDirectoryIsVisible(const std::shared_ptr<FormatTable>& table,
                                             const std::string& directory, const std::string& what);

    /// Fails when `partition` does not name exactly the table's partition keys.
    static Status ValidatePartitionKeys(const std::shared_ptr<FormatTable>& table,
                                        const std::map<std::string, std::string>& partition,
                                        const std::string& what);
};

}  // namespace paimon
