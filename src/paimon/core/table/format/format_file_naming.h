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
#include <string>

#include "paimon/result.h"

namespace paimon {

/// Names the data files one format table write produces.
///
/// `{prefix}{uuid}-{n}.{extension}`, the convention every paimon writer follows: the uuid belongs
/// to this write and the counter to its files, so two concurrent writers cannot collide.
///
/// A file is staged under `_temporary/.tmp.{uuid}` beside where it will end up and takes its real
/// name only on commit, as Java Paimon's `RenamingTwoPhaseOutputStream` does. Both the directory
/// and the name are hidden, which is the Hive-style convention for output that is not committed
/// table data and is what a scan of this table skips.
class FormatFileNaming {
 public:
    static constexpr char kDefaultDataFilePrefix[] = "data-";
    /// Directory a staged file waits in, shared with every other writer of the same table.
    static constexpr char kTempDirName[] = "_temporary";
    static constexpr char kTempFilePrefix[] = ".tmp.";

    /// @param extension File extension without its dot, which is the format's identifier.
    /// @param prefix File name prefix, from `data-file.prefix`. It may not be hidden by the
    ///        `_` / `.` convention, since a scan skips every such file.
    static Result<FormatFileNaming> Create(const std::string& extension, const std::string& prefix);

    FormatFileNaming() = default;

    /// The name the next file takes once committed.
    std::string NextFileName();

    /// Where the next file is staged, relative to the directory it will be published in. Each
    /// staged name carries a uuid of its own, so two writers sharing `_temporary` cannot
    /// collide.
    Result<std::string> NextTempFilePath();

    /// Whether `relative_path` is one `NextTempFilePath()` could have produced.
    static bool IsTempFilePath(const std::string& relative_path);

 private:
    FormatFileNaming(const std::string& uuid, const std::string& extension,
                     const std::string& prefix)
        : uuid_(uuid), extension_(extension), prefix_(prefix) {}

    std::string uuid_;
    std::string extension_;
    std::string prefix_ = kDefaultDataFilePrefix;
    int64_t file_count_ = 0;
};

}  // namespace paimon
