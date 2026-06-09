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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {
class BasicFileStatus;
class FileSystem;

/// Utils for file reading and writing.
class FileUtils {
 public:
    /// List versioned files for the directory.
    ///
    /// @return Status
    static Status ListVersionedFiles(const std::shared_ptr<FileSystem>& fs, const std::string& dir,
                                     const std::string& prefix, std::vector<int64_t>* files);

    /// List original versioned files for the directory.
    ///
    /// @return Status
    static Status ListOriginalVersionedFiles(const std::shared_ptr<FileSystem>& fs,
                                             const std::string& dir, const std::string& prefix,
                                             std::vector<std::string>* files);

    /// List versioned file status for the directory.
    ///
    /// @return Status
    static Status ListVersionedFileStatus(
        const std::shared_ptr<FileSystem>& fs, const std::string& dir, const std::string& prefix,
        std::vector<std::unique_ptr<BasicFileStatus>>* file_status_list);
};

}  // namespace paimon
