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

#include <memory>
#include <string>

#include "paimon/fs/file_system.h"
namespace paimon {
/// Abstract interface for writing global index files to storage.
class PAIMON_EXPORT GlobalIndexFileWriter {
 public:
    virtual ~GlobalIndexFileWriter() = default;

    /// Generates a unique full path for a new index file using the given prefix.
    /// @note This function may be called multiple times if the index consists of multiple files.
    virtual Result<std::string> NewPath(const std::string& prefix) const = 0;

    /// Opens a new output stream at the supplied full path, without selecting a directory.
    virtual Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_path) const = 0;

    /// Gets the size of the file at the supplied full path.
    virtual Result<int64_t> GetFileSize(const std::string& file_path) const = 0;
};

}  // namespace paimon
