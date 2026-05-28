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

#include "paimon/factories/factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// A factory for creating `FileSystem` instances.
class PAIMON_EXPORT FileSystemFactory : public Factory {
 public:
    /// Create a `FileSystem` of current factory with specific path.
    virtual Result<std::unique_ptr<FileSystem>> Create(
        const std::string& path, const std::map<std::string, std::string>& options) const = 0;

    /// Get `FileSystem` corresponding to identifier and specific path.
    /// @pre Factory is already registered.
    static Result<std::unique_ptr<FileSystem>> Get(
        const std::string& identifier, const std::string& path,
        const std::map<std::string, std::string>& fs_options);
};

}  // namespace paimon
