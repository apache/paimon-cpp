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
#include <map>
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {

class CoreOptions;

struct FileIndexDefinition {
    std::string column_name;
    std::string index_type;
    std::map<std::string, std::string> options;
};

/// Parsed write-side file index configuration.
class FileIndexOptions {
 public:
    static Result<FileIndexOptions> FromCoreOptions(const CoreOptions& options);

    const std::vector<FileIndexDefinition>& Definitions() const {
        return definitions_;
    }

    int64_t InManifestThreshold() const {
        return in_manifest_threshold_;
    }

    bool Empty() const {
        return definitions_.empty();
    }

 private:
    std::vector<FileIndexDefinition> definitions_;
    int64_t in_manifest_threshold_ = 0;
};

}  // namespace paimon
