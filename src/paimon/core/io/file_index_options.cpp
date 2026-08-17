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

#include "paimon/core/io/file_index_options.h"

#include <set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace paimon {
namespace {

constexpr char kFileIndexPrefix[] = "file-index.";
constexpr char kColumnsSuffix[] = ".columns";

}  // namespace

Result<FileIndexOptions> FileIndexOptions::FromCoreOptions(const CoreOptions& options) {
    FileIndexOptions result;
    const std::map<std::string, std::string>& raw_options = options.ToMap();
    result.in_manifest_threshold_ = options.FileIndexInManifestThreshold();

    std::set<std::pair<std::string, std::string>> declared;
    for (const auto& [key, value] : raw_options) {
        if (!StringUtils::StartsWith(key, kFileIndexPrefix) ||
            !StringUtils::EndsWith(key, kColumnsSuffix)) {
            continue;
        }
        const size_t index_type_length =
            key.size() - std::string(kFileIndexPrefix).size() - std::string(kColumnsSuffix).size();
        const std::string index_type =
            key.substr(std::string(kFileIndexPrefix).size(), index_type_length);
        if (index_type.empty()) {
            return Status::Invalid(fmt::format("Invalid file index option {}", key));
        }
        for (std::string column_name : StringUtils::Split(value, ",", /*ignore_empty=*/false)) {
            StringUtils::Trim(&column_name);
            if (column_name.empty()) {
                return Status::Invalid(
                    fmt::format("Wrong option in {}, should not have empty column", key));
            }
            if (column_name.find('[') != std::string::npos) {
                return Status::NotImplemented(
                    "Writing file indexes for nested map columns is not supported");
            }
            if (declared.emplace(column_name, index_type).second) {
                result.definitions_.push_back({column_name, index_type, {}});
            }
        }
    }

    for (const auto& [key, value] : raw_options) {
        if (!StringUtils::StartsWith(key, kFileIndexPrefix) ||
            StringUtils::EndsWith(key, kColumnsSuffix) ||
            key == Options::FILE_INDEX_IN_MANIFEST_THRESHOLD) {
            continue;
        }
        std::vector<std::string> parts = StringUtils::Split(
            key.substr(std::string(kFileIndexPrefix).size()), ".", /*ignore_empty=*/false);
        if (parts.size() != 3) {
            continue;
        }
        bool found = false;
        for (FileIndexDefinition& definition : result.definitions_) {
            if (definition.index_type == parts[0] && definition.column_name == parts[1]) {
                definition.options[parts[2]] = value;
                found = true;
                break;
            }
        }
        if (!found) {
            return Status::Invalid(
                fmt::format("Wrong file index option '{}': column '{}' is not declared in "
                            "'file-index.{}.columns'",
                            key, parts[1], parts[0]));
        }
    }
    return result;
}

}  // namespace paimon
