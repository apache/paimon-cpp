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

#include <cstddef>
#include <set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace paimon {
namespace {

constexpr char kFileIndexPrefix[] = "file-index.";
constexpr char kColumnsSuffix[] = ".columns";
constexpr size_t kFileIndexPrefixLength = sizeof(kFileIndexPrefix) - 1;
constexpr size_t kColumnsSuffixLength = sizeof(kColumnsSuffix) - 1;

}  // namespace

Result<FileIndexOptions> FileIndexOptions::FromCoreOptions(const CoreOptions& options) {
    return Parse(options.ToMap(), options.FileIndexInManifestThreshold());
}

Result<FileIndexOptions> FileIndexOptions::FromMap(
    const std::map<std::string, std::string>& raw_options) {
    int64_t in_manifest_threshold = CoreOptions().FileIndexInManifestThreshold();
    auto iter = raw_options.find(Options::FILE_INDEX_IN_MANIFEST_THRESHOLD);
    if (iter != raw_options.end()) {
        PAIMON_ASSIGN_OR_RAISE(in_manifest_threshold, MemorySize::ParseBytes(iter->second));
    }
    return Parse(raw_options, in_manifest_threshold);
}

Result<FileIndexOptions> FileIndexOptions::Parse(
    const std::map<std::string, std::string>& raw_options, int64_t in_manifest_threshold) {
    FileIndexOptions result;
    result.in_manifest_threshold_ = in_manifest_threshold;

    std::set<std::pair<std::string, std::string>> declared;
    for (const auto& [key, value] : raw_options) {
        if (!StringUtils::StartsWith(key, kFileIndexPrefix) ||
            !StringUtils::EndsWith(key, kColumnsSuffix)) {
            continue;
        }
        if (key.size() < kFileIndexPrefixLength + kColumnsSuffixLength) {
            return Status::Invalid(fmt::format("Invalid file index option {}", key));
        }
        const size_t index_type_length = key.size() - kFileIndexPrefixLength - kColumnsSuffixLength;
        const std::string index_type = key.substr(kFileIndexPrefixLength, index_type_length);
        if (index_type.empty()) {
            return Status::Invalid(fmt::format("Invalid file index option {}", key));
        }
        // TODO(jinli.zjw): Align malformed list option parsing (for example, "f1,f2,,") with Java.
        // Update this together with ConfigParser::ParseList to keep option parsing consistent.
        for (std::string column_name : StringUtils::Split(value, ",", /*ignore_empty=*/false)) {
            StringUtils::Trim(&column_name);
            if (column_name.empty()) {
                return Status::Invalid(
                    fmt::format("Wrong option in {}, should not have empty column", key));
            }
            if (column_name.find('[') != std::string::npos &&
                StringUtils::EndsWith(column_name, "]")) {
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
            StringUtils::EndsWith(key, kColumnsSuffix) || key == Options::FILE_INDEX_READ_ENABLED ||
            key == Options::FILE_INDEX_IN_MANIFEST_THRESHOLD) {
            continue;
        }
        std::vector<std::string> parts =
            StringUtils::Split(key.substr(kFileIndexPrefixLength), ".", /*ignore_empty=*/false);
        if (parts.size() < 3) {
            continue;
        }
        std::string option_name = parts[2];
        for (size_t i = 3; i < parts.size(); ++i) {
            option_name.append(".").append(parts[i]);
        }
        bool found = false;
        for (FileIndexDefinition& definition : result.definitions_) {
            if (definition.index_type == parts[0] && definition.column_name == parts[1]) {
                definition.options[option_name] = value;
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

Result<std::map<std::string, std::string>> FileIndexOptions::RewriteFieldNames(
    const std::map<std::string, std::string>& options,
    const std::map<std::string, std::string>& field_name_mapping) {
    PAIMON_ASSIGN_OR_RAISE(FileIndexOptions parsed_options, FromMap(options));
    std::map<std::string, std::string> result = options;
    for (auto iter = result.begin(); iter != result.end();) {
        const std::string& key = iter->first;
        if (StringUtils::StartsWith(key, kFileIndexPrefix) &&
            key != Options::FILE_INDEX_READ_ENABLED &&
            key != Options::FILE_INDEX_IN_MANIFEST_THRESHOLD &&
            (StringUtils::EndsWith(key, kColumnsSuffix) ||
             StringUtils::Split(key.substr(kFileIndexPrefixLength), ".",
                                /*ignore_empty=*/false)
                     .size() >= 3)) {
            iter = result.erase(iter);
        } else {
            ++iter;
        }
    }
    // Rebuild from the original definitions so renames cannot overwrite another field's options.
    for (const FileIndexDefinition& definition : parsed_options.Definitions()) {
        auto iter = field_name_mapping.find(definition.column_name);
        if (iter == field_name_mapping.end()) {
            continue;
        }
        const std::string& data_name = iter->second;
        std::string& columns = result[fmt::format("file-index.{}.columns", definition.index_type)];
        if (!columns.empty()) {
            columns.append(",");
        }
        columns.append(data_name);
        for (const auto& [name, value] : definition.options) {
            result[fmt::format("file-index.{}.{}.{}", definition.index_type, data_name, name)] =
                value;
        }
    }
    return result;
}

const std::map<std::string, std::string>& FileIndexOptions::GetIndexerOptions(
    const std::string& column_name, const std::string& index_type) const {
    static const std::map<std::string, std::string> kEmptyOptions;
    for (const FileIndexDefinition& definition : definitions_) {
        if (definition.column_name == column_name && definition.index_type == index_type) {
            return definition.options;
        }
    }
    return kEmptyOptions;
}

}  // namespace paimon
