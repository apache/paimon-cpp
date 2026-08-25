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

#include "paimon/core/table/format/format_path_validation.h"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/table/format/format_file_listing.h"
#include "paimon/core/utils/partition_path_utils.h"

namespace paimon {

namespace {

/// A table location as a prefix of the paths below it.
struct LocationPrefix {
    /// Without its trailing separator, so a location written either way compares the same.
    std::string root;
    /// Index in a path under the location where the first component below it starts.
    size_t components_at = 0;
};

/// Resolves `directory` into the prefix the paths below it share, so that no two checks here can
/// disagree about what "under the table location" means.
///
/// A location that is nothing but separators is the file system root, which is its own separator:
/// the component after it starts one character in, not two. An empty location names no directory,
/// and treating it as a prefix would make every absolute path pass.
Result<LocationPrefix> ResolveLocationPrefix(const std::string& directory, const char* subject,
                                             const std::string& what) {
    size_t end = directory.size();
    while (end > 0 && directory[end - 1] == '/') {
        end--;
    }
    if (end == 0) {
        if (directory.empty()) {
            return Status::Invalid(fmt::format(
                "{} cannot be checked: its {} is empty, and an empty path is a prefix of nothing",
                what, subject));
        }
        return LocationPrefix{"/", 1};
    }
    return LocationPrefix{directory.substr(0, end), end + 1};
}

bool IsUnderPrefix(const std::string& path, const LocationPrefix& prefix) {
    if (path.size() <= prefix.components_at ||
        path.compare(0, prefix.root.size(), prefix.root) != 0) {
        return false;
    }
    // The file system root is its own separator; every other location is followed by one.
    return prefix.components_at == prefix.root.size() || path[prefix.root.size()] == '/';
}

}  // namespace

Status FormatPathValidation::ValidatePathUnderLocation(const std::string& path,
                                                       const std::string& location,
                                                       const std::string& what) {
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix prefix,
                           ResolveLocationPrefix(location, "table location", what));
    if (!IsUnderPrefix(path, prefix)) {
        return Status::Invalid(fmt::format(
            "{} names '{}', which is not under the table location '{}'", what, path, location));
    }

    // `<table>/../victim` passes any prefix test and still resolves outside the table.
    size_t begin = prefix.components_at;
    while (begin <= path.size()) {
        size_t end = path.find('/', begin);
        if (end == std::string::npos) {
            end = path.size();
        }
        const std::string component = path.substr(begin, end - begin);
        if (component.empty() || component == "." || component == "..") {
            return Status::Invalid(fmt::format(
                "{} names '{}', whose path does not stay inside the table location", what, path));
        }
        begin = end + 1;
    }
    return Status::OK();
}

namespace {

/// Fails when a scan would not reach `path`, whose last component names a file when `ends_in_file`
/// and a directory otherwise. One walk serves both: the distinction matters only for the last
/// component, which as a directory may be reserved or stand for a null partition.
Status ValidateComponentsAreVisible(const std::shared_ptr<FormatTable>& table,
                                    const std::string& path, bool ends_in_file,
                                    const std::string& what) {
    const std::vector<std::string>& partition_keys = table->PartitionKeys();
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix prefix,
                           ResolveLocationPrefix(table->Location(), "table location", what));
    const bool only_value = table->PartitionOnlyValueInPath();

    size_t begin = prefix.components_at;
    size_t level = 0;
    while (begin <= path.size()) {
        size_t end = path.find('/', begin);
        const bool is_last = end == std::string::npos;
        if (is_last) {
            end = path.size();
        }
        const std::string component = path.substr(begin, end - begin);
        const bool is_directory = !is_last || !ends_in_file;

        // The one hidden name a scan reads: a null partition's directory in the value-only
        // layout, and only where a partition directory belongs.
        const bool is_default_partition_dir = is_directory && only_value &&
                                              level < partition_keys.size() &&
                                              component == table->PartitionDefaultName();
        if (PartitionPathUtils::IsHiddenName(component) && !is_default_partition_dir) {
            return Status::Invalid(fmt::format(
                "{} names '{}', which a scan of this table would skip: '{}' is hidden, and that is "
                "how an uncommitted job marks its output",
                what, path, component));
        }
        // Only right below the location, and only when the schema lives there. A value-only
        // partition lands here unescaped, so one named `schema` would be written over it.
        if (level == 0 && is_directory && table->LocationCarriesPaimonMetadata() &&
            FormatFileListing::IsReservedDirectory(component)) {
            return Status::Invalid(fmt::format(
                "{} names '{}', where '{}' is this table's own metadata rather than data", what,
                path, component));
        }
        begin = end + 1;
        level++;
    }
    return Status::OK();
}

}  // namespace

Status FormatPathValidation::ValidateFileIsVisible(const std::shared_ptr<FormatTable>& table,
                                                   const std::string& file_path,
                                                   const std::string& what) {
    return ValidateComponentsAreVisible(table, file_path, /*ends_in_file=*/true, what);
}

Result<bool> FormatPathValidation::IsTableLocation(const std::shared_ptr<FormatTable>& table,
                                                   const std::string& directory) {
    const std::string what = fmt::format("table {}", table->FullName());
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix location,
                           ResolveLocationPrefix(table->Location(), "table location", what));
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix candidate,
                           ResolveLocationPrefix(directory, "directory", what));
    return location.root == candidate.root;
}

Status FormatPathValidation::ValidateDirectoryIsVisible(const std::shared_ptr<FormatTable>& table,
                                                        const std::string& directory,
                                                        const std::string& what) {
    // Trailing separators go first, or the walk below sees an empty last component.
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix directory_prefix,
                           ResolveLocationPrefix(directory, "directory", what));
    return ValidateComponentsAreVisible(table, directory_prefix.root, /*ends_in_file=*/false, what);
}

Status FormatPathValidation::ValidatePartitionKeys(
    const std::shared_ptr<FormatTable>& table, const std::map<std::string, std::string>& partition,
    const std::string& what) {
    const std::vector<std::string>& partition_keys = table->PartitionKeys();
    if (partition.size() != partition_keys.size()) {
        return Status::Invalid(
            fmt::format("{} carries {} partition values but table {} is partitioned by {} fields",
                        what, partition.size(), table->FullName(), partition_keys.size()));
    }
    for (const std::string& partition_key : partition_keys) {
        if (partition.find(partition_key) == partition.end()) {
            return Status::Invalid(fmt::format("{} does not carry a value for partition field '{}'",
                                               what, partition_key));
        }
    }
    return Status::OK();
}

Status FormatPathValidation::ValidateFileInPartition(
    const std::shared_ptr<FormatTable>& table, const std::string& file_path,
    const std::map<std::string, std::string>& partition, const std::string& what) {
    const std::vector<std::string>& partition_keys = table->PartitionKeys();
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix prefix,
                           ResolveLocationPrefix(table->Location(), "table location", what));
    // The directory components between the location and the file name; the leading
    // `partition_keys.size()` of them are the partition directories.
    std::vector<std::string> components;
    size_t begin = prefix.components_at;
    while (begin < file_path.size()) {
        size_t end = file_path.find('/', begin);
        if (end == std::string::npos) {
            break;
        }
        components.push_back(file_path.substr(begin, end - begin));
        begin = end + 1;
    }
    if (components.size() < partition_keys.size()) {
        return Status::Invalid(
            fmt::format("{} names '{}', which sits above the {} partition directories of table {}",
                        what, file_path, partition_keys.size(), table->FullName()));
    }

    const bool only_value = table->PartitionOnlyValueInPath();
    for (size_t i = 0; i < partition_keys.size(); i++) {
        const std::string& partition_key = partition_keys[i];
        std::string value;
        if (only_value) {
            value = PartitionPathUtils::UnescapePathName(components[i]);
        } else {
            std::optional<std::pair<std::string, std::string>> key_value =
                PartitionPathUtils::ExtractPartitionKeyValue(components[i]);
            if (!key_value || key_value->first != partition_key) {
                return Status::Invalid(
                    fmt::format("{} names '{}', whose directory '{}' is not a partition of '{}'",
                                what, file_path, components[i], partition_key));
            }
            value = key_value->second;
        }
        auto iter = partition.find(partition_key);
        if (iter == partition.end() || iter->second != value) {
            return Status::Invalid(fmt::format(
                "{} sits in the '{}' partition of '{}' but claims '{}'", what, value, partition_key,
                iter == partition.end() ? std::string("nothing") : iter->second));
        }
    }
    return Status::OK();
}

Result<std::string> FormatPathValidation::BuildPartitionDirectory(
    const std::shared_ptr<FormatTable>& table,
    const std::map<std::string, std::string>& partition) {
    const std::vector<std::string>& partition_keys = table->PartitionKeys();
    std::vector<std::pair<std::string, std::string>> ordered_partition;
    ordered_partition.reserve(partition_keys.size());
    for (const std::string& partition_key : partition_keys) {
        auto iter = partition.find(partition_key);
        if (iter == partition.end()) {
            return Status::Invalid(fmt::format("no value for partition field '{}' of table {}",
                                               partition_key, table->FullName()));
        }
        ordered_partition.emplace_back(partition_key, iter->second);
    }
    PAIMON_ASSIGN_OR_RAISE(std::string partition_path,
                           PartitionPathUtils::GeneratePartitionPath(
                               ordered_partition, table->PartitionOnlyValueInPath()));
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix prefix,
                           ResolveLocationPrefix(table->Location(), "table location",
                                                 fmt::format("table {}", table->FullName())));
    if (partition_path.empty()) {
        return prefix.root;
    }
    const std::string directory = PathUtil::JoinPath(prefix.root, partition_path);
    // A directory a scan would skip could be written but never read back, and an overwrite of it
    // would clear whatever does live there. Every writer and commit derives its directory here.
    std::string described;
    for (const auto& [key, value] : ordered_partition) {
        described += described.empty() ? "" : ", ";
        described += fmt::format("{}={}", key, value);
    }
    PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidateDirectoryIsVisible(
        table, directory, fmt::format("partition {}", described)));
    return directory;
}

}  // namespace paimon
