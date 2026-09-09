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
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/table/format/format_file_listing.h"
#include "paimon/core/utils/partition_path_utils.h"

namespace paimon {

namespace {

/// A table location as a prefix of the paths below it.
struct LocationPrefix {
    /// As it was written, without its trailing separator: what a path below it is built from,
    /// so a built path keeps the table's scheme.
    std::string root;
    /// A path is compared against these rather than against `root`, so how each was written
    /// cannot decide whether one is under the other.
    std::string scheme;
    std::string authority;
    /// The location's path inside that file system, without its trailing separator.
    std::string inner_root;
    /// Index in a path's own inner path where the first component below the location starts.
    size_t components_at = 0;
};

/// A scheme naming the local file system, which a scheme-less path names too.
bool IsLocalScheme(const std::string& scheme) {
    return scheme.empty() || StringUtils::EqualsIgnoreCase(scheme, "file");
}

/// Whether both name the same file system. A scheme is compared ignoring case, as a URI scheme
/// is, and an absent one reads as `file`, so `/tmp/t` and `file:///tmp/t` are one place.
bool NamesSameFileSystem(const LocationPrefix& location, const Path& candidate) {
    if (location.authority != candidate.authority) {
        return false;
    }
    if (IsLocalScheme(location.scheme) && IsLocalScheme(candidate.scheme)) {
        return true;
    }
    return StringUtils::EqualsIgnoreCase(location.scheme, candidate.scheme);
}

/// `path` split into the file system it names and the path inside it, with repeated separators
/// collapsed. A `..` is left alone: only the component walk can tell whether it climbs out.
///
/// A relative local path is resolved against the working directory, exactly as `LocalFile` does
/// as it opens one: a table may be located at `./table` while the listing of it comes back under
/// the absolute path the file system opened, and the two have to compare equal.
Result<Path> ResolveComparablePath(const std::string& path, const char* subject,
                                   const std::string& what) {
    Result<Path> parsed = PathUtil::ToPath(path);
    if (!parsed.ok()) {
        return Status::Invalid(fmt::format("{} cannot be checked: its {} '{}' is not a path: {}",
                                           what, subject, path, parsed.status().message()));
    }
    Path resolved = std::move(parsed).value();
    if (!IsLocalScheme(resolved.scheme) || !resolved.authority.empty() ||
        (!resolved.path.empty() && resolved.path[0] == '/')) {
        return resolved;
    }
    Result<std::string> working_directory = PathUtil::GetWorkingDirectory();
    if (!working_directory.ok()) {
        return Status::Invalid(
            fmt::format("{} cannot be checked: its {} '{}' is relative to the working directory, "
                        "which cannot be read: {}",
                        what, subject, path, working_directory.status().message()));
    }
    // Joined the way `LocalFile` joins it, so a `.` component the file system keeps in the paths
    // it hands back is kept here too and the two still compare equal.
    resolved.path = PathUtil::JoinPath(working_directory.value(), resolved.path);
    return resolved;
}

/// Resolves `directory` into the prefix the paths below it share, so that no two checks here can
/// disagree about what "under the table location" means.
///
/// A location of nothing but separators, or an authority with no path, is its file system's root,
/// which is its own separator: what follows starts one character in, not two. An empty location
/// is refused, since as a prefix it would pass every absolute path.
Result<LocationPrefix> ResolveLocationPrefix(const std::string& directory, const char* subject,
                                             const std::string& what) {
    if (directory.empty()) {
        return Status::Invalid(fmt::format(
            "{} cannot be checked: its {} is empty, and an empty path is a prefix of nothing", what,
            subject));
    }
    PAIMON_ASSIGN_OR_RAISE(Path parsed, ResolveComparablePath(directory, subject, what));

    LocationPrefix prefix;
    prefix.scheme = std::move(parsed.scheme);
    prefix.authority = std::move(parsed.authority);
    size_t root_end = directory.size();
    while (root_end > 0 && directory[root_end - 1] == '/') {
        root_end--;
    }
    size_t inner_end = parsed.path.size();
    while (inner_end > 0 && parsed.path[inner_end - 1] == '/') {
        inner_end--;
    }
    if (inner_end == 0) {
        prefix.root = root_end == 0 ? "/" : directory.substr(0, root_end);
        prefix.inner_root = "/";
        prefix.components_at = 1;
        return prefix;
    }
    prefix.root = directory.substr(0, root_end);
    prefix.inner_root = parsed.path.substr(0, inner_end);
    prefix.components_at = inner_end + 1;
    return prefix;
}

/// Whether `candidate` names something below `prefix`, on the same file system and by whole path
/// components.
bool IsUnderPrefix(const Path& candidate, const LocationPrefix& prefix) {
    if (!NamesSameFileSystem(prefix, candidate)) {
        return false;
    }
    if (candidate.path.size() <= prefix.components_at ||
        candidate.path.compare(0, prefix.inner_root.size(), prefix.inner_root) != 0) {
        return false;
    }
    // The file system root is its own separator; every other location is followed by one.
    return prefix.components_at == prefix.inner_root.size() ||
           candidate.path[prefix.inner_root.size()] == '/';
}

}  // namespace

Status FormatPathValidation::ValidatePathUnderLocation(const std::string& path,
                                                       const std::string& location,
                                                       const std::string& what) {
    PAIMON_ASSIGN_OR_RAISE(LocationPrefix prefix,
                           ResolveLocationPrefix(location, "table location", what));
    PAIMON_ASSIGN_OR_RAISE(Path candidate, ResolveComparablePath(path, "path", what));
    if (!IsUnderPrefix(candidate, prefix)) {
        return Status::Invalid(fmt::format(
            "{} names '{}', which is not under the table location '{}'", what, path, location));
    }

    // `<table>/../victim` passes any prefix test and still resolves outside the table.
    size_t begin = prefix.components_at;
    while (begin <= candidate.path.size()) {
        size_t end = candidate.path.find('/', begin);
        if (end == std::string::npos) {
            end = candidate.path.size();
        }
        const std::string component = candidate.path.substr(begin, end - begin);
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
    // Both sides as their file system holds them, so a `file:` URI checked against a bare local
    // location does not start the walk part way through the location itself.
    PAIMON_ASSIGN_OR_RAISE(Path candidate, ResolveComparablePath(path, "path", what));
    const bool only_value = table->PartitionOnlyValueInPath();

    size_t begin = prefix.components_at;
    size_t level = 0;
    while (begin <= candidate.path.size()) {
        size_t end = candidate.path.find('/', begin);
        const bool is_last = end == std::string::npos;
        if (is_last) {
            end = candidate.path.size();
        }
        const std::string component = candidate.path.substr(begin, end - begin);
        const bool is_directory = !is_last || !ends_in_file;

        // The one hidden name a scan reads, and only where a partition directory belongs.
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
    return NamesSameFileSystem(location,
                               Path(candidate.scheme, candidate.authority, candidate.inner_root)) &&
           location.inner_root == candidate.inner_root;
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
    PAIMON_ASSIGN_OR_RAISE(Path candidate, ResolveComparablePath(file_path, "path", what));
    // The directory components between the location and the file name; the leading
    // `partition_keys.size()` of them are the partition directories.
    std::vector<std::string> components;
    size_t begin = prefix.components_at;
    while (begin < candidate.path.size()) {
        size_t end = candidate.path.find('/', begin);
        if (end == std::string::npos) {
            break;
        }
        components.push_back(candidate.path.substr(begin, end - begin));
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
    // A directory a scan would skip could be written but never read back, and an overwrite of
    // it would clear whatever does live there.
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
