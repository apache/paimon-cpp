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

#include "paimon/core/table/format/format_table_commit.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/table/format/format_file_listing.h"
#include "paimon/core/table/format/format_file_naming.h"
#include "paimon/core/table/format/format_path_validation.h"
#include "paimon/core/utils/duration.h"
#include "paimon/core/utils/partition_path_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"

namespace paimon {

namespace {

Logger* CommitLogger() {
    static std::unique_ptr<Logger> logger = Logger::GetLogger("FormatTableCommit");
    return logger.get();
}

/// Fails when a commit message does not describe a file of this table. Only the shape of a
/// message is checked, not who produced it.
Status ValidateCommitMessage(const FormatCommitMessage& message,
                             const std::shared_ptr<FormatTable>& table,
                             const std::map<std::string, std::string>& static_partition) {
    const std::string what = fmt::format("commit message {}", message.ToString());
    PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidatePathUnderLocation(message.file_path,
                                                                         table->Location(), what));
    PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidatePathUnderLocation(message.temp_file_path,
                                                                         table->Location(), what));

    // `<publish directory>/_temporary/.tmp.<uuid>`, which a scan skips and which is what Java
    // Paimon's `RenamingTwoPhaseOutputStream` stages under.
    const std::string publish_directory = PathUtil::GetParentDirPath(message.file_path);
    const std::string directory_prefix = publish_directory + "/";
    if (!StringUtils::StartsWith(message.temp_file_path, directory_prefix) ||
        !FormatFileNaming::IsTempFilePath(message.temp_file_path.substr(directory_prefix.size()))) {
        return Status::Invalid(fmt::format(
            "{} does not stage its file under '{}/{}...' beside where it will be published, so it "
            "may already be visible or belong to another directory",
            what, FormatFileNaming::kTempDirName, FormatFileNaming::kTempFilePrefix));
    }
    // Otherwise an overwrite could clear the old data and publish a file nothing can ever read.
    PAIMON_RETURN_NOT_OK(
        FormatPathValidation::ValidateFileIsVisible(table, message.file_path, what));
    if (message.record_count < 0 || message.file_size < 0) {
        return Status::Invalid(fmt::format("{} reports a negative row count or file size", what));
    }

    // Otherwise an overwrite would clear the wrong partition.
    PAIMON_RETURN_NOT_OK(
        FormatPathValidation::ValidatePartitionKeys(table, message.partition, what));
    PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidateFileInPartition(table, message.file_path,
                                                                       message.partition, what));

    // Otherwise the file would be published into a partition this commit never cleared.
    for (const auto& [key, value] : static_partition) {
        auto iter = message.partition.find(key);
        if (iter == message.partition.end() || iter->second != value) {
            return Status::Invalid(fmt::format(
                "{} is not in the static partition '{}={}' this commit writes", what, key, value));
        }
    }
    return Status::OK();
}

/// Fails when `static_partition` cannot name a directory of this table. The keys must be a prefix
/// of the partition keys, since a partition directory nests below the one before it.
Status ValidateStaticPartition(const std::map<std::string, std::string>& static_partition,
                               const std::vector<std::string>& partition_keys,
                               const std::string& table_name) {
    if (static_partition.empty()) {
        return Status::OK();
    }
    if (partition_keys.empty()) {
        return Status::Invalid(fmt::format(
            "format table {} is not partitioned, so a static partition names nothing", table_name));
    }
    for (const auto& entry : static_partition) {
        const std::string& key = entry.first;
        if (std::find(partition_keys.begin(), partition_keys.end(), key) == partition_keys.end()) {
            return Status::Invalid(
                fmt::format("'{}' is not a partition key of format table {}", key, table_name));
        }
    }
    bool missing_leading_key = false;
    for (const std::string& partition_key : partition_keys) {
        const bool named = static_partition.find(partition_key) != static_partition.end();
        if (named && missing_leading_key) {
            return Status::Invalid(
                fmt::format("static partition column '{}' of format table {} cannot be given "
                            "without the partition columns it nests under",
                            partition_key, table_name));
        }
        if (!named) {
            missing_leading_key = true;
        }
    }
    return Status::OK();
}

}  // namespace

FormatTableCommit::FormatTableCommit(const std::shared_ptr<FormatTable>& table, bool overwrite,
                                     const std::map<std::string, std::string>& static_partition,
                                     bool dynamic_partition_overwrite)
    : table_(table),
      overwrite_(overwrite),
      static_partition_(static_partition),
      dynamic_partition_overwrite_(dynamic_partition_overwrite) {}

FormatTableCommit::~FormatTableCommit() = default;

Result<std::unique_ptr<FormatTableCommit>> FormatTableCommit::Create(
    const std::shared_ptr<FormatTable>& table, bool overwrite,
    const std::map<std::string, std::string>& static_partition) {
    if (table == nullptr) {
        return Status::Invalid("format table commit requires a table");
    }
    PAIMON_RETURN_NOT_OK(
        ValidateStaticPartition(static_partition, table->PartitionKeys(), table->FullName()));
    // Through `CoreOptions`, so an option given at the call reaches this as it does the read.
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(table->Options(), table->GetFileSystem()));
    return std::unique_ptr<FormatTableCommit>(new FormatTableCommit(
        table, overwrite, static_partition, core_options.DynamicPartitionOverwrite()));
}

bool FormatTableCommit::ReplacesOnlyWrittenPartitions() const {
    return !table_->PartitionKeys().empty() && dynamic_partition_overwrite_;
}

Status FormatTableCommit::DeletePreviousDataFiles(const std::string& directory,
                                                  int32_t partition_levels) const {
    std::shared_ptr<FileSystem> file_system = table_->GetFileSystem();
    FormatDataFileListingOptions listing;
    listing.partition_levels = partition_levels;
    // The keys of the levels still below `directory`: a static partition naming the leading keys
    // leaves the trailing ones. Without them the listing would descend any directory it found,
    // and this deletes what it lists.
    const std::vector<std::string>& partition_keys = table_->PartitionKeys();
    if (partition_levels > 0 && static_cast<size_t>(partition_levels) <= partition_keys.size()) {
        listing.partition_keys.assign(partition_keys.end() - partition_levels,
                                      partition_keys.end());
    }
    listing.only_value_in_path = table_->PartitionOnlyValueInPath();
    listing.default_part_name = table_->PartitionDefaultName();
    // Only right at the location are `schema` and `branch` metadata; below it they are data.
    PAIMON_ASSIGN_OR_RAISE(bool at_location,
                           FormatPathValidation::IsTableLocation(table_, directory));
    listing.skip_reserved_directories = at_location && table_->LocationCarriesPaimonMetadata();
    std::vector<FormatDataSplit::FileMeta> files;
    // Committed data files only: a staging directory holds another writer's uncommitted output.
    Status listed = FormatFileListing::ListDataFiles(file_system, directory, listing, &files);
    // A format table's directory need not exist yet: an external table may be loaded from a schema
    // alone, and the first write to it may be an overwrite. Nothing is there to replace, which is
    // what Java's `previousDataFiles()` makes of a missing directory too.
    if (listed.IsNotExist()) {
        return Status::OK();
    }
    PAIMON_RETURN_NOT_OK(listed);
    for (const FormatDataSplit::FileMeta& file : files) {
        Status status = file_system->Delete(file.file_path, /*recursive=*/false);
        if (!status.ok() && !status.IsNotExist()) {
            return status;
        }
    }
    return Status::OK();
}

Status FormatTableCommit::Commit(const std::vector<FormatCommitMessage>& commit_messages) {
    Status status = CommitImpl(commit_messages);
    if (!status.ok()) {
        // The write has already prepared its commit, so nothing else will clean up what is staged.
        Status abort_status = Abort(commit_messages);
        if (!abort_status.ok()) {
            PAIMON_LOG_WARN(CommitLogger(), "Failed to clean up table %s after a failed commit: %s",
                            table_->FullName().c_str(), abort_status.ToString().c_str());
        }
    }
    return status;
}

Status FormatTableCommit::CommitImpl(const std::vector<FormatCommitMessage>& commit_messages) {
    std::shared_ptr<FileSystem> file_system = table_->GetFileSystem();
    const std::vector<std::string>& partition_keys = table_->PartitionKeys();
    Duration duration;
    PAIMON_LOG_INFO(CommitLogger(), "Ready to %s %zu messages to format table %s",
                    overwrite_ ? "overwrite with" : "commit", commit_messages.size(),
                    table_->FullName().c_str());

    for (const FormatCommitMessage& message : commit_messages) {
        PAIMON_RETURN_NOT_OK(ValidateCommitMessage(message, table_, static_partition_));
    }

    // Checked against disk before anything moves: an overwrite clears the old data first, so a
    // staged file that turns out to be missing would lose the old rows without gaining the new.
    std::set<std::string> targets;
    for (const FormatCommitMessage& message : commit_messages) {
        if (!targets.insert(message.file_path).second) {
            return Status::Invalid(fmt::format(
                "two commit messages would publish {}, so one would overwrite the other",
                message.file_path));
        }
        Result<FileStatus> staged = file_system->GetFileStatus(message.temp_file_path);
        if (!staged.ok()) {
            // `Invalid` whatever the file system said, since the fault is the message.
            return Status::Invalid(fmt::format("the staged file {} cannot be read: {}",
                                               message.temp_file_path, staged.status().ToString()));
        }
        // `rename` moves a directory as readily as a file.
        if (staged.value().IsDir()) {
            return Status::Invalid(fmt::format("the staged path {} is a directory, not a file",
                                               message.temp_file_path));
        }
        if (message.file_size != staged.value().GetLen()) {
            return Status::Invalid(
                fmt::format("the staged file {} is {} bytes but the commit message says {}",
                            message.temp_file_path, staged.value().GetLen(), message.file_size));
        }
    }

    // What an overwrite replaces is cleared first: it removes committed files only, and this
    // commit's own are still hidden, so neither can take the other out.
    std::vector<std::string> replaced_directories;
    if (!static_partition_.empty()) {
        // The spec names the leading keys in order, so the path may be a prefix with the
        // partitions of the unnamed keys below it.
        std::vector<std::pair<std::string, std::string>> ordered_partition;
        ordered_partition.reserve(static_partition_.size());
        for (const std::string& partition_key : partition_keys) {
            auto iter = static_partition_.find(partition_key);
            if (iter == static_partition_.end()) {
                break;
            }
            ordered_partition.emplace_back(partition_key, iter->second);
        }
        PAIMON_ASSIGN_OR_RAISE(std::string partition_path,
                               PartitionPathUtils::GeneratePartitionPath(
                                   ordered_partition, table_->PartitionOnlyValueInPath()));
        std::string directory = PathUtil::JoinPath(table_->Location(), partition_path);
        // The spec may name a partition no message covers: an overwrite of a directory a scan
        // skips would clear files that are not this table's data.
        PAIMON_RETURN_NOT_OK(FormatPathValidation::ValidateDirectoryIsVisible(table_, directory,
                                                                              "static partition"));
        PAIMON_ASSIGN_OR_RAISE(bool exists, file_system->Exists(directory));
        if (!exists) {
            // Nothing to clear, but created regardless: an overwrite leaves an empty partition
            // behind rather than removing it from the table.
            PAIMON_RETURN_NOT_OK(file_system->Mkdirs(directory));
        } else if (overwrite_) {
            PAIMON_RETURN_NOT_OK(DeletePreviousDataFiles(
                directory, static_cast<int32_t>(partition_keys.size() - ordered_partition.size())));
            replaced_directories.push_back(std::move(directory));
        }
    } else if (overwrite_ && ReplacesOnlyWrittenPartitions()) {
        // The directory the message's partition names, not the file's own parent: a file may sit
        // below its partition directory, and an overwrite replaces the whole partition.
        std::set<std::string> directories;
        for (const FormatCommitMessage& message : commit_messages) {
            PAIMON_ASSIGN_OR_RAISE(
                std::string directory,
                FormatPathValidation::BuildPartitionDirectory(table_, message.partition));
            directories.insert(std::move(directory));
        }
        for (const std::string& directory : directories) {
            // A complete partition directory, so no partition level is left below it.
            PAIMON_RETURN_NOT_OK(DeletePreviousDataFiles(directory, /*partition_levels=*/0));
            replaced_directories.push_back(directory);
        }
    } else if (overwrite_) {
        // Replacing the table rather than the partitions this commit wrote: unpartitioned, or
        // `dynamic-partition-overwrite` off. What goes is everything the table holds, so a commit
        // carrying no message still empties it - Java's `tableDataDirectories()`.
        PAIMON_RETURN_NOT_OK(DeletePreviousDataFiles(table_->Location(),
                                                     static_cast<int32_t>(partition_keys.size())));
        replaced_directories.push_back(table_->Location());
    }
    if (overwrite_) {
        // Nothing else records what an overwrite emptied. Only a sample of the paths, since a
        // table of many partitions would put every directory in one line.
        constexpr size_t kLoggedDirectories = 5;
        const char* replaced_what = !static_partition_.empty() ? "the static partition"
                                    : ReplacesOnlyWrittenPartitions()
                                        ? "the partitions this commit writes"
                                        : "the whole table";
        const size_t logged = std::min(replaced_directories.size(), kLoggedDirectories);
        std::vector<std::string> sample(
            replaced_directories.begin(),
            replaced_directories.begin() + static_cast<std::ptrdiff_t>(logged));
        PAIMON_LOG_INFO(
            CommitLogger(),
            "Cleared the data files of %zu directories of format table %s, replacing %s: %s%s",
            replaced_directories.size(), table_->FullName().c_str(), replaced_what,
            fmt::format("{}", fmt::join(sample, ", ")).c_str(),
            replaced_directories.size() > logged ? ", ..." : "");
    }

    // Recorded before its rename, not after: a failed rename may have published the file all the
    // same - on an object store it is a copy followed by a delete - and a target left behind reads
    // back as this table's data. Removing it is safe: the name carries the staging write's uuid.
    std::vector<std::string> attempted_publish_paths;
    attempted_publish_paths.reserve(commit_messages.size());
    for (const FormatCommitMessage& message : commit_messages) {
        attempted_publish_paths.push_back(message.file_path);
        Status status = file_system->Rename(message.temp_file_path, message.file_path);
        if (!status.ok()) {
            // Only when nothing was replaced, so the table holds all of this write or none of it.
            // An overwrite has already deleted what it replaces, so taking its files back would
            // leave neither the old rows nor the new - Java's
            // `markPublishedTargetsToPreserveOnAbort()` preserves them for the same reason.
            if (!overwrite_) {
                for (const std::string& file_path : attempted_publish_paths) {
                    Status rollback = file_system->Delete(file_path, /*recursive=*/false);
                    // Never published is the ordinary case for the target the rename failed on.
                    if (!rollback.ok() && !rollback.IsNotExist()) {
                        PAIMON_LOG_WARN(CommitLogger(),
                                        "Failed to take back the published file %s: %s",
                                        file_path.c_str(), rollback.ToString().c_str());
                    }
                }
            }
            return Status::IOError(fmt::format("failed to commit {} of format table {}: {}",
                                               message.ToString(), table_->FullName(),
                                               status.ToString()));
        }
    }
    // Publishing is a loop of renames, not one atomic step: without this line a commit that
    // started and never finished would look from the log like one that did.
    PAIMON_LOG_INFO(CommitLogger(), "Finished %s %zu files to format table %s, duration %lu ms",
                    overwrite_ ? "overwriting with" : "committing", commit_messages.size(),
                    table_->FullName().c_str(), duration.Get());
    return Status::OK();
}

Status FormatTableCommit::Abort(const std::vector<FormatCommitMessage>& commit_messages) {
    std::shared_ptr<FileSystem> file_system = table_->GetFileSystem();
    // Best effort: a file that cannot be removed stays behind under its hidden name, where a scan
    // ignores it. Java's `abort()` reports its failures instead; here the log is the signal, so
    // one bad message cannot stop the rest being cleaned up.
    for (const FormatCommitMessage& message : commit_messages) {
        // Returning at the first bad one would strand the staged files of the good ones.
        Status valid = ValidateCommitMessage(message, table_, static_partition_);
        if (!valid.ok()) {
            PAIMON_LOG_WARN(CommitLogger(),
                            "Refusing to discard a file that does not describe this table: %s",
                            valid.ToString().c_str());
            continue;
        }
        Status status = file_system->Delete(message.temp_file_path, /*recursive=*/false);
        // Already gone is the expected case after a rollback took the file back.
        if (!status.ok() && !status.IsNotExist()) {
            PAIMON_LOG_WARN(CommitLogger(), "Failed to discard the staged file %s: %s",
                            message.temp_file_path.c_str(), status.ToString().c_str());
        }
    }
    return Status::OK();
}

}  // namespace paimon
