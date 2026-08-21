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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/core/operation/commit/realtime_commit_properties.h"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/jsonizable.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/core/utils/partition_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"

namespace paimon {
namespace {

class OffsetEntryJson : public Jsonizable<OffsetEntryJson> {
 public:
    OffsetEntryJson() = default;

    OffsetEntryJson(std::map<std::string, std::string> partition, int32_t bucket, int64_t offset)
        : partition_(std::move(partition)), bucket_(bucket), offset_(offset) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value value(rapidjson::kObjectType);
        value.AddMember("partition", RapidJsonUtil::SerializeValue(partition_, allocator),
                        *allocator);
        value.AddMember("bucket", RapidJsonUtil::SerializeValue(bucket_, allocator), *allocator);
        value.AddMember("offset", RapidJsonUtil::SerializeValue(offset_, allocator), *allocator);
        return value;
    }

    void FromJson(const rapidjson::Value& value) noexcept(false) override {
        partition_ = RapidJsonUtil::DeserializeKeyValue<std::map<std::string, std::string>>(
            value, "partition");
        bucket_ = RapidJsonUtil::DeserializeKeyValue<int32_t>(value, "bucket");
        offset_ = RapidJsonUtil::DeserializeKeyValue<int64_t>(value, "offset");
    }

    const std::map<std::string, std::string>& Partition() const {
        return partition_;
    }

    int32_t Bucket() const {
        return bucket_;
    }

    int64_t Offset() const {
        return offset_;
    }

 private:
    std::map<std::string, std::string> partition_;
    int32_t bucket_ = -1;
    int64_t offset_ = -1;
};

class OffsetsJson : public Jsonizable<OffsetsJson> {
 public:
    OffsetsJson() = default;

    explicit OffsetsJson(const RealtimeOffsetMap& offsets) : offsets_(offsets) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value value(rapidjson::kObjectType);
        value.AddMember(
            "version",
            RapidJsonUtil::SerializeValue(RealtimeCommitProperties::kOffsetsVersion, allocator),
            *allocator);
        std::vector<OffsetEntryJson> entries;
        entries.reserve(offsets_.size());
        for (const auto& [partition_bucket, offset] : offsets_) {
            if (partition_bucket.bucket < 0) {
                throw std::invalid_argument(
                    fmt::format("invalid bucket {} in offsets", partition_bucket.bucket));
            }
            if (offset < 0) {
                throw std::invalid_argument(fmt::format("invalid offset {} for bucket {}", offset,
                                                        partition_bucket.bucket));
            }
            entries.emplace_back(partition_bucket.partition, partition_bucket.bucket, offset);
        }
        value.AddMember("offsets", RapidJsonUtil::SerializeValue(entries, allocator), *allocator);
        return value;
    }

    void FromJson(const rapidjson::Value& value) noexcept(false) override {
        auto version = RapidJsonUtil::DeserializeKeyValue<int32_t>(value, "version");
        if (version != RealtimeCommitProperties::kOffsetsVersion) {
            throw std::invalid_argument(fmt::format("unsupported offsets version {}", version));
        }
        auto entries =
            RapidJsonUtil::DeserializeKeyValue<std::vector<OffsetEntryJson>>(value, "offsets");
        offsets_.clear();
        for (const OffsetEntryJson& entry : entries) {
            if (entry.Bucket() < 0) {
                throw std::invalid_argument(
                    fmt::format("invalid bucket {} in offsets", entry.Bucket()));
            }
            if (entry.Offset() < 0) {
                throw std::invalid_argument(
                    fmt::format("invalid offset {} in offsets", entry.Offset()));
            }
            RealtimePartitionBucket partition_bucket(entry.Partition(), entry.Bucket());
            if (!offsets_.emplace(std::move(partition_bucket), entry.Offset()).second) {
                throw std::invalid_argument(
                    fmt::format("duplicate partition-bucket {} in offsets", entry.Bucket()));
            }
        }
    }

    const RealtimeOffsetMap& Offsets() const {
        return offsets_;
    }

 private:
    RealtimeOffsetMap offsets_;
};

}  // namespace

void RealtimeCommitProperties::Sort(std::vector<RealtimeCommitProgress>* commits) {
    std::stable_sort(commits->begin(), commits->end(),
                     [](const RealtimeCommitProgress& lhs, const RealtimeCommitProgress& rhs) {
                         if (lhs.partition_bucket != rhs.partition_bucket) {
                             return lhs.partition_bucket < rhs.partition_bucket;
                         }
                         return lhs.offset_range.begin < rhs.offset_range.begin;
                     });
}

std::string RealtimeCommitProperties::OffsetsDirectory(const std::string& table_root,
                                                       const std::string& branch) {
    return PathUtil::JoinPath(BranchManager::BranchPath(table_root, branch), "metadata");
}

std::optional<std::string> RealtimeCommitProperties::GetOffsetsPath(const Snapshot& snapshot) {
    if (!snapshot.Properties()) {
        return std::nullopt;
    }
    const std::map<std::string, std::string>& properties = snapshot.Properties().value();
    auto iter = properties.find(kOffsetsKey);
    if (iter == properties.end()) {
        return std::nullopt;
    }
    return iter->second;
}

Result<RealtimeOffsetMap> RealtimeCommitProperties::ReadOffsets(
    const std::optional<Snapshot>& snapshot, const std::shared_ptr<FileSystem>& file_system) {
    if (!snapshot) {
        return RealtimeOffsetMap{};
    }
    std::optional<std::string> offsets_path = GetOffsetsPath(snapshot.value());
    if (!offsets_path) {
        return RealtimeOffsetMap{};
    }
    if (file_system == nullptr) {
        return Status::Invalid("file system is null when reading real-time offsets");
    }
    std::string content;
    PAIMON_RETURN_NOT_OK(file_system->ReadFile(offsets_path.value(), &content));
    return ParseOffsets(content);
}

Result<bool> RealtimeCommitProperties::AreRangesCommitted(
    const RealtimeOffsetMap& committed_offsets,
    const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges) {
    std::optional<bool> all_committed;
    for (const auto& [partition_bucket, offset_range] : realtime_ranges) {
        if (partition_bucket.bucket < 0) {
            return Status::Invalid(
                fmt::format("real-time commit bucket {} is invalid", partition_bucket.bucket));
        }
        if (offset_range.begin < 0 || offset_range.begin >= offset_range.end) {
            return Status::Invalid("real-time commit offset range is invalid");
        }

        auto offset_iter = committed_offsets.find(partition_bucket);
        int64_t committed_end_offset =
            offset_iter == committed_offsets.end() ? 0 : offset_iter->second;
        bool range_committed = offset_range.end <= committed_end_offset;
        if (!range_committed && offset_range.begin < committed_end_offset) {
            return Status::Invalid(fmt::format(
                "real-time commit offset range partially overlaps committed offset for bucket {}",
                partition_bucket.bucket));
        }
        if (!range_committed && offset_range.begin != committed_end_offset) {
            return Status::Invalid(
                fmt::format("real-time commit offsets for bucket {} are not contiguous",
                            partition_bucket.bucket));
        }
        if (all_committed && all_committed.value() != range_committed) {
            return Status::Invalid(
                "real-time commit ranges are only partially covered by committed offsets");
        }
        all_committed = range_committed;
    }
    return all_committed.value_or(false);
}

Result<std::string> RealtimeCommitProperties::SerializeOffsets(const RealtimeOffsetMap& offsets) {
    std::string result;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(OffsetsJson(offsets), &result));
    return result;
}

Result<std::map<std::string, std::string>> RealtimeCommitProperties::Build(
    const std::map<std::string, std::string>& properties,
    const std::optional<Snapshot>& latest_snapshot,
    const std::map<RealtimePartitionBucket, OffsetRange>& realtime_ranges,
    bool reset_all_realtime_progress,
    const std::vector<std::map<std::string, std::string>>& removed_realtime_partitions,
    const BinaryRowPartitionComputer& partition_computer,
    const std::shared_ptr<FileSystem>& file_system, const std::string& table_root,
    const std::string& branch) {
    std::map<std::string, std::string> merged_properties = properties;
    if (reset_all_realtime_progress || !removed_realtime_partitions.empty()) {
        merged_properties.erase(kOffsetsKey);
    }
    if (realtime_ranges.empty() && removed_realtime_partitions.empty()) {
        if (!reset_all_realtime_progress && latest_snapshot && latest_snapshot->Properties()) {
            const std::map<std::string, std::string>& latest_properties =
                latest_snapshot->Properties().value();
            auto offsets_iter = latest_properties.find(kOffsetsKey);
            if (offsets_iter != latest_properties.end()) {
                merged_properties[kOffsetsKey] = offsets_iter->second;
            }
        }
        return merged_properties;
    }

    PAIMON_ASSIGN_OR_RAISE(
        RealtimeOffsetMap merged_offsets,
        ReadOffsets(reset_all_realtime_progress ? std::nullopt : latest_snapshot, file_system));
    for (auto iter = merged_offsets.begin(); iter != merged_offsets.end();) {
        bool removed = false;
        for (const auto& partition_spec : removed_realtime_partitions) {
            PAIMON_ASSIGN_OR_RAISE(
                removed, PartitionUtils::MatchPartitionSpec(iter->first.partition, partition_spec,
                                                            partition_computer));
            if (removed) {
                break;
            }
        }
        if (removed) {
            iter = merged_offsets.erase(iter);
        } else {
            ++iter;
        }
    }
    for (const auto& [partition_bucket, offset_range] : realtime_ranges) {
        if (partition_bucket.bucket < 0) {
            return Status::Invalid(
                fmt::format("real-time commit bucket {} is invalid", partition_bucket.bucket));
        }
        if (offset_range.begin >= offset_range.end) {
            return Status::Invalid("real-time commit offset range is invalid");
        }
        auto offset_iter = merged_offsets.find(partition_bucket);
        int64_t previous_end_offset = offset_iter == merged_offsets.end() ? 0 : offset_iter->second;
        if (offset_range.begin != previous_end_offset) {
            return Status::Invalid(
                fmt::format("real-time commit offsets for bucket {} are not contiguous",
                            partition_bucket.bucket));
        }
        merged_offsets[partition_bucket] = offset_range.end;
    }
    if (merged_offsets.empty()) {
        merged_properties.erase(kOffsetsKey);
        return merged_properties;
    }
    PAIMON_ASSIGN_OR_RAISE(
        merged_properties[kOffsetsKey],
        WriteOffsets(merged_offsets, file_system, OffsetsDirectory(table_root, branch)));
    return merged_properties;
}

Result<std::string> RealtimeCommitProperties::WriteOffsets(
    const RealtimeOffsetMap& offsets, const std::shared_ptr<FileSystem>& file_system,
    const std::string& offsets_directory) {
    if (file_system == nullptr) {
        return Status::Invalid("file system is null when writing real-time offsets");
    }
    if (offsets_directory.empty()) {
        return Status::Invalid("real-time offsets directory is empty");
    }
    std::string uuid;
    if (!UUID::Generate(&uuid)) {
        return Status::Invalid("fail to generate uuid for real-time offsets file");
    }
    PAIMON_RETURN_NOT_OK(file_system->Mkdirs(offsets_directory));
    std::string path = PathUtil::JoinPath(offsets_directory, uuid + ".offsets");
    PAIMON_ASSIGN_OR_RAISE(std::string content, SerializeOffsets(offsets));
    PAIMON_RETURN_NOT_OK(file_system->WriteFile(path, content, /*overwrite=*/false));
    return path;
}

Result<RealtimeOffsetMap> RealtimeCommitProperties::ParseOffsets(const std::string& value) {
    OffsetsJson offsets_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(value, &offsets_json));
    return offsets_json.Offsets();
}

}  // namespace paimon
