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

#include "paimon/core/table/sink/commit_message_serializer.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/index/index_file_meta_serializer.h"
#include "paimon/core/index/index_file_meta_v1_deserializer.h"
#include "paimon/core/index/index_file_meta_v2_deserializer.h"
#include "paimon/core/index/index_file_meta_v3_deserializer.h"
#include "paimon/core/index/index_file_meta_v4_deserializer.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta_09_serializer.h"
#include "paimon/core/io/data_file_meta_10_serializer.h"
#include "paimon/core/io/data_file_meta_12_serializer.h"
#include "paimon/core/io/data_file_meta_first_row_id_legacy_serializer.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/table/format/format_commit_message.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/io/data_input_stream.h"

namespace paimon {
class MemoryPool;

const int32_t CommitMessageSerializer::CURRENT_VERSION = 12;

CommitMessageSerializer::CommitMessageSerializer(const std::shared_ptr<MemoryPool>& pool)
    : memory_pool_(pool),
      data_file_serializer_(std::make_unique<DataFileMetaSerializer>(pool)),
      index_entry_serializer_(std::make_unique<IndexFileMetaSerializer>(pool)) {}

CommitMessageSerializer::~CommitMessageSerializer() = default;

Status CommitMessageSerializer::Serialize(const std::shared_ptr<CommitMessage>& obj,
                                          MemorySegmentOutputStream* out) {
    if (std::dynamic_pointer_cast<FormatCommitMessage>(obj) != nullptr) {
        // A format table's commit message names a staged path rather than files to record in a
        // manifest, and has no cross-runtime encoding.
        return Status::NotImplemented(
            "a FormatCommitMessage cannot be serialized: a format table's write and its commit "
            "belong to one process");
    }
    auto message = std::dynamic_pointer_cast<CommitMessageImpl>(obj);
    if (message == nullptr) {
        return Status::Invalid("failed to cast commit message to commit message impl");
    }
    PAIMON_RETURN_NOT_OK(SerializationUtils::SerializeBinaryRow(message->Partition(), out));
    out->WriteValue<int32_t>(message->Bucket());
    std::optional<int32_t> total_buckets = message->TotalBuckets();
    if (total_buckets == std::nullopt) {
        out->WriteValue<bool>(false);
    } else {
        out->WriteValue<bool>(true);
        out->WriteValue<int32_t>(total_buckets.value());
    }
    // data increment
    PAIMON_RETURN_NOT_OK(
        data_file_serializer_->SerializeList(message->GetNewFilesIncrement().NewFiles(), out));
    PAIMON_RETURN_NOT_OK(
        data_file_serializer_->SerializeList(message->GetNewFilesIncrement().DeletedFiles(), out));
    PAIMON_RETURN_NOT_OK(data_file_serializer_->SerializeList(
        message->GetNewFilesIncrement().ChangelogFiles(), out));
    PAIMON_RETURN_NOT_OK(index_entry_serializer_->SerializeList(
        message->GetNewFilesIncrement().NewIndexFiles(), out));
    PAIMON_RETURN_NOT_OK(index_entry_serializer_->SerializeList(
        message->GetNewFilesIncrement().DeletedIndexFiles(), out));
    // compact increment
    PAIMON_RETURN_NOT_OK(
        data_file_serializer_->SerializeList(message->GetCompactIncrement().CompactBefore(), out));
    PAIMON_RETURN_NOT_OK(
        data_file_serializer_->SerializeList(message->GetCompactIncrement().CompactAfter(), out));
    PAIMON_RETURN_NOT_OK(
        data_file_serializer_->SerializeList(message->GetCompactIncrement().ChangelogFiles(), out));
    PAIMON_RETURN_NOT_OK(index_entry_serializer_->SerializeList(
        message->GetCompactIncrement().NewIndexFiles(), out));
    PAIMON_RETURN_NOT_OK(index_entry_serializer_->SerializeList(
        message->GetCompactIncrement().DeletedIndexFiles(), out));

    return Status::OK();
}

Status CommitMessageSerializer::SerializeList(
    const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
    MemorySegmentOutputStream* out) {
    out->WriteValue<int32_t>(commit_messages.size());
    for (const auto& commit_message : commit_messages) {
        PAIMON_RETURN_NOT_OK(Serialize(commit_message, out));
    }
    return Status::OK();
}

Result<std::vector<std::shared_ptr<CommitMessage>>> CommitMessageSerializer::DeserializeList(
    int32_t version, DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(int32_t length, in->ReadValue<int32_t>());
    std::vector<std::shared_ptr<CommitMessage>> commit_messages;
    for (int32_t i = 0; i < length; i++) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CommitMessage> commit_message,
                               Deserialize(version, in));
        commit_messages.push_back(commit_message);
    }
    return commit_messages;
}

Result<std::shared_ptr<CommitMessage>> CommitMessageSerializer::Deserialize(
    int32_t version,
    const ObjectSerializer<std::shared_ptr<DataFileMeta>>* data_file_meta_serializer,
    const ObjectSerializer<std::shared_ptr<IndexFileMeta>>* index_entry_serializer,
    DataInputStream* in) {
    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           SerializationUtils::DeserializeBinaryRow(in, memory_pool_.get()));
    PAIMON_ASSIGN_OR_RAISE(int32_t bucket, in->ReadValue<int32_t>());

    std::optional<int32_t> total_buckets;
    if (version >= 7) {
        PAIMON_ASSIGN_OR_RAISE(bool total_buckets_exist, in->ReadValue<bool>());
        if (total_buckets_exist) {
            PAIMON_ASSIGN_OR_RAISE(total_buckets, in->ReadValue<int32_t>());
        }
    }
    if (version >= 10) {
        // data increment
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> new_files,
                               data_file_meta_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> deleted_files,
                               data_file_meta_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> changelog_files,
                               data_file_meta_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> new_data_index,
                               index_entry_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> deleted_data_index,
                               index_entry_serializer->DeserializeList(in));
        // compact increment
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> before,
                               data_file_meta_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> after,
                               data_file_meta_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> changelog,
                               data_file_meta_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> new_compact_index,
                               index_entry_serializer->DeserializeList(in));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> deleted_compact_index,
                               index_entry_serializer->DeserializeList(in));
        return std::make_shared<CommitMessageImpl>(
            partition, bucket, total_buckets,
            DataIncrement(std::move(new_files), std::move(deleted_files),
                          std::move(changelog_files), std::move(new_data_index),
                          std::move(deleted_data_index)),
            CompactIncrement(std::move(before), std::move(after), std::move(changelog),
                             std::move(new_compact_index), std::move(deleted_compact_index)));
    }
    // data increment
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> new_files,
                           data_file_meta_serializer->DeserializeList(in));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> deleted_files,
                           data_file_meta_serializer->DeserializeList(in));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> changelog_files,
                           data_file_meta_serializer->DeserializeList(in));
    // compact increment
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> before,
                           data_file_meta_serializer->DeserializeList(in));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> after,
                           data_file_meta_serializer->DeserializeList(in));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> changelog,
                           data_file_meta_serializer->DeserializeList(in));
    // index increment
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> new_index,
                           index_entry_serializer->DeserializeList(in));
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> deleted_index,
                           index_entry_serializer->DeserializeList(in));

    DataIncrement data_increment(std::move(new_files), std::move(deleted_files),
                                 std::move(changelog_files));
    CompactIncrement compact_increment(std::move(before), std::move(after), std::move(changelog));

    if (compact_increment.IsEmpty()) {
        data_increment.AddNewIndexFiles(std::move(new_index));
        data_increment.AddDeletedIndexFiles(std::move(deleted_index));
    } else {
        compact_increment.AddNewIndexFiles(std::move(new_index));
        compact_increment.AddDeletedIndexFiles(std::move(deleted_index));
    }
    return std::make_shared<CommitMessageImpl>(partition, bucket, total_buckets, data_increment,
                                               compact_increment);
}

Result<std::shared_ptr<CommitMessage>> CommitMessageSerializer::Deserialize(int32_t version,
                                                                            DataInputStream* in) {
    if (version == CURRENT_VERSION) {
        return Deserialize(version, data_file_serializer_.get(), index_entry_serializer_.get(), in);
    } else if (version == 11) {
        auto index_entry_v4_deserializer =
            std::make_unique<IndexFileMetaV4Deserializer>(memory_pool_);
        return Deserialize(version, data_file_serializer_.get(), index_entry_v4_deserializer.get(),
                           in);
    } else if (version == 9 || version == 10) {
        auto index_entry_v3_deserializer =
            std::make_unique<IndexFileMetaV3Deserializer>(memory_pool_);
        return Deserialize(version, data_file_serializer_.get(), index_entry_v3_deserializer.get(),
                           in);
    } else if (version == 8) {
        auto data_file_meta_first_row_id_legacy_serializer =
            std::make_unique<DataFileMetaFirstRowIdLegacySerializer>(memory_pool_);
        auto index_entry_v2_deserializer =
            std::make_unique<IndexFileMetaV2Deserializer>(memory_pool_);
        return Deserialize(version, data_file_meta_first_row_id_legacy_serializer.get(),
                           index_entry_v2_deserializer.get(), in);
    } else if (version == 6 || version == 7) {
        auto index_entry_v2_deserializer =
            std::make_unique<IndexFileMetaV2Deserializer>(memory_pool_);
        auto data_file_meta_12_serializer =
            std::make_unique<DataFileMeta12Serializer>(memory_pool_);
        return Deserialize(version, data_file_meta_12_serializer.get(),
                           index_entry_v2_deserializer.get(), in);
    } else if (version == 5) {
        auto index_entry_v2_deserializer =
            std::make_unique<IndexFileMetaV2Deserializer>(memory_pool_);
        auto data_file_meta_10_serializer =
            std::make_unique<DataFileMeta10Serializer>(memory_pool_);
        return Deserialize(version, data_file_meta_10_serializer.get(),
                           index_entry_v2_deserializer.get(), in);
    } else if (version == 4) {
        auto index_entry_v1_deserializer =
            std::make_unique<IndexFileMetaV1Deserializer>(memory_pool_);
        auto data_file_meta_10_serializer =
            std::make_unique<DataFileMeta10Serializer>(memory_pool_);
        return Deserialize(version, data_file_meta_10_serializer.get(),
                           index_entry_v1_deserializer.get(), in);
    } else if (version == 3) {
        auto data_file_meta_09_serializer =
            std::make_unique<DataFileMeta09Serializer>(memory_pool_);
        auto index_entry_v1_deserializer =
            std::make_unique<IndexFileMetaV1Deserializer>(memory_pool_);
        return Deserialize(version, data_file_meta_09_serializer.get(),
                           index_entry_v1_deserializer.get(), in);
    } else if (version <= 2) {
        return Status::NotImplemented("deserialize 08 not implemented");
    } else {
        return Status::Invalid(
            fmt::format("Expecting CommitMessageSerializer version to be smaller or equal than {}, "
                        "but found {}.",
                        CURRENT_VERSION, version));
    }
}

}  // namespace paimon
