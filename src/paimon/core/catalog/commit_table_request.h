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

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/utils/jsonizable.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/snapshot.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace paimon {

/// Request to commit a table snapshot and its partition statistics.
class CommitTableRequest : public Jsonizable<CommitTableRequest> {
 public:
    /// @param table_id Catalog table UUID used to detect table recreation; null if unavailable.
    /// @param base_snapshot_uuid Base snapshot UUID; null for an absent or legacy snapshot.
    /// @param snapshot Snapshot to be committed.
    /// @param statistics Partition statistics for this change.
    CommitTableRequest(const std::optional<std::string>& table_id,
                       const std::optional<std::string>& base_snapshot_uuid,
                       const Snapshot& snapshot, const std::vector<PartitionStatistics>& statistics)
        : table_id_(table_id),
          base_snapshot_uuid_(base_snapshot_uuid),
          snapshot_(snapshot),
          statistics_(statistics) {}

    const std::optional<std::string>& GetTableId() const {
        return table_id_;
    }

    const std::optional<std::string>& GetBaseSnapshotUuid() const {
        return base_snapshot_uuid_;
    }

    const Snapshot& GetSnapshot() const {
        return snapshot_;
    }

    const std::vector<PartitionStatistics>& GetStatistics() const {
        return statistics_;
    }

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(rapidjson::StringRef(FIELD_TABLE_ID),
                      RapidJsonUtil::SerializeValue(table_id_, allocator).Move(), *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_BASE_SNAPSHOT_UUID),
                      RapidJsonUtil::SerializeValue(base_snapshot_uuid_, allocator).Move(),
                      *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_SNAPSHOT),
                      RapidJsonUtil::SerializeValue(snapshot_, allocator).Move(), *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_STATISTICS),
                      RapidJsonUtil::SerializeValue(statistics_, allocator).Move(), *allocator);
        return obj;
    }

    void FromJson(const rapidjson::Value& obj) noexcept(false) override {
        table_id_ =
            RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(obj, FIELD_TABLE_ID);
        base_snapshot_uuid_ = RapidJsonUtil::DeserializeKeyValue<std::optional<std::string>>(
            obj, FIELD_BASE_SNAPSHOT_UUID);
        snapshot_ = RapidJsonUtil::DeserializeKeyValue<Snapshot>(obj, FIELD_SNAPSHOT);
        statistics_ = RapidJsonUtil::DeserializeKeyValue<std::vector<PartitionStatistics>>(
            obj, FIELD_STATISTICS);
    }

    bool TEST_Equal(const CommitTableRequest& other) const {
        if (this == &other) {
            return true;
        }
        return table_id_ == other.table_id_ && base_snapshot_uuid_ == other.base_snapshot_uuid_ &&
               snapshot_.TEST_Equal(other.snapshot_) && statistics_ == other.statistics_;
    }

    bool operator==(const CommitTableRequest& other) const {
        return table_id_ == other.table_id_ && base_snapshot_uuid_ == other.base_snapshot_uuid_ &&
               snapshot_ == other.snapshot_ && statistics_ == other.statistics_;
    }

 private:
    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(CommitTableRequest);

 private:
    static constexpr const char* FIELD_TABLE_ID = "tableId";
    static constexpr const char* FIELD_BASE_SNAPSHOT_UUID = "baseSnapshotUuid";
    static constexpr const char* FIELD_SNAPSHOT = "snapshot";
    static constexpr const char* FIELD_STATISTICS = "statistics";

    std::optional<std::string> table_id_;
    std::optional<std::string> base_snapshot_uuid_;
    Snapshot snapshot_;
    std::vector<PartitionStatistics> statistics_;
};

}  // namespace paimon
