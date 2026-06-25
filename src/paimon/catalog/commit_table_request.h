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

class CommitTableRequest : public Jsonizable<CommitTableRequest> {
 public:
    CommitTableRequest(const Snapshot& snapshot, const std::vector<PartitionStatistics>& statistics)
        : snapshot_(snapshot), statistics_(statistics) {}

    rapidjson::Value ToJson(rapidjson::Document::AllocatorType* allocator) const
        noexcept(false) override {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember(rapidjson::StringRef(FIELD_SNAPSHOT),
                      RapidJsonUtil::SerializeValue(snapshot_, allocator).Move(), *allocator);
        obj.AddMember(rapidjson::StringRef(FIELD_STATISTICS),
                      RapidJsonUtil::SerializeValue(statistics_, allocator).Move(), *allocator);
        return obj;
    }

    void FromJson(const rapidjson::Value& obj) noexcept(false) override {
        snapshot_ = RapidJsonUtil::DeserializeKeyValue<Snapshot>(obj, FIELD_SNAPSHOT);
        statistics_ = RapidJsonUtil::DeserializeKeyValue<std::vector<PartitionStatistics>>(
            obj, FIELD_STATISTICS);
    }

    bool TEST_Equal(const CommitTableRequest& other) const {
        if (this == &other) {
            return true;
        }
        return snapshot_.TEST_Equal(other.snapshot_) && statistics_ == other.statistics_;
    }

    bool operator==(const CommitTableRequest& other) const {
        return snapshot_ == other.snapshot_ && statistics_ == other.statistics_;
    }

 private:
    JSONIZABLE_FRIEND_AND_DEFAULT_CTOR(CommitTableRequest);

 private:
    static constexpr const char* FIELD_SNAPSHOT = "snapshot";
    static constexpr const char* FIELD_STATISTICS = "statistics";

    Snapshot snapshot_;
    std::vector<PartitionStatistics> statistics_;
};

}  // namespace paimon
