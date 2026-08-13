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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/table/source/split.h"

namespace paimon {

/// Split combining committed disk splits and a ticket for one immutable memory view.
///
/// The current fields are process-independent except that `opaque_ticket` can only be resolved by
/// the `RealtimeContext` that created it. The ticket is single-use: creating a reader consumes it,
/// so retrying a reader task requires planning a new `RealtimeSplit`.
class RealtimeSplit : public Split {
 public:
    /// Current metadata version of a real-time split.
    static constexpr int32_t CURRENT_VERSION = 1;

    RealtimeSplit(int32_t version, std::optional<int64_t> snapshot_id,
                  std::map<std::string, std::string> partition, int32_t bucket,
                  std::vector<std::shared_ptr<Split>>&& disk_splits, int64_t committed_offset,
                  int64_t memory_upper_offset, std::string opaque_ticket)
        : version_(version),
          snapshot_id_(std::move(snapshot_id)),
          partition_(std::move(partition)),
          bucket_(bucket),
          disk_splits_(std::move(disk_splits)),
          committed_offset_(committed_offset),
          memory_upper_offset_(memory_upper_offset),
          opaque_ticket_(std::move(opaque_ticket)) {}

    int32_t Version() const {
        return version_;
    }

    const std::optional<int64_t>& SnapshotId() const {
        return snapshot_id_;
    }

    const std::map<std::string, std::string>& Partition() const {
        return partition_;
    }

    int32_t Bucket() const {
        return bucket_;
    }

    const std::vector<std::shared_ptr<Split>>& DiskSplits() const {
        return disk_splits_;
    }

    int64_t CommittedOffset() const {
        return committed_offset_;
    }

    int64_t MemoryUpperOffset() const {
        return memory_upper_offset_;
    }

    const std::string& OpaqueTicket() const {
        return opaque_ticket_;
    }

 private:
    int32_t version_;
    std::optional<int64_t> snapshot_id_;
    std::map<std::string, std::string> partition_;
    int32_t bucket_;
    std::vector<std::shared_ptr<Split>> disk_splits_;
    int64_t committed_offset_;
    int64_t memory_upper_offset_;
    std::string opaque_ticket_;
};

}  // namespace paimon
