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
/// `committed_end_offset` and `memory_end_offset` are exclusive bounds. Disk covers the committed
/// prefix and memory readers return the remaining `[committed_end_offset, memory_end_offset)`
/// range.
///
/// The current fields are process-independent except that `opaque_ticket` can only be resolved by
/// the `RealtimeContext` that created it. Successful reader creation consumes the ticket; a failed
/// creation can retry the same split until the ticket expires.
class RealtimeSplit : public Split {
 public:
    /// Current metadata version of a real-time split.
    static constexpr int32_t kCurrentVersion = 1;

    RealtimeSplit(int32_t version, std::optional<int64_t> snapshot_id,
                  std::map<std::string, std::string> partition, int32_t bucket,
                  std::vector<std::shared_ptr<Split>>&& disk_splits, int64_t committed_end_offset,
                  int64_t memory_end_offset, std::string opaque_ticket)
        : version_(version),
          snapshot_id_(std::move(snapshot_id)),
          partition_(std::move(partition)),
          bucket_(bucket),
          disk_splits_(std::move(disk_splits)),
          committed_end_offset_(committed_end_offset),
          memory_end_offset_(memory_end_offset),
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

    /// Returns the exclusive end offset covered by committed disk data.
    int64_t CommittedEndOffset() const {
        return committed_end_offset_;
    }

    /// Returns the exclusive end offset captured by the memory view.
    int64_t MemoryEndOffset() const {
        return memory_end_offset_;
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
    int64_t committed_end_offset_;
    int64_t memory_end_offset_;
    std::string opaque_ticket_;
};

}  // namespace paimon
