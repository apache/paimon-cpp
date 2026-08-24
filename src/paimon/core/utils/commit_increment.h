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

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/compact/compact_deletion_file.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/realtime/offset_range.h"

namespace paimon {

// Changes to commit.
class CommitIncrement {
 public:
    CommitIncrement(const DataIncrement& data_increment, const CompactIncrement& compact_increment,
                    const std::shared_ptr<CompactDeletionFile>& compact_deletion_file)
        : data_increment_(data_increment),
          compact_increment_(compact_increment),
          compact_deletion_file_(compact_deletion_file) {}

    const DataIncrement& GetNewFilesIncrement() const {
        return data_increment_;
    }

    const CompactIncrement& GetCompactIncrement() const {
        return compact_increment_;
    }

    DataIncrement& GetNewFilesIncrement() {
        return data_increment_;
    }

    CompactIncrement& GetCompactIncrement() {
        return compact_increment_;
    }

    std::shared_ptr<CompactDeletionFile> GetCompactDeletionFile() const {
        return compact_deletion_file_;
    }

    const std::optional<OffsetRange>& GetRealtimeOffsetRange() const {
        return realtime_offset_range_;
    }

    void SetRealtimeOffsetRange(const OffsetRange& offset_range) {
        realtime_offset_range_ = offset_range;
    }

    /// Paths of the managed blob packs this increment's writer created and handed over, which a
    /// rollback of the resulting commit has to delete. Empty for every table without managed
    /// blob fields.
    ///
    /// Taking rather than reading: ownership moves on to the commit message built from this
    /// increment, and leaving a second copy behind would invite two owners deleting one pack.
    std::vector<std::string> TakeOwnedManagedBlobPacks() {
        return std::move(owned_managed_blob_packs_);
    }

    void SetOwnedManagedBlobPacks(std::vector<std::string> packs) {
        owned_managed_blob_packs_ = std::move(packs);
    }

 private:
    DataIncrement data_increment_;
    CompactIncrement compact_increment_;
    std::shared_ptr<CompactDeletionFile> compact_deletion_file_;
    std::optional<OffsetRange> realtime_offset_range_;
    std::vector<std::string> owned_managed_blob_packs_;
};

}  // namespace paimon
