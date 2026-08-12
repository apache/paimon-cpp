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
#include <string>
#include <utility>
#include <vector>

#include "paimon/realtime/mem_indexer.h"
#include "paimon/table/source/split.h"

namespace paimon {

/// Process-local split combining committed disk splits and one immutable memory view.
///
/// This split intentionally has no serialized representation because it carries plugin objects.
class RealtimeSplit : public Split {
 public:
    RealtimeSplit(std::map<std::string, std::string> partition, int32_t bucket,
                  std::vector<std::shared_ptr<Split>>&& disk_splits,
                  const std::shared_ptr<MemIndexer>& indexer,
                  const std::shared_ptr<MemReadView>& read_view, int64_t committed_offset)
        : partition_(std::move(partition)),
          bucket_(bucket),
          disk_splits_(std::move(disk_splits)),
          indexer_(indexer),
          read_view_(read_view),
          committed_offset_(committed_offset) {}

    const std::map<std::string, std::string>& Partition() const {
        return partition_;
    }

    int32_t Bucket() const {
        return bucket_;
    }

    const std::vector<std::shared_ptr<Split>>& DiskSplits() const {
        return disk_splits_;
    }

    const std::shared_ptr<MemIndexer>& Indexer() const {
        return indexer_;
    }

    const std::shared_ptr<MemReadView>& ReadView() const {
        return read_view_;
    }

    int64_t CommittedOffset() const {
        return committed_offset_;
    }

 private:
    std::map<std::string, std::string> partition_;
    int32_t bucket_;
    std::vector<std::shared_ptr<Split>> disk_splits_;
    std::shared_ptr<MemIndexer> indexer_;
    std::shared_ptr<MemReadView> read_view_;
    int64_t committed_offset_;
};

}  // namespace paimon
