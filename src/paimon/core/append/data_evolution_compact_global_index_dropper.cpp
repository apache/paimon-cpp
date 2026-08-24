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

#include "paimon/core/append/data_evolution_compact_global_index_dropper.h"

#include <unordered_set>
#include <utility>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/index_manifest_entry.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/data_evolution_utils.h"

namespace paimon {

namespace {

/// The partitions whose row ids this round reassigned. An empty output counts too: a range
/// whose rows were all deleted leaves those row ids gone for good, so an index over them is
/// just as invalid as one over renumbered rows.
Result<std::unordered_set<BinaryRow>> MaterializedPartitions(
    const std::vector<std::shared_ptr<CommitMessage>>& compact_messages) {
    std::unordered_set<BinaryRow> result;
    for (const auto& message : compact_messages) {
        auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(message);
        if (message_impl == nullptr) {
            return Status::Invalid(
                "Data evolution compaction produced an unexpected commit message type.");
        }
        const CompactIncrement& compact_increment = message_impl->GetCompactIncrement();
        if (DataEvolutionUtils::IsMaterializedCompaction(compact_increment.CompactBefore(),
                                                         compact_increment.CompactAfter())) {
            result.insert(message_impl->Partition());
        }
    }
    return result;
}

}  // namespace

Result<std::vector<std::shared_ptr<CommitMessage>>>
DataEvolutionCompactGlobalIndexDropper::DropGlobalIndexes(
    const std::vector<std::shared_ptr<CommitMessage>>& compact_messages,
    const Snapshot& latest_snapshot, const std::shared_ptr<IndexFileHandler>& index_file_handler) {
    PAIMON_ASSIGN_OR_RAISE(std::unordered_set<BinaryRow> partitions,
                           MaterializedPartitions(compact_messages));
    if (partitions.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }

    // Scanned at the latest snapshot rather than the compaction's base, so an index another
    // engine committed in between — still built on the pre-materialization row ids — is dropped
    // too instead of surviving as a silently wrong index.
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<IndexManifestEntry> entries,
        index_file_handler->Scan(latest_snapshot,
                                 [&partitions](const IndexManifestEntry& entry) -> Result<bool> {
                                     return partitions.count(entry.partition) != 0 &&
                                            entry.index_file->GetGlobalIndexMeta() != std::nullopt;
                                 }));
    if (entries.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }

    // One message per partition and bucket, since an index file is addressed through both.
    LinkedHashMap<BinaryRow, LinkedHashMap<int32_t, std::vector<std::shared_ptr<IndexFileMeta>>>>
        deleted_by_group;
    for (const IndexManifestEntry& entry : entries) {
        deleted_by_group[entry.partition][entry.bucket].push_back(entry.index_file);
    }

    std::vector<std::shared_ptr<CommitMessage>> result;
    for (const auto& [partition, by_bucket] : deleted_by_group) {
        for (const auto& [bucket, deleted_index_files] : by_bucket) {
            std::vector<std::shared_ptr<IndexFileMeta>> deleted = deleted_index_files;
            CompactIncrement compact_increment(/*compact_before=*/{}, /*compact_after=*/{},
                                               /*changelog_files=*/{}, /*new_index_files=*/{},
                                               std::move(deleted));
            DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{},
                                         /*changelog_files=*/{});
            result.push_back(std::make_shared<CommitMessageImpl>(
                partition, bucket, /*total_buckets=*/std::nullopt, data_increment,
                compact_increment));
        }
    }
    return result;
}

}  // namespace paimon
