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

#include "paimon/core/operation/commit/materialized_index_changes_provider.h"

#include <memory>
#include <vector>

#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/index_manifest_entry.h"

namespace paimon {

Result<std::shared_ptr<CommitChanges>> MaterializedIndexChangesProvider::Provide(
    const std::optional<Snapshot>& latest_snapshot) const {
    std::vector<IndexManifestEntry> index_entries;
    index_entries.reserve(index_entries_.size());
    for (const IndexManifestEntry& entry : index_entries_) {
        // Global index deletions are re-derived below against the snapshot this attempt commits
        // on top of; the prepared ones were decided against an older one. Everything else this
        // commit carries - the rewritten deletion vector index files, above all - stands.
        bool prepared_global_index_deletion =
            entry.kind == FileKind::Delete() &&
            entry.index_file->GetGlobalIndexMeta() != std::nullopt &&
            materialized_buckets_.count(MaterializedBucket{entry.partition, entry.bucket}) != 0;
        if (!prepared_global_index_deletion) {
            index_entries.push_back(entry);
        }
    }

    if (latest_snapshot.has_value() && index_scan_) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<IndexManifestEntry> live_entries,
                               index_scan_(latest_snapshot.value()));
        for (const IndexManifestEntry& entry : live_entries) {
            if (entry.index_file->GetGlobalIndexMeta() == std::nullopt ||
                materialized_buckets_.count(MaterializedBucket{entry.partition, entry.bucket}) ==
                    0) {
                continue;
            }
            index_entries.emplace_back(FileKind::Delete(), entry.partition, entry.bucket,
                                       entry.index_file);
        }
    }

    return std::make_shared<CommitChanges>(delta_files_, changelog_files_,
                                           std::move(index_entries));
}

}  // namespace paimon
