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

#include "paimon/core/append/data_evolution_compact_deletion_vector_rewriter.h"

#include <map>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/range_helper.h"
#include "paimon/core/core_options.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_handler.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/utils/data_evolution_utils.h"
#include "paimon/logging.h"
#include "paimon/utils/range.h"

namespace paimon {

namespace {

/// Bucket every data-evolution compact message writes to. Unaware-bucket tables keep the
/// legacy single bucket, and the rewritten index files have to land in the same one.
constexpr int32_t kUnawareBucket = 0;

/// One deletion-vector index file of a partition, described by metadata alone: which data
/// files it stores a vector for and where in the file each vector sits.
///
/// Nothing is deserialized to build this. A vector is read only when a move actually takes it
/// away, and the vectors that stay are read only for the index files a move touched, so an
/// untouched index file is never opened.
struct IndexFileState {
    std::shared_ptr<IndexFileMeta> meta;
    std::string path;
    /// The vectors this index file still owns, in write order.
    LinkedHashMap<std::string, DeletionVectorMeta> dv_metas;
    bool dirty;
};

/// The recorded position of one vector inside its index file.
DeletionFile ToDeletionFile(const std::string& index_file_path, const DeletionVectorMeta& dv_meta) {
    return DeletionFile(index_file_path, dv_meta.GetOffset(), dv_meta.GetLength(),
                        dv_meta.GetCardinality());
}

/// The files a rewritten deletion vector has to be keyed by after one compact task: the
/// task's inputs, whose per-group anchors hold the deletions today, and the single output
/// file they were merged into.
///
/// A materialized task has no such output. It applied the deletions while rewriting, so its
/// rows carry fresh row ids the commit assigns and the old vectors are dropped rather than
/// moved; `after` is null and `after_range` unused for one of those.
struct CompactedGroup {
    std::vector<std::shared_ptr<DataFileMeta>> before;
    std::shared_ptr<DataFileMeta> after;
    Range after_range;
    bool materialized = false;
};

std::vector<std::shared_ptr<DataFileMeta>> NormalFiles(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    std::vector<std::shared_ptr<DataFileMeta>> result;
    result.reserve(files.size());
    for (const auto& file : files) {
        if (DataEvolutionUtils::IsNormalFile(file->file_name)) {
            result.push_back(file);
        }
    }
    return result;
}

Result<Range> RowRangeOf(const std::shared_ptr<DataFileMeta>& file) {
    PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
    return Range(first_row_id, first_row_id + file->row_count - 1);
}

/// Merges the files of one compact task into row range groups: files covering the same rows
/// belong to the same group and share one deletion vector, keyed by the group's anchor.
Result<std::vector<std::vector<std::shared_ptr<DataFileMeta>>>> RowRangeGroups(
    std::vector<std::shared_ptr<DataFileMeta>>&& files) {
    RangeHelper<std::shared_ptr<DataFileMeta>> helper(
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            return file->NonNullFirstRowId();
        },
        [](const std::shared_ptr<DataFileMeta>& file) -> Result<int64_t> {
            PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
            return first_row_id + file->row_count - 1;
        });
    return helper.MergeOverlappingRanges(std::move(files));
}

/// Collects the compacted groups of each partition, validating the shape data-evolution
/// compaction is expected to produce.
Result<LinkedHashMap<BinaryRow, std::vector<CompactedGroup>>> CollectCompactedGroups(
    const std::vector<std::shared_ptr<CommitMessage>>& compact_messages) {
    LinkedHashMap<BinaryRow, std::vector<CompactedGroup>> result;
    for (const auto& message : compact_messages) {
        auto message_impl = std::dynamic_pointer_cast<CommitMessageImpl>(message);
        if (message_impl == nullptr) {
            return Status::Invalid(
                "Data evolution compaction produced an unexpected commit message type.");
        }
        const CompactIncrement& compact_increment = message_impl->GetCompactIncrement();
        // The rewriter owns every index change of this round; a task that already produced
        // one would be silently dropped by the index-only messages built below.
        if (!compact_increment.NewIndexFiles().empty() ||
            !compact_increment.DeletedIndexFiles().empty()) {
            return Status::Invalid(
                "Data evolution compaction should not produce index changes before the "
                "deletion vector rewrite.");
        }
        if (message_impl->Bucket() != kUnawareBucket ||
            message_impl->TotalBuckets() != std::nullopt) {
            return Status::Invalid(fmt::format(
                "Data evolution compaction should only produce unaware-bucket commit messages, "
                "but got bucket {}.",
                message_impl->Bucket()));
        }

        std::vector<std::shared_ptr<DataFileMeta>> before =
            NormalFiles(compact_increment.CompactBefore());
        std::vector<std::shared_ptr<DataFileMeta>> after =
            NormalFiles(compact_increment.CompactAfter());
        // A blob-only or index-only message carries no rows whose deletions could move.
        if (before.empty() && after.empty()) {
            continue;
        }
        // A materialized task wrote its rows without row ids, so the commit assigns fresh ones
        // and the deletions it applied must not follow them: the old vectors are dropped
        // rather than moved. The global index dropper and the commit's conflict check decide
        // the same shape through the same helper.
        bool materialized = DataEvolutionUtils::IsMaterializedCompaction(
            compact_increment.CompactBefore(), compact_increment.CompactAfter());
        if (materialized) {
            result[message_impl->Partition()].push_back(
                CompactedGroup{std::move(before), /*after=*/nullptr, Range(0, 0),
                               /*materialized=*/true});
            continue;
        }
        if (after.size() != 1) {
            return Status::Invalid(
                "One data evolution compact task should produce exactly one normal file.");
        }
        PAIMON_ASSIGN_OR_RAISE(Range after_range, RowRangeOf(after[0]));
        // A task that keeps its rows in place must cover exactly the rows it replaced, or the
        // deletions would move onto the wrong ones.
        PAIMON_ASSIGN_OR_RAISE(Range before_range,
                               DataEvolutionUtils::CheckContiguousRowRange(before));
        if (!(before_range == after_range)) {
            return Status::Invalid(fmt::format(
                "Data evolution compaction must keep the same row id range, but compacted {} "
                "into {}.",
                before_range.ToString(), after_range.ToString()));
        }
        result[message_impl->Partition()].push_back(
            CompactedGroup{std::move(before), after[0], after_range, /*materialized=*/false});
    }
    return result;
}

/// Moves one row range group's deletion vector from its anchor file onto `merged`, whose
/// positions are relative to `after_range`.
Status MoveDeletionVector(const std::shared_ptr<DeletionVector>& old_vector,
                          const Range& anchor_range, const Range& after_range,
                          const std::shared_ptr<DeletionVector>& merged) {
    if (anchor_range == after_range) {
        // The group already spans the whole compacted file: the vector only changes its key.
        return merged->Merge(old_vector);
    }
    // The compacted file starts at or before the group it absorbed, so a group's positions
    // shift right by the distance between the two starts.
    int64_t shift = anchor_range.from - after_range.from;
    int64_t after_count = after_range.Count();
    return old_vector->ForEachDeletedPosition([&](int64_t position) -> Status {
        int64_t moved = position + shift;
        if (moved < 0 || moved >= after_count) {
            return Status::Invalid(fmt::format(
                "Cannot move deletion position {} of row range {} into row range {}.",
                anchor_range.from + position, anchor_range.ToString(), after_range.ToString()));
        }
        return merged->Delete(moved);
    });
}

}  // namespace

Result<std::vector<std::shared_ptr<CommitMessage>>>
DataEvolutionCompactDeletionVectorRewriter::RewriteDeletionVectors(
    const std::vector<std::shared_ptr<CommitMessage>>& compact_messages, const Snapshot& snapshot,
    const CoreOptions& core_options, const std::shared_ptr<IndexFileHandler>& index_file_handler) {
    // Guarded here as well as at the call site: a table without deletion vectors has nothing
    // to move, and a future caller that forgets to pre-check stays correct.
    if (!core_options.DeletionVectorsEnabled() || compact_messages.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }
    // Declared first: a template type with a comma cannot be spelled inside the macro, which
    // would take it for a second macro argument.
    LinkedHashMap<BinaryRow, std::vector<CompactedGroup>> compacted_groups_by_partition;
    PAIMON_ASSIGN_OR_RAISE(compacted_groups_by_partition, CollectCompactedGroups(compact_messages));
    // A round that replaced no normal file - a blob-only message, say - holds no row range
    // whose deletions could move, so it never reads the index at all.
    if (compacted_groups_by_partition.empty()) {
        return std::vector<std::shared_ptr<CommitMessage>>{};
    }

    // One scan for every partition of the round rather than one per partition: the index
    // manifest is a single file, and scanning it per partition would read it once per
    // partition the round touched.
    std::unordered_set<BinaryRow> partitions;
    for (const auto& [partition, groups] : compacted_groups_by_partition) {
        partitions.insert(partition);
    }
    IndexFileHandler::IndexFileMetaGroups index_metas_by_group;
    PAIMON_ASSIGN_OR_RAISE(
        index_metas_by_group,
        index_file_handler->Scan(snapshot, DeletionVectorsIndexFile::DELETION_VECTORS_INDEX,
                                 partitions));

    auto logger = Logger::GetLogger("DataEvolutionCompactDeletionVectorRewriter");
    std::vector<std::shared_ptr<CommitMessage>> result;
    for (const auto& [partition, groups] : compacted_groups_by_partition) {
        auto index_metas_entry = index_metas_by_group.find({partition, kUnawareBucket});
        // A partition without a deletion-vector index has nothing to move.
        if (index_metas_entry == index_metas_by_group.end() || index_metas_entry->second.empty()) {
            continue;
        }
        std::vector<std::shared_ptr<IndexFileMeta>> index_metas =
            std::move(index_metas_entry->second);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<DeletionVectorsIndexFile> dv_index,
                               index_file_handler->DvIndex(partition, kUnawareBucket));

        // Ownership comes from the index metadata alone: DvRanges already records which data
        // files an index file stores a vector for, and where. No vector is deserialized here.
        std::vector<IndexFileState> states;
        states.reserve(index_metas.size());
        std::map<std::string, size_t> owner_by_data_file;
        for (auto& index_meta : index_metas) {
            std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> dv_metas =
                index_meta->DvRanges();
            if (dv_metas == std::nullopt) {
                return Status::Invalid(
                    fmt::format("Deletion vector index file {} has no deletion vector metas.",
                                index_meta->FileName()));
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::string index_file_path,
                index_file_handler->FilePath(partition, kUnawareBucket, index_meta));
            for (const auto& entry : dv_metas.value()) {
                auto inserted = owner_by_data_file.emplace(entry.first, states.size());
                if (!inserted.second) {
                    return Status::Invalid(
                        fmt::format("Data file {} has a deletion vector in more than one index "
                                    "file of the same partition.",
                                    entry.first));
                }
            }
            states.push_back(IndexFileState{std::move(index_meta), std::move(index_file_path),
                                            std::move(dv_metas).value(), /*dirty=*/false});
        }

        // Vectors of the rewritten files belong to no existing index file yet, so they are
        // collected apart and written as their own index files.
        std::map<std::string, std::shared_ptr<DeletionVector>> moved_vectors;
        int64_t taken_vectors = 0;
        for (const auto& group : groups) {
            // A moved vector has to be built as the kind the table already holds, since the
            // two refuse to merge.
            std::shared_ptr<DeletionVector> merged =
                DeletionVector::Create(core_options.DeletionVectorsBitmap64());
            std::vector<std::shared_ptr<DataFileMeta>> before = group.before;
            PAIMON_ASSIGN_OR_RAISE(
                std::vector<std::vector<std::shared_ptr<DataFileMeta>>> subgroups,
                RowRangeGroups(std::move(before)));
            for (const auto& subgroup : subgroups) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileMeta> anchor,
                                       DataEvolutionUtils::RetrieveAnchorFile(subgroup));
                auto owner = owner_by_data_file.find(anchor->file_name);
                if (owner == owner_by_data_file.end()) {
                    continue;
                }
                IndexFileState& state = states[owner->second];
                auto dv_meta = state.dv_metas.find(anchor->file_name);
                if (dv_meta == state.dv_metas.end()) {
                    continue;
                }
                if (group.materialized) {
                    // The rewrite already dropped these rows, so the vector goes away with the
                    // file it belonged to instead of being read and moved.
                    state.dv_metas.erase(anchor->file_name);
                    state.dirty = true;
                    taken_vectors++;
                    continue;
                }
                // Only the anchor's vector is read, by its recorded position.
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<DeletionVector> old_vector,
                    dv_index->ReadDeletionVector(ToDeletionFile(state.path, dv_meta->second)));
                // Taken away before it is looked at, and unconditionally: the anchor is a file
                // this commit deletes, so an entry left behind would key a vector by a data
                // file that no longer exists — exactly the shape
                // `ConflictDetection::CheckDeletionVectorMigrationIsComplete` refuses.
                state.dv_metas.erase(anchor->file_name);
                state.dirty = true;
                taken_vectors++;
                if (old_vector->IsEmpty()) {
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(Range anchor_range, RowRangeOf(anchor));
                PAIMON_RETURN_NOT_OK(
                    MoveDeletionVector(old_vector, anchor_range, group.after_range, merged));
            }
            if (!group.materialized && !merged->IsEmpty()) {
                moved_vectors[group.after->file_name] = merged;
            }
        }

        // Only an index file a move touched is replaced. Its remaining vectors are read now,
        // the ones it lost are already gone, and an index file left with nothing is deleted
        // outright.
        int64_t target_size = core_options.DeletionVectorTargetFileSize();
        std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
        std::vector<std::shared_ptr<IndexFileMeta>> deleted_index_files;
        for (auto& state : states) {
            if (!state.dirty) {
                continue;
            }
            deleted_index_files.push_back(std::move(state.meta));
            if (state.dv_metas.empty()) {
                continue;
            }
            // Read through one opened stream: an index file may hold many vectors of which a
            // move took only one, and reading the rest one by one would cost one open each.
            std::map<std::string, DeletionFile> unchanged_deletion_files;
            for (const auto& entry : state.dv_metas) {
                unchanged_deletion_files.emplace(entry.first,
                                                 ToDeletionFile(state.path, entry.second));
            }
            std::map<std::string, std::shared_ptr<DeletionVector>> unchanged_vectors;
            PAIMON_ASSIGN_OR_RAISE(unchanged_vectors,
                                   dv_index->ReadDeletionVectors(unchanged_deletion_files));
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> rewritten,
                                   dv_index->WriteWithRolling(unchanged_vectors, target_size));
            for (auto& rewritten_file : rewritten) {
                new_index_files.push_back(std::move(rewritten_file));
            }
        }
        if (!moved_vectors.empty()) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> moved_index_files,
                                   dv_index->WriteWithRolling(moved_vectors, target_size));
            for (auto& moved_index_file : moved_index_files) {
                new_index_files.push_back(std::move(moved_index_file));
            }
        }
        if (new_index_files.empty() && deleted_index_files.empty()) {
            continue;
        }
        // What the round actually moved in this partition, which is the first thing to look at
        // when a migration check rejects the commit or a deleted row reappears.
        PAIMON_LOG_DEBUG(logger,
                         "Migrated deletion vectors of partition %s: %ld vector(s) taken from "
                         "their replaced files, %zu re-keyed onto rewritten files, %zu index "
                         "file(s) replaced by %zu new one(s).",
                         partition.ToString().c_str(), taken_vectors, moved_vectors.size(),
                         deleted_index_files.size(), new_index_files.size());

        CompactIncrement compact_increment(/*compact_before=*/{}, /*compact_after=*/{},
                                           /*changelog_files=*/{}, std::move(new_index_files),
                                           std::move(deleted_index_files));
        DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{},
                                     /*changelog_files=*/{});
        result.push_back(std::make_shared<CommitMessageImpl>(partition, kUnawareBucket,
                                                             /*total_buckets=*/std::nullopt,
                                                             data_increment, compact_increment));
    }
    return result;
}

}  // namespace paimon
