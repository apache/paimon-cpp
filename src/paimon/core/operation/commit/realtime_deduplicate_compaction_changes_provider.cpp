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

#include "paimon/core/operation/commit/realtime_deduplicate_compaction_changes_provider.h"

#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <unordered_map>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/read_context.h"
#include "paimon/utils/range.h"

namespace paimon {
namespace {

Status CollectDeletedOffsets(const ManifestEntry& source, const std::shared_ptr<DeletionVector>& dv,
                             const std::shared_ptr<arrow::Schema>& offset_schema,
                             const std::shared_ptr<MemoryPool>& pool, RawFileSplitRead* read,
                             RoaringBitmap64* deleted_offsets) {
    std::vector<Range> ranges;
    bool invalid_position = false;
    PAIMON_RETURN_NOT_OK(dv->ForEachDeletedPosition([&](int64_t position) {
        if (position < 0 || position >= source.File()->row_count ||
            position >= std::numeric_limits<int32_t>::max()) {
            invalid_position = true;
        } else if (!ranges.empty() && position == ranges.back().to + 1) {
            ranges.back().to = position;
        } else {
            ranges.emplace_back(position, position);
        }
    }));
    if (invalid_position) {
        return Status::Invalid("compaction input DV contains an invalid row position");
    }
    if (ranges.empty()) {
        return Status::OK();
    }
    ranges = Range::SortAndMergeOverlap(ranges, /*adjacent=*/true);
    int64_t expected_rows = 0;
    for (const Range& range : ranges) {
        expected_rows += range.Count();
    }
    // Read deleted physical rows to recover their stable offsets. Applying the input DV here
    // would hide precisely the rows whose deletions need to be transferred to the output.
    DeletionVector::Factory no_deletions =
        [](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
        return std::shared_ptr<DeletionVector>();
    };
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                           read->CreateReader(source.Partition(), source.Bucket(), {source.File()},
                                              no_deletions, ranges, offset_schema,
                                              /*predicate=*/nullptr));
    ScopeGuard reader_guard([&]() { reader->Close(); });
    const std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool);
    int64_t emitted_rows = 0;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                               reader->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch_with_bitmap)) {
            break;
        }
        auto& [batch, selected_rows] = batch_with_bitmap;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(batch.first.get(), batch.second.get()));
        if (array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("compaction offset reader returned a non-struct array");
        }
        std::shared_ptr<arrow::Array> raw_offsets =
            checked_pointer_cast<arrow::StructArray>(array)->GetFieldByName(
                SpecialFields::RealtimeOffset().Name());
        if (raw_offsets && raw_offsets->type_id() == arrow::Type::DICTIONARY) {
            PAIMON_ASSIGN_OR_RAISE(
                raw_offsets,
                CastingUtils::Cast(raw_offsets, arrow::int64(), arrow::compute::CastOptions::Safe(),
                                   arrow_pool.get()));
        }
        if (!raw_offsets || raw_offsets->type_id() != arrow::Type::INT64) {
            return Status::Invalid("compaction input has no valid realtime offsets");
        }
        const auto offsets = checked_pointer_cast<arrow::Int64Array>(raw_offsets);
        // The raw batch may contain unselected rows. Only selected offsets participate in
        // migration and row-count validation; do not materialize a filtered Arrow array.
        emitted_rows += selected_rows.Cardinality();
        for (auto selected = selected_rows.Begin(); selected != selected_rows.End(); ++selected) {
            const int64_t row = *selected;
            if (row >= offsets->length()) {
                return Status::Invalid("compaction selected row is out of bounds");
            }
            if (offsets->IsNull(row)) {
                return Status::Invalid("compaction input contains a null realtime offset");
            }
            if (offsets->Value(row) < 0) {
                return Status::Invalid("compaction input contains a negative realtime offset");
            }
            deleted_offsets->Add(offsets->Value(row));
        }
    }
    if (emitted_rows != expected_rows) {
        return Status::Invalid("compaction offset reader did not return all deleted rows");
    }
    return Status::OK();
}

}  // namespace

Result<RealtimeCompactionPlan> RealtimeCompactionPlan::Create(
    const std::vector<ManifestEntry>& entries) {
    std::unordered_map<FileEntry::Identifier, const ManifestEntry*> inputs;
    std::unordered_map<FileEntry::Identifier, const ManifestEntry*> outputs;
    for (const ManifestEntry& entry : entries) {
        if (!entry.File() || entry.File()->row_count < 0) {
            return Status::Invalid("invalid realtime compaction file");
        }
        if (!(entry.Kind() == FileKind::Add()) && !(entry.Kind() == FileKind::Delete())) {
            return Status::Invalid("invalid realtime compaction entry kind");
        }
        auto& files = entry.Kind() == FileKind::Add() ? outputs : inputs;
        if (!files.emplace(entry.CreateIdentifier(), &entry).second) {
            return Status::Invalid("duplicate realtime compaction input or output: ",
                                   entry.FileName());
        }
    }
    // For F1/F2 -> F3 -> F4, delta_files contains DELETE F1/F2 and ADD F4.
    // intermediate_files contains ADD F3, whose matching DELETE cancels it from the final changes.
    RealtimeCompactionPlan plan;
    for (const ManifestEntry& entry : entries) {
        const auto id = entry.CreateIdentifier();
        if (entry.Kind() == FileKind::Delete()) {
            if (outputs.count(id) == 0) {
                plan.delta_files.push_back(entry);
            } else {
                const auto& output = outputs.at(id)->File();
                if (entry.File()->row_count != output->row_count ||
                    entry.File()->schema_id != output->schema_id ||
                    entry.File()->file_size != output->file_size) {
                    return Status::Invalid("inconsistent intermediate compaction file: ",
                                           entry.FileName());
                }
            }
        } else if (inputs.count(id) == 0) {
            plan.delta_files.push_back(entry);
        } else {
            plan.intermediate_files.push_back(entry);
        }
    }
    return plan;
}

RealtimeDeduplicateCompactionChangesProvider::RealtimeDeduplicateCompactionChangesProvider(
    RealtimeCompactionPlan plan, std::vector<ManifestEntry> changelog_files,
    std::vector<IndexManifestEntry> index_entries, std::shared_ptr<TableSchema> table_schema,
    std::shared_ptr<FileStorePathFactory> path_factory, std::shared_ptr<CommitScanner> scanner,
    const CoreOptions& options, std::shared_ptr<Executor> executor,
    std::shared_ptr<MemoryPool> pool)
    : plan_(std::move(plan)),
      changelog_files_(std::move(changelog_files)),
      index_entries_(std::move(index_entries)),
      table_schema_(std::move(table_schema)),
      path_factory_(std::move(path_factory)),
      options_(options),
      executor_(std::move(executor)),
      pool_(std::move(pool)),
      materializer_(table_schema_, path_factory_, std::move(scanner), options_, pool_) {}

Result<std::shared_ptr<CommitChanges>> RealtimeDeduplicateCompactionChangesProvider::Provide(
    const std::optional<Snapshot>& latest_snapshot) const {
    // Example: F1 offsets [10, 11, 12], DV {0} -> F2 offsets [11, 12].
    // A later deletion of offset 11 makes F1's latest DV {0, 1}; publish F2's DV {0},
    // retire F1's DV, and preserve DVs of other active files in the bucket.
    // TODO(xinyu.lxy): Consider generating candidate DVs during compaction and carrying their
    // bucket DV baseline in the commit message. Reuse candidates when the inputs remain active
    // and the latest bucket baseline matches; otherwise rebase against the latest DVs here.
    // Check the baseline on the first attempt too, not only after a snapshot CAS conflict.
    if (!latest_snapshot) {
        return Status::Invalid("realtime compaction requires a committed input snapshot");
    }
    auto result =
        std::make_shared<CommitChanges>(plan_.delta_files, changelog_files_, index_entries_);
    for (const IndexManifestEntry& entry : index_entries_) {
        if (entry.index_file->IndexType() == DeletionVectorsIndexFile::DELETION_VECTORS_INDEX) {
            return Status::Invalid("realtime compaction must generate deletion vectors at commit");
        }
    }
    ScopeGuard cleanup([&]() { materializer_.CleanUpUnpublishedFiles(*result); });
    // 1. Group the normalized compaction changes by partition and bucket.
    using BucketFiles = std::map<int32_t, std::vector<ManifestEntry>>;
    std::unordered_map<BinaryRow, BucketFiles> buckets;
    std::vector<BinaryRow> affected_partitions;
    for (const ManifestEntry& entry : plan_.delta_files) {
        buckets[entry.Partition()][entry.Bucket()].push_back(entry);
        affected_partitions.push_back(entry.Partition());
    }
    for (const ManifestEntry& entry : plan_.intermediate_files) {
        affected_partitions.push_back(entry.Partition());
    }
    // 2. Load the latest snapshot and verify intermediate outputs are not already committed.
    PAIMON_ASSIGN_OR_RAISE(RealtimeDeduplicateDvMaterializer::SnapshotBuckets snapshot_buckets,
                           materializer_.LoadSnapshot(latest_snapshot, affected_partitions));
    for (const ManifestEntry& entry : plan_.intermediate_files) {
        const auto id = entry.CreateIdentifier();
        for (const ManifestEntry& active :
             snapshot_buckets[entry.Partition()][entry.Bucket()].files) {
            if (active.CreateIdentifier() == id) {
                return Status::Invalid("intermediate realtime compaction file is already active: ",
                                       entry.FileName());
            }
        }
    }
    PAIMON_ASSIGN_OR_RAISE(result->files_to_delete_after_commit,
                           CollectIntermediateFilesToDelete(snapshot_buckets));
    ReadContextBuilder builder(path_factory_->RootPath());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context,
                           builder.SetOptions(options_.ToMap())
                               .WithFileSystem(options_.GetFileSystem())
                               .WithMemoryPool(pool_)
                               .Finish());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> context,
        InternalReadContext::Create(read_context, table_schema_, options_.ToMap()));
    RawFileSplitRead read(path_factory_, context, pool_, executor_);
    const auto schema = DataField::ConvertDataFieldsToArrowSchema(table_schema_->Fields());
    const auto offset_schema =
        arrow::schema({schema->GetFieldByName(SpecialFields::RealtimeOffset().Name())});
    // 3. Remap latest deletions independently for each partition and bucket.
    for (const auto& [partition, bucket_files] : buckets) {
        for (const auto& [bucket, files] : bucket_files) {
            PAIMON_RETURN_NOT_OK(MaterializeBucketDeletions(partition, bucket, files,
                                                            snapshot_buckets[partition][bucket],
                                                            offset_schema, &read, result.get()));
        }
    }
    cleanup.Release();
    return result;
}

Result<std::vector<std::string>>
RealtimeDeduplicateCompactionChangesProvider::CollectIntermediateFilesToDelete(
    const RealtimeDeduplicateDvMaterializer::SnapshotBuckets& snapshot_buckets) const {
    std::set<std::string> paths_to_delete;
    const auto collect_files =
        [this](const ManifestEntry& entry) -> Result<std::vector<std::string>> {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFilePathFactory> paths,
            path_factory_->CreateDataFilePathFactory(entry.Partition(), entry.Bucket()));
        return paths->CollectFiles(entry.File());
    };
    for (const ManifestEntry& entry : plan_.intermediate_files) {
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> paths, collect_files(entry));
        paths_to_delete.insert(paths.begin(), paths.end());
    }
    if (paths_to_delete.empty()) {
        return std::vector<std::string>();
    }
    // Keep paths referenced by current files or this commit, even if metadata identities differ.
    // This also protects original inputs retained by older snapshots and shared sidecars.
    const auto preserve_files = [&](const std::vector<ManifestEntry>& entries) -> Status {
        for (const ManifestEntry& entry : entries) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> paths, collect_files(entry));
            for (const std::string& path : paths) {
                paths_to_delete.erase(path);
            }
        }
        return Status::OK();
    };
    PAIMON_RETURN_NOT_OK(preserve_files(plan_.delta_files));
    PAIMON_RETURN_NOT_OK(preserve_files(changelog_files_));
    for (const auto& [partition, buckets] : snapshot_buckets) {
        for (const auto& [bucket, snapshot] : buckets) {
            PAIMON_RETURN_NOT_OK(preserve_files(snapshot.files));
        }
    }
    return std::vector<std::string>(paths_to_delete.begin(), paths_to_delete.end());
}

Status RealtimeDeduplicateCompactionChangesProvider::MaterializeBucketDeletions(
    const BinaryRow& partition, int32_t bucket, const std::vector<ManifestEntry>& files,
    const RealtimeDeduplicateDvMaterializer::BucketSnapshot& snapshot,
    const std::shared_ptr<arrow::Schema>& offset_schema, RawFileSplitRead* read,
    CommitChanges* changes) const {
    std::vector<ManifestEntry> inputs;
    std::unordered_map<FileEntry::Identifier, ManifestEntry> active_files;
    RealtimeDeduplicateDvMaterializer::BucketUpdate update;
    for (const ManifestEntry& entry : snapshot.files) {
        active_files.emplace(entry.CreateIdentifier(), entry);
        update.final_file_names.insert(entry.FileName());
    }
    // Validate inputs and build the final file set; only final outputs need lookup.
    for (const ManifestEntry& entry : files) {
        if (entry.Kind() == FileKind::Delete()) {
            const auto iter = active_files.find(entry.CreateIdentifier());
            if (iter == active_files.end()) {
                return Status::Invalid("realtime compaction input is no longer active: ",
                                       entry.FileName());
            }
            // DELETE entries may omit stats; use the snapshot's complete metadata.
            inputs.push_back(iter->second);
            update.final_file_names.erase(entry.FileName());
        } else {
            if (active_files.count(entry.CreateIdentifier()) != 0) {
                return Status::Invalid("realtime compaction output is already active: ",
                                       entry.FileName());
            }
            update.lookup_files.push_back(entry.File());
            update.final_file_names.insert(entry.FileName());
        }
    }
    // TODO(lxy): Align FileEntry::Identifier with Java by including extra_files and
    // embedded_index to support file-index rebuild commits through normal DELETE/ADD
    // entries. For unchanged physical files and row positions, preserve the latest DV
    // and skip redundant offset reads, lookups, and DV writes as a performance
    // optimization.
    // Load latest bucket DVs and collect deleted offsets from the original inputs.
    std::map<std::string, std::shared_ptr<DeletionVector>> baseline;
    PAIMON_ASSIGN_OR_RAISE(baseline,
                           materializer_.ReadDeletionVectors(partition, bucket, snapshot));
    if (!update.lookup_files.empty()) {
        // Collect from the complete baseline before the common materializer retires
        // input DVs. All inputs contribute to one bucket-level offset lookup.
        for (const ManifestEntry& input : inputs) {
            const auto iter = baseline.find(input.FileName());
            if (iter != baseline.end()) {
                PAIMON_RETURN_NOT_OK(CollectDeletedOffsets(input, iter->second, offset_schema,
                                                           pool_, read, &update.deleted_offsets));
            }
        }
    }
    // Map the aggregated deleted offsets to physical positions in the final outputs.
    PAIMON_ASSIGN_OR_RAISE(RealtimeOffsetFileIndexLookup::FilePositions positions,
                           materializer_.LookupFilePositions(partition, bucket, update));
    // Retire input DVs and merge output deletions, preserving other active files' DVs.
    // Run even with no output hits, e.g. only offset 10 was deleted before the rewrite.
    return materializer_.MaterializeBucket(partition, bucket, snapshot, std::move(baseline), update,
                                           positions, changes);
}

}  // namespace paimon
