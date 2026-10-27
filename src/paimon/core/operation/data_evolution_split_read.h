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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "paimon/common/data/blob_view_struct.h"
#include "paimon/common/reader/data_evolution_file_reader.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/abstract_split_read.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}

namespace paimon {
class DataFilePathFactory;
class DataSplit;
class DataSplitImpl;
class Executor;
class FileBatchReader;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class Predicate;
class BinaryRow;
struct DeletionFile;

/// If the class name below is enclosed in parentheses, it might be present in the read path;
/// otherwise, it must be present in the read path.
///
/// Readers Overview: (ConcatBatchReader across
/// splits)->(BlobViewResolvingBatchReader)->(CompleteIndexScoreBatchReader)->
/// CompleteRowKindBatchReader->(PredicateBatchReader)
/// ->ConcatBatchReader across files->DataEvolutionFileReader
/// ->(ConcatBatchReader across vector files | BlobFallbackBatchReader across blob sequence layers)
/// ->FieldMappingReader->(ApplyDeletionVectorBatchReader)->(ApplyBitmapIndexBatchReader)
/// ->(CompleteRowTrackingFieldsBatchReader)->(ShreddingFileReader)
/// ->(VectorFileBatchReader)->(DelegatingPrefetchReader)->(PrefetchFileBatchReader)
/// ->(LateMaterializingFileBatchReader)->FormatReader
///
///
/// A union `SplitRead` to read multiple inner files to merge columns. A single-file row range
/// group gets both file-index and format-level predicate pushdown. A merged group of normal
/// files sharing one row-id range shares the intersection of its file-index selections across
/// all child readers to preserve alignment.
///
/// Deletion vectors are supported: a row range group's vector is maintained against the
/// group's anchor file (DataEvolutionUtils::RetrieveAnchorFile), so its positions are
/// anchor-relative. Every reader of the group must drop the same rows to keep the column merge
/// aligned: file readers apply it shifted by the file's offset inside the anchor range, and the
/// blob fallback path drops the deleted row ids from its placeholder gap segments.
class DataEvolutionSplitRead : public AbstractSplitRead {
 public:
    DataEvolutionSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                           const std::shared_ptr<InternalReadContext>& context,
                           const std::shared_ptr<MemoryPool>& memory_pool,
                           const std::shared_ptr<Executor>& executor);

    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;

    Result<bool> Match(const std::shared_ptr<Split>& split, bool force_keep_delete) const override;

    Result<std::unique_ptr<FileBatchReader>> ApplyIndexAndDvReaderIfNeeded(
        std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate, DeletionVector::Factory dv_factory,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const std::map<std::string, std::string>& file_index_options,
        std::vector<float>* index_scores) const override;

 private:
    /// Files for partial field.
    class FieldBunch {
     public:
        virtual ~FieldBunch() = default;
        virtual Result<int64_t> RowCount() const = 0;
        virtual const std::vector<std::shared_ptr<DataFileMeta>>& Files() const = 0;
    };

    class DataBunch : public FieldBunch {
     public:
        explicit DataBunch(const std::shared_ptr<DataFileMeta>& data_file)
            : data_files_({data_file}) {}
        Result<int64_t> RowCount() const override {
            return data_files_[0]->row_count;
        }
        const std::vector<std::shared_ptr<DataFileMeta>>& Files() const override {
            return data_files_;
        }

     private:
        std::vector<std::shared_ptr<DataFileMeta>> data_files_;
    };

    /// Compatible VECTOR files tiling one main file's row range without gaps or overlaps.
    class VectorBunch : public FieldBunch {
     public:
        VectorBunch(std::vector<std::shared_ptr<DataFileMeta>> files, int64_t row_count)
            : files_(std::move(files)), row_count_(row_count) {}
        Result<int64_t> RowCount() const override {
            return row_count_;
        }
        const std::vector<std::shared_ptr<DataFileMeta>>& Files() const override {
            return files_;
        }

     private:
        std::vector<std::shared_ptr<DataFileMeta>> files_;
        int64_t row_count_;
    };

    /// All blob files of one blob field in a merge group. Unlike data files, every file is kept
    /// (aligned with Java's BlobFileBunch): files whose max sequence numbers differ form layers
    /// of a data-evolution partial update, where newer layers record untouched rows as
    /// placeholder entries and reading falls back row by row to older layers.
    class BlobBunch : public FieldBunch {
     public:
        BlobBunch(const std::optional<Range>& expected_row_range, bool has_row_ids_selection)
            : expected_row_range_(expected_row_range),
              has_row_ids_selection_(has_row_ids_selection) {}
        /// The normal file owns the logical rows, including rows with no physical blob value.
        Result<int64_t> RowCount() const override;
        Result<Range> LogicalRange() const;
        const std::vector<std::shared_ptr<DataFileMeta>>& Files() const override {
            return files_;
        }
        Status Add(const std::shared_ptr<DataFileMeta>& file);

     private:
        std::optional<Range> expected_row_range_;
        bool has_row_ids_selection_ = false;
        std::vector<Range> ranges_;
        std::vector<std::shared_ptr<DataFileMeta>> files_;
    };

    /// The deletion vector of one row range group, read from the group's anchor file. Its
    /// positions are relative to `anchor_range`, the anchor file's row id range.
    struct GroupDeletionVector {
        std::shared_ptr<DeletionVector> deletion_vector;
        Range anchor_range;
    };

 private:
    Result<std::unique_ptr<BatchReader>> InnerCreateReader(
        const std::shared_ptr<DataSplit>& data_split,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<arrow::Schema>& read_schema) const;

    /// Keeps top-level conjuncts whose fields all belong to `read_schema`, excluding conjuncts
    /// over system fields. The returned predicate is for pushdown only; the original predicate is
    /// still evaluated as a residual filter when requested by the read context.
    static Result<std::shared_ptr<Predicate>> CreatePushDownPredicate(
        const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<arrow::Schema>& read_schema);

    /// Evaluates the winning files' indexes before opening the group's DV or data files. Shared
    /// bitmap selection is restricted to normal files with the same row-id range; a single-file
    /// group may always use its bitmap. Nullopt selects all rows; an empty vector rejects the
    /// group.
    Result<std::optional<std::vector<Range>>> SelectByFileIndex(
        const std::shared_ptr<Predicate>& predicate,
        const std::vector<std::shared_ptr<DataFileMeta>>& files,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        bool* has_bitmap_selection) const;

    /// Builds the deletion vector factory over the split's deletion files, keyed by data file
    /// name. Only anchor files carry one. Returns a null factory when the split has none.
    DeletionVector::Factory CreateSplitDvFactory(const DataSplitImpl& split_impl) const;

    /// Reads the deletion vector of one row range group from its anchor file. Returns nullopt
    /// when `split_dv_factory` is null, or when the anchor carries no deletion vector or an
    /// empty one.
    static Result<std::optional<GroupDeletionVector>> ReadGroupDeletionVector(
        const std::vector<std::shared_ptr<DataFileMeta>>& group,
        const DeletionVector::Factory& split_dv_factory);

    /// Builds the per-file deletion vector factory of one row range group: each file gets the
    /// group's deletion vector at its own local positions, shifted by the file's offset inside
    /// the anchor range and limited to its row count. Null factory when the group has no vector.
    static Result<DeletionVector::Factory> CreateGroupDvFactory(
        const std::vector<std::shared_ptr<DataFileMeta>>& group,
        const std::optional<GroupDeletionVector>& group_dv,
        const std::optional<std::vector<Range>>& row_ranges);

    /// Removes the row ids deleted by `group_dv` from the sorted disjoint `ranges` (absolute
    /// row ids). Iterates over deletions when they are sparser than the selected rows.
    static Result<std::vector<Range>> ExcludeDeletedRowIds(const std::vector<Range>& ranges,
                                                           const GroupDeletionVector& group_dv);

    struct VectorReadRange {
        Range range;
        std::vector<std::shared_ptr<DataFileMeta>> files;
    };

    /// Split at VECTOR file boundaries so each field has one latest provider per range.
    static Result<std::vector<VectorReadRange>> PlanVectorReadRanges(
        const std::vector<std::shared_ptr<DataFileMeta>>& files,
        const std::vector<int32_t>& read_field_ids,
        const std::function<Result<std::vector<int32_t>>(const std::shared_ptr<DataFileMeta>&)>&
            file_to_field_ids);

    /// Empty means the layout needs range planning. Errors resolving providers still propagate.
    static Result<std::vector<std::shared_ptr<FieldBunch>>> TryPlanSequentialVectorRead(
        const std::vector<std::shared_ptr<DataFileMeta>>& files,
        const std::function<Result<std::vector<int32_t>>(const std::shared_ptr<DataFileMeta>&)>&
            file_to_field_ids);

    static Result<std::vector<std::shared_ptr<DataEvolutionSplitRead::FieldBunch>>>
    SplitFieldBunches(const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
                      const std::function<Result<int32_t>(const std::shared_ptr<DataFileMeta>&)>&
                          blob_field_to_field_id,
                      bool has_row_ranges_selection);
    static Result<std::vector<std::vector<std::shared_ptr<DataFileMeta>>>> MergeRangesAndSort(
        std::vector<std::shared_ptr<DataFileMeta>>&& files);

    static bool HasIndexScoreField(const std::shared_ptr<arrow::Schema>& read_schema);

    static std::vector<std::string> HasBlobViewField(
        const CoreOptions& options, const std::shared_ptr<arrow::Schema>& read_schema);

    static Result<std::unordered_set<BlobViewStruct>> ExtractBlobViewStructs(BatchReader* reader);

    /// Pre-reads the blob view columns of `data_split` to collect the BlobViewStructs to
    /// resolve. It applies the same `row_ranges` selection and the same row range group deletion
    /// vectors as the main read, so a reference held only by a row those two drop is never
    /// resolved.
    ///
    /// It stays wider than the main read: it passes no predicate and reads a group's normal
    /// files one by one instead of merging their columns, so a reference the main read would
    /// filter out or overwrite is still resolved here, and a dangling one still fails the query.
    Result<std::unique_ptr<BatchReader>> CreateBlobViewReader(
        const std::shared_ptr<DataSplit>& data_split,
        const std::vector<std::string>& read_blob_view_fields,
        const std::optional<std::vector<Range>>& row_ranges) const;

    Result<std::unique_ptr<BatchReader>> WrapWithBlobViewResolverIfNeeded(
        const std::shared_ptr<DataSplit>& data_split, std::unique_ptr<BatchReader>&& inner_reader,
        const std::optional<std::vector<Range>>& row_ranges) const;

 private:
    Result<std::unique_ptr<BatchReader>> CreateUnionReader(
        const BinaryRow& partition,
        const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const DeletionVector::Factory& group_dv_factory,
        const std::optional<GroupDeletionVector>& group_dv,
        const std::shared_ptr<arrow::Schema>& read_schema) const;

    Result<std::unique_ptr<BatchReader>> CreateFieldUnionReader(
        const BinaryRow& partition,
        const std::vector<std::shared_ptr<DataFileMeta>>& need_merge_files,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const DeletionVector::Factory& group_dv_factory,
        const std::optional<GroupDeletionVector>& group_dv,
        const std::shared_ptr<arrow::Schema>& read_schema) const;

    Result<std::unique_ptr<BatchReader>> CreateFieldUnionReader(
        const BinaryRow& partition, const std::vector<std::shared_ptr<FieldBunch>>& fields_files,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const DeletionVector::Factory& group_dv_factory,
        const std::optional<GroupDeletionVector>& group_dv,
        const std::shared_ptr<arrow::Schema>& read_schema) const;

    /// Builds the row-level fallback reader for a blob bunch spanning multiple max sequence
    /// number layers: groups the files by max sequence number, pads uncovered row id ranges of
    /// each layer with placeholder gap segments, and resolves each row to the newest
    /// non-placeholder layer. See BlobFallbackBatchReader. `group_dv_factory` wraps the file
    /// readers and `group_dv` drops deleted row ids from the gap segments, so every layer keeps
    /// emitting the same surviving rows.
    Result<std::unique_ptr<BatchReader>> CreateBlobFallbackReader(
        const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files,
        const Range& logical_range, const std::shared_ptr<arrow::Schema>& file_read_schema,
        const std::optional<std::vector<Range>>& row_ranges,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const DeletionVector::Factory& group_dv_factory,
        const std::optional<GroupDeletionVector>& group_dv) const;

    /// Set only on a per-group reader after SelectByFileIndex has evaluated its indexes.
    /// Lazy child readers inherit it so no sidecar is reopened during reader construction.
    bool file_indexes_evaluated_ = false;
};

}  // namespace paimon
