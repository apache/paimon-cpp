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

#include "paimon/core/append/data_evolution_normal_compact_task.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/shredding/shredding_write_plan_factories.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/long_counter.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/append_data_file_writer_factory.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/io/shredding_append_data_file_writer_factory.h"
#include "paimon/core/operation/data_evolution_split_read.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/data_evolution_utils.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"

namespace paimon {

DataEvolutionNormalCompactTask::DataEvolutionNormalCompactTask(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const Range& row_range)
    : partition_(partition), compact_before_(files), row_range_(row_range) {}

Result<DataEvolutionNormalCompactTask> DataEvolutionNormalCompactTask::Create(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    PAIMON_ASSIGN_OR_RAISE(Range row_range, DataEvolutionUtils::CheckContiguousRowRange(files));
    return DataEvolutionNormalCompactTask(partition, files, row_range);
}

Result<std::shared_ptr<CommitMessage>> DataEvolutionNormalCompactTask::DoCompact(
    const DataEvolutionCompactContext& context) {
    if (compact_before_.empty()) {
        return Status::Invalid("DataEvolutionNormalCompactTask needs at least one file input.");
    }
    // A task rewrites its input once. Running it again would append a second output to
    // `compact_after_` and commit two files for the same rows, so a repeat is refused rather
    // than silently duplicating the group.
    if (!compact_after_.empty()) {
        return Status::Invalid(
            "DataEvolutionNormalCompactTask has already been compacted and cannot run twice.");
    }

    // The read/write schema drops blob fields stored in dedicated .blob files: they are not
    // rewritten by data-evolution compaction. Inline blob fields (descriptor / view) live in
    // the normal data files and stay in the schema.
    std::vector<std::string> inline_field_names = context.core_options.GetBlobInlineFields();
    std::set<std::string> inline_fields(inline_field_names.begin(), inline_field_names.end());
    std::vector<std::string> dedicated_field_names =
        BlobUtils::ManagedBlobFieldNames(context.arrow_schema, inline_fields);
    std::set<std::string> dedicated_fields(dedicated_field_names.begin(),
                                           dedicated_field_names.end());
    std::vector<std::string> read_write_field_names;
    arrow::FieldVector write_fields;
    for (const auto& field : context.arrow_schema->fields()) {
        if (dedicated_fields.count(field->name()) != 0) {
            continue;
        }
        read_write_field_names.push_back(field->name());
        write_fields.push_back(field);
    }
    if (write_fields.empty()) {
        return Status::Invalid(
            "Data evolution compaction requires at least one non-blob column to rewrite.");
    }
    auto write_schema = arrow::schema(write_fields);

    // A file holding every table column stores no write cols; a partial-column file names the
    // columns it holds.
    std::optional<std::vector<std::string>> write_cols = read_write_field_names;
    if (context.arrow_schema->Equals(*write_schema)) {
        write_cols = std::nullopt;
    }

    // Deletion files are intentionally not attached to the read, so the output file keeps
    // every input row and the deletions stay logical. They are moved onto the output file by
    // DataEvolutionCompactDeletionVectorRewriter, which runs over the whole round's commit
    // messages before anything is committed.
    ReadContextBuilder context_builder(context.table_path);
    context_builder.SetOptions(context.core_options.ToMap())
        .WithFileSystem(context.core_options.GetFileSystem())
        .SetReadFieldNames(read_write_field_names)
        .EnablePrefetch(true)
        .SetPrefetchMaxParallelNum(1)
        .SetPrefetchBatchCount(3)
        .WithMemoryPool(context.pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    // TODO(xinyu.lxy): temporarily disabled pre-buffer for parquet, which may cause high
    // memory usage during compaction. Will fix via parquet format refactor.
    std::map<std::string, std::string> read_options = context.core_options.ToMap();
    if (read_options.find("parquet.read.enable-pre-buffer") == read_options.end()) {
        read_options["parquet.read.enable-pre-buffer"] = "false";
    }
    // Blob-view columns must be rewritten as their serialized BlobViewStruct bytes: resolving
    // them here would bake descriptors into the blob-view column and break later reads.
    read_options[Options::BLOB_VIEW_RESOLVE_ENABLED] = "false";
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> internal_read_context,
        InternalReadContext::Create(read_context, context.table_schema, read_options));
    auto split_read = std::make_unique<DataEvolutionSplitRead>(
        context.path_factory, internal_read_context, context.pool, context.executor);

    PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                           context.path_factory->BucketPath(partition_, /*bucket=*/0));
    auto data_files = compact_before_;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataSplitImpl> split,
        DataSplitImpl::Builder(partition_, /*bucket=*/0, bucket_path, std::move(data_files))
            .RawConvertible(false)
            .Build());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, split_read->CreateReader(split));
    ScopeGuard reader_guard([&]() {
        if (reader) {
            reader->Close();
        }
    });

    // A single output file: rolling is disabled so the rewritten rows keep exactly the input
    // group's row range. The sequence counter is irrelevant, the real sequence numbers are
    // assigned on the result below.
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataFilePathFactory> data_file_path_factory,
        context.path_factory->CreateDataFilePathFactory(partition_, /*bucket=*/0));
    auto seq_num_counter = std::make_shared<LongCounter>(0);
    std::shared_ptr<SingleFileWriterFactory<::ArrowArray*, std::shared_ptr<DataFileMeta>>>
        writer_factory;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ShreddingWritePlanFactory> plan_factory,
                           ShreddingWritePlanFactories::SelectActive(context.core_options,
                                                                     write_schema, context.pool));
    if (plan_factory != nullptr) {
        writer_factory = std::make_shared<ShreddingAppendDataFileWriterFactory>(
            context.core_options, context.table_schema->Id(), write_schema, write_cols,
            seq_num_counter, FileSource::Compact(), data_file_path_factory, plan_factory,
            context.pool);
    } else {
        writer_factory = std::make_shared<AppendDataFileWriterFactory>(
            context.core_options, context.table_schema->Id(), write_schema, write_cols,
            seq_num_counter, FileSource::Compact(), data_file_path_factory, context.pool);
    }
    auto rewriter =
        std::make_unique<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
            /*target_file_size=*/std::numeric_limits<int64_t>::max(),
            /*target_file_row_num=*/std::numeric_limits<int64_t>::max(), writer_factory);
    // Abort on any failure up to and including result validation: a half-written or
    // unvalidated output must be deleted, never committed. RollingFileWriter::Abort also
    // removes files already sealed by Close().
    ScopeGuard rewriter_guard([&]() {
        if (rewriter) {
            rewriter->Abort();
        }
    });

    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        if (!struct_array) {
            return Status::Invalid(
                "cannot cast array to StructArray in DataEvolutionNormalCompactTask");
        }
        PAIMON_ASSIGN_OR_RAISE(struct_array, ArrowUtils::RemoveFieldFromStructArray(
                                                 struct_array, SpecialFields::ValueKind().Name()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*struct_array, c_array.get(), c_schema.get()));
        ArrowSchemaRelease(c_schema.get());
        ScopeGuard guard([array = c_array.get()]() { ArrowArrayRelease(array); });
        PAIMON_RETURN_NOT_OK(rewriter->Write(c_array.get()));
        guard.Release();
    }
    PAIMON_RETURN_NOT_OK(rewriter->Close());
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<DataFileMeta>> write_result,
                           rewriter->GetResult());
    if (write_result.size() != 1) {
        return Status::Invalid(
            fmt::format("Data evolution compaction should produce exactly one file, but got {}.",
                        write_result.size()));
    }
    rewriter_guard.Release();

    // Keep the row ids and the merged [min, max] sequence number range of the input group:
    // _ROW_ID values stay unchanged and the rewritten file keeps its position in the "newest
    // field group wins" ordering, while per-row _SEQUENCE_NUMBER values, derived from the
    // file-level maximum, may rise to the group's maximum.
    int64_t min_sequence_number = std::numeric_limits<int64_t>::max();
    int64_t max_sequence_number = std::numeric_limits<int64_t>::min();
    for (const auto& file : compact_before_) {
        min_sequence_number = std::min(min_sequence_number, file->min_sequence_number);
        max_sequence_number = std::max(max_sequence_number, file->max_sequence_number);
    }
    const std::shared_ptr<DataFileMeta>& compacted_file = write_result[0];
    compacted_file->AssignFirstRowId(row_range_.from);
    compacted_file->AssignSequenceNumber(min_sequence_number, max_sequence_number);
    compact_after_.push_back(compacted_file);

    auto compact_before_copy = compact_before_;
    auto compact_after_copy = compact_after_;
    CompactIncrement compact_increment(std::move(compact_before_copy),
                                       std::move(compact_after_copy),
                                       /*changelog_files=*/{},
                                       /*new_index_files=*/{},
                                       /*deleted_index_files=*/{});
    DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{});

    // Bucket 0 is the bucket for unaware-bucket table, for compatibility with the old design.
    // Total buckets stay unset for a data-evolution compact message; the deletion vector
    // rewriter rejects one that carries them.
    auto commit_message = std::make_shared<CommitMessageImpl>(
        partition_,
        /*bucket=*/0, /*total_buckets=*/std::nullopt, data_increment, compact_increment);
    return commit_message;
}

std::string DataEvolutionNormalCompactTask::ToString() const {
    std::vector<std::string> before_names;
    before_names.reserve(compact_before_.size());
    for (const auto& file : compact_before_) {
        before_names.emplace_back(file->file_name);
    }

    std::vector<std::string> after_names;
    after_names.reserve(compact_after_.size());
    for (const auto& file : compact_after_) {
        after_names.emplace_back(file->file_name);
    }

    return fmt::format(
        "DataEvolutionNormalCompactTask {{partition = {}, rowRange = {}, compactBefore = [{}], "
        "compactAfter = [{}]}}",
        partition_.ToString(), row_range_.ToString(), fmt::join(before_names, ", "),
        fmt::join(after_names, ", "));
}

}  // namespace paimon
