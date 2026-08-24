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

#include "paimon/core/append/data_evolution_materialize_deletion_compact_task.h"

#include <algorithm>
#include <map>
#include <string>
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
#include "paimon/common/utils/vector_store_utils.h"
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

DataEvolutionMaterializeDeletionCompactTask::DataEvolutionMaterializeDeletionCompactTask(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const std::vector<std::optional<DeletionFile>>& deletion_files, const Range& row_range)
    : partition_(partition),
      compact_before_(files),
      deletion_files_(deletion_files),
      row_range_(row_range) {}

Result<DataEvolutionMaterializeDeletionCompactTask>
DataEvolutionMaterializeDeletionCompactTask::Create(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& files,
    const std::vector<std::optional<DeletionFile>>& deletion_files) {
    if (files.size() != deletion_files.size()) {
        return Status::Invalid(fmt::format(
            "A materialize deletion task needs one deletion file slot per data file, but got {} "
            "files and {} slots.",
            files.size(), deletion_files.size()));
    }
    // Rewriting only the normal files of a range would leave the dedicated ones indexed by row
    // ids that no longer exist, so a range covering one is refused rather than half rewritten.
    for (const auto& file : files) {
        if (BlobUtils::IsBlobFile(file->file_name)) {
            return Status::NotImplemented(fmt::format(
                "Materializing deletion vectors is not supported for a row range covered by the "
                "blob file {}: Paimon C++ does not rewrite blob files, so their row ids cannot "
                "be reassigned with the rows they belong to.",
                file->file_name));
        }
        if (VectorStoreUtils::IsVectorStoreFile(file->file_name)) {
            return Status::NotImplemented(fmt::format(
                "Materializing deletion vectors is not supported for a row range covered by the "
                "vector-store file {}.",
                file->file_name));
        }
    }
    bool has_deletions = std::any_of(deletion_files.begin(), deletion_files.end(),
                                     [](const std::optional<DeletionFile>& deletion_file) {
                                         return deletion_file != std::nullopt;
                                     });
    if (!has_deletions) {
        return Status::Invalid(
            "A materialize deletion task needs at least one file carrying a deletion vector.");
    }
    PAIMON_ASSIGN_OR_RAISE(Range row_range, DataEvolutionUtils::CheckContiguousRowRange(files));
    return DataEvolutionMaterializeDeletionCompactTask(partition, files, deletion_files, row_range);
}

Result<std::shared_ptr<CommitMessage>> DataEvolutionMaterializeDeletionCompactTask::DoCompact(
    const DataEvolutionCompactContext& context) {
    if (compact_before_.empty()) {
        return Status::Invalid(
            "DataEvolutionMaterializeDeletionCompactTask needs at least one file input.");
    }
    // A task rewrites its input once. Running it again would append a second set of outputs and
    // commit the same surviving rows twice.
    if (!compact_after_.empty()) {
        return Status::Invalid(
            "DataEvolutionMaterializeDeletionCompactTask has already been compacted and cannot "
            "run twice.");
    }

    // Every column is rewritten: materializing changes the row ids, so a column left in place
    // would still be addressed by the old ones. Create() already refused the ranges whose
    // columns live in dedicated files this cannot rewrite.
    std::vector<std::string> read_write_field_names;
    read_write_field_names.reserve(context.arrow_schema->fields().size());
    for (const auto& field : context.arrow_schema->fields()) {
        read_write_field_names.push_back(field->name());
    }
    if (read_write_field_names.empty()) {
        return Status::Invalid("Materializing deletion vectors requires at least one column.");
    }
    auto write_schema = context.arrow_schema;

    // Unlike the normal task, the deletion files *are* attached: the read drops the deleted rows
    // so they never reach the writer. DataEvolutionSplitRead resolves them per row range group,
    // which is how a vector keyed by a group's anchor reaches the group's other files.
    ReadContextBuilder context_builder(context.table_path);
    context_builder.SetOptions(context.core_options.ToMap())
        .WithFileSystem(context.core_options.GetFileSystem())
        .SetReadFieldNames(read_write_field_names)
        .EnablePrefetch(true)
        .SetPrefetchMaxParallelNum(1)
        .SetPrefetchBatchCount(3)
        .WithMemoryPool(context.pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context, context_builder.Finish());
    std::map<std::string, std::string> read_options = context.core_options.ToMap();
    if (read_options.find("parquet.read.enable-pre-buffer") == read_options.end()) {
        read_options["parquet.read.enable-pre-buffer"] = "false";
    }
    // Blob-view columns must be rewritten as their serialized BlobViewStruct bytes, exactly as
    // the normal task does; resolving them here would bake descriptors into the column.
    read_options[Options::BLOB_VIEW_RESOLVE_ENABLED] = "false";
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> internal_read_context,
        InternalReadContext::Create(read_context, context.table_schema, read_options));
    auto split_read = std::make_unique<DataEvolutionSplitRead>(
        context.path_factory, internal_read_context, context.pool, context.executor);

    PAIMON_ASSIGN_OR_RAISE(std::string bucket_path,
                           context.path_factory->BucketPath(partition_, /*bucket=*/0));
    auto data_files = compact_before_;
    auto deletion_files = deletion_files_;
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<DataSplitImpl> split,
        DataSplitImpl::Builder(partition_, /*bucket=*/0, bucket_path, std::move(data_files))
            .WithDataDeletionFiles(std::move(deletion_files))
            .RawConvertible(false)
            .Build());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader, split_read->CreateReader(split));
    ScopeGuard reader_guard([&]() {
        if (reader) {
            reader->Close();
        }
    });

    // The surviving rows are written through the table's ordinary rolling rules rather than into
    // one file pinned to the input range: the range is not preserved anyway, so there is no
    // reason to produce a single oversized file.
    //
    // FileSource::Append(), not Compact(): that is what makes the commit assign fresh row ids to
    // these files. The rewriter recognises the missing row ids and drops the old vectors.
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
            context.core_options, context.table_schema->Id(), write_schema,
            /*write_cols=*/std::nullopt, seq_num_counter, FileSource::Append(),
            data_file_path_factory, plan_factory, context.pool);
    } else {
        writer_factory = std::make_shared<AppendDataFileWriterFactory>(
            context.core_options, context.table_schema->Id(), write_schema,
            /*write_cols=*/std::nullopt, seq_num_counter, FileSource::Append(),
            data_file_path_factory, context.pool);
    }
    auto rewriter =
        std::make_unique<RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
            context.core_options.GetTargetFileSize(/*has_primary_key=*/false),
            context.core_options.GetTargetFileRowNum(), writer_factory);
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
                "cannot cast array to StructArray in DataEvolutionMaterializeDeletionCompactTask");
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
    rewriter_guard.Release();

    // No row ids and no sequence numbers are assigned here: the commit gives the surviving rows
    // fresh row ids, which is the whole point of materializing, and the writer's own sequence
    // numbers stand because the output no longer competes with a field group.
    compact_after_ = std::move(write_result);

    auto compact_before_copy = compact_before_;
    auto compact_after_copy = compact_after_;
    CompactIncrement compact_increment(std::move(compact_before_copy),
                                       std::move(compact_after_copy),
                                       /*changelog_files=*/{},
                                       /*new_index_files=*/{},
                                       /*deleted_index_files=*/{});
    DataIncrement data_increment(/*new_files=*/{}, /*deleted_files=*/{}, /*changelog_files=*/{});

    // Bucket 0 is the bucket for unaware-bucket tables, and total buckets stay unset, matching
    // the contract the deletion vector rewriter asserts on.
    return std::make_shared<CommitMessageImpl>(partition_,
                                               /*bucket=*/0, /*total_buckets=*/std::nullopt,
                                               data_increment, compact_increment);
}

std::string DataEvolutionMaterializeDeletionCompactTask::ToString() const {
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
        "DataEvolutionMaterializeDeletionCompactTask {{partition = {}, rowRange = {}, "
        "compactBefore = [{}], compactAfter = [{}]}}",
        partition_.ToString(), row_range_.ToString(), fmt::join(before_names, ", "),
        fmt::join(after_names, ", "));
}

}  // namespace paimon
