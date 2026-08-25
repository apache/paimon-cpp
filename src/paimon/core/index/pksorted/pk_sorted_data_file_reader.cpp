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

#include "paimon/core/index/pksorted/pk_sorted_data_file_reader.h"

#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"

namespace paimon {

Result<std::unique_ptr<PkSortedDataFileReader>> PkSortedDataFileReader::Create(
    const std::string& root_path, const std::shared_ptr<TableSchema>& table_schema,
    int32_t field_id, const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::string& branch, const CoreOptions& options,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    std::map<std::string, std::string> read_options = options.ToMap();
    read_options[Options::BRANCH] = branch;
    ReadContextBuilder builder(root_path);
    builder.SetReadFieldIds({field_id})
        .SetOptions(read_options)
        .WithBranch(branch)
        .WithFileSystem(options.GetFileSystem())
        .WithExecutor(executor)
        .WithMemoryPool(pool)
        .EnablePrefetch(false)
        .EnablePredicateFilter(false);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context, builder.Finish());
    auto shared_read_context = std::shared_ptr<ReadContext>(std::move(read_context));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<InternalReadContext> internal_context,
        InternalReadContext::Create(shared_read_context, table_schema, read_options));
    auto shared_internal_context =
        std::shared_ptr<InternalReadContext>(std::move(internal_context));
    return std::unique_ptr<PkSortedDataFileReader>(
        new PkSortedDataFileReader(path_factory, shared_internal_context, pool, executor));
}

PkSortedDataFileReader::PkSortedDataFileReader(
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<InternalReadContext>& context, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor)
    : RawFileSplitRead(path_factory, context, pool, executor) {}

Status PkSortedDataFileReader::ReadFile(const BinaryRow& partition, int32_t bucket,
                                        const std::shared_ptr<DataFileMeta>& file,
                                        const BatchConsumer& consumer) const {
    if (file == nullptr) {
        return Status::Invalid("Primary-key sorted-index source file is null.");
    }
    if (file->row_count < 0) {
        return Status::Invalid(fmt::format("Source file {} has negative row count {}.",
                                           file->file_name, file->row_count));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           path_factory_->CreateDataFilePathFactory(partition, bucket));
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<FileBatchReader>> readers,
        CreateRawFileReaders(partition, {file}, raw_read_schema_, /*predicate=*/nullptr,
                             /*dv_factory=*/{}, /*row_ranges=*/std::nullopt, data_file_path_factory,
                             /*extra_format_options=*/{}));
    if (readers.size() != 1) {
        return Status::Invalid(
            fmt::format("Expected one physical reader for source file {}, but got {}.",
                        file->file_name, readers.size()));
    }
    std::unique_ptr<FileBatchReader> reader = std::move(readers[0]);
    ScopeGuard close_guard([&]() { reader->Close(); });
    PAIMON_ASSIGN_OR_RAISE(uint64_t physical_row_count, reader->GetNumberOfRows());
    if (physical_row_count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        static_cast<int64_t>(physical_row_count) != file->row_count) {
        return Status::Invalid(fmt::format(
            "Physical row count {} of source file {} does not match metadata row count {}.",
            physical_row_count, file->file_name, file->row_count));
    }

    int64_t rows_read = 0;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                               reader->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(batch_with_bitmap)) {
            break;
        }
        auto& [batch, bitmap] = batch_with_bitmap;
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (array == nullptr || array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid(
                fmt::format("Source file {} did not return a struct batch.", file->file_name));
        }
        auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
        if (struct_array->num_fields() != 1) {
            return Status::Invalid(
                fmt::format("Source file {} returned {} fields for a single-column index build.",
                            file->file_name, struct_array->num_fields()));
        }
        if (static_cast<int64_t>(bitmap.Cardinality()) != struct_array->length()) {
            return Status::Invalid(
                fmt::format("Source file {} was filtered while building a physical-row index.",
                            file->file_name));
        }
        std::vector<int64_t> positions;
        positions.reserve(static_cast<size_t>(struct_array->length()));
        for (int64_t index = 0; index < struct_array->length(); ++index) {
            PAIMON_ASSIGN_OR_RAISE(uint64_t physical_position,
                                   reader->GetPreviousBatchFileRowId(static_cast<uint64_t>(index)));
            if (physical_position > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
                static_cast<int64_t>(physical_position) != rows_read + index) {
                return Status::Invalid(fmt::format(
                    "Source file {} returned non-contiguous physical row position {} at row {}.",
                    file->file_name, physical_position, rows_read + index));
            }
            positions.push_back(static_cast<int64_t>(physical_position));
        }
        PAIMON_RETURN_NOT_OK(consumer(struct_array, positions));
        if (__builtin_add_overflow(rows_read, struct_array->length(), &rows_read)) {
            return Status::Invalid("Physical source row count overflows int64.");
        }
    }
    if (rows_read != file->row_count) {
        return Status::Invalid(
            fmt::format("Read {} physical rows from source file {}, but metadata declares {}.",
                        rows_read, file->file_name, file->row_count));
    }
    return Status::OK();
}

Result<std::unique_ptr<FileBatchReader>> PkSortedDataFileReader::ApplyIndexAndDvReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>&,
    const std::shared_ptr<arrow::Schema>&, const std::shared_ptr<arrow::Schema>& read_schema,
    const std::shared_ptr<Predicate>&, DeletionVector::Factory,
    const std::optional<std::vector<Range>>&, const std::shared_ptr<DataFilePathFactory>&) const {
    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));
    PAIMON_RETURN_NOT_OK(file_reader->SetReadSchema(&c_read_schema, /*predicate=*/nullptr,
                                                    /*selection_bitmap=*/std::nullopt));
    return std::move(file_reader);
}

}  // namespace paimon
