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

#include "paimon/core/index/pksorted/pk_sorted_index_file.h"

#include <cstddef>
#include <limits>
#include <optional>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/core/io/async_key_value_producer_and_consumer.h"
#include "paimon/core/io/key_value_meta_projection_consumer.h"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"

namespace paimon {
namespace {

Result<int64_t> ValidateAndCountSourceRows(
    const std::vector<PrimaryKeyIndexSourceFile>& source_files) {
    int64_t source_row_count = 0;
    for (const PrimaryKeyIndexSourceFile& source_file : source_files) {
        if (source_file.row_count < 0 ||
            __builtin_add_overflow(source_row_count, source_file.row_count, &source_row_count)) {
            return Status::Invalid("Source row count overflows in sorted index build.");
        }
    }
    if (source_row_count <= 0) {
        return Status::Invalid("A sorted index group must reference at least one source row.");
    }
    return source_row_count;
}

Result<std::shared_ptr<IndexFileMeta>> FinishIndexFile(
    int32_t field_id, const std::string& index_type, int64_t source_row_count,
    const PrimaryKeyIndexSourceMeta& source_meta, const std::vector<GlobalIndexIOMeta>& io_metas,
    bool is_external_path, const std::shared_ptr<MemoryPool>& pool) {
    if (io_metas.size() != 1) {
        return Status::Invalid(fmt::format(
            "Sorted index build must produce exactly one payload file, but produced {}.",
            io_metas.size()));
    }
    const GlobalIndexIOMeta& io_meta = io_metas[0];
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> source_meta_bytes, source_meta.Serialize(pool));
    std::optional<std::string> external_path;
    if (is_external_path) {
        PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(io_meta.file_path));
        external_path = path.ToString();
    }
    return std::make_shared<IndexFileMeta>(
        index_type, PathUtil::GetName(io_meta.file_path), io_meta.file_size, source_row_count,
        /*dv_ranges=*/std::nullopt, external_path,
        GlobalIndexMeta(0, source_row_count - 1, field_id,
                        /*extra_field_ids=*/std::nullopt, io_meta.metadata, source_meta_bytes));
}

}  // namespace

Result<std::shared_ptr<IndexFileMeta>> PkSortedIndexFile::Build(
    const DataField& field, const std::string& index_type,
    const std::map<std::string, std::string>& options, int32_t data_level,
    const std::vector<PrimaryKeyIndexSourceFile>& source_files,
    const std::shared_ptr<arrow::Array>& sorted_values, std::vector<int64_t> sorted_ordinals,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer, bool is_external_path,
    const std::shared_ptr<MemoryPool>& pool) {
    // TODO(wangyong9999): Replace the all-in-memory sorted values and ordinals with an
    // external sort buffer and feed the index writer in bounded batches.
    PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexSourceMeta source_meta,
                           PrimaryKeyIndexSourceMeta::Create(data_level, source_files));
    PAIMON_ASSIGN_OR_RAISE(int64_t source_row_count, ValidateAndCountSourceRows(source_files));
    if (sorted_values == nullptr || sorted_values->length() != source_row_count ||
        static_cast<int64_t>(sorted_ordinals.size()) != source_row_count) {
        return Status::Invalid(
            fmt::format("Sorted index input row count {} does not match source row count {}.",
                        sorted_values == nullptr ? 0 : sorted_values->length(), source_row_count));
    }
    std::vector<bool> seen_ordinals(static_cast<size_t>(source_row_count), false);
    for (int64_t ordinal : sorted_ordinals) {
        if (ordinal < 0 || ordinal >= source_row_count) {
            return Status::Invalid(
                fmt::format("Row id {} is outside sorted index group row range [0, {}).", ordinal,
                            source_row_count));
        }
        if (seen_ordinals[ordinal]) {
            return Status::Invalid(fmt::format("Row id {} appears more than once.", ordinal));
        }
        seen_ordinals[ordinal] = true;
    }

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           GlobalIndexerFactory::Get(index_type, options));
    if (indexer == nullptr) {
        return Status::Invalid(fmt::format("Index type {} is not registered.", index_type));
    }
    auto arrow_field = DataField::ConvertDataFieldToArrowField(field);
    auto arrow_schema = arrow::schema({arrow_field});
    ArrowSchema c_arrow_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
    ScopeGuard schema_guard([&]() { ArrowSchemaRelease(&c_arrow_schema); });
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> writer,
                           indexer->CreateWriter(field.Name(), &c_arrow_schema, file_writer, pool));

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> struct_array,
                                      arrow::StructArray::Make({sorted_values}, {field.Name()}));
    ::ArrowArray c_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*struct_array, &c_array));
    ScopeGuard array_guard([&]() { ArrowArrayRelease(&c_array); });
    PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array, std::move(sorted_ordinals)));
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> io_metas, writer->Finish());
    return FinishIndexFile(field.Id(), index_type, source_row_count, source_meta, io_metas,
                           is_external_path, pool);
}

Result<std::shared_ptr<IndexFileMeta>> PkSortedIndexFile::BuildFromSortedReader(
    const DataField& field, const std::string& index_type,
    const std::map<std::string, std::string>& options, int32_t data_level,
    const std::vector<PrimaryKeyIndexSourceFile>& source_files,
    std::unique_ptr<SortMergeReader>&& sorted_reader,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer, bool is_external_path,
    int32_t write_batch_size, const std::shared_ptr<MemoryPool>& pool) {
    if (sorted_reader == nullptr) {
        return Status::Invalid("Sorted index reader is null.");
    }
    if (write_batch_size <= 0) {
        return Status::Invalid("Sorted index write batch size must be positive.");
    }
    PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexSourceMeta source_meta,
                           PrimaryKeyIndexSourceMeta::Create(data_level, source_files));
    PAIMON_ASSIGN_OR_RAISE(int64_t source_row_count, ValidateAndCountSourceRows(source_files));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexer> indexer,
                           GlobalIndexerFactory::Get(index_type, options));
    if (indexer == nullptr) {
        return Status::Invalid(fmt::format("Index type {} is not registered.", index_type));
    }
    auto arrow_schema = arrow::schema({DataField::ConvertDataFieldToArrowField(field)});
    ArrowSchema c_arrow_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
    ScopeGuard schema_guard([&]() { ArrowSchemaRelease(&c_arrow_schema); });
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> writer,
                           indexer->CreateWriter(field.Name(), &c_arrow_schema, file_writer, pool));

    auto projection_schema = SpecialFields::CompleteSequenceAndValueKindField(arrow_schema);
    auto create_consumer =
        [projection_schema,
         pool]() -> Result<std::unique_ptr<RowToArrowArrayConverter<KeyValue, KeyValueBatch>>> {
        return KeyValueMetaProjectionConsumer::Create(projection_schema, pool);
    };
    std::unique_ptr<AsyncKeyValueBatchProducer> batch_producer =
        std::make_unique<SortMergeReaderBatchProducer>(std::move(sorted_reader), write_batch_size);
    auto producer = std::make_unique<AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>>(
        std::move(batch_producer), create_consumer, /*consumer_thread_num=*/1);
    ScopeGuard close_guard([&]() { producer->Close(); });
    int64_t rows_written = 0;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(KeyValueBatch key_value_batch, producer->NextBatch());
        if (key_value_batch.batch == nullptr) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::RecordBatch> record_batch,
            arrow::ImportRecordBatch(key_value_batch.batch.get(), projection_schema));
        if (record_batch->num_columns() != 3 ||
            record_batch->column(0)->type_id() != arrow::Type::INT64) {
            return Status::Invalid("Sorted index projection produced an invalid batch.");
        }
        auto sequence_numbers = checked_pointer_cast<arrow::Int64Array>(record_batch->column(0));
        std::vector<int64_t> ordinals;
        ordinals.reserve(static_cast<size_t>(sequence_numbers->length()));
        for (int64_t index = 0; index < sequence_numbers->length(); ++index) {
            if (sequence_numbers->IsNull(index)) {
                return Status::Invalid("Sorted index row id must not be null.");
            }
            int64_t ordinal = sequence_numbers->Value(index);
            if (ordinal < 0 || ordinal >= source_row_count) {
                return Status::Invalid(
                    fmt::format("Row id {} is outside sorted index group row range [0, {}).",
                                ordinal, source_row_count));
            }
            ordinals.push_back(ordinal);
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> values,
            arrow::StructArray::Make({record_batch->column(2)}, {field.Name()}));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*values, &c_array));
        ScopeGuard array_guard([&]() { ArrowArrayRelease(&c_array); });
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array, std::move(ordinals)));
        if (__builtin_add_overflow(rows_written, values->length(), &rows_written)) {
            return Status::Invalid("Sorted index output row count overflows int64.");
        }
    }
    if (rows_written != source_row_count) {
        return Status::Invalid(
            fmt::format("Sorted index output row count {} does not match source row count {}.",
                        rows_written, source_row_count));
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> io_metas, writer->Finish());
    return FinishIndexFile(field.Id(), index_type, source_row_count, source_meta, io_metas,
                           is_external_path, pool);
}

}  // namespace paimon
