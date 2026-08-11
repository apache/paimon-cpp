/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/index/pksorted/pk_sorted_index_file.h"

#include <cstddef>
#include <optional>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"

namespace paimon {
Result<std::shared_ptr<IndexFileMeta>> PkSortedIndexFile::Build(
    const DataField& field, const std::string& index_type,
    const std::map<std::string, std::string>& options, int32_t data_level,
    const std::vector<PrimaryKeyIndexSourceFile>& source_files,
    const std::shared_ptr<arrow::Array>& sorted_values, std::vector<int64_t> sorted_ordinals,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer, bool is_external_path,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(PrimaryKeyIndexSourceMeta source_meta,
                           PrimaryKeyIndexSourceMeta::Create(data_level, source_files));
    int64_t source_row_count = 0;
    for (const PrimaryKeyIndexSourceFile& source_file : source_files) {
        if (__builtin_add_overflow(source_row_count, source_file.row_count, &source_row_count)) {
            return Status::Invalid("Source row count overflows in sorted index build.");
        }
    }
    if (source_row_count <= 0) {
        return Status::Invalid("A sorted index group must reference at least one source row.");
    }
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
    if (io_metas.size() != 1) {
        return Status::Invalid(fmt::format(
            "Sorted index build must produce exactly one payload file, but produced {}.",
            io_metas.size()));
    }
    const GlobalIndexIOMeta& io_meta = io_metas[0];

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> source_meta_bytes,
                           source_meta.Serialize(pool.get()));
    std::optional<std::string> external_path;
    if (is_external_path) {
        external_path = io_meta.file_path;
    }
    return std::make_shared<IndexFileMeta>(
        index_type, PathUtil::GetName(io_meta.file_path), io_meta.file_size, source_row_count,
        /*dv_ranges=*/std::nullopt, external_path,
        GlobalIndexMeta(0, source_row_count - 1, field.Id(),
                        /*extra_field_ids=*/std::nullopt, io_meta.metadata, source_meta_bytes));
}

}  // namespace paimon
