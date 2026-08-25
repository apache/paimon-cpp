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

#include "paimon/core/index/pksorted/pk_sorted_index_builder.h"

#include <algorithm>
#include <limits>
#include <map>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/core/index/pk/primary_key_index_source_policy.h"
#include "paimon/core/index/pksorted/pk_sorted_data_file_reader.h"
#include "paimon/core/index/pksorted/pk_sorted_index_file.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/mergetree/compact/sort_merge_reader_with_min_heap.h"
#include "paimon/core/mergetree/external_sort_buffer.h"
#include "paimon/core/mergetree/in_memory_sort_buffer.h"
#include "paimon/core/mergetree/sort_buffer.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/record_batch.h"

namespace paimon {
namespace {

constexpr char kRowIdFieldName[] = "_PK_INDEX_ROW_ID";

class TrackingGlobalIndexFileWriter : public GlobalIndexFileWriter {
 public:
    explicit TrackingGlobalIndexFileWriter(const std::shared_ptr<GlobalIndexFileManager>& delegate)
        : delegate_(delegate) {}

    Result<std::string> NewFileName(const std::string& prefix) const override {
        PAIMON_ASSIGN_OR_RAISE(std::string file_name, delegate_->NewFileName(prefix));
        created_file_names_.push_back(file_name);
        return file_name;
    }

    Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_name) const override {
        return delegate_->NewOutputStream(file_name);
    }

    Result<int64_t> GetFileSize(const std::string& file_name) const override {
        return delegate_->GetFileSize(file_name);
    }

    std::string ToPath(const std::string& file_name) const override {
        return delegate_->ToPath(file_name);
    }

    void Cleanup(const std::shared_ptr<FileSystem>& fs) const {
        for (const std::string& file_name : created_file_names_) {
            [[maybe_unused]] Status status = fs->Delete(delegate_->ToPath(file_name));
        }
    }

 private:
    std::shared_ptr<GlobalIndexFileManager> delegate_;
    mutable std::vector<std::string> created_file_names_;
};

}  // namespace

Result<std::unique_ptr<PkSortedIndexBuilder>> PkSortedIndexBuilder::Create(
    const std::string& root_path, const std::string& branch, const BinaryRow& partition,
    int32_t bucket, const std::shared_ptr<TableSchema>& table_schema,
    const PrimaryKeyIndexDefinition& definition,
    const std::shared_ptr<FileStorePathFactory>& path_factory, const CoreOptions& options,
    const std::shared_ptr<IOManager>& io_manager, bool enable_multi_thread_spill,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool) {
    if (definition.GetFamily() != PrimaryKeyIndexDefinition::Family::BTREE) {
        return Status::Invalid("PkSortedIndexBuilder only supports BTree definitions.");
    }
    PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(definition.FieldId()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<PkSortedDataFileReader> data_file_reader,
        PkSortedDataFileReader::Create(root_path, table_schema, definition.FieldId(), path_factory,
                                       branch, options, executor, pool));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexPathFactory> index_path_factory,
                           path_factory->CreateIndexFileFactory(partition, bucket));
    return std::unique_ptr<PkSortedIndexBuilder>(new PkSortedIndexBuilder(
        partition, bucket, std::move(field), definition,
        std::shared_ptr<PkSortedDataFileReader>(std::move(data_file_reader)),
        options.GetFileSystem(), std::shared_ptr<IndexPathFactory>(std::move(index_path_factory)),
        options, io_manager, enable_multi_thread_spill, pool));
}

Result<std::shared_ptr<IndexFileMeta>> PkSortedIndexBuilder::Build(
    const std::vector<std::shared_ptr<DataFileMeta>>& source_files) const {
    if (source_files.empty()) {
        return Status::Invalid("Cannot build a sorted index for an empty data level.");
    }
    for (const std::shared_ptr<DataFileMeta>& file : source_files) {
        if (file == nullptr) {
            return Status::Invalid("A sorted index source file is null.");
        }
    }
    std::vector<std::shared_ptr<DataFileMeta>> ordered_files = source_files;
    std::sort(
        ordered_files.begin(), ordered_files.end(),
        [](const std::shared_ptr<DataFileMeta>& left, const std::shared_ptr<DataFileMeta>& right) {
            return left->file_name < right->file_name;
        });
    int32_t data_level = ordered_files.front()->level;
    std::vector<PrimaryKeyIndexSourceFile> source_metas;
    source_metas.reserve(ordered_files.size());
    for (const std::shared_ptr<DataFileMeta>& file : ordered_files) {
        if (file == nullptr || file->level != data_level ||
            !PrimaryKeyIndexSourcePolicy::ShouldRead(*file)) {
            return Status::Invalid(
                "A sorted index can only cover compacted files from one positive data level.");
        }
        source_metas.emplace_back(file->file_name, file->row_count);
    }

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldsComparator> unique_comparator,
                           FieldsComparator::Create({field_}, /*is_ascending_order=*/true));
    auto comparator = std::shared_ptr<FieldsComparator>(std::move(unique_comparator));
    DataField row_id_field(std::numeric_limits<int32_t>::max(),
                           arrow::field(kRowIdFieldName, arrow::int64(), false));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FieldsComparator> unique_sequence_comparator,
        FieldsComparator::Create({field_, row_id_field}, {1}, /*is_ascending_order=*/true));
    auto sequence_comparator =
        std::shared_ptr<FieldsComparator>(std::move(unique_sequence_comparator));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FieldsComparator> unique_in_memory_comparator,
        FieldsComparator::Create({field_, row_id_field}, /*is_ascending_order=*/true));
    auto in_memory_comparator =
        std::shared_ptr<FieldsComparator>(std::move(unique_in_memory_comparator));
    auto value_schema = arrow::schema({field_.ArrowField(), row_id_field.ArrowField()});
    // Keep the Arrow memory-pool adapter alive for as long as the sort buffer can retain
    // arrays allocated through it.
    std::unique_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool_);
    auto in_memory_buffer = std::make_unique<InMemorySortBuffer>(
        /*last_sequence_number=*/-1, arrow::struct_(value_schema->fields()),
        std::vector<std::string>{field_.Name()},
        /*user_defined_sequence_fields=*/std::vector<std::string>{kRowIdFieldName},
        /*sequence_fields_ascending=*/true, comparator, options_.GetWriteBufferSize(), pool_,
        in_memory_comparator);
    std::unique_ptr<SortBuffer> sort_buffer;
    if (options_.GetWriteBufferSpillable() && io_manager_ != nullptr) {
        PAIMON_ASSIGN_OR_RAISE(
            sort_buffer,
            ExternalSortBuffer::Create(std::move(in_memory_buffer), value_schema, {field_.Name()},
                                       comparator, sequence_comparator, options_, io_manager_,
                                       enable_multi_thread_spill_, pool_));
    } else {
        sort_buffer = std::move(in_memory_buffer);
    }
    ScopeGuard sort_cleanup([&]() { sort_buffer->Clear(); });

    int64_t rows_buffered = 0;
    for (const std::shared_ptr<DataFileMeta>& file : ordered_files) {
        Status read_status = data_file_reader_->ReadFile(
            partition_, bucket_, file,
            [&](const std::shared_ptr<arrow::StructArray>& batch,
                const std::vector<int64_t>& positions) -> Status {
                if (positions.size() != static_cast<size_t>(batch->length())) {
                    return Status::Invalid(
                        "Physical row positions do not match the source batch length.");
                }
                int64_t next_rows_buffered = 0;
                if (__builtin_add_overflow(rows_buffered, batch->length(), &next_rows_buffered)) {
                    return Status::Invalid("Primary-key index row id overflows int64.");
                }
                std::vector<int64_t> group_ordinals;
                group_ordinals.reserve(positions.size());
                for (int64_t index = 0; index < batch->length(); ++index) {
                    group_ordinals.push_back(rows_buffered + index);
                }
                arrow::Int64Builder row_id_builder(arrow_pool.get());
                PAIMON_RETURN_NOT_OK_FROM_ARROW(row_id_builder.AppendValues(group_ordinals));
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> row_ids,
                                                  row_id_builder.Finish());
                std::shared_ptr<arrow::Array> indexed_values = batch->field(0);
                if (indexed_values->type_id() == arrow::Type::DICTIONARY) {
                    const auto* dictionary_type =
                        checked_cast<const arrow::DictionaryType*>(indexed_values->type().get());
                    arrow::Type::type value_type = dictionary_type->value_type()->id();
                    if (value_type != arrow::Type::STRING &&
                        value_type != arrow::Type::LARGE_STRING) {
                        return Status::Invalid(fmt::format(
                            "Cannot decode dictionary-backed primary-key index field with value "
                            "type {}.",
                            dictionary_type->value_type()->ToString()));
                    }
                    PAIMON_ASSIGN_OR_RAISE(
                        indexed_values,
                        CastingUtils::Cast(indexed_values, field_.ArrowField()->type(),
                                           arrow::compute::CastOptions::Safe(), arrow_pool.get()));
                }
                // The physical reader owns the pool adapter behind its batch buffers. Copy the
                // indexed values into the Build-scoped pool before retaining them in sort_buffer.
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> values,
                    arrow::Concatenate({indexed_values}, arrow_pool.get()));
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::StructArray> sort_batch,
                    arrow::StructArray::Make({values, row_ids}, {field_.Name(), kRowIdFieldName}));
                ArrowArray c_array;
                PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*sort_batch, &c_array));
                auto record_batch = std::make_unique<RecordBatch>(
                    std::map<std::string, std::string>{}, /*bucket=*/0,
                    std::vector<RecordBatch::RowKind>{}, &c_array);
                PAIMON_ASSIGN_OR_RAISE(bool has_remaining_quota,
                                       sort_buffer->Write(std::move(record_batch)));
                if (!has_remaining_quota) {
                    return Status::Invalid(
                        "Primary-key index external-sort quota is exhausted. Configure a "
                        "temporary directory and sufficient spill capacity.");
                }
                rows_buffered = next_rows_buffered;
                return Status::OK();
            });
        PAIMON_RETURN_NOT_OK(read_status);
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<std::unique_ptr<KeyValueRecordReader>> readers,
                           sort_buffer->CreateReaders());
    auto sorted_reader = std::make_unique<SortMergeReaderWithMinHeap>(
        std::move(readers), comparator, sequence_comparator,
        /*merge_function_wrapper=*/nullptr);
    auto file_manager = std::make_shared<GlobalIndexFileManager>(fs_, index_path_factory_);
    auto tracking_writer = std::make_shared<TrackingGlobalIndexFileWriter>(file_manager);
    Result<std::shared_ptr<IndexFileMeta>> result = PkSortedIndexFile::BuildFromSortedReader(
        field_, definition_.IndexType(), definition_.Options(), data_level, source_metas,
        std::move(sorted_reader), tracking_writer, index_path_factory_->IsExternalPath(),
        options_.GetWriteBatchSize(), pool_);
    if (!result.ok()) {
        tracking_writer->Cleanup(fs_);
        return result.status();
    }
    return std::move(result).value();
}

Status PkSortedIndexBuilder::DeletePayload(const std::shared_ptr<IndexFileMeta>& payload) const {
    if (payload == nullptr) {
        return Status::OK();
    }
    return fs_->Delete(index_path_factory_->ToPath(payload));
}

}  // namespace paimon
