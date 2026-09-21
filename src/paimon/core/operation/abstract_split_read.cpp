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

#include "paimon/core/operation/abstract_split_read.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <future>
#include <map>
#include <set>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/shredding/map_shared_shredding_read_plan_factory.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/shredding_file_reader.h"
#include "paimon/common/data/variant/variant_shredding_read_plan_factory.h"
#include "paimon/common/data/variant/variant_type_utils.h"
#include "paimon/common/executor/future.h"
#include "paimon/common/executor/reader_build_executor.h"
#include "paimon/common/file_index/bitmap/apply_bitmap_index_batch_reader.h"
#include "paimon/common/reader/data_file_reader_factory.h"
#include "paimon/common/reader/delegating_prefetch_reader.h"
#include "paimon/common/reader/late_materializing_reader_builder.h"
#include "paimon/common/reader/predicate_batch_reader.h"
#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/io/complete_row_tracking_fields_reader.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/field_mapping_reader.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/io/vector_file_batch_reader.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/partition/partition_info.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/field_mapping.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class BinaryRow;
class Executor;
class FileStorePathFactory;
class MemoryPool;
class Predicate;

AbstractSplitRead::AbstractSplitRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                                     const std::shared_ptr<InternalReadContext>& context,
                                     std::unique_ptr<SchemaManager>&& schema_manager,
                                     const std::shared_ptr<MemoryPool>& memory_pool,
                                     const std::shared_ptr<Executor>& executor)
    : pool_(memory_pool),
      arrow_pool_(context->GetArrowMemoryPool()),
      executor_(executor),
      path_factory_(path_factory),
      options_(context->GetCoreOptions()),
      raw_read_schema_(context->GetReadSchema()),
      context_(context),
      schema_manager_(std::move(schema_manager)) {}

Result<std::vector<std::unique_ptr<FileBatchReader>>> AbstractSplitRead::CreateRawFileReaders(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    DeletionVector::Factory dv_factory, const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::map<std::string, std::string>& extra_format_options) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<RawFileReaderWithMeta> readers_with_meta,
        CreateRawFileReadersWithMeta(partition, data_files, read_schema, predicate, dv_factory,
                                     row_ranges, data_file_path_factory, extra_format_options));
    std::vector<std::unique_ptr<FileBatchReader>> raw_file_readers;
    raw_file_readers.reserve(readers_with_meta.size());
    for (auto& reader_with_meta : readers_with_meta) {
        raw_file_readers.push_back(std::move(reader_with_meta.reader));
    }
    return raw_file_readers;
}

Result<std::vector<AbstractSplitRead::RawFileReaderWithMeta>>
AbstractSplitRead::CreateRawFileReadersWithMeta(
    const BinaryRow& partition, const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    DeletionVector::Factory dv_factory, const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::map<std::string, std::string>& extra_format_options) const {
    if (data_files.empty()) {
        return std::vector<RawFileReaderWithMeta>();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<FieldMappingBuilder> field_mapping_builder,
        FieldMappingBuilder::Create(read_schema, context_->GetPartitionKeys(), predicate));

    std::vector<RawFileReaderWithMeta> raw_file_readers;
    raw_file_readers.reserve(data_files.size());
    const uint32_t parallel_num =
        static_cast<uint32_t>(std::min<size_t>(kReaderBuildMaxParallelNum, data_files.size()));
    if (parallel_num <= 1) {
        for (const auto& file : data_files) {
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<FileBatchReader> file_reader,
                CreateRawFileReader(partition, file, field_mapping_builder.get(), dv_factory,
                                    row_ranges, data_file_path_factory, extra_format_options));
            if (file_reader) {
                raw_file_readers.push_back({file, std::move(file_reader)});
            }
        }
        return std::move(raw_file_readers);
    }

    // Opening a file, reading its footer and building the reader is mostly waiting on remote
    // I/O, so files are built concurrently. The tasks only reference locals of this frame,
    // which stay alive because CollectAll below drains every future before returning.
    std::shared_ptr<Executor> build_executor = GetReaderBuildExecutor();
    std::vector<std::future<Result<std::unique_ptr<FileBatchReader>>>> futures;
    futures.reserve(data_files.size());
    for (const auto& file : data_files) {
        futures.push_back(Via(
            build_executor.get(), [this, file, &partition, &field_mapping_builder, &dv_factory,
                                   &row_ranges, &data_file_path_factory, &extra_format_options]() {
                return CreateRawFileReader(partition, file, field_mapping_builder.get(), dv_factory,
                                           row_ranges, data_file_path_factory,
                                           extra_format_options);
            }));
    }
    // Readers keep the file order of the split, and CollectAll preserves the submit order.
    Status first_error;
    size_t file_index = 0;
    for (auto& built : CollectAll(futures)) {
        const std::shared_ptr<DataFileMeta>& file = data_files[file_index++];
        if (!built.ok()) {
            if (first_error.ok()) {
                first_error = built.status();
            }
            continue;
        }
        std::unique_ptr<FileBatchReader> file_reader = std::move(built).value();
        if (file_reader) {
            raw_file_readers.push_back({file, std::move(file_reader)});
        }
    }
    PAIMON_RETURN_NOT_OK(first_error);
    return std::move(raw_file_readers);
}

Result<std::unique_ptr<FileBatchReader>> AbstractSplitRead::ApplyIndexAndDvReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader, const std::shared_ptr<DataFileMeta>& file,
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::shared_ptr<arrow::Schema>& read_schema, const std::shared_ptr<Predicate>& predicate,
    DeletionVector::Factory dv_factory, const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    std::shared_ptr<FileIndexResult> file_index_result;
    if (options_.FileIndexReadEnabled()) {
        PAIMON_ASSIGN_OR_RAISE(
            file_index_result,
            FileIndexEvaluator::Evaluate(data_schema, predicate, data_file_path_factory, file,
                                         options_.GetFileSystem(), pool_));
        PAIMON_ASSIGN_OR_RAISE(bool is_remain, file_index_result->IsRemain());
        if (!is_remain) {
            return std::unique_ptr<FileBatchReader>();
        }
    }

    // prepare selection bitmap for index
    const RoaringBitmap32* selection = nullptr;
    if (auto* bitmap_file_index = dynamic_cast<BitmapIndexResult*>(file_index_result.get())) {
        PAIMON_ASSIGN_OR_RAISE(selection, bitmap_file_index->GetBitmap());
    }

    // narrow the selection to the file-local row positions of an indexed split
    std::optional<RoaringBitmap32> row_ranges_selection;
    if (row_ranges) {
        RoaringBitmap32 row_ranges_bitmap;
        for (const Range& range : row_ranges.value()) {
            row_ranges_bitmap.AddRange(static_cast<int32_t>(range.from),
                                       static_cast<int32_t>(range.to + 1));
        }
        row_ranges_selection = selection ? RoaringBitmap32::And(*selection, row_ranges_bitmap)
                                         : std::move(row_ranges_bitmap);
        selection = &row_ranges_selection.value();
    }

    // prepare deletion bitmap for deletion vector
    std::shared_ptr<DeletionVector> deletion_vector;
    if (dv_factory) {
        PAIMON_ASSIGN_OR_RAISE(deletion_vector, dv_factory(file->file_name));
    }
    const RoaringBitmap32* deletion = nullptr;
    if (auto* bitmap_dv = dynamic_cast<BitmapDeletionVector*>(deletion_vector.get())) {
        deletion = bitmap_dv->GetBitmap();
    }

    // merge deletion and bitmap index selection
    std::optional<RoaringBitmap32> actual_selection;
    if (selection && deletion) {
        actual_selection = RoaringBitmap32::AndNot(*selection, *deletion);
    } else if (selection) {
        actual_selection = *selection;
    } else if (deletion) {
        actual_selection = *deletion;
        PAIMON_ASSIGN_OR_RAISE(uint64_t num_rows, file_reader->GetNumberOfRows());
        actual_selection->Flip(0, num_rows);
    }

    if (actual_selection && actual_selection->IsEmpty()) {
        return std::unique_ptr<FileBatchReader>();
    }

    ::ArrowSchema c_read_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_read_schema));
    PAIMON_RETURN_NOT_OK(file_reader->SetReadSchema(&c_read_schema, predicate, actual_selection));

    std::unique_ptr<FileBatchReader> reader;
    if (!file_reader->SupportPreciseBitmapSelection() && actual_selection) {
        reader = std::make_unique<ApplyBitmapIndexBatchReader>(std::move(file_reader),
                                                               std::move(actual_selection).value());
    } else {
        reader = std::move(file_reader);
    }

    if (deletion_vector && !deletion && !deletion_vector->IsEmpty()) {
        // TODO(xinyu.lxy): if deletion vector is bitmap64, use ApplyBitmapIndexBatchReader to
        // filter result
        return Status::NotImplemented("Only support BitmapDeletionVector");
    }
    return reader;
}

Result<std::unique_ptr<FileBatchReader>> AbstractSplitRead::CreateRawFileReader(
    const BinaryRow& partition, const std::shared_ptr<DataFileMeta>& file,
    const FieldMappingBuilder* field_mapping_builder, DeletionVector::Factory dv_factory,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::map<std::string, std::string>& extra_format_options) const {
    auto data_file_path = data_file_path_factory->ToPath(file);
    PAIMON_ASSIGN_OR_RAISE(std::string data_file_identifier, file->FileFormat());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                           PrepareReaderBuilder(data_file_identifier, extra_format_options));
    return CreateFieldMappingReader(data_file_path, file, partition, std::move(reader_builder),
                                    field_mapping_builder, dv_factory, row_ranges,
                                    data_file_path_factory);
}

bool AbstractSplitRead::NeedCompleteRowTrackingFields(
    bool row_tracking_enabled, const std::shared_ptr<arrow::Schema>& read_schema) {
    if (row_tracking_enabled &&
        (read_schema->GetFieldIndex(SpecialFields::RowId().Name()) != -1 ||
         read_schema->GetFieldIndex(SpecialFields::SequenceNumber().Name()) != -1)) {
        return true;
    }
    return false;
}
Result<std::unique_ptr<BatchReader>> AbstractSplitRead::ApplyPredicateFilterIfNeeded(
    std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<Predicate>& predicate) const {
    if (!context_->EnablePredicateFilter() || predicate == nullptr) {
        return std::move(reader);
    }
    return PredicateBatchReader::Create(std::move(reader), predicate, arrow_pool_);
}

DataFileReadOptions AbstractSplitRead::BuildDataFileReadOptions() const {
    DataFileReadOptions read_options;
    read_options.cache = options_.GetCache();
    read_options.read_batch_size = options_.GetReadBatchSize();
    read_options.prefetch_enabled = context_->EnablePrefetch();
    read_options.prefetch_max_parallel_num = context_->GetPrefetchMaxParallelNum();
    read_options.prefetch_batch_count = context_->GetPrefetchBatchCount();
    read_options.adaptive_prefetch_strategy = options_.EnableAdaptivePrefetchStrategy();
    read_options.read_ahead_cache_enabled = context_->ReadAheadCacheEnabled();
    read_options.cache_config = context_->GetCacheConfig();
    read_options.prefetch_io_metrics_enabled = options_.PrefetchIoMetricsEnabled();
    read_options.warmup_level = context_->GetWarmupLevel();
    return read_options;
}

Result<std::unique_ptr<ReaderBuilder>> AbstractSplitRead::PrepareReaderBuilder(
    const std::string& format_identifier,
    const std::map<std::string, std::string>& extra_format_options) const {
    return DataFileReaderFactory::CreateReaderBuilder(format_identifier, options_.ToMap(),
                                                      extra_format_options,
                                                      BuildDataFileReadOptions(), pool_);
}

Result<std::unique_ptr<FileBatchReader>> AbstractSplitRead::CreateFileBatchReader(
    const std::string& file_format_identifier, const std::string& data_file_path,
    int64_t data_file_size, std::unique_ptr<ReaderBuilder> reader_builder) const {
    if (context_->EnableLateMaterializing()) {
        reader_builder = std::make_unique<LateMaterializingReaderBuilder>(std::move(reader_builder),
                                                                          arrow_pool_);
    }
    return DataFileReaderFactory::Open(file_format_identifier, data_file_path, data_file_size,
                                       reader_builder.get(), BuildDataFileReadOptions(),
                                       options_.GetFileSystem(), executor_, pool_, arrow_pool_);
}

Result<std::unique_ptr<FileBatchReader>> AbstractSplitRead::CreateFieldMappingReader(
    const std::string& data_file_path, const std::shared_ptr<DataFileMeta>& file_meta,
    const BinaryRow& partition, std::unique_ptr<ReaderBuilder> reader_builder,
    const FieldMappingBuilder* field_mapping_builder, DeletionVector::Factory dv_factory,
    const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory) const {
    std::shared_ptr<TableSchema> data_schema;
    if (file_meta->schema_id == context_->GetTableSchema()->Id()) {
        data_schema = context_->GetTableSchema();
    } else {
        // load schema to get data schema
        PAIMON_ASSIGN_OR_RAISE(data_schema, schema_manager_->ReadSchema(file_meta->schema_id));
    }
    PAIMON_ASSIGN_OR_RAISE(CoreOptions data_options,
                           CoreOptions::FromMap(data_schema->Options(), options_.GetFileSystem()));
    auto blob_inline_fields = data_options.GetBlobInlineFields();

    std::unique_ptr<FieldMapping> field_mapping;
    if (!data_schema->PrimaryKeys().empty()) {
        // for pk table, add special fields to file schema when field mapping
        std::vector<DataField> file_fields = {SpecialFields::SequenceNumber(),
                                              SpecialFields::ValueKind()};
        file_fields.insert(file_fields.end(), data_schema->Fields().begin(),
                           data_schema->Fields().end());
        PAIMON_ASSIGN_OR_RAISE(field_mapping,
                               field_mapping_builder->CreateFieldMapping(file_fields));
    } else {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<DataField> projected_data_fields,
            ProjectFieldsForRowTrackingAndDataEvolution(data_schema, file_meta->write_cols));
        auto converted_fields =
            BlobUtils::ConvertBlobInlineDataFields(projected_data_fields, blob_inline_fields);
        PAIMON_ASSIGN_OR_RAISE(field_mapping,
                               field_mapping_builder->CreateFieldMapping(converted_fields));
    }

    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(
        field_mapping->non_partition_info.non_partition_data_schema);

    PAIMON_ASSIGN_OR_RAISE(std::string file_format_identifier, file_meta->FileFormat());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> file_reader,
                           CreateFileBatchReader(file_format_identifier, data_file_path,
                                                 file_meta->file_size, std::move(reader_builder)));
    if (VectorFileBatchReader::ContainsVector(read_schema)) {
        file_reader = std::make_unique<VectorFileBatchReader>(std::move(file_reader), arrow_pool_);
    }
    std::set<int32_t> skip_map_selected_keys_filter_field_ids;
    if (file_format_identifier != "blob") {
        std::pair<std::unique_ptr<FileBatchReader>, std::set<int32_t>> shredding_result;
        PAIMON_ASSIGN_OR_RAISE(shredding_result,
                               ApplyShreddingReaderIfNeeded(std::move(file_reader), read_schema));
        file_reader = std::move(shredding_result.first);
        skip_map_selected_keys_filter_field_ids = std::move(shredding_result.second);
    }
    if (NeedCompleteRowTrackingFields(options_.RowTrackingEnabled(), read_schema)) {
        // A blob file has no self-describing schema: its physical fields are declared by the
        // file meta's write cols instead of queried from the format reader.
        std::optional<std::vector<std::string>> file_field_names;
        if (file_format_identifier == "blob") {
            file_field_names = file_meta->write_cols;
        }
        file_reader = std::make_unique<CompleteRowTrackingFieldsBatchReader>(
            std::move(file_reader), file_meta->first_row_id, file_meta->max_sequence_number,
            file_field_names, arrow_pool_);
    }
    const auto& predicate = field_mapping->non_partition_info.non_partition_filter;
    auto all_data_schema = DataField::ConvertDataFieldsToArrowSchema(data_schema->Fields());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> final_reader,
                           ApplyIndexAndDvReaderIfNeeded(
                               std::move(file_reader), file_meta, all_data_schema, read_schema,
                               predicate, dv_factory, row_ranges, data_file_path_factory));
    if (!final_reader) {
        // file is skipped by index or dv
        return std::unique_ptr<FileBatchReader>();
    }

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldMappingReader> mapping_reader,
                           FieldMappingReader::Create(
                               field_mapping_builder->GetReadFieldCount(), std::move(final_reader),
                               partition, std::move(field_mapping),
                               std::move(skip_map_selected_keys_filter_field_ids), arrow_pool_));
    return mapping_reader;
}

Result<std::pair<std::unique_ptr<FileBatchReader>, std::set<int32_t>>>
AbstractSplitRead::ApplyShreddingReaderIfNeeded(
    std::unique_ptr<FileBatchReader>&& file_reader,
    const std::shared_ptr<arrow::Schema>& read_schema) const {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> file_schema,
                           file_reader->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_arrow_schema,
                                      arrow::ImportSchema(file_schema.get()));

    std::set<int32_t> handled_shared_shredding_field_ids;
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans;
    for (const auto& read_field : read_schema->fields()) {
        const auto& field_name = read_field->name();
        auto file_field = file_arrow_schema->GetFieldByName(field_name);
        if (!file_field) {
            // may exists field _ROW_ID in read schema
            continue;
        }
        std::shared_ptr<arrow::KeyValueMetadata> metadata =
            std::const_pointer_cast<arrow::KeyValueMetadata>(file_field->metadata());
        bool is_shared_shredding_file = MapSharedShreddingUtils::HasShreddingMetadata(metadata);
        bool is_shared_shredding_map_access =
            NestedProjectionUtils::IsMapSharedShreddingAccessField(read_field);
        if (!is_shared_shredding_file && !is_shared_shredding_map_access) {
            // Neither a shared-shredding file field nor a selected-key STRUCT projection.
            continue;
        }

        std::shared_ptr<ShreddingColumnReadPlan> plan;
        if (is_shared_shredding_map_access) {
            if (is_shared_shredding_file) {
                PAIMON_ASSIGN_OR_RAISE(MapSharedShreddingFieldMeta meta,
                                       MapSharedShreddingUtils::DeserializeMetadata(metadata));
                PAIMON_ASSIGN_OR_RAISE(
                    plan, MapSharedShreddingReadPlanFactory::CreateSharedSelectedKeysReadPlan(
                              read_field, meta));
            } else {
                PAIMON_ASSIGN_OR_RAISE(
                    plan, MapSharedShreddingReadPlanFactory::CreateDefaultSelectedKeysReadPlan(
                              file_field, read_field));
            }
        } else {
            PAIMON_ASSIGN_OR_RAISE(MapSharedShreddingFieldMeta meta,
                                   MapSharedShreddingUtils::DeserializeMetadata(metadata));
            PAIMON_ASSIGN_OR_RAISE(
                plan, MapSharedShreddingReadPlanFactory::CreateMapReadPlan(read_field, meta));
        }
        plans.emplace(field_name, std::move(plan));
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id,
                               NestedProjectionUtils::GetPaimonFieldId(read_field));
        handled_shared_shredding_field_ids.insert(field_id);
    }

    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> variant_plans;
    PAIMON_ASSIGN_OR_RAISE(variant_plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                              read_schema, file_arrow_schema, pool_));
    for (auto& [field_name, plan] : variant_plans) {
        if (!plans.emplace(field_name, std::move(plan)).second) {
            return Status::Invalid(
                fmt::format("multiple shredding read plans exist for field {}", field_name));
        }
    }

    if (!plans.empty()) {
        file_reader = std::make_unique<ShreddingFileReader>(std::move(file_reader),
                                                            std::move(plans), arrow_pool_);
    }
    return std::make_pair(std::move(file_reader), std::move(handled_shared_shredding_field_ids));
}

Result<std::vector<DataField>> AbstractSplitRead::ProjectFieldsForRowTrackingAndDataEvolution(
    const std::shared_ptr<TableSchema>& data_schema,
    const std::optional<std::vector<std::string>>& write_cols) {
    std::vector<DataField> projected_fields;
    const std::vector<std::string>& partition_keys = data_schema->PartitionKeys();
    if (write_cols == std::nullopt) {
        projected_fields = data_schema->Fields();
    } else {
        if (write_cols.value().empty()) {
            return Status::Invalid("write cols cannot be empty");
        }
        for (const auto& write_col : write_cols.value()) {
            if (write_col == SpecialFields::RowId().Name() ||
                write_col == SpecialFields::SequenceNumber().Name()) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, data_schema->GetField(write_col));
            projected_fields.push_back(field);
        }
        for (const auto& partition_key : partition_keys) {
            if (!ObjectUtils::Contains(write_cols.value(), partition_key)) {
                PAIMON_ASSIGN_OR_RAISE(DataField partition_field,
                                       data_schema->GetField(partition_key));
                projected_fields.push_back(partition_field);
            }
        }
    }

    projected_fields.push_back(SpecialFields::RowId());
    projected_fields.push_back(SpecialFields::SequenceNumber());
    return projected_fields;
}

}  // namespace paimon
