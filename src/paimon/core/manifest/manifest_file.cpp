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

#include "paimon/core/manifest/manifest_file.h"

#include <cassert>
#include <limits>
#include <optional>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/reader/late_materializing_file_batch_reader.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/core/manifest/manifest_entry_writer_factory.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/core/utils/versioned_object_serializer.h"
#include "paimon/format/file_format.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;

namespace {
constexpr int32_t kVersionFieldIndex = 0;
constexpr int32_t kBucketFieldIndex = 3;
constexpr int32_t kTotalBucketsFieldIndex = 4;
}  // namespace

ManifestFile::ManifestFile(const std::shared_ptr<FileSystem>& file_system,
                           const std::shared_ptr<ReaderBuilder>& reader_builder,
                           const std::shared_ptr<WriterBuilder>& writer_builder,
                           const std::string& file_format_identifier,
                           const std::string& compression,
                           const std::shared_ptr<PathFactory>& path_factory,
                           int64_t target_file_size, const std::shared_ptr<MemoryPool>& pool,
                           const CoreOptions& options,
                           const std::shared_ptr<arrow::Schema>& partition_type)
    : ObjectsFile<ManifestEntry>(file_system, reader_builder, writer_builder,
                                 file_format_identifier,
                                 std::make_unique<ManifestEntrySerializer>(pool), compression,
                                 path_factory, options.GetCache(), pool),
      target_file_size_(target_file_size),
      options_(options),
      partition_type_(partition_type) {}

Result<std::unique_ptr<ManifestFile>> ManifestFile::Create(
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<FileFormat>& file_format,
    const std::string& compression, const std::shared_ptr<FileStorePathFactory>& path_factory,
    int64_t target_file_size, const std::shared_ptr<MemoryPool>& pool, const CoreOptions& options,
    const std::shared_ptr<arrow::Schema>& partition_type) {
    assert(partition_type);
    std::shared_ptr<arrow::DataType> data_type =
        VersionedObjectSerializer<ManifestEntry>::VersionType(ManifestEntry::DataType());

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReaderBuilder> reader_builder,
                           file_format->CreateReaderBuilder(options.GetReadBatchSize()));
    reader_builder->WithMemoryPool(pool);
    ArrowSchema schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*data_type, &schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<WriterBuilder> writer_builder,
                           file_format->CreateWriterBuilder(&schema, options.GetWriteBatchSize()));
    writer_builder->WithMemoryPool(pool);

    std::shared_ptr<PathFactory> manifest_file_factory = path_factory->CreateManifestFileFactory();
    return std::unique_ptr<ManifestFile>(new ManifestFile(
        file_system, reader_builder, writer_builder, file_format->Identifier(), compression,
        manifest_file_factory, target_file_size, pool, options, partition_type));
}

Status ManifestFile::ReadBucketEntries(const std::string& file_name, int32_t bucket,
                                       const std::optional<int32_t>& expected_total_buckets,
                             std::optional<int64_t> file_size,
                                       std::vector<ManifestEntry>* entries) const {
    // Readers without precise bitmap selection still filter aligned Arrow columns
    // before constructing ManifestEntry and DataFileMeta objects.
    return ReadArrowBatches(
        file_name, file_size,
        [this, bucket, expected_total_buckets,
         entries](const std::shared_ptr<arrow::StructArray>& batch) -> Status {
            ColumnarRow row(batch->fields(), pool_, /*row_id=*/0);
            for (int64_t i = 0; i < batch->length(); ++i) {
                row.SetRowId(i);
                PAIMON_RETURN_NOT_OK(
                    ManifestEntrySerializer::ValidateVersion(row.GetInt(kVersionFieldIndex)));
                // Different or unknown bucket counts must reach the compatibility checks.
                const bool historical_layout =
                    expected_total_buckets &&
                    (row.IsNullAt(kBucketFieldIndex) || row.IsNullAt(kTotalBucketsFieldIndex) ||
                     row.GetInt(kTotalBucketsFieldIndex) != expected_total_buckets.value());
                if (!historical_layout && ManifestEntrySerializer::GetBucket(row) != bucket) {
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer_->FromRow(row));
                entries->push_back(std::move(entry));
            }
            return Status::OK();
        },
        [this, bucket, expected_total_buckets](std::unique_ptr<FileBatchReader>* reader) {
            return PrepareBucketRead(bucket, expected_total_buckets, reader);
        });
}

Status ManifestFile::PrepareBucketRead(int32_t bucket,
                                       const std::optional<int32_t>& expected_total_buckets,
                                       std::unique_ptr<FileBatchReader>* reader) const {
    if (!(*reader)->SupportPreciseBitmapSelection()) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ArrowSchema> c_schema, (*reader)->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                      arrow::ImportSchema(c_schema.get()));
    const auto& target_type = serializer_->GetDataType();
    const std::string& bucket_name = target_type->field(kBucketFieldIndex)->name();
    std::shared_ptr<Predicate> selector = PredicateBuilder::Equal(
        file_schema->GetFieldIndex(bucket_name), bucket_name, FieldType::INT, Literal(bucket));
    if (expected_total_buckets) {
        const std::string& total_name = target_type->field(kTotalBucketsFieldIndex)->name();
        const int32_t total_index = file_schema->GetFieldIndex(total_name);
        PAIMON_ASSIGN_OR_RAISE(
            selector,
            PredicateBuilder::Or(
                {selector, PredicateBuilder::IsNull(total_index, total_name, FieldType::INT),
                 PredicateBuilder::NotEqual(total_index, total_name, FieldType::INT,
                                            Literal(expected_total_buckets.value()))}));
    }
    // Retain unsupported versions regardless of bucket so the consumer validates every version
    // before bucket filtering, including when the probe would otherwise select no entries.
    const std::string& version_name = target_type->field(kVersionFieldIndex)->name();
    const int32_t version_index = file_schema->GetFieldIndex(version_name);
    PAIMON_ASSIGN_OR_RAISE(
        selector,
        PredicateBuilder::Or(
            {selector, PredicateBuilder::IsNull(version_index, version_name, FieldType::INT),
             PredicateBuilder::NotEqual(
                 version_index, version_name, FieldType::INT,
                 Literal(
                     checked_cast<ManifestEntrySerializer*>(serializer_.get())->GetVersion()))}));
    if (!PredicateValidator::ValidatePredicateWithSchema(*file_schema, selector,
                                                         /*validate_field_idx=*/true)
             .ok()) {
        // An incompatible projection must fall back to ordinary schema evolution.
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LateMaterializingFileBatchReader> selective_reader,
        LateMaterializingFileBatchReader::Create(std::move(*reader), arrow_pool_));
    // Keep the on-disk schema; ManifestMetaReader still performs schema evolution afterwards.
    ArrowSchema full_c_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema, &full_c_schema));
    PAIMON_RETURN_NOT_OK(selective_reader->SetReadSchema(&full_c_schema, selector, std::nullopt));
    *reader = std::move(selective_reader);
    return Status::OK();
}

Result<std::vector<ManifestFileMeta>> ManifestFile::Write(
    const std::vector<ManifestEntry>& entries) {
    if (entries.empty()) {
        return std::vector<ManifestFileMeta>();
    }
    PAIMON_RETURN_NOT_OK(ValidateWrite());
    auto converter = [this](ManifestEntry entry, ::ArrowArray* dest) -> Status {
        if (!to_array_converter_) {
            PAIMON_ASSIGN_OR_RAISE(to_array_converter_, MetaToArrowArrayConverter::Create(
                                                            serializer_->GetDataType(), pool_));
        }
        PAIMON_ASSIGN_OR_RAISE(BinaryRow entry_row, serializer_->ToRow(entry));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array,
                               to_array_converter_->NextBatch({entry_row}));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, dest));
        return Status::OK();
    };

    auto writer_factory = std::make_shared<ManifestEntryWriterFactory>(
        options_.GetManifestCompression(), converter, pool_, partition_type_,
        options_.GetFileSystem(), path_factory_, writer_builder_);
    std::unique_ptr<RollingFileWriter<const ManifestEntry&, ManifestFileMeta>> writer =
        std::make_unique<RollingFileWriter<const ManifestEntry&, ManifestFileMeta>>(
            target_file_size_, /*target_file_row_num=*/std::numeric_limits<int64_t>::max(),
            writer_factory);
    for (const auto& entry : entries) {
        auto s = writer->Write(entry);
        if (!s.ok()) {
            writer->Abort();
            return s;
        }
    }
    PAIMON_RETURN_NOT_OK(writer->Close());
    return writer->GetResult();
}

}  // namespace paimon
