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
#include <utility>

#include "arrow/array/array_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/columnar/columnar_row.h"
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
#include "paimon/status.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;

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
                                       std::vector<ManifestEntry>* entries) const {
    return ReadArrowBatches(
        file_name,
        [this, bucket, entries](const std::shared_ptr<arrow::StructArray>& batch) -> Status {
            const arrow::ArrayVector& fields = batch->fields();
            ColumnarRow row(fields, pool_, /*row_id=*/0);
            for (int64_t i = 0; i < batch->length(); i++) {
                row.SetRowId(i);
                PAIMON_RETURN_NOT_OK(ManifestEntrySerializer::ValidateVersion(row.GetInt(0)));
                if (ManifestEntrySerializer::GetBucket(row) != bucket) {
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer_->FromRow(row));
                entries->push_back(std::move(entry));
            }
            return Status::OK();
        },
        [this, bucket](FileBatchReader* reader) { return PrepareBucketRead(reader, bucket); });
}

Status ManifestFile::PrepareBucketRead(FileBatchReader* reader, int32_t bucket) const {
    if (!reader->SupportPreciseBitmapSelection()) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ArrowSchema> c_schema, reader->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                      arrow::ImportSchema(c_schema.get()));
    const auto& target_type = serializer_->GetDataType();
    const std::string& version_name = target_type->field(0)->name();
    const std::string& bucket_name = target_type->field(3)->name();
    auto version_field = file_schema->GetFieldByName(version_name);
    auto bucket_field = file_schema->GetFieldByName(bucket_name);
    if (!version_field || !bucket_field || version_field->type()->id() != arrow::Type::INT32 ||
        bucket_field->type()->id() != arrow::Type::INT32) {
        return Status::OK();
    }
    arrow::FieldVector probe_fields;
    for (const auto& field : file_schema->fields()) {
        if (field->name() == version_name || field->name() == bucket_name) {
            probe_fields.push_back(field);
        }
    }
    auto probe_schema = arrow::schema(probe_fields);
    ArrowSchema probe_c_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*probe_schema, &probe_c_schema));
    PAIMON_RETURN_NOT_OK(
        reader->SetReadSchema(&probe_c_schema, /*predicate=*/nullptr, std::nullopt));
    RoaringBitmap32 selected;
    bool row_ids_fit = true;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(batch.first.get(), batch.second.get()));
        if (!array || array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("Manifest bucket probe must return a struct array");
        }
        auto rows = checked_pointer_cast<arrow::StructArray>(array);
        auto version_array = rows->GetFieldByName(version_name);
        auto bucket_array = rows->GetFieldByName(bucket_name);
        if (!version_array || !bucket_array || version_array->type_id() != arrow::Type::INT32 ||
            bucket_array->type_id() != arrow::Type::INT32) {
            return Status::Invalid("Manifest bucket probe must return int32 version and bucket");
        }
        auto versions = checked_pointer_cast<arrow::Int32Array>(version_array);
        auto buckets = checked_pointer_cast<arrow::Int32Array>(bucket_array);
        for (int64_t i = 0; i < rows->length(); ++i) {
            if (rows->IsNull(i) || versions->IsNull(i) || buckets->IsNull(i)) {
                return Status::Invalid("Manifest version and bucket must not be null");
            }
            // Validate every entry, including buckets not selected by this scan.
            PAIMON_RETURN_NOT_OK(ManifestEntrySerializer::ValidateVersion(versions->Value(i)));
            if (buckets->Value(i) == bucket) {
                PAIMON_ASSIGN_OR_RAISE(uint64_t file_row, reader->GetPreviousBatchFileRowId(i));
                if (file_row > std::numeric_limits<uint32_t>::max()) {
                    row_ids_fit = false;
                } else {
                    selected.Add(static_cast<uint32_t>(file_row));
                }
            }
        }
    }
    // Keep the on-disk schema; ManifestMetaReader still performs schema evolution afterwards.
    ArrowSchema full_c_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*file_schema, &full_c_schema));
    return reader->SetReadSchema(
        &full_c_schema, /*predicate=*/nullptr,
        row_ids_fit ? std::optional<RoaringBitmap32>(std::move(selected)) : std::nullopt);
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
