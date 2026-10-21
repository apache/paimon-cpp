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
#include "paimon/common/data/serializer/binary_serializer_utils.h"
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
#include "paimon/utils/row_range_index.h"

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
                                 path_factory, /*cache=*/nullptr, pool),
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

Status ManifestFile::ReadRows(
    const std::string& file_name, std::optional<int64_t> file_size,
    const IndexedManifestEntries::Filter& filter,
    const std::function<Status(const InternalRow&)>& consumer,
    const std::function<Status(std::unique_ptr<FileBatchReader>*)>& prepare_reader) const {
    auto consume_batch = [&](const std::shared_ptr<arrow::StructArray>& batch) -> Status {
        ColumnarRow row(batch->fields(), pool_, /*row_id=*/0);
        for (int64_t i = 0; i < batch->length(); ++i) {
            row.SetRowId(i);
            PAIMON_RETURN_NOT_OK(ManifestEntrySerializer::ValidateVersion(row.GetInt(0)));
            const bool historical =
                filter.total_buckets &&
                (row.IsNullAt(3) || row.IsNullAt(4) || row.GetInt(4) != *filter.total_buckets);
            if (filter.bucket && !historical && row.GetInt(3) != *filter.bucket) {
                continue;
            }
            if (filter.partition) {
                PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                                       ManifestEntrySerializer::GetPartitionView(row));
                PAIMON_ASSIGN_OR_RAISE(bool keep, filter.partition(partition));
                if (!keep) {
                    continue;
                }
            }
            PAIMON_RETURN_NOT_OK(consumer(row));
        }
        return Status::OK();
    };
    const auto& cache = options_.GetCache();
    if (!cache) {
        return ReadUncachedArrowBatches(file_name, file_size, consume_batch, prepare_reader);
    }
    // Preserve whole-file invalidation keys. The versioned value rejects old IPC entries.
    // Manifest lists and index manifests still use ObjectsFile's IPC cache; data manifests only
    // publish the complete indexed file, independent of snapshots and query filters.
    auto key = CacheKey::ForKind(path_factory_->ToPath(file_name), /*position=*/0,
                                 /*length=*/-1, CacheKind::MANIFEST);
    std::optional<Status> read_status;
    std::vector<std::shared_ptr<arrow::StructArray>> batches;
    auto load = [&](const std::shared_ptr<CacheKey>&) -> Result<std::shared_ptr<CacheValue>> {
        IndexedManifestEntries index(pool_);
        Status serialization_status;
        read_status = ReadUncachedArrowBatches(
            file_name, file_size,
            [&](const std::shared_ptr<arrow::StructArray>& batch) -> Status {
                batches.push_back(batch);
                if (!serialization_status.ok()) {
                    return Status::OK();
                }
                auto encode = [&]() -> Status {
                    auto row = std::make_shared<ColumnarRow>(batch->fields(), pool_, 0);
                    for (int64_t i = 0; i < batch->length(); ++i) {
                        row->SetRowId(i);
                        // The canonical manifest schema stores creation_time in milliseconds.
                        // Avoid narrowing a non-canonical physical timestamp in the cache.
                        if (!row->IsNullAt(ManifestEntry::kFileFieldIndex + 1)) {
                            auto file = row->GetRow(ManifestEntry::kFileFieldIndex + 1,
                                                    DataFileMeta::DataType()->num_fields());
                            if (!file->IsNullAt(12) &&
                                file->GetTimestamp(12, 3).GetNanoOfMillisecond() != 0) {
                                return Status::Invalid(
                                    "manifest cache requires millisecond creation_time");
                            }
                        }
                        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BinaryRow> binary,
                                               BinarySerializerUtils::WriteBinaryRow(
                                                   row, serializer_->GetDataType(), pool_.get()));
                        PAIMON_RETURN_NOT_OK(index.Add(*binary));
                    }
                    return Status::OK();
                };
                serialization_status = encode();
                return Status::OK();
            },
            /*prepare_reader=*/nullptr);
        PAIMON_RETURN_NOT_OK(*read_status);
        PAIMON_RETURN_NOT_OK(serialization_status);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, index.Serialize(pool_));
        return std::make_shared<CacheValue>(MemorySegment::Wrap(bytes), CacheCallback(),
                                            bytes->size() + sizeof(Bytes));
    };
    auto cached = cache->Get(key, load);
    if (!read_status && cached.ok() && cached.value()) {
        bool consumer_or_version_error = false;
        auto selected = IndexedManifestEntries::Read(cached.value()->GetSegment(), filter, consumer,
                                                     &consumer_or_version_error);
        if (selected.ok() || consumer_or_version_error) {
            return selected.status();
        }
        // A malformed or obsolete private cache value is a miss. Predicate errors above are
        // returned directly and must never be retried against the source.
        cache->Invalidate(key);
        auto rebuilt = load(key);
        if (rebuilt.ok()) {
            auto status = cache->Put(key, rebuilt.value());
            (void)status;
        }
    }
    if (read_status) {
        PAIMON_RETURN_NOT_OK(*read_status);
        for (const auto& batch : batches) {
            PAIMON_RETURN_NOT_OK(consume_batch(batch));
        }
        return Status::OK();
    }
    return ReadUncachedArrowBatches(file_name, file_size, consume_batch, prepare_reader);
}

Status ManifestFile::Read(const std::string& file_name,
                          const std::function<Result<bool>(const ManifestEntry&)>& filter,
                          std::optional<int64_t> file_size,
                          std::vector<ManifestEntry>* entries) const {
    return ReadSelectedEntries(file_name, file_size, nullptr, nullptr, filter, entries);
}

Status ManifestFile::ReadBucketEntries(const std::string& file_name, int32_t bucket,
                                       const std::optional<int32_t>& expected_total_buckets,
                                       std::optional<int64_t> file_size,
                                       std::vector<ManifestEntry>* entries) const {
    IndexedManifestEntries::Filter filter;
    filter.bucket = bucket;
    filter.total_buckets = expected_total_buckets;
    return ReadRows(
        file_name, file_size, filter,
        [&](const InternalRow& row) -> Status {
            if (row.IsNullAt(ManifestEntry::kFileFieldIndex + 1)) {
                return Status::Invalid(
                    "ManifestEntry convert from row failed, with null DataFileMeta");
            }
            PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer_->FromRow(row));
            entries->push_back(std::move(entry));
            return Status::OK();
        },
        [this, bucket, expected_total_buckets](std::unique_ptr<FileBatchReader>* reader) {
            return PrepareBucketRead(bucket, expected_total_buckets, reader);
        });
}

Status ManifestFile::ReadSelectedEntries(
    const std::string& file_name, std::optional<int64_t> file_size, const RowRangeIndex* row_ranges,
    const std::function<Result<bool>(const InternalRow&)>& row_filter,
    const std::function<Result<bool>(const ManifestEntry&)>& entry_filter,
    std::vector<ManifestEntry>* entries, const IndexedManifestEntries::Filter& filter) const {
    return ReadRows(
        file_name, file_size, filter,
        [&](const InternalRow& row) -> Status {
            if (row_ranges) {
                if (row.IsNullAt(ManifestEntry::kFileFieldIndex + 1)) {
                    return Status::Invalid(
                        "ManifestEntry convert from row failed, with null DataFileMeta");
                }
                auto file = row.GetRow(ManifestEntry::kFileFieldIndex + 1,
                                       DataFileMeta::DataType()->num_fields());
                if (!file->IsNullAt(DataFileMeta::kFirstRowIdFieldIndex) &&
                    !file->IsNullAt(DataFileMeta::kRowCountFieldIndex)) {
                    const int64_t first = file->GetLong(DataFileMeta::kFirstRowIdFieldIndex);
                    const int64_t count = file->GetLong(DataFileMeta::kRowCountFieldIndex);
                    if (first >= 0 && count > 0 &&
                        first <= std::numeric_limits<int64_t>::max() - (count - 1) &&
                        !row_ranges->Intersects(first, first + (count - 1))) {
                        return Status::OK();
                    }
                }
            }
            if (row_filter) {
                PAIMON_ASSIGN_OR_RAISE(bool keep, row_filter(row));
                if (!keep) {
                    return Status::OK();
                }
            }
            if (row.IsNullAt(ManifestEntry::kFileFieldIndex + 1)) {
                return Status::Invalid(
                    "ManifestEntry convert from row failed, with null DataFileMeta");
            }
            PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer_->FromRow(row));
            if (entry_filter) {
                PAIMON_ASSIGN_OR_RAISE(bool keep, entry_filter(entry));
                if (!keep) {
                    return Status::OK();
                }
            }
            entries->push_back(std::move(entry));
            return Status::OK();
        },
        /*prepare_reader=*/nullptr);
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
    std::shared_ptr<Predicate> selector =
        PredicateBuilder::Equal(kBucketFieldIndex, bucket_name, FieldType::INT, Literal(bucket));
    if (expected_total_buckets) {
        const std::string& total_name = target_type->field(kTotalBucketsFieldIndex)->name();
        PAIMON_ASSIGN_OR_RAISE(
            selector,
            PredicateBuilder::Or(
                {selector,
                 PredicateBuilder::IsNull(kTotalBucketsFieldIndex, total_name, FieldType::INT),
                 PredicateBuilder::NotEqual(kTotalBucketsFieldIndex, total_name, FieldType::INT,
                                            Literal(expected_total_buckets.value()))}));
    }
    // Retain unsupported versions regardless of bucket so the consumer validates every version
    // before bucket filtering, including when the probe would otherwise select no entries.
    const std::string& version_name = target_type->field(kVersionFieldIndex)->name();
    PAIMON_ASSIGN_OR_RAISE(
        selector,
        PredicateBuilder::Or(
            {selector, PredicateBuilder::IsNull(kVersionFieldIndex, version_name, FieldType::INT),
             PredicateBuilder::NotEqual(
                 kVersionFieldIndex, version_name, FieldType::INT,
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
