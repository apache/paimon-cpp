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

#pragma once

#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "fmt/format.h"
#include "paimon/cache/cache.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/meta_to_arrow_array_converter.h"
#include "paimon/core/utils/manifest_meta_reader.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/record_batch.h"
#include "paimon/table/source/scan_metrics.h"

namespace paimon {
/// A file which contains several `T`s, provides read and write.
class PredicateFilter;
template <typename T>
class ObjectsFile {
 public:
    ObjectsFile(const std::shared_ptr<FileSystem>& file_system,
                const std::shared_ptr<ReaderBuilder>& reader_builder,
                const std::shared_ptr<WriterBuilder>& writer_builder,
                const std::string& file_format_identifier,
                std::unique_ptr<ObjectSerializer<T>>&& serializer, const std::string& compression,
                const std::shared_ptr<PathFactory>& path_factory,
                const std::shared_ptr<Cache>& cache, const std::shared_ptr<MemoryPool>& pool);

    virtual ~ObjectsFile() = default;

    /// @param file_size Length of the file when planning already knows it, which lets the read
    ///                  skip the metadata request a bare `Open` issues on a remote store. Pass
    ///                  std::nullopt when the length is not known; the read then discovers it
    ///                  itself.
    Status Read(const std::string& file_name, const std::function<Result<bool>(const T&)>& filter,
                std::optional<int64_t> file_size, std::vector<T>* result) const;
    Status ReadIfFileExist(const std::string& file_name,
                           const std::function<Result<bool>(const T&)>& filter,
                           std::optional<int64_t> file_size, std::vector<T>* result) const;

    void DeleteQuietly(const std::string& file_name) {
        std::string path = path_factory_->ToPath(file_name);
        auto status = file_system_->Delete(path);
        // delete quietly will ignore any status error
        (void)status;
    }

    Result<std::pair<std::string, int64_t>> WriteWithoutRolling(const std::vector<T>& records);

    /// Cumulative decoded cache hits and misses, including concurrent reads.
    std::shared_ptr<Metrics> GetReadMetrics() const {
        auto snapshot = std::make_shared<MetricsImpl>();
        snapshot->Overwrite(read_metrics_);
        return snapshot;
    }

 protected:
    Status ValidateWrite() const {
        if (file_format_identifier_ != "avro") {
            return Status::Invalid("manifest.format '", file_format_identifier_,
                                   "' is read-only; only 'avro' can be used for writing manifests");
        }
        return Status::OK();
    }

    // Cached batches are query-independent. Optional preparation only applies to uncached reads.
    Status ReadArrowBatches(
        const std::string& file_name, std::optional<int64_t> file_size,
        const std::function<Status(const std::shared_ptr<arrow::StructArray>&)>& consumer,
        const std::function<Status(std::unique_ptr<FileBatchReader>*)>& prepare_reader) const;

    std::shared_ptr<MetricsImpl> read_metrics_ = std::make_shared<MetricsImpl>();
    std::shared_ptr<PathFactory> path_factory_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::unique_ptr<ObjectSerializer<T>> serializer_;
    std::shared_ptr<WriterBuilder> writer_builder_;
    std::unique_ptr<MetaToArrowArrayConverter> to_array_converter_;

 private:
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<ReaderBuilder> reader_builder_;
    const std::string file_format_identifier_;
    std::string compression_;
    std::shared_ptr<Cache> cache_;

    Result<std::shared_ptr<CacheValue>> SerializeArrowBatches(
        const std::string& file_name, std::optional<int64_t> file_size,
        std::vector<std::shared_ptr<arrow::StructArray>>* batches,
        std::optional<Status>* read_status) const;

    Status ReadUncachedArrowBatches(
        const std::string& file_name, std::optional<int64_t> file_size,
        const std::function<Status(const std::shared_ptr<arrow::StructArray>&)>& consumer,
        const std::function<Status(std::unique_ptr<FileBatchReader>*)>& prepare_reader) const;

    /// Opens the file for reading, handing over the length when the caller already has it.
    Result<std::unique_ptr<InputStream>> OpenForRead(const std::string& file_path,
                                                     const std::optional<int64_t>& file_size) const;
};

template <typename T>
ObjectsFile<T>::ObjectsFile(const std::shared_ptr<FileSystem>& file_system,
                            const std::shared_ptr<ReaderBuilder>& reader_builder,
                            const std::shared_ptr<WriterBuilder>& writer_builder,
                            const std::string& file_format_identifier,
                            std::unique_ptr<ObjectSerializer<T>>&& serializer,
                            const std::string& compression,
                            const std::shared_ptr<PathFactory>& path_factory,
                            const std::shared_ptr<Cache>& cache,
                            const std::shared_ptr<MemoryPool>& pool)
    : path_factory_(path_factory),
      pool_(pool),
      arrow_pool_(GetArrowPool(pool)),
      serializer_(std::move(serializer)),
      writer_builder_(std::move(writer_builder)),
      file_system_(file_system),
      reader_builder_(std::move(reader_builder)),
      file_format_identifier_(file_format_identifier),
      compression_(compression),
      cache_(cache) {}

template <typename T>
Status ObjectsFile<T>::ReadIfFileExist(const std::string& file_name,
                                       const std::function<Result<bool>(const T&)>& filter,
                                       std::optional<int64_t> file_size,
                                       std::vector<T>* result) const {
    std::string file_path = path_factory_->ToPath(file_name);
    PAIMON_ASSIGN_OR_RAISE(bool path_exist, file_system_->Exists(file_path));
    if (path_exist) {
        return Read(file_name, filter, file_size, result);
    }
    return Status::OK();
}

template <typename T>
Status ObjectsFile<T>::Read(const std::string& file_name,
                            const std::function<Result<bool>(const T&)>& filter,
                            std::optional<int64_t> file_size, std::vector<T>* result) const {
    return ReadArrowBatches(
        file_name, file_size,
        [this, &filter, result](const std::shared_ptr<arrow::StructArray>& struct_array) -> Status {
            result->reserve(result->size() + struct_array->length());
            const arrow::ArrayVector& fields = struct_array->fields();
            ColumnarRow row(fields, pool_, /*row_id=*/0);
            for (int64_t i = 0; i < struct_array->length(); i++) {
                row.SetRowId(i);
                PAIMON_ASSIGN_OR_RAISE(T obj, serializer_->FromRow(row));
                if (filter) {
                    PAIMON_ASSIGN_OR_RAISE(bool filter_res, filter(obj));
                    if (filter_res) {
                        result->push_back(std::move(obj));
                    }
                } else {
                    result->push_back(std::move(obj));
                }
            }
            return Status::OK();
        },
        /*prepare_reader=*/nullptr);
}

template <typename T>
Status ObjectsFile<T>::ReadUncachedArrowBatches(
    const std::string& file_name, std::optional<int64_t> file_size,
    const std::function<Status(const std::shared_ptr<arrow::StructArray>&)>& consumer,
    const std::function<Status(std::unique_ptr<FileBatchReader>*)>& prepare_reader) const {
    std::string file_path = path_factory_->ToPath(file_name);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> file_input_stream,
                           OpenForRead(file_path, file_size));

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> batch_reader,
                           reader_builder_->Build(file_input_stream));
    if (prepare_reader) {
        PAIMON_RETURN_NOT_OK(prepare_reader(&batch_reader));
    }
    auto reader = std::make_unique<ManifestMetaReader>(std::move(batch_reader),
                                                       serializer_->GetDataType(), arrow_pool_);
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch arrow_array, reader->NextBatch());
        auto& c_array = arrow_array.first;
        auto& c_schema = arrow_array.second;
        if (!c_array) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> typed_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (!typed_array || typed_array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid(fmt::format("file {}, cannot cast to struct array", file_name));
        }
        std::shared_ptr<arrow::StructArray> struct_array =
            checked_pointer_cast<arrow::StructArray>(typed_array);
        PAIMON_RETURN_NOT_OK(consumer(struct_array));
    }
    return Status::OK();
}

template <typename T>
Result<std::unique_ptr<InputStream>> ObjectsFile<T>::OpenForRead(
    const std::string& file_path, const std::optional<int64_t>& file_size) const {
    if (file_size.has_value()) {
        // Planning already read this length out of the manifest metadata, and `Open(FileStatus)` is
        // documented to let the file system skip the metadata request a bare open issues. That
        // request is a round trip of its own on a remote store, paid before a single byte of the
        // file is read. The files here are written once and never rewritten, so a length recorded
        // at planning time cannot go stale underneath the read.
        return file_system_->Open(FileStatus(file_path, file_size.value()));
    }
    return file_system_->Open(file_path);
}

template <typename T>
Result<std::shared_ptr<CacheValue>> ObjectsFile<T>::SerializeArrowBatches(
    const std::string& file_name, std::optional<int64_t> file_size,
    std::vector<std::shared_ptr<arrow::StructArray>>* batches,
    std::optional<Status>* read_status) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::io::BufferOutputStream> output,
        arrow::io::BufferOutputStream::Create(4096, arrow_pool_.get()));
    auto write_options = arrow::ipc::IpcWriteOptions::Defaults();
    write_options.memory_pool = arrow_pool_.get();
    write_options.use_threads = false;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    Status cache_status;
    *read_status = ReadUncachedArrowBatches(
        file_name, file_size,
        [&](const std::shared_ptr<arrow::StructArray>& batch) -> Status {
            // The loading caller consumes these original batches, without an IPC round trip.
            batches->push_back(batch);
            if (!cache_status.ok()) {
                return Status::OK();
            }
            auto write_batch = [&]() -> Status {
                // Preserve physical types, including ORC nanosecond timestamps.
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::RecordBatch> record_batch,
                    arrow::RecordBatch::FromStructArray(batch, arrow_pool_.get()));
                if (!writer) {
                    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                        writer, arrow::ipc::MakeStreamWriter(output, record_batch->schema(),
                                                             write_options));
                }
                PAIMON_RETURN_NOT_OK_FROM_ARROW(writer->WriteRecordBatch(*record_batch));
                return Status::OK();
            };
            // Optional serialization failures must not interrupt the source read.
            cache_status = write_batch();
            return Status::OK();
        },
        /*prepare_reader=*/nullptr);
    PAIMON_RETURN_NOT_OK(read_status->value());
    PAIMON_RETURN_NOT_OK(cache_status);
    if (!writer) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            writer,
            arrow::ipc::MakeStreamWriter(
                output, arrow::schema(serializer_->GetDataType()->fields()), write_options));
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(writer->Close());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> buffer, output->Finish());
    if (buffer->size() > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Manifest Arrow cache entry exceeds the memory segment limit");
    }
    // Retain the IPC buffer and its allocator without copying the complete stream into Bytes.
    // The aliasing CacheValue keeps both alive, including after eviction with active readers.
    struct OwnedBuffer {
        OwnedBuffer(const std::shared_ptr<arrow::MemoryPool>& owner,
                    const std::shared_ptr<arrow::Buffer>& data)
            : pool(owner),
              buffer(data),
              value(MemorySegment::WrapView(reinterpret_cast<const char*>(data->data()),
                                            static_cast<int32_t>(data->size())),
                    CacheCallback()) {}
        std::shared_ptr<arrow::MemoryPool> pool;
        std::shared_ptr<arrow::Buffer> buffer;
        CacheValue value;
    };
    auto owner = std::make_shared<OwnedBuffer>(arrow_pool_, buffer);
    auto* value = &owner->value;
    return std::shared_ptr<CacheValue>(std::move(owner), value);
}

template <typename T>
Status ObjectsFile<T>::ReadArrowBatches(
    const std::string& file_name, std::optional<int64_t> file_size,
    const std::function<Status(const std::shared_ptr<arrow::StructArray>&)>& consumer,
    const std::function<Status(std::unique_ptr<FileBatchReader>*)>& prepare_reader) const {
    if (!cache_) {
        return ReadUncachedArrowBatches(file_name, file_size, consumer, prepare_reader);
    }
    auto metrics = std::make_shared<MetricsImpl>();
    ScopeGuard record_metrics([&]() { read_metrics_->Merge(metrics); });
    const std::string path = path_factory_->ToPath(file_name);
    auto key = CacheKey::ForKind(path, /*position=*/0, /*length=*/-1, CacheKind::MANIFEST);
    bool loaded = false;
    std::optional<Status> read_status;
    std::vector<std::shared_ptr<arrow::StructArray>> batches;
    auto cached = cache_->Get(key, [&](const std::shared_ptr<CacheKey>&) {
        loaded = true;
        return SerializeArrowBatches(file_name, file_size, &batches, &read_status);
    });
    const bool usable = cached.ok() && cached.value() && cached.value()->GetSegment().Data();
    if (usable) {
        metrics->SetCounter(loaded ? ScanMetrics::MANIFEST_ARROW_CACHE_MISSES
                                   : ScanMetrics::MANIFEST_ARROW_CACHE_HITS,
                            1);
    }
    if (read_status) {
        // Publish the query-independent cache before invoking user filters. A filter error
        // does not invalidate the cache or affect other readers of this file.
        PAIMON_RETURN_NOT_OK(read_status.value());
        for (const auto& batch : batches) {
            PAIMON_RETURN_NOT_OK(consumer(batch));
        }
        return Status::OK();
    }
    if (!usable) {
        return ReadUncachedArrowBatches(file_name, file_size, consumer, prepare_reader);
    }
    const auto& segment = cached.value()->GetSegment();
    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(segment.Data()),
                                                  segment.Size());
    auto input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto read_options = arrow::ipc::IpcReadOptions::Defaults();
    read_options.memory_pool = arrow_pool_.get();
    read_options.use_threads = false;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader,
        arrow::ipc::RecordBatchStreamReader::Open(input, read_options));
    while (true) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::RecordBatch> batch,
                                          reader->Next());
        if (!batch) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> array,
                                          batch->ToStructArray());
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> aligned,
                               ManifestMetaReader::AlignArrayWithSchema(
                                   array, serializer_->GetDataType(), arrow_pool_.get()));
        PAIMON_RETURN_NOT_OK(consumer(checked_pointer_cast<arrow::StructArray>(aligned)));
    }
    return Status::OK();
}

template <typename T>
Result<std::pair<std::string, int64_t>> ObjectsFile<T>::WriteWithoutRolling(
    const std::vector<T>& records) {
    PAIMON_RETURN_NOT_OK(ValidateWrite());
    std::string file_path = path_factory_->NewPath();
    std::vector<BinaryRow> rows;
    rows.reserve(records.size());
    for (const auto& record : records) {
        PAIMON_ASSIGN_OR_RAISE(BinaryRow row, serializer_->ToRow(record));
        rows.push_back(std::move(row));
    }
    if (!to_array_converter_) {
        PAIMON_ASSIGN_OR_RAISE(to_array_converter_, MetaToArrowArrayConverter::Create(
                                                        serializer_->GetDataType(), pool_));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array,
                           to_array_converter_->NextBatch(rows));
    ::ArrowArray c_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
    ScopeGuard guard([&]() {
        ArrowArrayRelease(&c_array);
        DeleteQuietly(file_path);
    });
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                           file_system_->Create(file_path, /*overwrite=*/false));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> format_writer,
                           writer_builder_->Build(out, compression_));
    PAIMON_RETURN_NOT_OK(format_writer->AddBatch(&c_array));
    PAIMON_RETURN_NOT_OK(format_writer->Flush());
    PAIMON_RETURN_NOT_OK(format_writer->Finish());
    PAIMON_RETURN_NOT_OK(out->Flush());
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, out->GetPos());
    PAIMON_RETURN_NOT_OK(out->Close());
    guard.Release();
    return std::make_pair(PathUtil::GetName(file_path), pos);
}

}  // namespace paimon
