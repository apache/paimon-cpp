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

#include "paimon/core/io/data_file_index_writer.h"

#include <cassert>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
Result<std::unique_ptr<DataFileIndexWriter>> DataFileIndexWriter::Create(
    const std::shared_ptr<arrow::Schema>& logical_schema, const FileIndexOptions& options,
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& pool) {
    assert(logical_schema);
    assert(file_system);
    assert(path_factory);
    assert(pool);
    std::vector<IndexWriterEntry> writers;
    writers.reserve(options.Definitions().size());
    for (const FileIndexDefinition& definition : options.Definitions()) {
        int32_t field_index = logical_schema->GetFieldIndex(definition.column_name);
        if (field_index < 0) {
            return Status::Invalid(
                fmt::format("File index column '{}' does not exist in the write schema",
                            definition.column_name));
        }
        std::shared_ptr<arrow::Field> field = logical_schema->field(field_index);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexer> indexer,
                               FileIndexerFactory::Get(definition.index_type, definition.options));
        if (!indexer) {
            return Status::Invalid(
                fmt::format("File index type '{}' is not registered", definition.index_type));
        }
        ::ArrowSchema c_schema;
        ArrowSchemaMarkReleased(&c_schema);
        ScopeGuard schema_guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow::schema({field}), &c_schema));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexWriter> writer,
                               indexer->CreateWriter(&c_schema, pool));
        writers.push_back(
            {definition.column_name, definition.index_type, field_index, field, std::move(writer)});
    }
    return std::unique_ptr<DataFileIndexWriter>(new DataFileIndexWriter(
        std::move(writers), options.InManifestThreshold(), file_system, path_factory, pool));
}

DataFileIndexWriter::DataFileIndexWriter(std::vector<IndexWriterEntry>&& writers,
                                         int64_t in_manifest_threshold,
                                         const std::shared_ptr<FileSystem>& file_system,
                                         const std::shared_ptr<DataFilePathFactory>& path_factory,
                                         const std::shared_ptr<MemoryPool>& pool)
    : writers_(std::move(writers)),
      in_manifest_threshold_(in_manifest_threshold),
      file_system_(file_system),
      path_factory_(path_factory),
      pool_(pool) {}

Status DataFileIndexWriter::AddBatch(const std::shared_ptr<arrow::StructArray>& logical_batch) {
    for (IndexWriterEntry& entry : writers_) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> projected,
            arrow::StructArray::Make({logical_batch->field(entry.field_index)}, {entry.field}));
        ::ArrowArray c_array;
        ArrowArrayMarkReleased(&c_array);
        ScopeGuard array_guard([&c_array]() { ArrowArrayRelease(&c_array); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*projected, &c_array));
        PAIMON_RETURN_NOT_OK(entry.writer->AddBatch(&c_array));
    }
    return Status::OK();
}

Result<std::shared_ptr<Bytes>> DataFileIndexWriter::SerializeContainer() {
    FileIndexFormat::ColumnIndexes column_indexes;
    for (IndexWriterEntry& entry : writers_) {
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> serialized,
                               entry.writer->SerializedBytes());
        column_indexes[entry.column_name][entry.index_type] =
            std::shared_ptr<Bytes>(std::move(serialized));
    }

    std::shared_ptr<ByteArrayOutputStream> output = std::make_shared<ByteArrayOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Writer> format_writer,
                           FileIndexFormat::CreateWriter(output, pool_));
    PAIMON_RETURN_NOT_OK(format_writer->WriteColumnIndexes(column_indexes));
    PAIMON_RETURN_NOT_OK(format_writer->Close());
    return output->Finish();
}

Result<FileIndexWriteResult> DataFileIndexWriter::Finish(const std::string& data_file_path) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, SerializeContainer());
    if (static_cast<int64_t>(bytes->size()) <= in_manifest_threshold_) {
        return FileIndexWriteResult{bytes, {}};
    }

    external_index_path_ = path_factory_->ToFileIndexPath(data_file_path);
    PAIMON_RETURN_NOT_OK(WriteExternal(external_index_path_.value(), bytes));
    return FileIndexWriteResult{nullptr, {PathUtil::GetName(external_index_path_.value())}};
}

Status DataFileIndexWriter::WriteExternal(const std::string& path,
                                          const std::shared_ptr<Bytes>& bytes) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                           file_system_->Create(path, /*overwrite=*/false));
    ScopeGuard guard([this, &output]() {
        [[maybe_unused]] Status _ = output->Close();
        Abort();
    });
    PAIMON_ASSIGN_OR_RAISE(int64_t written,
                           output->Write(bytes->data(), static_cast<int64_t>(bytes->size())));
    if (written != static_cast<int64_t>(bytes->size())) {
        return Status::IOError(fmt::format("Short write for file index {}: expected {}, wrote {}",
                                           path, bytes->size(), written));
    }
    PAIMON_RETURN_NOT_OK(output->Flush());
    PAIMON_RETURN_NOT_OK(output->Close());
    output.reset();
    guard.Release();
    return Status::OK();
}

void DataFileIndexWriter::Abort() {
    if (external_index_path_) {
        [[maybe_unused]] Status _ = file_system_->Delete(external_index_path_.value());
    }
}

}  // namespace paimon
