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

#include "paimon/core/io/arrow_ipc_file.h"

#include <utility>

#include "arrow/ipc/api.h"
#include "arrow/util/compression.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_output_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"

namespace paimon {

ArrowIpcFileWriter::ArrowIpcFileWriter(const std::shared_ptr<FileSystem>& fs, std::string path,
                                       const std::shared_ptr<arrow::Schema>& schema,
                                       std::string compression, int32_t compression_level,
                                       bool use_threads,
                                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : fs_(fs),
      path_(std::move(path)),
      schema_(schema),
      compression_(std::move(compression)),
      compression_level_(compression_level),
      use_threads_(use_threads),
      arrow_pool_(arrow_pool) {}

ArrowIpcFileWriter::~ArrowIpcFileWriter() {
    [[maybe_unused]] Status status = Close();
}

Result<std::unique_ptr<ArrowIpcFileWriter>> ArrowIpcFileWriter::Create(
    const std::shared_ptr<FileSystem>& fs, const std::string& path,
    const std::shared_ptr<arrow::Schema>& schema, const std::string& compression,
    int32_t compression_level, bool use_threads,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (!fs || !schema || !arrow_pool || path.empty()) {
        return Status::Invalid("invalid Arrow IPC file writer arguments");
    }
    std::unique_ptr<ArrowIpcFileWriter> writer(new ArrowIpcFileWriter(
        fs, path, schema, compression, compression_level, use_threads, arrow_pool));
    PAIMON_RETURN_NOT_OK(writer->Open());
    return writer;
}

Status ArrowIpcFileWriter::Open() {
    auto cleanup_guard = ScopeGuard([&]() {
        arrow_writer_.reset();
        arrow_output_stream_adapter_.reset();
        if (out_stream_) {
            [[maybe_unused]] Status status = out_stream_->Close();
            out_stream_.reset();
        }
        [[maybe_unused]] Status status = fs_->Delete(path_);
    });
    auto write_options = arrow::ipc::IpcWriteOptions::Defaults();
    write_options.memory_pool = arrow_pool_.get();
    write_options.use_threads = use_threads_;
    PAIMON_ASSIGN_OR_RAISE(arrow::Compression::type arrow_compression,
                           ArrowUtils::GetCompressionType(compression_));
    if (!arrow::util::Codec::SupportsCompressionLevel(arrow_compression)) {
        compression_level_ = arrow::util::Codec::UseDefaultCompressionLevel();
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        write_options.codec, arrow::util::Codec::Create(arrow_compression, compression_level_));
    PAIMON_ASSIGN_OR_RAISE(out_stream_, fs_->Create(path_, /*overwrite=*/false));
    arrow_output_stream_adapter_ = std::make_shared<ArrowOutputStreamAdapter>(out_stream_);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow_writer_,
        arrow::ipc::MakeFileWriter(arrow_output_stream_adapter_, schema_, write_options));
    cleanup_guard.Release();
    return Status::OK();
}

Status ArrowIpcFileWriter::WriteBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (closed_ || !arrow_writer_) {
        return Status::Invalid("Arrow IPC file writer is closed");
    }
    if (!batch) {
        return Status::Invalid("Arrow IPC record batch is null");
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow_writer_->WriteRecordBatch(*batch));
    return Status::OK();
}

Status ArrowIpcFileWriter::Close() {
    if (closed_) {
        return Status::OK();
    }
    if (arrow_writer_) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow_writer_->Close());
        arrow_writer_.reset();
    }
    if (out_stream_) {
        PAIMON_RETURN_NOT_OK(out_stream_->Close());
        out_stream_.reset();
    }
    closed_ = true;
    return Status::OK();
}

Result<int64_t> ArrowIpcFileWriter::GetFileSize() const {
    if (!closed_ && arrow_output_stream_adapter_) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(int64_t size, arrow_output_stream_adapter_->Tell());
        return size;
    }
    PAIMON_ASSIGN_OR_RAISE(FileStatus status, fs_->GetFileStatus(path_));
    return status.GetLen();
}

ArrowIpcFileReader::ArrowIpcFileReader(const std::shared_ptr<FileSystem>& fs, std::string path,
                                       bool use_threads,
                                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : fs_(fs), path_(std::move(path)), use_threads_(use_threads), arrow_pool_(arrow_pool) {}

ArrowIpcFileReader::~ArrowIpcFileReader() {
    Close();
}

Result<std::unique_ptr<ArrowIpcFileReader>> ArrowIpcFileReader::Open(
    const std::shared_ptr<FileSystem>& fs, const std::string& path, bool use_threads,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (!fs || !arrow_pool || path.empty()) {
        return Status::Invalid("invalid Arrow IPC file reader arguments");
    }
    std::unique_ptr<ArrowIpcFileReader> reader(
        new ArrowIpcFileReader(fs, path, use_threads, arrow_pool));
    PAIMON_RETURN_NOT_OK(reader->DoOpen());
    return reader;
}

Status ArrowIpcFileReader::DoOpen() {
    PAIMON_ASSIGN_OR_RAISE(in_stream_, fs_->Open(path_));
    PAIMON_ASSIGN_OR_RAISE(FileStatus status, fs_->GetFileStatus(path_));
    arrow_input_stream_adapter_ =
        std::make_shared<ArrowInputStreamAdapter>(in_stream_, status.GetLen(), arrow_pool_);
    auto read_options = arrow::ipc::IpcReadOptions::Defaults();
    read_options.memory_pool = arrow_pool_.get();
    read_options.use_threads = use_threads_;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow_reader_,
        arrow::ipc::RecordBatchFileReader::Open(arrow_input_stream_adapter_, read_options));
    record_batch_count_ = arrow_reader_->num_record_batches();
    return Status::OK();
}

int32_t ArrowIpcFileReader::GetRecordBatchCount() const {
    return record_batch_count_;
}

Result<std::shared_ptr<arrow::RecordBatch>> ArrowIpcFileReader::ReadRecordBatch(
    int32_t batch_index) {
    if (closed_ || !arrow_reader_) {
        return Status::Invalid("Arrow IPC file reader is closed");
    }
    if (batch_index < 0 || batch_index >= record_batch_count_) {
        return Status::Invalid("Arrow IPC record batch index out of range: ", batch_index);
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::RecordBatch> batch,
                                      arrow_reader_->ReadRecordBatch(batch_index));
    return batch;
}

void ArrowIpcFileReader::Close() {
    if (closed_) {
        return;
    }
    arrow_reader_.reset();
    arrow_input_stream_adapter_.reset();
    if (in_stream_) {
        [[maybe_unused]] Status status = in_stream_->Close();
        in_stream_.reset();
    }
    closed_ = true;
}

}  // namespace paimon
