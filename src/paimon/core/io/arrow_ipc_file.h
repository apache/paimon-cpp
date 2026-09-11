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

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class RecordBatch;
class Schema;
namespace ipc {
class RecordBatchFileReader;
class RecordBatchWriter;
}  // namespace ipc
}  // namespace arrow

namespace paimon {

class ArrowInputStreamAdapter;
class ArrowOutputStreamAdapter;
class FileSystem;
class InputStream;
class OutputStream;

/// Low-level Arrow IPC file writer shared by temporary spill implementations.
class ArrowIpcFileWriter {
 public:
    static Result<std::unique_ptr<ArrowIpcFileWriter>> Create(
        const std::shared_ptr<FileSystem>& fs, const std::string& path,
        const std::shared_ptr<arrow::Schema>& schema, const std::string& compression,
        int32_t compression_level, bool use_threads,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~ArrowIpcFileWriter();

    ArrowIpcFileWriter(const ArrowIpcFileWriter&) = delete;
    ArrowIpcFileWriter& operator=(const ArrowIpcFileWriter&) = delete;

    Status WriteBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    Status Close();
    Result<int64_t> GetFileSize() const;

 private:
    ArrowIpcFileWriter(const std::shared_ptr<FileSystem>& fs, std::string path,
                       const std::shared_ptr<arrow::Schema>& schema, std::string compression,
                       int32_t compression_level, bool use_threads,
                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Status Open();

    std::shared_ptr<FileSystem> fs_;
    std::string path_;
    std::shared_ptr<arrow::Schema> schema_;
    std::string compression_;
    int32_t compression_level_;
    bool use_threads_;
    std::shared_ptr<OutputStream> out_stream_;
    std::shared_ptr<ArrowOutputStreamAdapter> arrow_output_stream_adapter_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> arrow_writer_;
    bool closed_ = false;
};

/// One independently opened Arrow IPC file reader.
class ArrowIpcFileReader {
 public:
    static Result<std::unique_ptr<ArrowIpcFileReader>> Open(
        const std::shared_ptr<FileSystem>& fs, const std::string& path, bool use_threads,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    ~ArrowIpcFileReader();

    ArrowIpcFileReader(const ArrowIpcFileReader&) = delete;
    ArrowIpcFileReader& operator=(const ArrowIpcFileReader&) = delete;

    int32_t GetRecordBatchCount() const;
    Result<std::shared_ptr<arrow::RecordBatch>> ReadRecordBatch(int32_t batch_index);
    void Close();

 private:
    ArrowIpcFileReader(const std::shared_ptr<FileSystem>& fs, std::string path, bool use_threads,
                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool);

    Status DoOpen();

    std::shared_ptr<FileSystem> fs_;
    std::string path_;
    bool use_threads_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<InputStream> in_stream_;
    std::shared_ptr<ArrowInputStreamAdapter> arrow_input_stream_adapter_;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> arrow_reader_;
    int32_t record_batch_count_ = 0;
    bool closed_ = false;
};

}  // namespace paimon
