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

#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <utility>

#include "arrow/api.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

namespace {

arrow::Status ValidateArrowIoRange(int64_t value, const char* name) {
    Status status = ValidateValueNonNegative(value, name);
    if (!status.ok()) {
        return ToArrowStatus(status);
    }
    return arrow::Status::OK();
}

struct BufferWithMemoryPool {
    std::shared_ptr<arrow::MemoryPool> pool;
    std::shared_ptr<arrow::Buffer> buffer;
};

std::shared_ptr<arrow::Buffer> KeepMemoryPoolAlive(std::shared_ptr<arrow::Buffer> buffer,
                                                   const std::shared_ptr<arrow::MemoryPool>& pool) {
    // TODO(lxy): Optimize the extra allocation introduced to retain the memory pool.
    auto holder =
        std::make_shared<BufferWithMemoryPool>(BufferWithMemoryPool{pool, std::move(buffer)});
    auto* buffer_ptr = holder->buffer.get();
    return std::shared_ptr<arrow::Buffer>(std::move(holder), buffer_ptr);
}

}  // namespace

ArrowInputStreamAdapter::ArrowInputStreamAdapter(
    const std::shared_ptr<paimon::InputStream>& input_stream, int64_t file_size,
    const std::shared_ptr<arrow::MemoryPool>& pool)
    : input_stream_(input_stream),
      pool_(pool),
      file_size_(file_size),
      storage_read_bytes_(std::make_shared<std::atomic<uint64_t>>(0)) {
    assert(file_size >= 0);
}

ArrowInputStreamAdapter::~ArrowInputStreamAdapter() {
    [[maybe_unused]] auto status = DoClose();
}

arrow::Status ArrowInputStreamAdapter::Seek(int64_t position) {
    return ToArrowStatus(input_stream_->Seek(position, SeekOrigin::FS_SEEK_SET));
}

arrow::Result<int64_t> ArrowInputStreamAdapter::Read(int64_t nbytes, void* out) {
    ARROW_RETURN_NOT_OK(ValidateArrowIoRange(nbytes, "nbytes"));
    Result<int64_t> read_bytes = input_stream_->Read(static_cast<char*>(out), nbytes);
    if (!read_bytes.ok()) {
        return ToArrowStatus(read_bytes.status());
    }
    if (storage_read_bytes_) {
        storage_read_bytes_->fetch_add(static_cast<uint64_t>(read_bytes.value()));
    }
    return read_bytes.value();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamAdapter::Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ResizableBuffer> buffer,
                          arrow::AllocateResizableBuffer(nbytes, pool_.get()));
    ARROW_ASSIGN_OR_RAISE(int64_t read_bytes, Read(nbytes, buffer->mutable_data()));
    if (read_bytes < nbytes) {
        ARROW_RETURN_NOT_OK(buffer->Resize(read_bytes));
    }
    return KeepMemoryPoolAlive(std::shared_ptr<arrow::Buffer>(std::move(buffer)), pool_);
}

arrow::Result<int64_t> ArrowInputStreamAdapter::ReadAt(int64_t position, int64_t nbytes,
                                                       void* out) {
    ARROW_RETURN_NOT_OK(ValidateArrowIoRange(position, "position"));
    ARROW_RETURN_NOT_OK(ValidateArrowIoRange(nbytes, "nbytes"));
    Result<int64_t> read_bytes = input_stream_->Read(static_cast<char*>(out), nbytes, position);
    if (!read_bytes.ok()) {
        return ToArrowStatus(read_bytes.status());
    }
    if (storage_read_bytes_) {
        storage_read_bytes_->fetch_add(static_cast<uint64_t>(read_bytes.value()));
    }
    return read_bytes.value();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamAdapter::ReadAt(int64_t position,
                                                                              int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ResizableBuffer> buffer,
                          arrow::AllocateResizableBuffer(nbytes, pool_.get()));
    ARROW_ASSIGN_OR_RAISE(int64_t read_bytes, ReadAt(position, nbytes, buffer->mutable_data()));
    if (read_bytes < nbytes) {
        ARROW_RETURN_NOT_OK(buffer->Resize(read_bytes));
    }
    return KeepMemoryPoolAlive(std::shared_ptr<arrow::Buffer>(std::move(buffer)), pool_);
}

arrow::Future<std::shared_ptr<arrow::Buffer>> ArrowInputStreamAdapter::ReadAsync(
    const arrow::io::IOContext&, int64_t position, int64_t nbytes, bool allow_short_read) {
    using ReadFuture = arrow::Future<std::shared_ptr<arrow::Buffer>>;
    if (closed()) {
        return ReadFuture::MakeFinished(arrow::Status::Invalid("Read on closed file"));
    }
    auto range_status = ValidateArrowIoRange(position, "position");
    if (!range_status.ok()) {
        return ReadFuture::MakeFinished(range_status);
    }
    range_status = ValidateArrowIoRange(nbytes, "nbytes");
    if (!range_status.ok()) {
        return ReadFuture::MakeFinished(range_status);
    }
    if (position > file_size_) {
        return ReadFuture::MakeFinished(
            arrow::Status::IOError("Read offset ", position, " exceeds file size ", file_size_));
    }

    // InputStream::ReadAsync requires an exact read and reports only status on completion.
    const int64_t read_size = std::min(nbytes, file_size_ - position);
    if (!allow_short_read && read_size < nbytes) {
        return ReadFuture::MakeFinished(arrow::Status::IOError("File too short: expected to read ",
                                                               nbytes, " bytes, got ", read_size));
    }
    if (read_size == 0) {
        return ReadFuture::MakeFinished(std::make_shared<arrow::Buffer>(nullptr, 0));
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> buffer_result =
        arrow::AllocateResizableBuffer(read_size, pool_.get());
    if (PAIMON_UNLIKELY(!buffer_result.ok())) {
        return ReadFuture::MakeFinished(buffer_result.status());
    }
    auto buffer = KeepMemoryPoolAlive(std::move(buffer_result).ValueUnsafe(), pool_);
    auto* read_data = reinterpret_cast<char*>(buffer->mutable_data());
    auto future = ReadFuture::Make();
    input_stream_->ReadAsync(
        read_data, read_size, position,
        [future, buffer = std::move(buffer), storage_read_bytes = storage_read_bytes_,
         read_size](Status callback_status) mutable {
            if (!callback_status.ok()) {
                future.MarkFinished(ToArrowStatus(callback_status));
                return;
            }
            storage_read_bytes->fetch_add(static_cast<uint64_t>(read_size));
            future.MarkFinished(std::move(buffer));
        });
    return future;
}

arrow::Result<int64_t> ArrowInputStreamAdapter::Tell() const {
    Result<int64_t> position = input_stream_->GetPos();
    if (!position.ok()) {
        return ToArrowStatus(position.status());
    }
    return position.value();
}

arrow::Result<int64_t> ArrowInputStreamAdapter::GetSize() {
    return file_size_;
}

bool ArrowInputStreamAdapter::closed() const {
    return closed_;
}

arrow::Status ArrowInputStreamAdapter::DoClose() {
    if (!closed_) {
        Status status = input_stream_->Close();
        if (!status.ok()) {
            return ToArrowStatus(status);
        }
        closed_ = true;
    }
    return arrow::Status::OK();
}

}  // namespace paimon
