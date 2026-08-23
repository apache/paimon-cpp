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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/mosaic/mosaic_stream.h"

#include <utility>

#include "paimon/common/utils/math.h"
#include "paimon/format/mosaic/mosaic_ffi.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon::mosaic {

void MosaicInputContext::SetCallbackStatus(const Status& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (callback_status_.ok()) {
        callback_status_ = status;
    }
}

Status MosaicInputContext::GetCallbackStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return callback_status_;
}

int32_t MosaicInputContext::ReadAt(void* context, uint64_t offset, uint8_t* buffer,
                                   size_t length) noexcept {
    auto* input_context = static_cast<MosaicInputContext*>(context);
    if (input_context == nullptr || buffer == nullptr) {
        if (input_context != nullptr) {
            input_context->SetCallbackStatus(Status::Invalid("invalid Mosaic read request"));
        }
        return -1;
    }
    Status status = ValidateValueInRange<int64_t>(offset, "Mosaic read offset");
    if (status.ok()) {
        status = ValidateValueInRange<int64_t>(length, "Mosaic read length");
    }
    if (!status.ok()) {
        input_context->SetCallbackStatus(status);
        return -1;
    }
    int64_t read_length = static_cast<int64_t>(length);
    int64_t read_offset = static_cast<int64_t>(offset);
    Result<int64_t> result =
        input_context->input_->Read(reinterpret_cast<char*>(buffer), read_length, read_offset);
    if (!result.ok()) {
        input_context->SetCallbackStatus(result.status());
        return -1;
    }
    int64_t bytes_read = std::move(result).value();
    if (bytes_read != read_length) {
        input_context->SetCallbackStatus(Status::IOError("short read while reading Mosaic file"));
        return -1;
    }
    return 0;
}

uint64_t MosaicInputContext::Length(void* context) noexcept {
    auto* input_context = static_cast<MosaicInputContext*>(context);
    return input_context == nullptr ? 0 : input_context->length_;
}

void MosaicOutputContext::SetCallbackStatus(const Status& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (callback_status_.ok()) {
        callback_status_ = status;
    }
}

Status MosaicOutputContext::GetCallbackStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return callback_status_;
}

int32_t MosaicOutputContext::Write(void* context, const uint8_t* data, size_t length) noexcept {
    auto* output_context = static_cast<MosaicOutputContext*>(context);
    if (output_context == nullptr || data == nullptr) {
        if (output_context != nullptr) {
            output_context->SetCallbackStatus(Status::Invalid("invalid Mosaic write request"));
        }
        return -1;
    }
    Status status = ValidateValueInRange<int64_t>(length, "Mosaic write length");
    if (!status.ok()) {
        output_context->SetCallbackStatus(status);
        return -1;
    }
    int64_t write_length = static_cast<int64_t>(length);
    Result<int64_t> result =
        output_context->output_->Write(reinterpret_cast<const char*>(data), write_length);
    if (!result.ok()) {
        output_context->SetCallbackStatus(result.status());
        return -1;
    }
    int64_t bytes_written = std::move(result).value();
    if (bytes_written != write_length) {
        output_context->SetCallbackStatus(Status::IOError("short write while writing Mosaic file"));
        return -1;
    }
    return 0;
}

int32_t MosaicOutputContext::Flush(void* context) noexcept {
    auto* output_context = static_cast<MosaicOutputContext*>(context);
    if (output_context == nullptr) {
        return -1;
    }
    Status status = output_context->output_->Flush();
    if (!status.ok()) {
        output_context->SetCallbackStatus(status);
        return -1;
    }
    return 0;
}

int64_t MosaicOutputContext::GetPos(void* context) noexcept {
    auto* output_context = static_cast<MosaicOutputContext*>(context);
    if (output_context == nullptr) {
        return -1;
    }
    Result<int64_t> result = output_context->output_->GetPos();
    if (!result.ok()) {
        output_context->SetCallbackStatus(result.status());
        return -1;
    }
    return std::move(result).value();
}

Status MosaicFfiError(const std::string& operation, const Status& callback_status) {
    if (!callback_status.ok()) {
        return callback_status.WithMessage(operation, ": ", callback_status.message());
    }
    const char* error = mosaic_last_error();
    return Status::Invalid(operation, ": ", error == nullptr ? "unknown Mosaic error" : error);
}

}  // namespace paimon::mosaic
