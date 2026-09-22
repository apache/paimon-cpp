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

#include "paimon/format/vortex/vortex_io_callbacks.h"

#include <utility>

#include "paimon/common/utils/math.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon::vortex {

namespace {

/// The callbacks carry a heap-allocated `shared_ptr` copy, since only a raw pointer fits through
/// the FFI boundary. Recover the context from it, or null when the pointer is not usable.
template <typename Context>
Context* ContextFrom(void* ctx) {
    auto* holder = static_cast<std::shared_ptr<Context>*>(ctx);
    return holder == nullptr ? nullptr : holder->get();
}

}  // namespace

VortexInputContext::VortexInputContext(std::shared_ptr<InputStream> input)
    : input_(std::move(input)) {}

vx_input_callbacks VortexInputContext::MakeCallbacks(
    const std::shared_ptr<VortexInputContext>& context) {
    // Only a raw pointer fits through the FFI boundary, so a shared reference is handed over as
    // one: the holder is released here and taken back by `Release`, which frees it.
    auto holder = std::make_unique<std::shared_ptr<VortexInputContext>>(context);
    vx_input_callbacks callbacks = {};
    callbacks.ctx = holder.release();
    callbacks.read_at_fn = VortexInputContext::ReadAt;
    callbacks.release_fn = VortexInputContext::Release;
    return callbacks;
}

void VortexInputContext::Release(void* ctx) noexcept {
    // Take the shared reference back so it is dropped exactly once.
    const std::unique_ptr<std::shared_ptr<VortexInputContext>> holder(
        static_cast<std::shared_ptr<VortexInputContext>*>(ctx));
}

void VortexInputContext::SetCallbackStatus(const Status& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (callback_status_.ok()) {
        callback_status_ = status;
    }
}

Status VortexInputContext::GetCallbackStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return callback_status_;
}

int32_t VortexInputContext::ReadAt(void* ctx, uint64_t offset, uint8_t* dst,
                                   size_t length) noexcept {
    auto* context = ContextFrom<VortexInputContext>(ctx);
    if (context == nullptr || dst == nullptr) {
        if (context != nullptr) {
            context->SetCallbackStatus(Status::Invalid("invalid Vortex read request"));
        }
        return -1;
    }
    Status status = ValidateValueInRange<int64_t>(offset, "Vortex read offset");
    if (status.ok()) {
        status = ValidateValueInRange<int64_t>(length, "Vortex read length");
    }
    if (!status.ok()) {
        context->SetCallbackStatus(status);
        return -1;
    }
    auto read_length = static_cast<int64_t>(length);
    auto read_offset = static_cast<int64_t>(offset);
    Result<int64_t> result =
        context->input_->Read(reinterpret_cast<char*>(dst), read_length, read_offset);
    if (!result.ok()) {
        context->SetCallbackStatus(result.status());
        return -1;
    }
    int64_t bytes_read = std::move(result).value();
    if (bytes_read != read_length) {
        context->SetCallbackStatus(Status::IOError("short read while reading Vortex file"));
        return -1;
    }
    return 0;
}

Status VortexCallbackError(const std::string& operation, vx_error* error,
                           const Status& callback_status) {
    // Consume the Vortex error either way, then prefer the callback's error: it is the root cause,
    // while Vortex only sees an opaque failure code.
    Status ffi_status = VortexFfiError(operation, error);
    if (!callback_status.ok()) {
        return callback_status.WithMessage(operation, ": ", callback_status.message());
    }
    if (!ffi_status.ok()) {
        return ffi_status;
    }
    return Status::IOError(operation, ": unknown Vortex error");
}

VortexOutputContext::VortexOutputContext(std::shared_ptr<OutputStream> output)
    : output_(std::move(output)) {}

vx_output_callbacks VortexOutputContext::MakeCallbacks(
    const std::shared_ptr<VortexOutputContext>& context) {
    // Ownership is handed over the same way as on the read side; see MakeCallbacks above.
    auto holder = std::make_unique<std::shared_ptr<VortexOutputContext>>(context);
    vx_output_callbacks callbacks = {};
    callbacks.ctx = holder.release();
    callbacks.write_fn = VortexOutputContext::Write;
    callbacks.flush_fn = VortexOutputContext::Flush;
    callbacks.release_fn = VortexOutputContext::Release;
    return callbacks;
}

void VortexOutputContext::Release(void* ctx) noexcept {
    const std::unique_ptr<std::shared_ptr<VortexOutputContext>> holder(
        static_cast<std::shared_ptr<VortexOutputContext>*>(ctx));
}

void VortexOutputContext::SetCallbackStatus(const Status& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (callback_status_.ok()) {
        callback_status_ = status;
    }
}

Status VortexOutputContext::GetCallbackStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return callback_status_;
}

int64_t VortexOutputContext::BytesWritten() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bytes_written_;
}

int32_t VortexOutputContext::Write(void* ctx, const uint8_t* src, size_t length) noexcept {
    auto* context = ContextFrom<VortexOutputContext>(ctx);
    if (context == nullptr || src == nullptr) {
        if (context != nullptr) {
            context->SetCallbackStatus(Status::Invalid("invalid Vortex write request"));
        }
        return -1;
    }
    Status status = ValidateValueInRange<int64_t>(length, "Vortex write length");
    if (!status.ok()) {
        context->SetCallbackStatus(status);
        return -1;
    }
    auto write_length = static_cast<int64_t>(length);
    Result<int64_t> result =
        context->output_->Write(reinterpret_cast<const char*>(src), write_length);
    if (!result.ok()) {
        context->SetCallbackStatus(result.status());
        return -1;
    }
    int64_t bytes_written = std::move(result).value();
    if (bytes_written != write_length) {
        context->SetCallbackStatus(Status::IOError("short write while writing Vortex file"));
        return -1;
    }
    {
        std::lock_guard<std::mutex> lock(context->mutex_);
        context->bytes_written_ += bytes_written;
    }
    return 0;
}

int32_t VortexOutputContext::Flush(void* ctx) noexcept {
    auto* context = ContextFrom<VortexOutputContext>(ctx);
    if (context == nullptr) {
        return -1;
    }
    Status status = context->output_->Flush();
    if (!status.ok()) {
        context->SetCallbackStatus(status);
        return -1;
    }
    return 0;
}

}  // namespace paimon::vortex
