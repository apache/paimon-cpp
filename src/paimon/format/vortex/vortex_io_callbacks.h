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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "paimon/format/vortex/vortex_ffi.h"
#include "paimon/status.h"

namespace paimon {
class InputStream;
class OutputStream;
}  // namespace paimon

namespace paimon::vortex {

/// Bridges Vortex's positional reads to a paimon `InputStream`.
///
/// Vortex calls back into this context for every range it needs, so the file is never staged in
/// memory and any paimon `FileSystem` works. Callbacks cannot throw or return a `Status` across the
/// FFI boundary, so a failure is reported as a status code and the real error is stashed here for
/// the caller to pick up with `GetCallbackStatus()`.
class VortexInputContext {
 public:
    /// @param input The paimon stream every positional read is forwarded to.
    explicit VortexInputContext(std::shared_ptr<InputStream> input);

    /// Build the FFI callbacks for `context`.
    ///
    /// The callbacks own a shared reference to `context`, released by `Release`, so a read still
    /// running on a Vortex thread keeps the context (and its `InputStream`) alive even after the
    /// reader has dropped its own reference.
    ///
    /// @param context The context to forward callbacks to; a shared reference is retained.
    /// @return The callback struct to hand to `vx_data_source_new_callback`.
    static vx_input_callbacks MakeCallbacks(const std::shared_ptr<VortexInputContext>& context);

    /// `vx_read_at_fn`: fills `length` bytes at `offset` from the paimon stream.
    ///
    /// Called concurrently from several Vortex threads. This is safe because paimon's positional
    /// `InputStream::Read` does not touch the stream position (the local implementation uses
    /// `pread`), which is the same contract the mosaic format's callbacks rely on.
    ///
    /// @param ctx The `VortexInputContext` shared-reference holder passed as the callback context.
    /// @param offset Byte offset in the file to read from.
    /// @param dst Destination buffer, at least `length` bytes.
    /// @param length Number of bytes to read; a short read is treated as a failure.
    /// @return 0 on success, -1 on failure (the error is stashed for `GetCallbackStatus()`).
    static int32_t ReadAt(void* ctx, uint64_t offset, uint8_t* dst, size_t length) noexcept;

    /// `vx_release_fn`: drops the shared reference held by the callbacks.
    ///
    /// @param ctx The `VortexInputContext` shared-reference holder to release.
    static void Release(void* ctx) noexcept;

    /// @return The first error reported by a callback, or OK when none failed.
    Status GetCallbackStatus() const;

 private:
    void SetCallbackStatus(const Status& status);

    std::shared_ptr<InputStream> input_;
    mutable std::mutex mutex_;
    Status callback_status_;
};

/// Build a `Status` for a failed Vortex call, preferring the error a paimon IO callback stashed on
/// its context over Vortex's own message, since the callback error is the root cause. Must only be
/// called once the Vortex call is known to have failed.
///
/// @param operation Human-readable name of the Vortex operation that failed, used as a prefix.
/// @param error The Vortex error to consume; freed by this call. May be null.
/// @param callback_status The status stashed by the IO callback; preferred when not OK.
/// @return A non-OK `Status` describing the failure.
Status VortexCallbackError(const std::string& operation, vx_error* error,
                           const Status& callback_status);

/// Bridges Vortex's sequential writes to a paimon `OutputStream`.
///
/// Vortex's own file sink can only create a local file, so without this the writer would have to
/// stage the file locally and copy it back on finish. Errors are stashed the same way as on the
/// read side, because the callbacks can only return a status code.
class VortexOutputContext {
 public:
    /// @param output The paimon stream every write is forwarded to.
    explicit VortexOutputContext(std::shared_ptr<OutputStream> output);

    /// Build the FFI callbacks for `context`, which own a shared reference released by `Release`.
    ///
    /// @param context The context to forward callbacks to; a shared reference is retained.
    /// @return The callback struct to hand to `vx_callback_sink_open`.
    static vx_output_callbacks MakeCallbacks(const std::shared_ptr<VortexOutputContext>& context);

    /// `vx_write_fn`: appends `length` bytes from `src` to the paimon stream.
    ///
    /// @param ctx The `VortexOutputContext` shared-reference holder passed as the callback context.
    /// @param src Source buffer of `length` bytes.
    /// @param length Number of bytes to write; a short write is treated as a failure.
    /// @return 0 on success, -1 on failure (the error is stashed for `GetCallbackStatus()`).
    static int32_t Write(void* ctx, const uint8_t* src, size_t length) noexcept;

    /// `vx_flush_fn`: flushes the paimon stream.
    ///
    /// @param ctx The `VortexOutputContext` shared-reference holder passed as the callback context.
    /// @return 0 on success, -1 on failure (the error is stashed for `GetCallbackStatus()`).
    static int32_t Flush(void* ctx) noexcept;

    /// `vx_release_fn`: drops the shared reference held by the callbacks.
    ///
    /// @param ctx The `VortexOutputContext` shared-reference holder to release.
    static void Release(void* ctx) noexcept;

    /// @return The first error reported by a callback, or OK when none failed.
    Status GetCallbackStatus() const;

    /// Bytes handed to the paimon stream so far.
    ///
    /// This is a lower bound on the final file size: Vortex buffers data internally and only writes
    /// the footer on close, so a size-based rolling decision using this value errs on the side of
    /// writing a larger file.
    ///
    /// @return The number of bytes written to the paimon stream so far.
    int64_t BytesWritten() const;

 private:
    void SetCallbackStatus(const Status& status);

    std::shared_ptr<OutputStream> output_;
    mutable std::mutex mutex_;
    Status callback_status_;
    int64_t bytes_written_ = 0;
};

}  // namespace paimon::vortex
