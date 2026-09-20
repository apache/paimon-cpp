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

// vortex.h can define the Arrow C-data-interface structs (ArrowSchema /
// ArrowArray / ArrowArrayStream) itself, which would collide with
// arrow/c/abi.h. vortex.h guards its own copies with USE_OWN_ARROW, so include
// Arrow's abi.h first (canonical definitions) and define USE_OWN_ARROW to make
// vortex.h reuse them. The vx_* API references `struct Arrow*` directly, so the
// skipped FFI_* typedefs are not needed.
#include "arrow/c/abi.h"

#ifndef USE_OWN_ARROW
#define USE_OWN_ARROW
#endif

// vortex.h's API refers to the Arrow C-data-interface structs via these typedef
// names, which it skips defining under USE_OWN_ARROW. Provide them from Arrow's
// definitions, exactly as vortex.h's own header comment instructs.
typedef struct ArrowSchema FFI_ArrowSchema;
typedef struct ArrowArray FFI_ArrowArray;
typedef struct ArrowArrayStream FFI_ArrowArrayStream;

extern "C" {
#include "vortex.h"  // NOLINT(build/include_subdir)
}

// Callback-based data source, added to vortex-ffi by this repository (see
// crates/vortex_callback_io/callback_io.rs). Vortex's own C API can only read a whole file already
// in memory or a path it resolves itself, neither of which goes through paimon's FileSystem. These
// declarations are kept here because the upstream cbindgen-generated vortex.h does not know about
// them; they must stay in sync with the Rust definitions.
extern "C" {

/// Fill `length` bytes starting at `offset` into `dst`. Returns 0 on success and non-zero on
/// failure; a partial read must be reported as a failure. Only the status code crosses the
/// boundary, so the callback is expected to keep its error detail on `ctx`.
///
/// Must be thread-safe: Vortex issues concurrent positional reads for the same context.
typedef int32_t (*vx_read_at_fn)(void* ctx, uint64_t offset, uint8_t* dst, size_t length);

/// Release `ctx`. Called exactly once, when the owning data source is freed (including when it
/// fails to open).
typedef void (*vx_release_fn)(void* ctx);

/// Host callbacks backing a positional reader.
typedef struct vx_input_callbacks {
    /// Opaque host context, passed back to every callback.
    void* ctx;
    /// Positional read. Required.
    vx_read_at_fn read_at_fn;
    /// Context destructor. Optional; may be null.
    vx_release_fn release_fn;
} vx_input_callbacks;

/// Create a data source that reads through `callbacks`. `size` is the total file length in bytes.
/// Returns null and sets `err` on failure.
const vx_data_source* vx_data_source_new_callback(const vx_session* session,
                                                  vx_input_callbacks callbacks, uint64_t size,
                                                  vx_error** err);

/// Append `length` bytes from `src` to the host sink. Returns 0 on success and non-zero on failure;
/// a partial write must be reported as a failure. Writes for one sink are sequential, never
/// concurrent.
typedef int32_t (*vx_write_fn)(void* ctx, const uint8_t* src, size_t length);

/// Flush whatever the host has buffered. Returns 0 on success and non-zero on failure.
typedef int32_t (*vx_flush_fn)(void* ctx);

/// Host callbacks backing a sequential writer.
typedef struct vx_output_callbacks {
    /// Opaque host context, passed back to every callback.
    void* ctx;
    /// Sequential write. Required.
    vx_write_fn write_fn;
    /// Flush. Optional; may be null, in which case flush requests are ignored.
    vx_flush_fn flush_fn;
    /// Context destructor. Optional; may be null.
    vx_release_fn release_fn;
} vx_output_callbacks;

/// A sink writing a Vortex file through host callbacks. Mirrors `vx_array_sink`, which can only
/// target a local filesystem path.
typedef struct vx_callback_sink vx_callback_sink;

/// Open a sink writing through `callbacks`. Returns null and sets `err` on failure. Write errors
/// are reported by `vx_callback_sink_close`, since the bytes are produced by a background task.
vx_callback_sink* vx_callback_sink_open(const vx_session* session, vx_output_callbacks callbacks,
                                        const vx_dtype* dtype, vx_error** err);

/// Push an array into the sink. Does not take ownership of `array`.
void vx_callback_sink_push(vx_callback_sink* sink, const vx_array* array, vx_error** error_out);

/// Flush everything pushed so far and write the file footer. Consumes `sink` even on error.
void vx_callback_sink_close(vx_callback_sink* sink, vx_error** error_out);

/// Discard the sink without writing a footer, leaving whatever the host received invalid.
void vx_callback_sink_abort(vx_callback_sink* sink);
}
