// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Callback-based I/O for the Vortex C API.
//!
//! The stock C API can only read a whole file already materialized in memory
//! (`vx_data_source_new_buffer`) or a path that Vortex itself resolves
//! (`vx_data_source_new`), and it can only write to a local filesystem path
//! (`vx_array_sink_open_file`, which uses `async_fs::File::create`). paimon owns its own pluggable
//! filesystem, so none of these fit: reading forces the entire file into memory, and writing forces
//! a local temporary file that then has to be copied back.
//!
//! This module adds entry points that take host callbacks instead, so Vortex issues positional
//! reads and sequential writes straight against paimon's `InputStream` / `OutputStream`. Vortex's
//! core I/O traits are the plug points; the shape of the bridge mirrors `vortex-jni`'s
//! `JavaReadable` and `JavaWrite`.
//!
//! The write side needs its own handle type rather than reusing `vx_array_sink`, whose fields are
//! private to `crate::sink`.
//!
//! This file is maintained in the paimon-cpp tree (`crates/vortex_callback_io/`) and copied into
//! `vortex-ffi/src/` at build time; see `cmake_modules/vortex.diff`.
//!
//! The C declarations that mirror the types and entry points below live in paimon-cpp's
//! `src/paimon/format/vortex/vortex_ffi.h` (cbindgen does not see this module, so they are written
//! by hand). The two sides are only checked by human review, and a mismatch in field order or a
//! function signature is silent memory corruption, not a compile error. Any change to a
//! `#[repr(C)]` struct, a callback `type`, or a `#[no_mangle]` entry point here MUST be mirrored in
//! that header in the same change.

use std::ffi::c_void;
use std::io;
use std::sync::Arc;

use futures::FutureExt;
use futures::SinkExt;
use futures::TryStreamExt;
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::future::BoxFuture;
use vortex::array::ArrayRef;
use vortex::array::buffer::BufferHandle;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::buffer::Alignment;
use vortex::buffer::ByteBufferMut;
use vortex::dtype::DType;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_ensure;
use vortex::error::vortex_err;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::file::WriteStrategyBuilder;
use vortex::file::WriteSummary;
use vortex::io::CoalesceConfig;
use vortex::io::IoBuf;
use vortex::io::VortexReadAt;
use vortex::io::VortexWrite;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::Handle;
use vortex::io::runtime::Task;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::scan::multi::MultiLayoutDataSource;

use crate::RUNTIME;
use crate::array::vx_array;
use crate::data_source::vx_data_source;
use crate::dtype::vx_dtype;
use crate::error::try_or;
use crate::error::try_or_default;
use crate::error::vx_error;
use crate::session::vx_session;

/// Fill `length` bytes starting at `offset` into `dst`.
///
/// Returns 0 on success and non-zero on failure; a partial read must be reported as a failure.
/// The host is expected to keep its own error detail on the context, since only a status code
/// crosses the boundary.
pub type vx_read_at_fn =
    unsafe extern "C" fn(ctx: *mut c_void, offset: u64, dst: *mut u8, length: usize) -> i32;

/// Release the host context. Called exactly once, when the owning handle is freed.
pub type vx_release_fn = unsafe extern "C" fn(ctx: *mut c_void);

/// Append `length` bytes from `src` to the host sink.
///
/// Returns 0 on success and non-zero on failure; a partial write must be reported as a failure.
/// Unlike reads, writes are sequential and never concurrent for one sink.
pub type vx_write_fn = unsafe extern "C" fn(ctx: *mut c_void, src: *const u8, length: usize) -> i32;

/// Flush whatever the host has buffered. Returns 0 on success and non-zero on failure.
pub type vx_flush_fn = unsafe extern "C" fn(ctx: *mut c_void) -> i32;

/// Host callbacks backing a positional reader.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct vx_input_callbacks {
    /// Opaque host context, passed back to every callback.
    pub ctx: *mut c_void,
    /// Positional read. Required.
    ///
    /// Must be thread-safe: Vortex issues concurrent positional reads, so this is called from
    /// several blocking threads at once for the same context.
    pub read_at_fn: Option<vx_read_at_fn>,
    /// Context destructor. Optional; when set, it is the last callback invoked.
    pub release_fn: Option<vx_release_fn>,
}

/// Host callbacks backing a sequential writer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct vx_output_callbacks {
    /// Opaque host context, passed back to every callback.
    pub ctx: *mut c_void,
    /// Sequential write. Required.
    pub write_fn: Option<vx_write_fn>,
    /// Flush. Optional; when null, flush requests are ignored.
    pub flush_fn: Option<vx_flush_fn>,
    /// Context destructor. Optional; when set, it is the last callback invoked.
    pub release_fn: Option<vx_release_fn>,
}

/// The host context, owned by the reader.
///
/// Held behind an `Arc` so in-flight reads keep the context alive, and so `release_fn` runs exactly
/// once when the last reference goes away. It carries only the context, not the read callback, so
/// that ownership can be taken over before anything else is validated.
struct CallbackTarget {
    ctx: *mut c_void,
    release_fn: Option<vx_release_fn>,
}

// SAFETY: the host contract for `vx_input_callbacks` requires `read_at_fn` to be thread-safe for
// concurrent calls on the same `ctx`, which is what makes it sound to move the context across
// threads and share it between concurrent reads.
unsafe impl Send for CallbackTarget {}
// SAFETY: see the `Send` impl above.
unsafe impl Sync for CallbackTarget {}

impl Drop for CallbackTarget {
    fn drop(&mut self) {
        if let Some(release) = self.release_fn {
            // SAFETY: `ctx` was supplied by the host together with `release_fn`, and an `Arc`
            // guarantees this runs once, after every in-flight read has finished.
            unsafe { release(self.ctx) };
        }
    }
}

/// A [`VortexReadAt`] that forwards positional reads to host callbacks.
///
/// Reads run on the runtime's blocking pool, since the host callback is synchronous and must not
/// occupy an async executor thread. The file size is supplied at construction time, so `size()`
/// never crosses the FFI boundary.
struct CallbackReadAt {
    target: Arc<CallbackTarget>,
    read_at_fn: vx_read_at_fn,
    len: u64,
    handle: Handle,
    concurrency: usize,
}

/// Number of concurrent host read callbacks Vortex may have in flight for one file.
///
/// The host filesystem may be remote, where concurrency hides latency; it is also the bound on
/// blocking threads occupied by one reader.
const DEFAULT_CONCURRENCY: usize = 16;

impl VortexReadAt for CallbackReadAt {
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        // Each read costs an FFI hop plus a blocking-pool hand-off, and the host filesystem may be
        // remote, so favor fewer and larger reads.
        Some(CoalesceConfig::object_storage())
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let len = self.len;
        async move { Ok(len) }.boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let target = Arc::clone(&self.target);
        let read_at_fn = self.read_at_fn;
        let len = self.len;
        let handle = self.handle.clone();

        async move {
            handle
                .spawn_blocking(move || {
                    let end = offset
                        .checked_add(length as u64)
                        .ok_or_else(|| vortex_err!("read {offset}+{length} overflows u64"))?;
                    if end > len {
                        vortex_bail!("read {offset}..{end} out of bounds for file of length {len}");
                    }

                    let mut buffer = ByteBufferMut::with_capacity_aligned(length, alignment);
                    if length > 0 {
                        // SAFETY: the spare capacity covers `length` bytes of live allocation, and
                        // the host contract requires the callback to fill exactly that many bytes
                        // before returning success.
                        let status = unsafe {
                            read_at_fn(
                                target.ctx,
                                offset,
                                buffer.spare_capacity_mut().as_mut_ptr().cast::<u8>(),
                                length,
                            )
                        };
                        if status != 0 {
                            vortex_bail!(
                                "host read callback failed with status {status} for {offset}..{end}"
                            );
                        }
                    }
                    // SAFETY: the callback reported success, which per its contract means all
                    // `length` bytes were written.
                    unsafe { buffer.set_len(length) };
                    Ok(BufferHandle::new_host(buffer.freeze()))
                })
                .await
        }
        .boxed()
    }
}

unsafe fn data_source_new_callback(
    session: *const vx_session,
    callbacks: vx_input_callbacks,
    size: u64,
) -> VortexResult<*const vx_data_source> {
    // Take ownership of the host context before anything else, so an early error below still
    // releases it exactly once when this `Arc` is dropped.
    let target = Arc::new(CallbackTarget {
        ctx: callbacks.ctx,
        release_fn: callbacks.release_fn,
    });

    vortex_ensure!(!session.is_null());
    let read_at_fn = callbacks
        .read_at_fn
        .ok_or_else(|| vortex_err!("vx_input_callbacks.read_at_fn is required"))?;

    let session = vx_session::as_ref(session).clone();
    let reader: Arc<dyn VortexReadAt> = Arc::new(CallbackReadAt {
        target,
        read_at_fn,
        len: size,
        handle: session.handle(),
        concurrency: DEFAULT_CONCURRENCY,
    });

    let file = RUNTIME.block_on(async { session.open_options().open(reader).await })?;
    let data_source = MultiLayoutDataSource::new_with_first(
        file.layout_reader()?,
        Vec::new(),
        vec![Some(size)],
        &session,
    );
    Ok(vx_data_source::new(data_source))
}

/// Create a data source that reads through host callbacks.
///
/// `size` is the total length of the file in bytes; the host knows it up front, so it is passed
/// here instead of being fetched through another callback.
///
/// The callbacks (and the context they carry) are owned by the returned data source and released
/// when it is freed, including when this call fails.
///
/// On error, returns NULL and sets "err".
///
/// # Safety
///
/// `session` must be a valid `vx_session`. `callbacks.read_at_fn` must be non-null, thread-safe,
/// and valid for as long as the data source lives.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_data_source_new_callback(
    session: *const vx_session,
    callbacks: vx_input_callbacks,
    size: u64,
    err: *mut *mut vx_error,
) -> *const vx_data_source {
    try_or(err, std::ptr::null(), || unsafe {
        data_source_new_callback(session, callbacks, size)
    })
}

/// A [`VortexWrite`] that forwards sequential writes to host callbacks.
///
/// Writes run inline on the runtime thread driving the write task, the same way `vortex-jni`
/// forwards to Java sinks: the host callback is synchronous and the layout writer hands over one
/// buffer at a time, so there is nothing to overlap.
struct CallbackWrite {
    target: Arc<CallbackTarget>,
    write_fn: vx_write_fn,
    flush_fn: Option<vx_flush_fn>,
}

impl CallbackWrite {
    fn write_slice(&self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        // SAFETY: `bytes` points at a live slice for the duration of the call, and the host
        // contract forbids retaining it afterwards.
        let status = unsafe { (self.write_fn)(self.target.ctx, bytes.as_ptr(), bytes.len()) };
        if status != 0 {
            return Err(io::Error::other(format!(
                "host write callback failed with status {status}"
            )));
        }
        Ok(())
    }

    fn flush_host(&self) -> io::Result<()> {
        let Some(flush) = self.flush_fn else {
            return Ok(());
        };
        // SAFETY: the context is kept alive by `target`.
        let status = unsafe { flush(self.target.ctx) };
        if status != 0 {
            return Err(io::Error::other(format!(
                "host flush callback failed with status {status}"
            )));
        }
        Ok(())
    }
}

impl VortexWrite for CallbackWrite {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        self.write_slice(buffer.as_slice())?;
        Ok(buffer)
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.flush_host()
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        // The host owns its stream and closes it itself, so this only flushes.
        self.flush_host()
    }
}

/// A sink that writes a Vortex file through host callbacks.
///
/// Mirrors `vx_array_sink`: pushed arrays go through a channel feeding a write task on the session's
/// runtime, and errors surface when the sink is closed. This is a separate type because
/// `vx_array_sink`'s fields are private to its own module.
pub struct vx_callback_sink {
    sink: Sender<VortexResult<ArrayRef>>,
    writer: Task<VortexResult<WriteSummary>>,
    dtype: DType,
}

unsafe fn callback_sink_open(
    session: *const vx_session,
    callbacks: vx_output_callbacks,
    dtype: *const vx_dtype,
) -> VortexResult<*mut vx_callback_sink> {
    // Take ownership of the host context before anything else, so an early error below still
    // releases it exactly once when this `Arc` is dropped.
    let target = Arc::new(CallbackTarget {
        ctx: callbacks.ctx,
        release_fn: callbacks.release_fn,
    });

    vortex_ensure!(!session.is_null());
    vortex_ensure!(!dtype.is_null());
    let write_fn = callbacks
        .write_fn
        .ok_or_else(|| vortex_err!("vx_output_callbacks.write_fn is required"))?;

    let session = vx_session::as_ref(session).clone();
    let dtype = vx_dtype::as_ref(dtype).clone();
    let write = CallbackWrite {
        target,
        write_fn,
        flush_fn: callbacks.flush_fn,
    };

    // The channel size matches the stock file sink.
    let (sink, rx) = mpsc::channel(32);
    let array_stream = ArrayStreamAdapter::new(dtype.clone(), rx.into_stream());

    let strategy = WriteStrategyBuilder::default().build();
    let writer_session = session.clone();
    let writer = session.handle().spawn(async move {
        writer_session
            .write_options()
            .with_strategy(strategy)
            .write(write, array_stream)
            .await
    });

    Ok(Box::into_raw(Box::new(vx_callback_sink {
        sink,
        writer,
        dtype,
    })))
}

/// Open a sink that writes a Vortex file through `callbacks`.
///
/// The callbacks (and the context they carry) are owned by the returned sink and released when it is
/// closed or aborted, including when this call fails. Write errors are reported when the sink is
/// closed, since the bytes are produced by a background task.
///
/// On error, returns NULL and sets "err".
///
/// # Safety
///
/// `session` and `dtype` must be valid. `callbacks.write_fn` must be non-null and valid for as long
/// as the sink lives.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_callback_sink_open(
    session: *const vx_session,
    callbacks: vx_output_callbacks,
    dtype: *const vx_dtype,
    err: *mut *mut vx_error,
) -> *mut vx_callback_sink {
    try_or(err, std::ptr::null_mut(), || unsafe {
        callback_sink_open(session, callbacks, dtype)
    })
}

/// Push an array into a callback sink. Does not take ownership of `array`.
///
/// Errors if the array's DType does not match the sink's.
///
/// # Safety
///
/// `sink` must come from `vx_callback_sink_open` and must not have been closed or aborted.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_callback_sink_push(
    sink: *mut vx_callback_sink,
    array: *const vx_array,
    error_out: *mut *mut vx_error,
) {
    try_or_default(error_out, || {
        vortex_ensure!(!sink.is_null());
        vortex_ensure!(!array.is_null());

        let array = vx_array::as_ref(array);
        let sink = unsafe { &mut *sink };

        vortex_ensure!(
            *array.dtype() == sink.dtype,
            "array dtype {} does not match sink dtype {}",
            array.dtype(),
            sink.dtype
        );
        RUNTIME
            .block_on(sink.sink.send(Ok(array.clone())))
            .map_err(|e| vortex_err!("Send error: {e}"))
    })
}

/// Close a callback sink, flushing everything pushed so far and writing the file footer.
///
/// Consumes `sink` even when it reports an error; do not use it afterwards.
///
/// # Safety
///
/// `sink` must come from `vx_callback_sink_open` and must not have been closed or aborted.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_callback_sink_close(
    sink: *mut vx_callback_sink,
    error_out: *mut *mut vx_error,
) {
    try_or_default(error_out, || {
        vortex_ensure!(!sink.is_null());
        let vx_callback_sink {
            sink,
            writer,
            dtype: _,
        } = *unsafe { Box::from_raw(sink) };
        // Dropping the sender signals end of input to the write task.
        drop(sink);

        RUNTIME.block_on(async {
            let _summary = writer.await?;
            VortexResult::Ok(())
        })?;

        Ok(())
    })
}

/// Abort a callback sink. No footer is written, so whatever the host received is not a valid Vortex
/// file. Do not use `sink` afterwards.
///
/// # Safety
///
/// `sink` must come from `vx_callback_sink_open` and must not have been closed or aborted.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_callback_sink_abort(sink: *mut vx_callback_sink) {
    if sink.is_null() {
        return;
    }
    drop(unsafe { Box::from_raw(sink) });
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use std::fs::File;
    use std::os::unix::fs::FileExt;
    use std::ptr;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use tempfile::NamedTempFile;
    use vortex::array::IntoArray;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::validity::Validity;
    use vortex::buffer::buffer;
    use vortex::dtype::DType;
    use vortex::dtype::PType;

    use super::*;
    use crate::array::vx_array;
    use crate::array::vx_array_free;
    use crate::data_source::vx_data_source_free;
    use crate::data_source::vx_data_source_get_row_count;
    use crate::dtype::vx_dtype;
    use crate::dtype::vx_dtype_free;
    use crate::error::vx_error_free;
    use crate::scan::vx_estimate;
    use crate::scan::vx_estimate_type;
    use crate::session::vx_session_free;
    use crate::session::vx_session_new;
    use crate::sink::vx_array_sink_close;
    use crate::sink::vx_array_sink_open_file;
    use crate::sink::vx_array_sink_push;
    use crate::string::vx_view;

    /// Host context standing in for paimon's `InputStream`: positional reads against a file.
    struct FileContext {
        file: File,
    }

    unsafe extern "C" fn file_read_at(
        ctx: *mut c_void,
        offset: u64,
        dst: *mut u8,
        length: usize,
    ) -> i32 {
        // SAFETY: `ctx` is the `FileContext` handed to `vx_data_source_new_callback`, and `dst`
        // covers `length` bytes, per the callback contract.
        let context = unsafe { &*ctx.cast::<FileContext>() };
        let buffer = unsafe { std::slice::from_raw_parts_mut(dst, length) };
        match context.file.read_exact_at(buffer, offset) {
            Ok(()) => 0,
            Err(_) => -1,
        }
    }

    unsafe extern "C" fn file_release(ctx: *mut c_void) {
        // SAFETY: the context was leaked from a `Box` when the callbacks were built, and this runs
        // exactly once.
        drop(unsafe { Box::from_raw(ctx.cast::<FileContext>()) });
    }

    /// Writing a file with the stock sink and reading it back through the callback data source
    /// must yield the same dtype and row count: the callback plug point is wired up correctly.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_callback_data_source_round_trip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        let dtype = DType::Primitive(PType::I32, false.into());

        unsafe {
            let session = vx_session_new();
            let vx_dtype_ptr = vx_dtype::new(dtype.clone());
            let mut error = ptr::null_mut();

            let sink = vx_array_sink_open_file(
                session,
                vx_view::from_str(&path),
                vx_dtype_ptr,
                &raw mut error,
            );
            assert!(error.is_null());
            let array = PrimitiveArray::new(buffer![1i32, 2i32, 3i32], Validity::NonNullable);
            let vx_array_ptr = vx_array::new(array.into_array());
            vx_array_sink_push(sink, vx_array_ptr, &raw mut error);
            assert!(error.is_null());
            vx_array_sink_close(sink, &raw mut error);
            assert!(error.is_null());
            vx_array_free(vx_array_ptr);
            vx_dtype_free(vx_dtype_ptr);

            let file = File::open(&path).unwrap();
            let size = file.metadata().unwrap().len();
            let context = Box::into_raw(Box::new(FileContext { file }));
            let callbacks = vx_input_callbacks {
                ctx: context.cast::<c_void>(),
                read_at_fn: Some(file_read_at),
                release_fn: Some(file_release),
            };

            let data_source = vx_data_source_new_callback(session, callbacks, size, &raw mut error);
            assert!(error.is_null());
            assert!(!data_source.is_null());

            let mut row_count = vx_estimate::default();
            vx_data_source_get_row_count(data_source, &raw mut row_count);
            assert_eq!(row_count.r#type, vx_estimate_type::VX_ESTIMATE_EXACT);
            assert_eq!(row_count.estimate, 3);

            vx_data_source_free(data_source);
            vx_session_free(session);
        }
    }

    /// A rejected request must still release the host context: the entry point takes ownership
    /// before it validates anything, so the host never has to guess whether to clean up.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_rejected_callbacks_release_context() {
        static RELEASED: AtomicBool = AtomicBool::new(false);

        unsafe extern "C" fn mark_released(_ctx: *mut c_void) {
            RELEASED.store(true, Ordering::SeqCst);
        }

        unsafe {
            let session = vx_session_new();
            let mut error = ptr::null_mut();
            // No read callback, so the call must fail.
            let callbacks = vx_input_callbacks {
                ctx: ptr::null_mut(),
                read_at_fn: None,
                release_fn: Some(mark_released),
            };

            let data_source = vx_data_source_new_callback(session, callbacks, 0, &raw mut error);
            assert!(data_source.is_null());
            assert!(!error.is_null());
            vx_error_free(error);
            assert!(
                RELEASED.load(Ordering::SeqCst),
                "release_fn must run when the data source fails to open"
            );

            vx_session_free(session);
        }
    }

    /// Writing through the callback sink and reading the bytes back through the callback data source
    /// must round-trip, with no filesystem involved in either direction.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_callback_sink_round_trip() {
        /// Host context standing in for a paimon stream pair: an in-memory byte buffer.
        struct BufferContext {
            data: Mutex<Vec<u8>>,
        }

        unsafe extern "C" fn buffer_write(ctx: *mut c_void, src: *const u8, length: usize) -> i32 {
            // SAFETY: `ctx` is the `Arc<BufferContext>` handed to the sink, and `src` covers
            // `length` bytes, per the callback contract.
            let holder = unsafe { &*ctx.cast::<Arc<BufferContext>>() };
            let bytes = unsafe { std::slice::from_raw_parts(src, length) };
            holder.data.lock().unwrap().extend_from_slice(bytes);
            0
        }

        unsafe extern "C" fn buffer_read_at(
            ctx: *mut c_void,
            offset: u64,
            dst: *mut u8,
            length: usize,
        ) -> i32 {
            // SAFETY: as above; `dst` covers `length` writable bytes.
            let holder = unsafe { &*ctx.cast::<Arc<BufferContext>>() };
            let data = holder.data.lock().unwrap();
            let start = offset as usize;
            let Some(end) = start.checked_add(length) else {
                return -1;
            };
            if end > data.len() {
                return -1;
            }
            let dst = unsafe { std::slice::from_raw_parts_mut(dst, length) };
            dst.copy_from_slice(&data[start..end]);
            0
        }

        unsafe extern "C" fn buffer_release(ctx: *mut c_void) {
            // SAFETY: each handle was given its own leaked `Arc` clone, released exactly once.
            drop(unsafe { Box::from_raw(ctx.cast::<Arc<BufferContext>>()) });
        }

        let shared = Arc::new(BufferContext {
            data: Mutex::new(Vec::new()),
        });
        let dtype = DType::Primitive(PType::I32, false.into());

        unsafe {
            let session = vx_session_new();
            let vx_dtype_ptr = vx_dtype::new(dtype.clone());
            let mut error = ptr::null_mut();

            let out_ctx = Box::into_raw(Box::new(Arc::clone(&shared)));
            let sink = vx_callback_sink_open(
                session,
                vx_output_callbacks {
                    ctx: out_ctx.cast::<c_void>(),
                    write_fn: Some(buffer_write),
                    flush_fn: None,
                    release_fn: Some(buffer_release),
                },
                vx_dtype_ptr,
                &raw mut error,
            );
            assert!(error.is_null());
            assert!(!sink.is_null());

            let array =
                PrimitiveArray::new(buffer![10i32, 20i32, 30i32, 40i32], Validity::NonNullable);
            let vx_array_ptr = vx_array::new(array.into_array());
            vx_callback_sink_push(sink, vx_array_ptr, &raw mut error);
            assert!(error.is_null());
            vx_callback_sink_close(sink, &raw mut error);
            assert!(error.is_null());
            vx_array_free(vx_array_ptr);

            let size = shared.data.lock().unwrap().len() as u64;
            assert!(size > 0, "the sink must have written through the callback");

            let in_ctx = Box::into_raw(Box::new(Arc::clone(&shared)));
            let data_source = vx_data_source_new_callback(
                session,
                vx_input_callbacks {
                    ctx: in_ctx.cast::<c_void>(),
                    read_at_fn: Some(buffer_read_at),
                    release_fn: Some(buffer_release),
                },
                size,
                &raw mut error,
            );
            assert!(error.is_null());
            assert!(!data_source.is_null());

            let mut row_count = vx_estimate::default();
            vx_data_source_get_row_count(data_source, &raw mut row_count);
            assert_eq!(row_count.r#type, vx_estimate_type::VX_ESTIMATE_EXACT);
            assert_eq!(row_count.estimate, 4);

            vx_data_source_free(data_source);
            vx_dtype_free(vx_dtype_ptr);
            vx_session_free(session);
        }
    }
}
