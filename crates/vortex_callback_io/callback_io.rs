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

#![allow(non_camel_case_types)]

//! Callback I/O for Vortex using Paimon's `InputStream` and `OutputStream`.
//!
//! Copied into `vortex-ffi/src/` at build time; see `cmake_modules/vortex.diff`.
//! Keep FFI types and signatures in sync with `src/paimon/format/vortex/vortex_ffi.h`.
//!
//! Sink entry points are adapted from Vortex's `vortex-ffi/src/sink.rs`
//! (SPDX-License-Identifier: Apache-2.0, Copyright the Vortex contributors).

use std::ffi::CStr;
use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;
use std::io;
use std::ptr::write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use futures::FutureExt;
use futures::SinkExt;
use futures::TryStreamExt;
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::future::BoxFuture;
use vortex::array::ArrayRef;
use vortex::array::VortexSessionExecute;
use vortex::array::arrow::ArrowSessionExt;
use vortex::array::buffer::BufferHandle;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::buffer::Alignment;
use vortex::buffer::ByteBufferMut;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_ensure;
use vortex::error::vortex_err;
use vortex::extension::datetime::Date;
use vortex::extension::datetime::TemporalMetadata;
use vortex::extension::datetime::TimeUnit;
use vortex::extension::datetime::Timestamp;
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
use vortex::layout::scan::arrow::RecordBatchIteratorAdapter;
use vortex::scalar::PValue;
use vortex::scalar::Scalar;
use vortex::scalar::ScalarValue;

use crate::RUNTIME;
use crate::array::vx_array;
use crate::data_source::vx_data_source;
use crate::dtype::vx_dtype;
use crate::error::try_or;
use crate::error::try_or_default;
use crate::error::vx_error;
use crate::scalar::vx_scalar;
use crate::scan::VxPartitionScan;
use crate::scan::vx_partition;
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

/// Shared host context, released when the last reader or writer reference is dropped.
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

/// A [`VortexReadAt`] that runs host callbacks on the blocking pool.
struct CallbackReadAt {
    target: Arc<CallbackTarget>,
    read_at_fn: vx_read_at_fn,
    len: u64,
    handle: Handle,
    concurrency: usize,
    coalesce_config: CoalesceConfig,
}

/// Maximum concurrent host reads per file, also bounding blocking-pool usage.
const DEFAULT_CONCURRENCY: usize = 16;

impl VortexReadAt for CallbackReadAt {
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        // Coalesce reads to reduce remote I/O and blocking-pool overhead.
        Some(self.coalesce_config)
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
    coalesce_config: CoalesceConfig,
) -> VortexResult<*const vx_data_source> {
    // Take ownership before validation so errors also release the context.
    let target = Arc::new(CallbackTarget {
        ctx: callbacks.ctx,
        release_fn: callbacks.release_fn,
    });

    vortex_ensure!(!session.is_null());
    vortex_ensure!(
        coalesce_config.max_size > 0,
        "coalesced max_size must be positive"
    );
    vortex_ensure!(
        coalesce_config.distance <= coalesce_config.max_size,
        "coalesced max_gap must not exceed max_size"
    );
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
        coalesce_config,
    });

    let file = RUNTIME.block_on(async { session.open_options().open(reader).await })?;
    Ok(vx_data_source::new(file.data_source()?))
}

/// Create a data source that reads through host callbacks with explicit I/O coalescing.
///
/// `size` is the total file length in bytes. `max_size` must be positive, and `max_gap` must
/// not exceed it. A smaller gap avoids reading unrelated bytes between sparse requests.
///
/// The callbacks and their context are owned by the returned data source and released when
/// it is freed. They are consumed and released even when this call fails, including invalid
/// coalescing options. On error, returns NULL and sets `err`.
///
/// # Safety
///
/// `session` must be a valid `vx_session`. `callbacks.read_at_fn` must be non-null, thread-safe,
/// and valid for as long as the data source lives. `callbacks.ctx` must be valid for the supplied
/// callbacks until `release_fn` is called. `err` must be null or point to a null `*mut vx_error`.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_data_source_new_callback_with_coalesce(
    session: *const vx_session,
    callbacks: vx_input_callbacks,
    size: u64,
    max_gap: u64,
    max_size: u64,
    err: *mut *mut vx_error,
) -> *const vx_data_source {
    try_or(err, std::ptr::null(), || unsafe {
        data_source_new_callback(
            session,
            callbacks,
            size,
            CoalesceConfig::new(max_gap, max_size),
        )
    })
}

/// A [`VortexWrite`] that calls host callbacks inline on the writer's runtime thread.
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

/// A Vortex file sink backed by host callbacks and a background write task.
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
    // Take ownership before validation so errors also release the context.
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

    // Limit queued arrays to apply backpressure to the producer.
    let (sink, rx) = mpsc::channel(4);
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

/// Retain the compressed input chunk until its row slices have been consumed. In particular,
/// never materialize all of a dictionary's repeated values before applying the host's row limit.
struct BoundedArrayIterator<I> {
    input: I,
    current: Option<ArrayRef>,
    offset: usize,
    batch_size: usize,
    finished: bool,
}

impl<I: Iterator<Item = VortexResult<ArrayRef>>> BoundedArrayIterator<I> {
    fn new(input: I, batch_size: usize) -> VortexResult<Self> {
        vortex_ensure!(batch_size > 0, "Vortex Arrow batch size must be positive");
        Ok(Self {
            input,
            current: None,
            offset: 0,
            batch_size,
            finished: false,
        })
    }
}

impl<I: Iterator<Item = VortexResult<ArrayRef>>> Iterator for BoundedArrayIterator<I> {
    type Item = VortexResult<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            if let Some(chunk) = &self.current {
                // Subtract first so even a caller-provided usize::MAX row limit cannot overflow.
                let length = self.batch_size.min(chunk.len() - self.offset);
                let end = self.offset + length;
                let sliced = chunk.slice(self.offset..end);
                self.offset = end;
                if end == chunk.len() || sliced.is_err() {
                    self.current = None;
                    self.offset = 0;
                }
                if sliced.is_err() {
                    self.finished = true;
                }
                return Some(sliced);
            }
            match self.input.next() {
                Some(Ok(chunk)) if chunk.is_empty() => continue,
                Some(Ok(chunk)) => self.current = Some(chunk),
                Some(Err(error)) => {
                    self.finished = true;
                    return Some(Err(error));
                }
                None => {
                    self.finished = true;
                    return None;
                }
            }
        }
    }
}

/// Scan a partition to standard Arrow arrays, slicing compressed chunks before conversion.
///
/// Each exported batch contains at most `batch_size` rows. Conversion is lazy: requesting the
/// next Arrow batch converts only its slice, not every slice of the current native chunk.
/// This is a row limit, not a byte limit: each batch must still fit Arrow's Utf8/Binary offsets.
///
/// A non-null `partition` is consumed on success and failure, including a zero batch size. The
/// caller must release a successful `stream` through the Arrow C stream release callback.
///
/// # Safety
///
/// `partition` must be null or a valid, unconsumed partition handle. `session` must be null or
/// a valid session handle. `stream` must be null or point to writable, uninitialized/released storage.
/// On failure the stream storage is unchanged; error ownership follows the other Vortex FFI calls.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_partition_scan_arrow_with_batch_size(
    session: *const vx_session,
    partition: *mut vx_partition,
    batch_size: usize,
    stream: *mut FFI_ArrowArrayStream,
    err: *mut *mut vx_error,
) -> c_int {
    try_or(err, 1, || {
        vortex_ensure!(!partition.is_null(), "Vortex partition is null");
        // Take ownership before validating the other arguments, so callers never need to guess
        // whether an invalid request has already consumed the partition.
        let partition = match *vx_partition::into_box(partition) {
            VxPartitionScan::Pending(partition) => partition,
            _ => vortex_bail!("Vortex partition is already being consumed"),
        };
        vortex_ensure!(batch_size > 0, "Vortex Arrow batch size must be positive");
        vortex_ensure!(!session.is_null(), "Vortex session is null");
        vortex_ensure!(!stream.is_null(), "Vortex Arrow stream output is null");
        let array_stream = partition.execute()?;
        let schema = Arc::new(normalize_arrow_schema_views(
            array_stream.dtype().to_arrow_schema()?,
        ));
        let target = Field::new_struct("", schema.fields().clone(), false);
        let session = vx_session::as_ref(session).clone();
        let mut chunks =
            BoundedArrayIterator::new(RUNTIME.block_on_stream(array_stream), batch_size)?;
        let mut failed = false;
        let iter = std::iter::from_fn(move || {
            if failed {
                return None;
            }
            let chunk = chunks.next()?;
            let result = (|| -> VortexResult<RecordBatch> {
                let mut ctx = session.create_execution_ctx();
                let arrow = session
                    .arrow()
                    .execute_arrow(chunk?, Some(&target), &mut ctx)?;
                Ok(RecordBatch::from(arrow.as_struct().clone()))
            })();
            failed = result.is_err();
            Some(result.map_err(|error| ArrowError::ExternalError(Box::new(error))))
        });
        let reader = RecordBatchIteratorAdapter::new(iter, schema);
        unsafe { write(stream, FFI_ArrowArrayStream::new(Box::new(reader))) };
        Ok(0)
    })
}

/// Export standard Arrow string/binary types recursively so the declared schema matches the
/// buffer layout expected by Paimon's C++ reader.
pub(crate) fn normalize_arrow_schema_views(schema: arrow_schema::Schema) -> arrow_schema::Schema {
    let fields = schema
        .fields()
        .iter()
        .cloned()
        .map(normalize_field_views)
        .collect::<arrow_schema::Fields>();
    arrow_schema::Schema::new_with_metadata(fields, schema.metadata)
}

fn normalize_field_views(field: arrow_schema::FieldRef) -> arrow_schema::FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(normalize_dtype_views(field.data_type())),
    )
}

fn normalize_dtype_views(dtype: &arrow_schema::DataType) -> arrow_schema::DataType {
    use arrow_schema::DataType;
    match dtype {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().cloned().map(normalize_field_views).collect())
        }
        DataType::List(field) => DataType::List(normalize_field_views(Arc::clone(field))),
        DataType::LargeList(field) => DataType::LargeList(normalize_field_views(Arc::clone(field))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(normalize_field_views(Arc::clone(field)), *size)
        }
        other => other.clone(),
    }
}

/// Time unit of a timestamp scalar literal. Mirrored in `vortex_ffi.h`; the discriminants are an
/// ABI contract, so do not reorder or renumber them.
#[repr(C)]
#[allow(non_camel_case_types, dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum vx_time_unit {
    /// Seconds since the Unix epoch.
    VX_TIME_UNIT_SECOND = 0,
    /// Milliseconds since the Unix epoch.
    VX_TIME_UNIT_MILLISECOND = 1,
    /// Microseconds since the Unix epoch.
    VX_TIME_UNIT_MICROSECOND = 2,
    /// Nanoseconds since the Unix epoch.
    VX_TIME_UNIT_NANOSECOND = 3,
}

/// Create a timestamp scalar literal: `value` units since the Unix epoch under `unit`.
///
/// `timezone` is a null-terminated UTF-8 IANA time-zone name, or null for a local (wall-clock)
/// timestamp. Vortex's own C ABI has no timestamp/date scalar constructor, so paimon-cpp adds these
/// two here to push down temporal predicates. Returns null without an error when a valid value
/// exceeds the native temporal range; other failures return null and populate `err`.
///
/// # Safety
///
/// `timezone`, when not null, must point to a valid null-terminated UTF-8 string that outlives this
/// call. `err` must be null or point to a null `*mut vx_error`.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_scalar_new_timestamp(
    value: i64,
    unit: vx_time_unit,
    timezone: *const c_char,
    is_nullable: bool,
    err: *mut *mut vx_error,
) -> *mut vx_scalar {
    try_or(err, std::ptr::null_mut(), || {
        let time_unit = match unit {
            vx_time_unit::VX_TIME_UNIT_SECOND => TimeUnit::Seconds,
            vx_time_unit::VX_TIME_UNIT_MILLISECOND => TimeUnit::Milliseconds,
            vx_time_unit::VX_TIME_UNIT_MICROSECOND => TimeUnit::Microseconds,
            vx_time_unit::VX_TIME_UNIT_NANOSECOND => TimeUnit::Nanoseconds,
        };
        let tz = if timezone.is_null() {
            None
        } else {
            let name = unsafe { CStr::from_ptr(timezone) }
                .to_str()
                .map_err(|e| vortex_err!("timezone is not valid utf8: {e}"))?;
            Some(Arc::from(name))
        };
        // Check timezone errors independently of the value, including values outside the native
        // range. A malformed timezone must not be mistaken for an unsupported constant.
        if tz.is_some() {
            TemporalMetadata::Timestamp(&TimeUnit::Seconds, &tz).to_jiff(0)?;
        }
        // Timestamp::unpack_native uses Jiff Timestamp even without a timezone. Check both its
        // Span setters and epoch range without introducing a timezone-database lookup.
        let Ok(span) = time_unit.to_jiff_span(value) else {
            return Ok(std::ptr::null_mut());
        };
        if jiff::Timestamp::UNIX_EPOCH.checked_add(span).is_err() {
            return Ok(std::ptr::null_mut());
        }
        let dtype = DType::Extension(
            Timestamp::new_with_tz(time_unit, tz, Nullability::from(is_nullable)).erased(),
        );
        Ok(vx_scalar::new(Scalar::try_new(
            dtype,
            Some(ScalarValue::Primitive(PValue::I64(value))),
        )?))
    })
}

/// Create a date scalar literal: `days` days since the Unix epoch (1970-01-01).
/// Returns null without an error when the value exceeds the native civil-date range; other
/// failures return null and populate `err`.
///
/// # Safety
///
/// `err` must be null or point to a null `*mut vx_error`.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vx_scalar_new_date(
    days: i32,
    is_nullable: bool,
    err: *mut *mut vx_error,
) -> *mut vx_scalar {
    try_or(err, std::ptr::null_mut(), || {
        // The fixed Days unit and epoch are valid; only a native range limit can fail here.
        // Date's storage accepts all i32 values, but formatting needs a representable civil date.
        if TemporalMetadata::Date(&TimeUnit::Days)
            .to_jiff(i64::from(days))
            .is_err()
        {
            return Ok(std::ptr::null_mut());
        }
        let dtype =
            DType::Extension(Date::new(TimeUnit::Days, Nullability::from(is_nullable)).erased());
        Ok(vx_scalar::new(Scalar::try_new(
            dtype,
            Some(ScalarValue::Primitive(PValue::I32(days))),
        )?))
    })
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use std::fs::File;
    use std::os::unix::fs::FileExt;
    use std::ptr;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use arrow_array::Array;
    use arrow_array::ffi_stream::ArrowArrayStreamReader;
    use futures::StreamExt;
    use parking_lot::Mutex;
    use tempfile::NamedTempFile;
    use vortex::VortexSessionDefault;
    use vortex::array::Canonical;
    use vortex::array::ExecutionCtx;
    use vortex::array::IntoArray;
    use vortex::array::arrays::BoolArray;
    use vortex::array::arrays::ConstantArray;
    use vortex::array::arrays::Dict;
    use vortex::array::arrays::DictArray;
    use vortex::array::arrays::ListArray;
    use vortex::array::arrays::ListViewArray;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::arrays::Struct;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::VarBinViewArray;
    use vortex::array::arrays::scalar_fn::ScalarFnFactoryExt;
    use vortex::array::arrays::struct_::StructArrayExt;
    use vortex::array::arrow::ArrowExport;
    use vortex::array::arrow::ArrowExportVTable;
    use vortex::array::arrow::ArrowSession;
    use vortex::array::expr::stats::Precision;
    use vortex::array::scalar_fn::Arity;
    use vortex::array::scalar_fn::ChildName;
    use vortex::array::scalar_fn::EmptyOptions;
    use vortex::array::scalar_fn::ExecutionArgs;
    use vortex::array::scalar_fn::ScalarFnId;
    use vortex::array::scalar_fn::ScalarFnVTable;
    use vortex::array::stream::SendableArrayStream;
    use vortex::array::validity::Validity;
    use vortex::buffer::buffer;
    use vortex::dtype::DType;
    use vortex::dtype::PType;
    use vortex::scan::Partition;
    use vortex::session::VortexSession;
    use vortex::session::registry::Id;

    use super::*;
    use crate::array::vx_array;
    use crate::array::vx_array_free;
    use crate::data_source::vx_data_source_free;
    use crate::data_source::vx_data_source_get_row_count;
    use crate::dtype::vx_dtype;
    use crate::dtype::vx_dtype_free;
    use crate::error::vx_error_free;
    use crate::error::vx_error_get_message;
    use crate::scalar::vx_scalar_free;
    use crate::scan::vx_estimate;
    use crate::scan::vx_estimate_type;
    use crate::session::vx_session_free;
    use crate::session::vx_session_new;
    use crate::sink::vx_array_sink_close;
    use crate::sink::vx_array_sink_open_file;
    use crate::sink::vx_array_sink_push;
    use crate::string::vx_string;

    fn dictionary_view_chunk() -> ArrayRef {
        let codes = PrimitiveArray::new(buffer![0u32, 1, 0, 2, 0, 0, 1], Validity::NonNullable)
            .into_array();
        let text = "repeated long dictionary value ".repeat(128);
        let bytes = vec![0xfdu8; 4096];
        let text_values =
            VarBinViewArray::from_iter_nullable_str([Some(text.as_str()), None, Some("")]);
        let byte_values = VarBinViewArray::from_iter_nullable_bin([
            Some(bytes.as_slice()),
            None,
            Some(b"".as_slice()),
        ]);
        StructArray::new(
            ["text", "bytes"].into(),
            vec![
                DictArray::new(codes.clone(), text_values.into_array()).into_array(),
                DictArray::new(codes, byte_values.into_array()).into_array(),
            ],
            7,
            Validity::NonNullable,
        )
        .into_array()
    }

    // Test-only exporter intercepts the exact input handed to the child Arrow conversion.
    // Rejecting a too-large input proves that trimming occurs before expansion, rather than
    // merely checking the shape of an Arrow child that has already been materialized.
    #[derive(Debug)]
    struct ChildExportBound {
        max_elements: usize,
        lengths: Arc<Mutex<Vec<usize>>>,
    }

    impl ArrowExportVTable for ChildExportBound {
        fn arrow_ext_id(&self) -> Id {
            Id::new("paimon.test.bounded-child")
        }

        fn vortex_id(&self) -> Id {
            self.arrow_ext_id()
        }

        fn to_arrow_field(
            &self,
            _name: &str,
            _dtype: &DType,
            _session: &ArrowSession,
        ) -> VortexResult<Option<Field>> {
            Ok(None)
        }

        fn execute_arrow(
            &self,
            array: ArrayRef,
            _target: &Field,
            _ctx: &mut ExecutionCtx,
        ) -> VortexResult<ArrowExport> {
            self.lengths.lock().push(array.len());
            vortex_ensure!(
                array.len() <= self.max_elements,
                "child export received {} values, expected at most {}",
                array.len(),
                self.max_elements,
            );
            Ok(ArrowExport::Unsupported(array))
        }
    }

    fn bounded_child_field(binary: bool) -> Field {
        Field::new(
            "item",
            if binary {
                arrow_schema::DataType::Binary
            } else {
                arrow_schema::DataType::Utf8
            },
            true,
        )
        .with_metadata(
            [(
                arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_owned(),
                "paimon.test.bounded-child".to_owned(),
            )]
            .into(),
        )
    }

    fn bounded_child_session(max_elements: usize) -> (VortexSession, Arc<Mutex<Vec<usize>>>) {
        let session = VortexSession::default();
        let lengths = Arc::new(Mutex::new(Vec::new()));
        session
            .arrow()
            .register_exporter(Arc::new(ChildExportBound {
                max_elements,
                lengths: Arc::clone(&lengths),
            }));
        (session, lengths)
    }

    fn nested_dictionary_elements(binary: bool) -> ArrayRef {
        let codes = PrimitiveArray::new(buffer![0u32, 1, 0, 2, 1, 0, 2, 0], Validity::NonNullable)
            .into_array();
        let text = "nested dictionary value ".repeat(128);
        let bytes = vec![0xfdu8; 4096];
        let values = if binary {
            VarBinViewArray::from_iter_nullable_bin([
                Some(bytes.as_slice()),
                None,
                Some(b"".as_slice()),
            ])
            .into_array()
        } else {
            VarBinViewArray::from_iter_nullable_str([Some(text.as_str()), None, Some("")])
                .into_array()
        };
        DictArray::new(codes, values).into_array()
    }

    fn nested_list_fixture(binary: bool, view: bool) -> ArrayRef {
        let elements = nested_dictionary_elements(binary);
        let validity = Validity::from_iter([true, true, false, true, true]);
        if view {
            let offsets = PrimitiveArray::new(buffer![0i64, 2, 2, 4, 6], Validity::NonNullable);
            let sizes = PrimitiveArray::new(buffer![2i64, 0, 2, 2, 2], Validity::NonNullable);
            // SAFETY: each view ends exactly at the next offset, and the last view ends at 8.
            unsafe {
                ListViewArray::new_unchecked(
                    elements,
                    offsets.into_array(),
                    sizes.into_array(),
                    validity,
                )
                .with_zero_copy_to_list(true)
            }
            .into_array()
        } else {
            ListArray::new(
                elements,
                PrimitiveArray::new(buffer![0i64, 2, 2, 4, 6, 8], Validity::NonNullable)
                    .into_array(),
                validity,
            )
            .into_array()
        }
    }

    fn check_nested_batches<O: arrow_array::OffsetSizeTrait>(
        binary: bool,
        view: bool,
        target: &Field,
    ) -> VortexResult<()> {
        let (session, lengths) = bounded_child_session(2);
        let mut ctx = session.create_execution_ctx();
        let input = nested_list_fixture(binary, view);
        let mut row_count = 0;
        for chunk in BoundedArrayIterator::new(std::iter::once(Ok(input)), 2)? {
            let array = session
                .arrow()
                .execute_arrow(chunk?, Some(target), &mut ctx)?;
            let list = array.as_list::<O>();
            assert_eq!(list.value_offsets()[0].as_usize(), 0);
            for row in 0..list.len() {
                let source_row = row_count + row;
                assert_eq!(list.is_null(row), source_row == 2);
                if source_row == 1 {
                    assert_eq!(list.value_length(row).as_usize(), 0);
                } else if source_row != 2 {
                    let child = list.value(row);
                    assert_eq!(child.len(), 2);
                    for element in 0..2 {
                        let index = [0, 0, 0, 4, 6][source_row] + element;
                        let code = [0, 1, 0, 2, 1, 0, 2, 0][index];
                        assert_eq!(child.is_null(element), code == 1);
                        if code != 1 {
                            if binary {
                                let value = child.as_binary::<i32>().value(element);
                                assert_eq!(
                                    value,
                                    if code == 0 { &[0xfdu8; 4096][..] } else { &[] }
                                );
                            } else {
                                let value = child.as_string::<i32>().value(element);
                                assert_eq!(
                                    value,
                                    if code == 0 {
                                        "nested dictionary value ".repeat(128)
                                    } else {
                                        String::new()
                                    },
                                );
                            }
                        }
                    }
                }
            }
            row_count += list.len();
        }
        assert_eq!(row_count, 5);
        assert_eq!(*lengths.lock(), [2, 2, 2]);
        Ok(())
    }

    #[test]
    fn bounded_nested_lists_limit_child_export_before_materialization() -> VortexResult<()> {
        check_null_list_spans_are_not_materialized(false)?;
        for binary in [false, true] {
            let child = Arc::new(bounded_child_field(binary));
            let target = Field::new("", arrow_schema::DataType::List(Arc::clone(&child)), true);
            check_nested_batches::<i32>(binary, false, &target)?;
            let target = Field::new("", arrow_schema::DataType::LargeList(child), true);
            check_nested_batches::<i64>(binary, false, &target)?;
        }
        Ok(())
    }

    #[test]
    fn bounded_nested_list_views_limit_child_export_before_materialization() -> VortexResult<()> {
        check_non_zctl_canonical_input_bound()?;
        check_null_list_spans_are_not_materialized(true)?;
        for binary in [false, true] {
            let child = Arc::new(bounded_child_field(binary));
            let target = Field::new("", arrow_schema::DataType::List(Arc::clone(&child)), true);
            check_nested_batches::<i32>(binary, true, &target)?;
            let target = Field::new("", arrow_schema::DataType::LargeList(child), true);
            check_nested_batches::<i64>(binary, true, &target)?;
        }
        Ok(())
    }

    // Unlike an Arrow exporter, this function runs when a lazy element array is first executed
    // to canonical form. ScalarFn's existing slice rule pushes each selected range through to
    // the child, so the guard also detects execution hidden inside a ListView rebuild or take.
    #[derive(Clone)]
    struct CanonicalInputBound {
        max_elements: usize,
        lengths: Arc<Mutex<Vec<usize>>>,
    }

    impl ScalarFnVTable for CanonicalInputBound {
        type Options = EmptyOptions;

        fn id(&self) -> ScalarFnId {
            Id::new("paimon.test.bounded-canonical-input")
        }

        fn arity(&self, _options: &Self::Options) -> Arity {
            Arity::Exact(1)
        }

        fn child_name(&self, _options: &Self::Options, _index: usize) -> ChildName {
            ChildName::new_arc("input".into())
        }

        fn return_dtype(&self, _options: &Self::Options, args: &[DType]) -> VortexResult<DType> {
            Ok(args[0].clone())
        }

        fn execute(
            &self,
            _options: &Self::Options,
            args: &dyn ExecutionArgs,
            ctx: &mut ExecutionCtx,
        ) -> VortexResult<ArrayRef> {
            self.lengths.lock().push(args.row_count());
            vortex_ensure!(
                args.row_count() <= self.max_elements,
                "canonical execution received {} elements, expected at most {}",
                args.row_count(),
                self.max_elements,
            );
            Ok(args.get(0)?.execute::<Canonical>(ctx)?.into_array())
        }
    }

    fn check_null_list_spans_are_not_materialized(view: bool) -> VortexResult<()> {
        for validity_kind in 0..3 {
            let (session, arrow_lengths) = bounded_child_session(2);
            let canonical_lengths = Arc::new(Mutex::new(Vec::new()));
            let mut ctx = session.create_execution_ctx();
            let elements = CanonicalInputBound {
                max_elements: 2,
                lengths: Arc::clone(&canonical_lengths),
            }
            .try_new_array(
                2048,
                EmptyOptions,
                vec![
                    VarBinViewArray::from_iter_str((0..2048).map(|index| index.to_string()))
                        .into_array(),
                ],
            )?;
            let validity = match validity_kind {
                0 => Validity::from_iter([true, false, true]),
                1 => Validity::AllInvalid,
                _ => Validity::Array(BoolArray::from_iter([false, false, false]).into_array()),
            };
            let input = if view {
                // SAFETY: offsets/sizes describe consecutive, non-overlapping ranges, even
                // though the NULL middle slot owns most of the elements array.
                unsafe {
                    ListViewArray::new_unchecked(
                        elements,
                        PrimitiveArray::new(buffer![0i64, 2, 2046], Validity::NonNullable)
                            .into_array(),
                        PrimitiveArray::new(buffer![2i64, 2044, 2], Validity::NonNullable)
                            .into_array(),
                        validity,
                    )
                    .with_zero_copy_to_list(true)
                }
                .into_array()
            } else {
                ListArray::new(
                    elements,
                    PrimitiveArray::new(buffer![0i64, 2, 2046, 2048], Validity::NonNullable)
                        .into_array(),
                    validity,
                )
                .into_array()
            };
            let target = Field::new(
                "",
                arrow_schema::DataType::List(Arc::new(bounded_child_field(false))),
                true,
            );
            let all_null = validity_kind != 0;
            let mut source_row = 0;
            for chunk in BoundedArrayIterator::new(std::iter::once(Ok(input)), 1)? {
                let array = session
                    .arrow()
                    .execute_arrow(chunk?, Some(&target), &mut ctx)?;
                let list = array.as_list::<i32>();
                let is_null = all_null || source_row == 1;
                assert_eq!(list.len(), 1);
                assert_eq!(list.is_null(0), is_null);
                assert_eq!(list.value_length(0), if is_null { 0 } else { 2 });
                assert_eq!(list.values().len(), if is_null { 0 } else { 2 });
                if !is_null {
                    let values = list.values().as_string::<i32>();
                    let start = if source_row == 0 { 0 } else { 2046 };
                    assert_eq!(values.value(0), start.to_string());
                    assert_eq!(values.value(1), (start + 1).to_string());
                }
                source_row += 1;
            }
            assert_eq!(source_row, 3);
            if all_null {
                assert!(canonical_lengths.lock().is_empty());
                assert_eq!(*arrow_lengths.lock(), [0, 0, 0]);
            } else {
                assert_eq!(*canonical_lengths.lock(), [2, 2]);
                assert_eq!(*arrow_lengths.lock(), [2, 0, 2]);
            }
        }
        Ok(())
    }

    fn check_non_zctl_canonical_input_bound() -> VortexResult<()> {
        // Two items trigger the old take path; 129 trigger the old list-by-list path. The
        // shared array stays small so even the unpatched negative control uses little memory.
        for size in [2usize, 129] {
            let size_i32 = i32::try_from(size)
                .map_err(|error| vortex_err!("Test list size out of i32 range: {error}"))?;
            let (session, arrow_lengths) = bounded_child_session(2 * size);
            let canonical_lengths = Arc::new(Mutex::new(Vec::new()));
            let mut ctx = session.create_execution_ctx();
            let elements = CanonicalInputBound {
                max_elements: size,
                lengths: Arc::clone(&canonical_lengths),
            }
            .try_new_array(
                2048,
                EmptyOptions,
                vec![
                    ConstantArray::new(
                        Scalar::utf8("a long nested value ".repeat(128), Nullability::NonNullable),
                        2048,
                    )
                    .into_array(),
                ],
            )?;
            // Overlap, repeated ranges and a large internal gap all occur among valid rows.
            // The final null row also has a nonzero size that must never be materialized.
            let input = ListViewArray::new(
                elements,
                PrimitiveArray::new(buffer![1i64, 2, 1, 1900, 0], Validity::NonNullable)
                    .into_array(),
                PrimitiveArray::new(
                    buffer![size as i64, size as i64, size as i64, size as i64, 2048],
                    Validity::NonNullable,
                )
                .into_array(),
                Validity::from_iter([true, true, true, true, false]),
            )
            .into_array();
            let target = Field::new(
                "",
                arrow_schema::DataType::List(Arc::new(bounded_child_field(false))),
                true,
            );
            let mut source_row = 0;
            for chunk in BoundedArrayIterator::new(std::iter::once(Ok(input)), 2)? {
                let array = session
                    .arrow()
                    .execute_arrow(chunk?, Some(&target), &mut ctx)?;
                let list = array.as_list::<i32>();
                for row in 0..list.len() {
                    assert_eq!(list.is_null(row), source_row == 4);
                    assert_eq!(
                        list.value_length(row),
                        if source_row == 4 { 0 } else { size_i32 },
                    );
                    if source_row != 4 {
                        let values = list.value(row);
                        for value in values.as_string::<i32>().iter() {
                            assert_eq!(value, Some("a long nested value ".repeat(128).as_str()));
                        }
                    }
                    source_row += 1;
                }
            }
            assert_eq!(source_row, 5);
            assert_eq!(*canonical_lengths.lock(), [size, size, size, size]);
            assert_eq!(*arrow_lengths.lock(), [2 * size, 2 * size, 0]);
        }
        Ok(())
    }

    #[test]
    fn bounded_empty_nested_lists_export_no_shared_children() -> VortexResult<()> {
        for view in [false, true] {
            for binary in [false, true] {
                let (session, lengths) = bounded_child_session(0);
                let mut ctx = session.create_execution_ctx();
                let input = nested_list_fixture(binary, view).slice(2..2)?;
                let target = Field::new(
                    "",
                    arrow_schema::DataType::List(Arc::new(bounded_child_field(binary))),
                    true,
                );
                let array = session
                    .arrow()
                    .execute_arrow(input, Some(&target), &mut ctx)?;
                let list = array.as_list::<i32>();
                assert_eq!(list.len(), 0);
                assert_eq!(list.values().len(), 0);
                assert_eq!(*lengths.lock(), [0]);
            }
        }
        Ok(())
    }

    fn check_null_nested_list_batch(
        view: bool,
        with_struct: bool,
        batch_size: usize,
        validity_kind: u8,
    ) -> VortexResult<()> {
        let (session, arrow_lengths) = bounded_child_session(2);
        let canonical_lengths = Arc::new(Mutex::new(Vec::new()));
        let mut ctx = session.create_execution_ctx();
        let values = CanonicalInputBound {
            max_elements: 2,
            lengths: Arc::clone(&canonical_lengths),
        }
        .try_new_array(
            2048,
            EmptyOptions,
            vec![
                VarBinViewArray::from_iter_str((0..2048).map(|index| index.to_string()))
                    .into_array(),
            ],
        )?;
        let inner = ListArray::new(
            values,
            PrimitiveArray::new(
                (0..=2048i64).collect::<vortex::buffer::Buffer<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            Validity::NonNullable,
        )
        .into_array();
        let inner_type = arrow_schema::DataType::List(Arc::new(bounded_child_field(false)));
        let (inner, inner_type) = if with_struct {
            (
                StructArray::new(["items"].into(), vec![inner], 2048, Validity::NonNullable)
                    .into_array(),
                arrow_schema::DataType::Struct(vec![Field::new("items", inner_type, false)].into()),
            )
        } else {
            (inner, inner_type)
        };
        let validity = match validity_kind {
            0 => Validity::from_iter([true, false, true]),
            1 => Validity::AllInvalid,
            _ => Validity::Array(BoolArray::from_iter([false, false, false]).into_array()),
        };
        let input = if view {
            // SAFETY: the three views refer to contiguous ranges, including the
            // unreferenced large middle range owned by a NULL outer list.
            unsafe {
                ListViewArray::new_unchecked(
                    inner,
                    PrimitiveArray::new(buffer![0i64, 2, 2046], Validity::NonNullable).into_array(),
                    PrimitiveArray::new(buffer![2i64, 2044, 2], Validity::NonNullable).into_array(),
                    validity,
                )
                .with_zero_copy_to_list(true)
            }
            .into_array()
        } else {
            ListArray::new(
                inner,
                PrimitiveArray::new(buffer![0i64, 2, 2046, 2048], Validity::NonNullable)
                    .into_array(),
                validity,
            )
            .into_array()
        };
        let target = Field::new(
            "",
            arrow_schema::DataType::List(Arc::new(Field::new("item", inner_type, false))),
            true,
        );
        let all_null = validity_kind != 0;
        let mut source_row = 0;
        for chunk in BoundedArrayIterator::new(std::iter::once(Ok(input)), batch_size)? {
            let array = session
                .arrow()
                .execute_arrow(chunk?, Some(&target), &mut ctx)?;
            let outer = array.as_list::<i32>();
            assert!(outer.len() <= batch_size);
            for row in 0..outer.len() {
                let is_null = all_null || source_row == 1;
                assert_eq!(outer.is_null(row), is_null);
                assert_eq!(outer.value_length(row), if is_null { 0 } else { 2 });
                if !is_null {
                    let value = outer.value(row);
                    let inner = if with_struct {
                        Arc::clone(value.as_struct().column(0))
                    } else {
                        value
                    };
                    let inner = inner.as_list::<i32>();
                    assert_eq!(inner.len(), 2);
                    let start = if source_row == 0 { 0 } else { 2046 };
                    for index in 0..2 {
                        let values = inner.value(index);
                        assert_eq!(values.len(), 1);
                        assert_eq!(
                            values.as_string::<i32>().value(0),
                            (start + index).to_string(),
                        );
                    }
                }
                source_row += 1;
            }
        }
        assert_eq!(source_row, 3);
        if all_null {
            assert!(canonical_lengths.lock().is_empty());
            assert!(arrow_lengths.lock().iter().all(|length| *length == 0));
        } else {
            assert_eq!(canonical_lengths.lock().iter().sum::<usize>(), 4);
            assert_eq!(arrow_lengths.lock().iter().sum::<usize>(), 4);
        }
        Ok(())
    }

    fn check_null_nested_list_batches(view: bool) -> VortexResult<()> {
        for with_struct in [false, true] {
            for batch_size in [1, 2] {
                for validity_kind in 0..3 {
                    check_null_nested_list_batch(view, with_struct, batch_size, validity_kind)?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn bounded_null_nested_lists_skip_unreferenced_children() -> VortexResult<()> {
        check_null_nested_list_batches(false)
    }

    #[test]
    fn bounded_null_nested_list_views_skip_unreferenced_children() -> VortexResult<()> {
        check_null_nested_list_batches(true)
    }

    #[test]
    fn bounded_struct_and_list_nesting_recursively_limits_child_export() -> VortexResult<()> {
        for with_struct in [false, true] {
            let (session, lengths) = bounded_child_session(2);
            let mut ctx = session.create_execution_ctx();
            let inner = ListArray::new(
                nested_dictionary_elements(false).slice(0..6)?,
                PrimitiveArray::new(buffer![0i64, 1, 2, 3, 4, 5, 6], Validity::NonNullable)
                    .into_array(),
                Validity::NonNullable,
            )
            .into_array();
            let inner_type = arrow_schema::DataType::List(Arc::new(bounded_child_field(false)));
            let (inner, inner_type) = if with_struct {
                (
                    StructArray::new(["items"].into(), vec![inner], 6, Validity::NonNullable)
                        .into_array(),
                    arrow_schema::DataType::Struct(
                        vec![Field::new("items", inner_type, false)].into(),
                    ),
                )
            } else {
                (inner, inner_type)
            };
            let outer = ListArray::new(
                inner,
                PrimitiveArray::new(buffer![0i64, 2, 4, 6], Validity::NonNullable).into_array(),
                Validity::NonNullable,
            )
            .into_array();
            let target = Field::new(
                "",
                arrow_schema::DataType::List(Arc::new(Field::new("item", inner_type, false))),
                false,
            );
            for chunk in BoundedArrayIterator::new(std::iter::once(Ok(outer)), 1)? {
                let array = session
                    .arrow()
                    .execute_arrow(chunk?, Some(&target), &mut ctx)?;
                let list = array.as_list::<i32>();
                assert_eq!(list.len(), 1);
                assert_eq!(list.values().len(), 2);
                let inner = if with_struct {
                    Arc::clone(list.values().as_struct().column(0))
                } else {
                    Arc::clone(list.values())
                };
                assert_eq!(inner.as_list::<i32>().values().len(), 2);
            }
            assert_eq!(*lengths.lock(), [2, 2, 2]);
        }
        Ok(())
    }

    #[test]
    fn bounded_nested_offsets_rebase_before_narrowing_without_large_allocation() -> VortexResult<()>
    {
        // ConstantArray stores one scalar even though the logical unused prefix exceeds i32.
        // This exercises the overflow boundary without allocating a multi-GiB array.
        let start = i32::MAX as usize + 17;
        for view in [false, true] {
            let (session, lengths) = bounded_child_session(2);
            let mut ctx = session.create_execution_ctx();
            let elements =
                ConstantArray::new(Scalar::utf8("x", Nullability::NonNullable), start + 2)
                    .into_array();
            let input = if view {
                // SAFETY: the single view refers to exactly the final two logical elements.
                unsafe {
                    ListViewArray::new_unchecked(
                        elements,
                        PrimitiveArray::new(buffer![start as i64], Validity::NonNullable)
                            .into_array(),
                        PrimitiveArray::new(buffer![2i64], Validity::NonNullable).into_array(),
                        Validity::NonNullable,
                    )
                    .with_zero_copy_to_list(true)
                }
                .into_array()
            } else {
                ListArray::new(
                    elements,
                    PrimitiveArray::new(
                        buffer![start as i64, (start + 2) as i64],
                        Validity::NonNullable,
                    )
                    .into_array(),
                    Validity::NonNullable,
                )
                .into_array()
            };
            let target = Field::new(
                "",
                arrow_schema::DataType::List(Arc::new(bounded_child_field(false))),
                false,
            );
            let array = session
                .arrow()
                .execute_arrow(input, Some(&target), &mut ctx)?;
            let list = array.as_list::<i32>();
            assert_eq!(list.value_offsets(), &[0, 2]);
            assert_eq!(list.values().as_string::<i32>().value(0), "x");
            assert_eq!(list.values().as_string::<i32>().value(1), "x");
            assert_eq!(*lengths.lock(), [2]);
        }
        Ok(())
    }

    #[derive(Default)]
    struct PartitionCounts {
        polls: AtomicUsize,
        executions: AtomicUsize,
        drops: AtomicUsize,
    }

    struct ArrayPartition {
        chunks: Vec<ArrayRef>,
        counts: Arc<PartitionCounts>,
    }

    impl Drop for ArrayPartition {
        fn drop(&mut self) {
            self.counts.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Partition for ArrayPartition {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn index(&self) -> usize {
            0
        }
        fn row_count(&self) -> Precision<u64> {
            Precision::exact(
                self.chunks
                    .iter()
                    .map(|chunk| chunk.len() as u64)
                    .sum::<u64>(),
            )
        }
        fn byte_size(&self) -> Precision<u64> {
            Precision::Absent
        }
        fn execute(self: Box<Self>) -> VortexResult<SendableArrayStream> {
            self.counts.executions.fetch_add(1, Ordering::SeqCst);
            let dtype = self.chunks[0].dtype().clone();
            let counts = Arc::clone(&self.counts);
            let stream =
                futures::stream::iter(self.chunks.clone().into_iter().map(Ok)).inspect(move |_| {
                    counts.polls.fetch_add(1, Ordering::SeqCst);
                });
            Ok(Box::pin(ArrayStreamAdapter::new(dtype, stream)))
        }
    }

    fn array_partition(chunks: Vec<ArrayRef>, counts: Arc<PartitionCounts>) -> *mut vx_partition {
        vx_partition::new(VxPartitionScan::Pending(Box::new(ArrayPartition {
            chunks,
            counts,
        })))
    }

    #[test]
    fn bounded_arrays_preserve_encoding_skip_empty_and_stop_after_error() {
        let chunk = dictionary_view_chunk();
        let empty = chunk.slice(0..0).unwrap();
        let polls = AtomicUsize::new(0);
        let input = vec![
            Ok(empty),
            Ok(chunk.clone()),
            Err(vortex_err!("input failure")),
            Ok(chunk.clone()),
        ]
        .into_iter()
        .inspect(|_| {
            polls.fetch_add(1, Ordering::SeqCst);
        });
        let mut slices = BoundedArrayIterator::new(input, 3).unwrap();
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let first = slices.next().unwrap().unwrap();
        assert_eq!(first.len(), 3);
        assert!(first.as_::<Struct>().unmasked_field(0).is::<Dict>());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(slices.next().unwrap().unwrap().len(), 3);
        assert_eq!(slices.next().unwrap().unwrap().len(), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(slices.next().unwrap().is_err());
        assert!(slices.next().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 3);

        let mut whole = BoundedArrayIterator::new(std::iter::once(Ok(chunk)), usize::MAX).unwrap();
        assert_eq!(whole.next().unwrap().unwrap().len(), 7);
        assert!(whole.next().is_none());
        assert!(
            BoundedArrayIterator::new(std::iter::empty::<VortexResult<ArrayRef>>(), 0).is_err()
        );
        assert!(
            BoundedArrayIterator::new(std::iter::empty::<VortexResult<ArrayRef>>(), 3)
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn partition_arrow_row_limit_applies_before_native_export() {
        // The original API is a negative control: C++ slicing cannot change its native batch.
        for bounded in [false, true] {
            let chunk = dictionary_view_chunk();
            let counts = Arc::new(PartitionCounts::default());
            let chunks = if bounded {
                vec![
                    chunk.slice(0..0).unwrap(),
                    chunk.clone(),
                    chunk.slice(0..0).unwrap(),
                ]
            } else {
                vec![chunk]
            };
            unsafe {
                let session = vx_session_new();
                let partition = array_partition(chunks, Arc::clone(&counts));
                let mut output = FFI_ArrowArrayStream::empty();
                let mut error = ptr::null_mut();
                let status = if bounded {
                    vx_partition_scan_arrow_with_batch_size(
                        session,
                        partition,
                        3,
                        &raw mut output,
                        &raw mut error,
                    )
                } else {
                    crate::scan::vx_partition_scan_arrow(
                        session,
                        partition,
                        &raw mut output,
                        &raw mut error,
                    )
                };
                assert_eq!(status, 0);
                assert!(error.is_null());
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                assert_eq!(counts.executions.load(Ordering::SeqCst), 1);
                assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                let mut reader = ArrowArrayStreamReader::try_new(output).unwrap();
                let mut rows = 0;
                let mut lengths = Vec::new();
                let expected_codes = [0, 1, 0, 2, 0, 0, 1];
                for batch in reader.by_ref() {
                    let batch = batch.unwrap();
                    lengths.push(batch.num_rows());
                    let text = batch.column(0).as_string::<i32>();
                    let bytes = batch.column(1).as_binary::<i32>();
                    for row in 0..batch.num_rows() {
                        let code = expected_codes[rows + row];
                        assert_eq!(text.is_null(row), code == 1);
                        assert_eq!(bytes.is_null(row), code == 1);
                        if code != 1 {
                            assert_eq!(
                                text.value(row),
                                if code == 0 {
                                    "repeated long dictionary value ".repeat(128)
                                } else {
                                    String::new()
                                }
                            );
                            assert_eq!(
                                bytes.value(row),
                                if code == 0 {
                                    vec![0xfd; 4096]
                                } else {
                                    Vec::new()
                                }
                            );
                        }
                    }
                    rows += batch.num_rows();
                    assert_eq!(
                        counts.polls.load(Ordering::SeqCst),
                        if bounded { 2 } else { 1 }
                    );
                }
                assert_eq!(rows, 7);
                assert_eq!(lengths, if bounded { vec![3, 3, 1] } else { vec![7] });
                assert_eq!(
                    counts.polls.load(Ordering::SeqCst),
                    if bounded { 3 } else { 1 }
                );
                drop(reader);
                vx_session_free(session);
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn rejected_partition_arrow_requests_consume_partition_without_polling() {
        unsafe {
            let session = vx_session_new();
            for invalid in 0..3 {
                let counts = Arc::new(PartitionCounts::default());
                let partition = array_partition(vec![dictionary_view_chunk()], Arc::clone(&counts));
                let mut output = FFI_ArrowArrayStream::empty();
                let mut error = ptr::null_mut();
                let status = vx_partition_scan_arrow_with_batch_size(
                    if invalid == 1 { ptr::null() } else { session },
                    partition,
                    if invalid == 0 { 0 } else { 3 },
                    if invalid == 2 {
                        ptr::null_mut()
                    } else {
                        &raw mut output
                    },
                    &raw mut error,
                );
                assert_eq!(status, 1);
                assert!(!error.is_null());
                assert!(output.release.is_none());
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                assert_eq!(counts.executions.load(Ordering::SeqCst), 0);
                assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                vx_error_free(error);
            }
            vx_session_free(session);
        }
    }

    #[test]
    fn scalar_new_timestamp_and_date_build_extension_scalars() {
        unsafe {
            let mut error = ptr::null_mut();
            let ts = vx_scalar_new_timestamp(
                1_700_000_000_123,
                vx_time_unit::VX_TIME_UNIT_MILLISECOND,
                ptr::null(),
                false,
                &raw mut error,
            );
            assert!(error.is_null());
            assert!(!ts.is_null());
            let DType::Extension(ext) = vx_scalar::as_ref(ts).dtype() else {
                panic!("timestamp scalar must carry an extension dtype");
            };
            assert!(ext.is::<Timestamp>());
            vx_scalar_free(ts);

            let tz = std::ffi::CString::new("America/New_York").unwrap();
            let ts_tz = vx_scalar_new_timestamp(
                0,
                vx_time_unit::VX_TIME_UNIT_SECOND,
                tz.as_ptr(),
                true,
                &raw mut error,
            );
            assert!(error.is_null());
            assert!(!ts_tz.is_null());
            vx_scalar_free(ts_tz);

            let date = vx_scalar_new_date(19_000, false, &raw mut error);
            assert!(error.is_null());
            assert!(!date.is_null());
            let DType::Extension(ext) = vx_scalar::as_ref(date).dtype() else {
                panic!("date scalar must carry an extension dtype");
            };
            assert!(ext.is::<Date>());
            vx_scalar_free(date);
        }
    }

    #[test]
    fn scalar_new_timestamp_declines_span_overflow_without_unwinding() {
        for (value, unit) in [
            (i64::MIN, vx_time_unit::VX_TIME_UNIT_SECOND),
            (i64::MAX, vx_time_unit::VX_TIME_UNIT_SECOND),
            (i64::MIN, vx_time_unit::VX_TIME_UNIT_MILLISECOND),
            (i64::MAX, vx_time_unit::VX_TIME_UNIT_MILLISECOND),
            (i64::MIN, vx_time_unit::VX_TIME_UNIT_MICROSECOND),
            (i64::MAX, vx_time_unit::VX_TIME_UNIT_MICROSECOND),
            (i64::MIN, vx_time_unit::VX_TIME_UNIT_NANOSECOND),
        ] {
            for timezone in [ptr::null(), c"UTC".as_ptr()] {
                unsafe {
                    let mut error = ptr::null_mut();
                    let scalar =
                        vx_scalar_new_timestamp(value, unit, timezone, false, &raw mut error);
                    assert!(scalar.is_null());
                    assert!(error.is_null());
                    assert!(
                        vx_scalar_new_timestamp(value, unit, timezone, true, ptr::null_mut())
                            .is_null()
                    );
                }
            }
        }
    }

    #[test]
    fn scalar_new_timestamp_declines_native_epoch_range_limits() {
        for (value, unit) in [
            (253_402_207_201, vx_time_unit::VX_TIME_UNIT_SECOND),
            (-377_705_023_202, vx_time_unit::VX_TIME_UNIT_SECOND),
            (253_402_207_201_000, vx_time_unit::VX_TIME_UNIT_MILLISECOND),
            (
                253_402_207_201_000_000,
                vx_time_unit::VX_TIME_UNIT_MICROSECOND,
            ),
            // Paimon permits the end of year 9999, beyond Jiff Timestamp's upper bound.
            (
                253_402_300_799_999_999,
                vx_time_unit::VX_TIME_UNIT_MICROSECOND,
            ),
        ] {
            for timezone in [ptr::null(), c"UTC".as_ptr()] {
                unsafe {
                    let mut error = ptr::null_mut();
                    let scalar =
                        vx_scalar_new_timestamp(value, unit, timezone, false, &raw mut error);
                    assert!(scalar.is_null());
                    assert!(error.is_null());
                }
            }
        }
    }

    #[test]
    fn scalar_new_timestamp_preserves_timezone_errors_outside_native_range() {
        for timezone in [c"Not/A/Timezone", c"\xff"] {
            for value in [0, i64::MAX] {
                unsafe {
                    let mut error = ptr::null_mut();
                    let scalar = vx_scalar_new_timestamp(
                        value,
                        vx_time_unit::VX_TIME_UNIT_MICROSECOND,
                        timezone.as_ptr(),
                        false,
                        &raw mut error,
                    );
                    assert!(scalar.is_null());
                    assert!(!error.is_null());
                    assert!(!vx_string::as_str(vx_error_get_message(error)).is_empty());
                    vx_error_free(error);
                }
            }
        }
    }

    #[test]
    fn scalar_new_timestamp_preserves_valid_storage_and_metadata() {
        let cases = [
            (vx_time_unit::VX_TIME_UNIT_SECOND, TimeUnit::Seconds, -1),
            (vx_time_unit::VX_TIME_UNIT_SECOND, TimeUnit::Seconds, 0),
            (
                vx_time_unit::VX_TIME_UNIT_MILLISECOND,
                TimeUnit::Milliseconds,
                1,
            ),
            (
                vx_time_unit::VX_TIME_UNIT_MICROSECOND,
                TimeUnit::Microseconds,
                -1,
            ),
            (
                vx_time_unit::VX_TIME_UNIT_MICROSECOND,
                TimeUnit::Microseconds,
                253_402_207_200_999_999,
            ),
            (
                vx_time_unit::VX_TIME_UNIT_NANOSECOND,
                TimeUnit::Nanoseconds,
                i64::MIN + 1,
            ),
            (
                vx_time_unit::VX_TIME_UNIT_NANOSECOND,
                TimeUnit::Nanoseconds,
                i64::MAX,
            ),
        ];
        for (unit, expected_unit, value) in cases {
            for timezone in [ptr::null(), c"UTC".as_ptr()] {
                for nullable in [false, true] {
                    unsafe {
                        let mut error = ptr::null_mut();
                        let scalar = vx_scalar_new_timestamp(
                            value,
                            unit,
                            timezone,
                            nullable,
                            &raw mut error,
                        );
                        assert!(
                            error.is_null(),
                            "unit={unit:?}, value={value}, timezone={}, nullable={nullable}: {}",
                            if timezone.is_null() { "none" } else { "UTC" },
                            if error.is_null() {
                                "no native error"
                            } else {
                                vx_string::as_str(vx_error_get_message(error))
                            }
                        );
                        assert!(!scalar.is_null());
                        let scalar_ref = vx_scalar::as_ref(scalar);
                        assert_eq!(
                            scalar_ref
                                .value()
                                .unwrap()
                                .as_primitive()
                                .cast::<i64>()
                                .unwrap(),
                            value
                        );
                        assert_eq!(scalar_ref.dtype().is_nullable(), nullable);
                        let DType::Extension(ext) = scalar_ref.dtype() else {
                            panic!("timestamp scalar must carry an extension dtype");
                        };
                        let metadata = ext.metadata_opt::<Timestamp>().unwrap();
                        assert_eq!(metadata.unit, expected_unit);
                        assert_eq!(
                            metadata.tz.as_deref(),
                            if timezone.is_null() {
                                None
                            } else {
                                Some("UTC")
                            }
                        );
                        assert!(!scalar_ref.as_extension().to_string().is_empty());
                        vx_scalar_free(scalar);
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_new_date_declines_out_of_range_civil_dates() {
        for days in [i32::MIN, i32::MAX] {
            unsafe {
                let mut error = ptr::null_mut();
                let scalar = vx_scalar_new_date(days, false, &raw mut error);
                assert!(scalar.is_null());
                assert!(error.is_null());
                assert!(vx_scalar_new_date(days, true, ptr::null_mut()).is_null());
            }
        }
    }

    #[test]
    fn scalar_new_date_preserves_supported_dates() {
        for days in [-719_528, -1, 0, 19_000, 2_932_896] {
            for nullable in [false, true] {
                unsafe {
                    let mut error = ptr::null_mut();
                    let scalar = vx_scalar_new_date(days, nullable, &raw mut error);
                    assert!(error.is_null());
                    assert!(!scalar.is_null());
                    let scalar_ref = vx_scalar::as_ref(scalar);
                    assert_eq!(
                        scalar_ref
                            .value()
                            .unwrap()
                            .as_primitive()
                            .cast::<i32>()
                            .unwrap(),
                        days
                    );
                    assert_eq!(scalar_ref.dtype().is_nullable(), nullable);
                    assert!(!scalar_ref.as_extension().to_string().is_empty());
                    vx_scalar_free(scalar);
                }
            }
        }
    }

    #[test]
    fn normalize_arrow_schema_views_rewrites_view_types_recursively() {
        use std::sync::Arc;

        use arrow_schema::DataType;
        use arrow_schema::Field;
        use arrow_schema::Schema;

        let schema = Schema::new(vec![
            Field::new("plain_utf8", DataType::Utf8, false),
            Field::new("utf8_view", DataType::Utf8View, true),
            Field::new("binary_view", DataType::BinaryView, true),
            Field::new(
                "struct_col",
                DataType::Struct(
                    vec![
                        Field::new("inner_view", DataType::Utf8View, true),
                        Field::new("inner_int", DataType::Int32, false),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "list_view",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true))),
                true,
            ),
            Field::new(
                "fsl_view",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::BinaryView, true)),
                    3,
                ),
                true,
            ),
        ]);

        let normalized = normalize_arrow_schema_views(schema);

        assert_eq!(
            normalized
                .field_with_name("plain_utf8")
                .unwrap()
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            normalized.field_with_name("utf8_view").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            normalized
                .field_with_name("binary_view")
                .unwrap()
                .data_type(),
            &DataType::Binary
        );
        match normalized
            .field_with_name("struct_col")
            .unwrap()
            .data_type()
        {
            DataType::Struct(fields) => {
                assert_eq!(fields[0].data_type(), &DataType::Utf8);
                assert_eq!(fields[1].data_type(), &DataType::Int32);
            }
            other => panic!("expected Struct, got {:?}", other),
        }
        match normalized.field_with_name("list_view").unwrap().data_type() {
            DataType::List(field) => assert_eq!(field.data_type(), &DataType::Utf8),
            other => panic!("expected List, got {:?}", other),
        }
        match normalized.field_with_name("fsl_view").unwrap().data_type() {
            DataType::FixedSizeList(field, size) => {
                assert_eq!(field.data_type(), &DataType::Binary);
                assert_eq!(*size, 3);
            }
            other => panic!("expected FixedSizeList, got {:?}", other),
        }
        assert_eq!(normalized.fields().len(), 6);
        assert!(
            normalized
                .field_with_name("utf8_view")
                .unwrap()
                .is_nullable()
        );
        assert!(
            !normalized
                .field_with_name("plain_utf8")
                .unwrap()
                .is_nullable()
        );
    }

    struct FileContext {
        file: File,
    }

    unsafe extern "C" fn file_read_at(
        ctx: *mut c_void,
        offset: u64,
        dst: *mut u8,
        length: usize,
    ) -> i32 {
        // SAFETY: `ctx` is the `FileContext` handed to the callback data source, and `dst`
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

    // Read a file written by the standard sink through host callbacks.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_callback_data_source_round_trip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        let dtype = DType::Primitive(PType::I32, false.into());

        unsafe {
            let session = vx_session_new();
            let vx_dtype_ptr = vx_dtype::new(Arc::new(dtype));
            let mut error = ptr::null_mut();

            let c_path = std::ffi::CString::new(path.clone()).unwrap();
            let sink =
                vx_array_sink_open_file(session, c_path.as_ptr(), vx_dtype_ptr, &raw mut error);
            assert!(error.is_null());
            let array = PrimitiveArray::new(buffer![1i32, 2i32, 3i32], Validity::NonNullable);
            let vx_array_ptr = vx_array::new(Arc::new(array.into_array()));
            vx_array_sink_push(sink, vx_array_ptr, &raw mut error);
            assert!(error.is_null());
            vx_array_sink_close(sink, &raw mut error);
            assert!(error.is_null());
            vx_array_free(vx_array_ptr);
            vx_dtype_free(vx_dtype_ptr);

            for config in [
                CoalesceConfig::object_storage(),
                CoalesceConfig::new(64 << 10, 4 << 20),
            ] {
                let file = File::open(&path).unwrap();
                let size = file.metadata().unwrap().len();
                let context = Box::into_raw(Box::new(FileContext { file }));
                let callbacks = vx_input_callbacks {
                    ctx: context.cast::<c_void>(),
                    read_at_fn: Some(file_read_at),
                    release_fn: Some(file_release),
                };

                let data_source = vx_data_source_new_callback_with_coalesce(
                    session,
                    callbacks,
                    size,
                    config.distance,
                    config.max_size,
                    &raw mut error,
                );
                assert!(error.is_null());
                assert!(!data_source.is_null());

                let mut row_count = vx_estimate::default();
                vx_data_source_get_row_count(data_source, &raw mut row_count);
                assert_eq!(row_count.r#type, vx_estimate_type::VX_ESTIMATE_EXACT);
                assert_eq!(row_count.estimate, 3);

                vx_data_source_free(data_source);
            }
            vx_session_free(session);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn callback_coalesce_preserves_bytes_bounds_and_context_lifetime() {
        struct MemoryContext {
            bytes: Vec<u8>,
            reads: AtomicUsize,
            releases: AtomicUsize,
        }

        unsafe extern "C" fn read_memory(
            ctx: *mut c_void,
            offset: u64,
            dst: *mut u8,
            length: usize,
        ) -> i32 {
            // SAFETY: target owns the boxed Arc, and CallbackReadAt validates the range first.
            let context = unsafe { &*ctx.cast::<Arc<MemoryContext>>() };
            context.reads.fetch_add(1, Ordering::SeqCst);
            let Ok(start) = usize::try_from(offset) else {
                return -1;
            };
            let Some(end) = start.checked_add(length) else {
                return -1;
            };
            let Some(bytes) = context.bytes.get(start..end) else {
                return -1;
            };
            unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), dst, length) };
            0
        }

        unsafe extern "C" fn release_memory(ctx: *mut c_void) {
            // SAFETY: CallbackTarget releases this boxed Arc exactly once after all readers.
            let context = unsafe { Box::from_raw(ctx.cast::<Arc<MemoryContext>>()) };
            context.releases.fetch_add(1, Ordering::SeqCst);
        }

        for config in [
            CoalesceConfig::object_storage(),
            CoalesceConfig::new(64 << 10, 4 << 20),
        ] {
            let context = Arc::new(MemoryContext {
                bytes: (0..200_000).map(|i| (i % 251) as u8).collect(),
                reads: AtomicUsize::new(0),
                releases: AtomicUsize::new(0),
            });
            let reader = Arc::new(CallbackReadAt {
                target: Arc::new(CallbackTarget {
                    ctx: Box::into_raw(Box::new(Arc::clone(&context))).cast::<c_void>(),
                    release_fn: Some(release_memory),
                }),
                read_at_fn: read_memory,
                len: context.bytes.len() as u64,
                handle: RUNTIME.handle(),
                concurrency: DEFAULT_CONCURRENCY,
                coalesce_config: config,
            });
            let actual_config = reader.coalesce_config().unwrap();
            assert_eq!(actual_config.distance, config.distance);
            assert_eq!(actual_config.max_size, config.max_size);
            RUNTIME.block_on(async {
                for (offset, length) in [(0, 1), (63_999, 4097), (200_000, 0)] {
                    let buffer = reader
                        .read_at(offset, length, Alignment::none())
                        .await
                        .unwrap()
                        .to_host()
                        .await;
                    let start = usize::try_from(offset).unwrap();
                    let end = start.checked_add(length).unwrap();
                    assert_eq!(buffer.as_ref(), &context.bytes[start..end]);
                }
                assert!(reader.read_at(199_999, 2, Alignment::none()).await.is_err());
                assert!(
                    reader
                        .read_at(u64::MAX, 1, Alignment::none())
                        .await
                        .is_err()
                );
            });
            assert_eq!(context.reads.load(Ordering::SeqCst), 2);
            assert_eq!(context.releases.load(Ordering::SeqCst), 0);
            drop(reader);
            assert_eq!(context.releases.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cancelled_in_flight_read_releases_context_after_callback_returns() {
        use std::sync::mpsc;
        use std::time::Duration;

        use futures::channel::oneshot;
        use parking_lot::Condvar;

        struct BlockingContext {
            entered: Mutex<Option<oneshot::Sender<()>>>,
            released: mpsc::Sender<()>,
            proceed: Mutex<bool>,
            wake: Condvar,
            completed: AtomicBool,
            released_while_active: AtomicBool,
            releases: AtomicUsize,
            status: i32,
        }

        unsafe extern "C" fn read_blocking(
            ctx: *mut c_void,
            _offset: u64,
            dst: *mut u8,
            length: usize,
        ) -> i32 {
            let context = unsafe { &*ctx.cast::<Arc<BlockingContext>>() };
            if let Some(entered) = context.entered.lock().take() {
                let _ = entered.send(());
            }
            let mut proceed = context.proceed.lock();
            let timeout =
                context
                    .wake
                    .wait_while_for(&mut proceed, |go| !*go, Duration::from_secs(20));
            if !timeout.timed_out() && context.status == 0 {
                unsafe { ptr::write_bytes(dst, 0x5a, length) };
            }
            context.completed.store(true, Ordering::SeqCst);
            if timeout.timed_out() {
                -99
            } else {
                context.status
            }
        }

        unsafe extern "C" fn release_blocking(ctx: *mut c_void) {
            let context = unsafe { Box::from_raw(ctx.cast::<Arc<BlockingContext>>()) };
            context
                .released_while_active
                .store(!context.completed.load(Ordering::SeqCst), Ordering::SeqCst);
            context.releases.fetch_add(1, Ordering::SeqCst);
            let _ = context.released.send(());
        }

        for status in [0, -17] {
            let (entered_tx, entered_rx) = oneshot::channel();
            let (released_tx, released_rx) = mpsc::channel();
            let context = Arc::new(BlockingContext {
                entered: Mutex::new(Some(entered_tx)),
                released: released_tx,
                proceed: Mutex::new(false),
                wake: Condvar::new(),
                completed: AtomicBool::new(false),
                released_while_active: AtomicBool::new(false),
                releases: AtomicUsize::new(0),
                status,
            });
            let reader = CallbackReadAt {
                target: Arc::new(CallbackTarget {
                    ctx: Box::into_raw(Box::new(Arc::clone(&context))).cast::<c_void>(),
                    release_fn: Some(release_blocking),
                }),
                read_at_fn: read_blocking,
                len: 16,
                handle: RUNTIME.handle(),
                concurrency: DEFAULT_CONCURRENCY,
                coalesce_config: CoalesceConfig::object_storage(),
            };
            RUNTIME.block_on(async {
                let future = reader.read_at(0, 16, Alignment::none());
                let pending = RUNTIME.handle().spawn(future);
                entered_rx.await.unwrap();
                drop(pending);
                drop(reader);
                assert_eq!(context.releases.load(Ordering::SeqCst), 0);
                assert!(!context.completed.load(Ordering::SeqCst));
                *context.proceed.lock() = true;
                context.wake.notify_all();
                RUNTIME
                    .handle()
                    .spawn_blocking(move || released_rx.recv_timeout(Duration::from_secs(20)))
                    .await
                    .unwrap();
            });
            assert!(context.completed.load(Ordering::SeqCst));
            assert!(!context.released_while_active.load(Ordering::SeqCst));
            assert_eq!(context.releases.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn callback_read_error_is_propagated_and_context_released_once() {
        unsafe extern "C" fn fail_read(
            _ctx: *mut c_void,
            _offset: u64,
            _dst: *mut u8,
            _length: usize,
        ) -> i32 {
            -31
        }

        unsafe extern "C" fn release_counter(ctx: *mut c_void) {
            let released = unsafe { Box::from_raw(ctx.cast::<Arc<AtomicUsize>>()) };
            released.fetch_add(1, Ordering::SeqCst);
        }

        let released = Arc::new(AtomicUsize::new(0));
        let reader = CallbackReadAt {
            target: Arc::new(CallbackTarget {
                ctx: Box::into_raw(Box::new(Arc::clone(&released))).cast::<c_void>(),
                release_fn: Some(release_counter),
            }),
            read_at_fn: fail_read,
            len: 16,
            handle: RUNTIME.handle(),
            concurrency: DEFAULT_CONCURRENCY,
            coalesce_config: CoalesceConfig::object_storage(),
        };
        let error = RUNTIME
            .block_on(reader.read_at(0, 16, Alignment::none()))
            .unwrap_err();
        assert!(error.to_string().contains("status -31"));
        assert_eq!(released.load(Ordering::SeqCst), 0);
        drop(reader);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn invalid_coalesce_releases_callback_context() {
        static RELEASED: AtomicUsize = AtomicUsize::new(0);

        unsafe extern "C" fn mark_released(_ctx: *mut c_void) {
            RELEASED.fetch_add(1, Ordering::SeqCst);
        }

        unsafe {
            let session = vx_session_new();
            for (max_gap, max_size) in [(0, 0), (1025, 1024)] {
                let mut error = ptr::null_mut();
                let previous_releases = RELEASED.load(Ordering::SeqCst);
                let callbacks = vx_input_callbacks {
                    ctx: ptr::null_mut(),
                    read_at_fn: Some(file_read_at),
                    release_fn: Some(mark_released),
                };
                let data_source = vx_data_source_new_callback_with_coalesce(
                    session,
                    callbacks,
                    0,
                    max_gap,
                    max_size,
                    &raw mut error,
                );
                assert!(data_source.is_null());
                assert!(!error.is_null());
                vx_error_free(error);
                assert_eq!(RELEASED.load(Ordering::SeqCst), previous_releases + 1);
            }
            vx_session_free(session);
        }
    }

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
            let callbacks = vx_input_callbacks {
                ctx: ptr::null_mut(),
                read_at_fn: None,
                release_fn: Some(mark_released),
            };

            let config = CoalesceConfig::object_storage();
            let data_source = vx_data_source_new_callback_with_coalesce(
                session,
                callbacks,
                0,
                config.distance,
                config.max_size,
                &raw mut error,
            );
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

    // Round-trip through an in-memory buffer using only callbacks.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_callback_sink_round_trip() {
        struct BufferContext {
            data: Mutex<Vec<u8>>,
        }

        unsafe extern "C" fn buffer_write(ctx: *mut c_void, src: *const u8, length: usize) -> i32 {
            // SAFETY: `ctx` is the `Arc<BufferContext>` handed to the sink, and `src` covers
            // `length` bytes, per the callback contract.
            let holder = unsafe { &*ctx.cast::<Arc<BufferContext>>() };
            let bytes = unsafe { std::slice::from_raw_parts(src, length) };
            holder.data.lock().extend_from_slice(bytes);
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
            let data = holder.data.lock();
            let Ok(start) = usize::try_from(offset) else {
                return -1;
            };
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
            let vx_dtype_ptr = vx_dtype::new(Arc::new(dtype));
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
            let vx_array_ptr = vx_array::new(Arc::new(array.into_array()));
            vx_callback_sink_push(sink, vx_array_ptr, &raw mut error);
            assert!(error.is_null());
            vx_callback_sink_close(sink, &raw mut error);
            assert!(error.is_null());
            vx_array_free(vx_array_ptr);

            let size = shared.data.lock().len() as u64;
            assert!(size > 0, "the sink must have written through the callback");

            let in_ctx = Box::into_raw(Box::new(Arc::clone(&shared)));
            let config = CoalesceConfig::object_storage();
            let data_source = vx_data_source_new_callback_with_coalesce(
                session,
                vx_input_callbacks {
                    ctx: in_ctx.cast::<c_void>(),
                    read_at_fn: Some(buffer_read_at),
                    release_fn: Some(buffer_release),
                },
                size,
                config.distance,
                config.max_size,
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
