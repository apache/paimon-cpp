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
    // Vortex adds the distance to file offsets, which are bounded by i64::MAX in Paimon.
    vortex_ensure!(
        coalesce_config.max_size <= i64::MAX as u64,
        "coalesced max_size must not exceed INT64_MAX"
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
/// `size` is the total file length in bytes. `max_size` must be in `1..=i64::MAX`, and `max_gap`
/// must not exceed it. A smaller gap avoids reading unrelated bytes between sparse requests.
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

            let file = File::open(&path).unwrap();
            let size = file.metadata().unwrap().len();
            let context = Box::into_raw(Box::new(FileContext { file }));
            let callbacks = vx_input_callbacks {
                ctx: context.cast::<c_void>(),
                read_at_fn: Some(file_read_at),
                release_fn: Some(file_release),
            };

            let config = CoalesceConfig::object_storage();
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

            let size = shared.data.lock().unwrap().len() as u64;
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
