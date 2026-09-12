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
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ffi::{c_char, c_void};
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, StructArray};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use futures::StreamExt;
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::v2::reader::{FileReader, FileReaderOptions, ReaderProjection};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::stream::RecordBatchStream;
use lance_io::utils::CachedFileSize;
use lance_io::ReadBatchParams;
use object_store::path::Path;

use crate::error::{clear_last_error, fail};
use crate::runtime::block_on;
use crate::util::{parse_options, parse_projection, parse_ranges, required_string};

pub struct PaimonLanceReader {
    reader: Arc<FileReader>,
}

pub struct PaimonLanceBatchReader {
    stream: Pin<Box<dyn RecordBatchStream>>,
}

/// # Safety
///
/// `uri` and every configured option must point to valid NUL-terminated strings.
/// `out_reader` must be valid for writes. Option arrays must contain
/// `storage_option_count` valid entries, or be null when the count is zero.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_reader_open(
    uri: *const c_char,
    storage_option_keys: *const *const c_char,
    storage_option_values: *const *const c_char,
    storage_option_count: usize,
    out_reader: *mut *mut PaimonLanceReader,
) -> i32 {
    clear_last_error();
    if out_reader.is_null() {
        return fail("out_reader is nullptr");
    }
    unsafe {
        *out_reader = ptr::null_mut();
    }
    let uri = match required_string(uri, "Lance reader URI") {
        Ok(uri) => uri,
        Err(error) => return fail(error),
    };
    let storage_options = match parse_options(
        storage_option_keys,
        storage_option_values,
        storage_option_count,
    ) {
        Ok(options) => options,
        Err(error) => return fail(error),
    };
    let reader = match block_on(async move {
        let params = ObjectStoreParams {
            storage_options: Some(storage_options),
            ..Default::default()
        };
        let (object_store, path) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            &uri,
            &params,
        )
        .await
        .map_err(|error| format!("open Lance object store for {uri}: {error}"))?;
        let config = SchedulerConfig::max_bandwidth(&object_store);
        let scheduler = ScanScheduler::new(object_store, config);
        let file_scheduler = scheduler
            .open_file(
                &Path::parse(&path).map_err(|error| format!("parse Lance path: {error}"))?,
                &CachedFileSize::unknown(),
            )
            .await
            .map_err(|error| format!("open Lance file {uri}: {error}"))?;
        FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .map_err(|error| format!("read Lance file metadata {uri}: {error}"))
    }) {
        Ok(reader) => reader,
        Err(error) => return fail(error),
    };
    unsafe {
        *out_reader = Box::into_raw(Box::new(PaimonLanceReader {
            reader: Arc::new(reader),
        }));
    }
    0
}

/// # Safety
///
/// `reader` must be a live handle returned by `paimon_lance_reader_open`.
/// `out_schema` must point to writable storage for an Arrow C schema.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_reader_export_schema(
    reader: *const PaimonLanceReader,
    out_schema: *mut c_void,
) -> i32 {
    clear_last_error();
    if reader.is_null() || out_schema.is_null() {
        return fail("Lance reader and Arrow schema must be non-null");
    }
    let reader = unsafe { &*reader };
    let arrow_schema = ArrowSchema::from(reader.reader.schema().as_ref());
    let ffi_schema = match FFI_ArrowSchema::try_from(&arrow_schema) {
        Ok(schema) => schema,
        Err(error) => return fail(format!("export Lance schema: {error}")),
    };
    unsafe {
        ptr::write(out_schema.cast::<FFI_ArrowSchema>(), ffi_schema);
    }
    0
}

/// # Safety
///
/// `reader` must be a live handle returned by `paimon_lance_reader_open`, and
/// `out_row_count` must be valid for writes.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_reader_num_rows(
    reader: *const PaimonLanceReader,
    out_row_count: *mut u64,
) -> i32 {
    clear_last_error();
    if reader.is_null() || out_row_count.is_null() {
        return fail("Lance reader and out_row_count must be non-null");
    }
    unsafe {
        *out_row_count = (*reader).reader.num_rows();
    }
    0
}

/// # Safety
///
/// `reader` must be a live reader handle and `out_batch_reader` must be valid
/// for writes. Projection and selection arrays must contain the declared
/// number of valid entries, or be null when their counts are zero.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_reader_open_stream(
    reader: *const PaimonLanceReader,
    batch_size: u32,
    batch_readahead: u32,
    projection_names: *const *const c_char,
    projection_count: usize,
    has_selection: bool,
    selection_starts: *const u64,
    selection_ends: *const u64,
    selection_range_count: usize,
    out_batch_reader: *mut *mut PaimonLanceBatchReader,
) -> i32 {
    clear_last_error();
    if reader.is_null() || out_batch_reader.is_null() {
        return fail("Lance reader and out_batch_reader must be non-null");
    }
    if batch_size == 0 || batch_readahead == 0 {
        return fail("Lance batch size and batch readahead must be positive");
    }
    unsafe {
        *out_batch_reader = ptr::null_mut();
    }
    let projection_names = match parse_projection(projection_names, projection_count) {
        Ok(names) => names,
        Err(error) => return fail(error),
    };
    let ranges = match parse_ranges(selection_starts, selection_ends, selection_range_count) {
        Ok(ranges) => ranges,
        Err(error) => return fail(error),
    };
    let reader = unsafe { &*reader };
    let projection = if projection_names.is_empty() {
        ReaderProjection::from_whole_schema(
            reader.reader.schema(),
            reader.reader.metadata().version(),
        )
    } else {
        let names: Vec<&str> = projection_names.iter().map(String::as_str).collect();
        match ReaderProjection::from_column_names(
            reader.reader.metadata().version(),
            reader.reader.schema(),
            &names,
        ) {
            Ok(projection) => projection,
            Err(error) => return fail(format!("create Lance projection: {error}")),
        }
    };
    let read_params = if has_selection {
        ReadBatchParams::Ranges(ranges.into_boxed_slice().into())
    } else {
        ReadBatchParams::RangeFull
    };
    let stream = match block_on(async {
        reader
            .reader
            .read_stream_projected(
                read_params,
                batch_size,
                batch_readahead,
                projection,
                FilterExpression::no_filter(),
            )
            .map_err(|error| format!("open Lance batch reader: {error}"))
    }) {
        Ok(stream) => stream,
        Err(error) => return fail(error),
    };
    unsafe {
        *out_batch_reader = Box::into_raw(Box::new(PaimonLanceBatchReader { stream }));
    }
    0
}

/// # Safety
///
/// `reader` must be a live batch reader handle. The output pointers must be
/// valid for writes and must not alias the reader.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_batch_reader_next(
    reader: *mut PaimonLanceBatchReader,
    out_array: *mut c_void,
    out_schema: *mut c_void,
    out_eof: *mut bool,
) -> i32 {
    clear_last_error();
    if reader.is_null() || out_array.is_null() || out_schema.is_null() || out_eof.is_null() {
        return fail("Lance batch reader, Arrow outputs, and EOF output must be non-null");
    }
    let reader = unsafe { &mut *reader };
    let next = match block_on(async {
        reader
            .stream
            .next()
            .await
            .transpose()
            .map_err(|error| format!("read Lance batch: {error}"))
    }) {
        Ok(next) => next,
        Err(error) => return fail(error),
    };
    match next {
        Some(batch) => {
            let struct_array = StructArray::from(batch);
            let (ffi_array, ffi_schema) = match to_ffi(&struct_array.to_data()) {
                Ok(output) => output,
                Err(error) => return fail(format!("export Lance batch: {error}")),
            };
            unsafe {
                ptr::write(out_array.cast::<FFI_ArrowArray>(), ffi_array);
                ptr::write(out_schema.cast::<FFI_ArrowSchema>(), ffi_schema);
                *out_eof = false;
            }
            0
        }
        None => {
            unsafe {
                *out_eof = true;
            }
            0
        }
    }
}

/// # Safety
///
/// `reader` must be null or a live handle returned by
/// `paimon_lance_reader_open_stream`, and it must be freed at most once.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_batch_reader_free(reader: *mut PaimonLanceBatchReader) {
    if !reader.is_null() {
        unsafe {
            drop(Box::from_raw(reader));
        }
    }
}

/// # Safety
///
/// `reader` must be null or a live handle returned by
/// `paimon_lance_reader_open`, and it must be freed at most once after all
/// associated batch readers have been freed.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_reader_free(reader: *mut PaimonLanceReader) {
    if !reader.is_null() {
        unsafe {
            drop(Box::from_raw(reader));
        }
    }
}
