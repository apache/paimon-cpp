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
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, RecordBatchOptions, StructArray};
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
use lance_file::v2::writer::{FileWriter, FileWriterOptions};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};

use crate::error::{clear_last_error, fail};
use crate::runtime::block_on;
use crate::util::{parse_options, required_string};

pub struct PaimonLanceWriter {
    writer: FileWriter,
    finished: bool,
}

/// # Safety
///
/// `uri` and every configured option must point to valid NUL-terminated strings.
/// `out_writer` must be valid for writes. Option arrays must contain
/// `storage_option_count` valid entries, or be null when the count is zero.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_writer_open(
    uri: *const c_char,
    storage_option_keys: *const *const c_char,
    storage_option_values: *const *const c_char,
    storage_option_count: usize,
    out_writer: *mut *mut PaimonLanceWriter,
) -> i32 {
    clear_last_error();
    if out_writer.is_null() {
        return fail("out_writer is nullptr");
    }
    unsafe {
        *out_writer = ptr::null_mut();
    }
    let uri = match required_string(uri, "Lance writer URI") {
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
    let writer = match block_on(async move {
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
        let object_store = Arc::new(object_store);
        let object_writer = object_store
            .create(&path)
            .await
            .map_err(|error| format!("create Lance file {uri}: {error}"))?;
        Ok::<FileWriter, String>(FileWriter::new_lazy(
            object_writer,
            FileWriterOptions::default(),
        ))
    }) {
        Ok(writer) => writer,
        Err(error) => return fail(error),
    };
    unsafe {
        *out_writer = Box::into_raw(Box::new(PaimonLanceWriter {
            writer,
            finished: false,
        }));
    }
    0
}

/// # Safety
///
/// `writer` must be a live handle returned by `paimon_lance_writer_open`.
/// `array` and `schema` must point to valid Arrow C Data Interface objects
/// whose ownership may be transferred by this call.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_writer_write(
    writer: *mut PaimonLanceWriter,
    array: *mut c_void,
    schema: *mut c_void,
) -> i32 {
    clear_last_error();
    if writer.is_null() || array.is_null() || schema.is_null() {
        return fail("Lance writer, Arrow array, and Arrow schema must be non-null");
    }
    let writer = unsafe { &mut *writer };
    if writer.finished {
        return fail("cannot write after Lance writer is finished");
    }
    let ffi_array = unsafe { FFI_ArrowArray::from_raw(array.cast()) };
    let ffi_schema = unsafe { FFI_ArrowSchema::from_raw(schema.cast()) };
    let data_type = match DataType::try_from(&ffi_schema) {
        Ok(data_type) => data_type,
        Err(error) => return fail(format!("import Arrow schema: {error}")),
    };
    if !matches!(&data_type, DataType::Struct(_)) {
        return fail(format!(
            "Lance writer requires a struct Arrow array, got {data_type}"
        ));
    }
    let array_data = match unsafe { from_ffi_and_data_type(ffi_array, data_type) } {
        Ok(array_data) => array_data,
        Err(error) => return fail(format!("import Arrow array: {error}")),
    };
    let struct_array = StructArray::from(array_data);
    if struct_array.null_count() != 0 {
        return fail("Lance writer does not accept null top-level rows");
    }
    let row_count = struct_array.len();
    let (fields, columns, _) = struct_array.into_parts();
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch = match RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        columns,
        &options,
    ) {
        Ok(batch) => batch,
        Err(error) => return fail(format!("create Lance record batch: {error}")),
    };
    match block_on(async {
        writer
            .writer
            .write_batch(&batch)
            .await
            .map_err(|error| format!("write Lance batch: {error}"))
    }) {
        Ok(()) => 0,
        Err(error) => fail(error),
    }
}

/// # Safety
///
/// `writer` must be a live writer handle and `out_position` must be valid for
/// writes.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_writer_tell(
    writer: *mut PaimonLanceWriter,
    out_position: *mut u64,
) -> i32 {
    clear_last_error();
    if writer.is_null() || out_position.is_null() {
        return fail("Lance writer and out_position must be non-null");
    }
    let writer = unsafe { &mut *writer };
    match block_on(async {
        writer
            .writer
            .tell()
            .await
            .map_err(|error| format!("get Lance writer position: {error}"))
    }) {
        Ok(position) => {
            unsafe {
                *out_position = position;
            }
            0
        }
        Err(error) => fail(error),
    }
}

/// # Safety
///
/// `writer` must be a live writer handle and `out_row_count` must be valid for
/// writes.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_writer_finish(
    writer: *mut PaimonLanceWriter,
    out_row_count: *mut u64,
) -> i32 {
    clear_last_error();
    if writer.is_null() || out_row_count.is_null() {
        return fail("Lance writer and out_row_count must be non-null");
    }
    let writer = unsafe { &mut *writer };
    if writer.finished {
        return fail("Lance writer is already finished");
    }
    match block_on(async {
        writer
            .writer
            .finish()
            .await
            .map_err(|error| format!("finish Lance writer: {error}"))
    }) {
        Ok(row_count) => {
            writer.finished = true;
            unsafe {
                *out_row_count = row_count;
            }
            0
        }
        Err(error) => fail(error),
    }
}

/// # Safety
///
/// `writer` must be null or a live handle returned by
/// `paimon_lance_writer_open`, and it must be freed at most once.
#[no_mangle]
pub unsafe extern "C" fn paimon_lance_writer_free(writer: *mut PaimonLanceWriter) {
    if !writer.is_null() {
        unsafe {
            let mut writer = Box::from_raw(writer);
            if !writer.finished {
                let _ = block_on(async {
                    writer.writer.abort().await;
                    Ok(())
                });
            }
        }
    }
}
