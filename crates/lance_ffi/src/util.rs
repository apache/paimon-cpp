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

use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::ops::Range;

pub(crate) fn required_string(value: *const c_char, name: &str) -> Result<String, String> {
    if value.is_null() {
        return Err(format!("{name} is nullptr"));
    }
    let value = unsafe { CStr::from_ptr(value) };
    value
        .to_str()
        .map(str::to_owned)
        .map_err(|error| format!("{name} is not valid UTF-8: {error}"))
}

pub(crate) fn parse_options(
    keys: *const *const c_char,
    values: *const *const c_char,
    count: usize,
) -> Result<HashMap<String, String>, String> {
    if count == 0 {
        return Ok(HashMap::new());
    }
    if keys.is_null() || values.is_null() {
        return Err("storage option keys and values must be non-null".to_string());
    }
    let keys = unsafe { std::slice::from_raw_parts(keys, count) };
    let values = unsafe { std::slice::from_raw_parts(values, count) };
    let mut options = HashMap::with_capacity(count);
    for index in 0..count {
        options.insert(
            required_string(keys[index], "storage option key")?,
            required_string(values[index], "storage option value")?,
        );
    }
    Ok(options)
}

pub(crate) fn parse_projection(
    names: *const *const c_char,
    count: usize,
) -> Result<Vec<String>, String> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if names.is_null() {
        return Err("projection names must be non-null".to_string());
    }
    let names = unsafe { std::slice::from_raw_parts(names, count) };
    names
        .iter()
        .map(|name| required_string(*name, "projection name"))
        .collect()
}

pub(crate) fn parse_ranges(
    starts: *const u64,
    ends: *const u64,
    count: usize,
) -> Result<Vec<Range<u64>>, String> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if starts.is_null() || ends.is_null() {
        return Err("selection range starts and ends must be non-null".to_string());
    }
    let starts = unsafe { std::slice::from_raw_parts(starts, count) };
    let ends = unsafe { std::slice::from_raw_parts(ends, count) };
    let mut ranges = Vec::with_capacity(count);
    for (start, end) in starts.iter().zip(ends.iter()) {
        if start >= end {
            return Err(format!("invalid selection range [{start}, {end})"));
        }
        ranges.push(*start..*end);
    }
    Ok(ranges)
}
