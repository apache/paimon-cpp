# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# A reused .patched marker can predate the scan/Arrow ABI additions. Validate the base patch
# before changing any reader source; unknown or older sources need a fresh pinned checkout.
set(_base_files vortex-ffi/src/lib.rs vortex-ffi/src/scan.rs vortex-ffi/src/dtype.rs
                vortex-ffi/cinclude/vortex.h)
set(_base_after
    3fdbba15c951286ea0e565e3e21d75de1d7d4065a358f16aba2d47750e7ace76
    00aba4a0d7339fe8d961fb302572deae58adb9f06eb9b29313aa9503e8352d49
    af3c5d2652a37cd8c7e7d1e2c377a290c9a5e0dac158bd4ed8d0b6922552bec0
    1ad352c8476bfa41972dd597261fb2753b6903f4e90e9d606b85f9b04ed4c895)
foreach(_index RANGE 0 3)
    list(GET _base_files ${_index} _file)
    list(GET _base_after ${_index} _expected)
    file(SHA256 "${VORTEX_SOURCE_DIR}/${_file}" _actual)
    if(NOT _actual STREQUAL _expected)
        message(FATAL_ERROR "Vortex base patch is missing, outdated or modified: ${_file}; use a fresh pinned source checkout"
        )
    endif()
endforeach()

# Accept clean reader sources, the reader patch alone, or both reader patches.
set(_files
    vortex-layout/src/layouts/dict/reader.rs
    vortex-ffi/src/session.rs
    vortex-layout/src/layouts/struct_/reader.rs
    vortex-file/src/v2/file_stats_reader.rs
    vortex-layout/src/segments/shared.rs)
set(_before
    760c3de9f14a93b25322441ad835f9ea42286f2f01b0c72c95c03941684b5581
    0a5bf14492f8e03f6dfa265b30c355b8f6ae166558a1655038ee51ae6d50a6ca
    d77641a60e1d4daf84f58f30d5cc0f5f64bc09f30ff1d711862b66606ec9a772
    e75aefbaa363748ab9e75cc21b1ec6d74701e2c973d1eb204e657c4e8076e7f6
    2da8289cbd44c64bcbdf102cdc2be291448fc3aa77a0ca34d2f7efb7dfde2419)
set(_reader_patched
    4bfb5f4bf8f707027702e9157d2d40d1b64445692d766a3a2d9f685998af3806
    093cf553ed52c01a31ff131b96f226157f37ffe67593136d3bd67832f85e362e
    d77641a60e1d4daf84f58f30d5cc0f5f64bc09f30ff1d711862b66606ec9a772
    e75aefbaa363748ab9e75cc21b1ec6d74701e2c973d1eb204e657c4e8076e7f6
    2da8289cbd44c64bcbdf102cdc2be291448fc3aa77a0ca34d2f7efb7dfde2419)
set(_after
    4bfb5f4bf8f707027702e9157d2d40d1b64445692d766a3a2d9f685998af3806
    093cf553ed52c01a31ff131b96f226157f37ffe67593136d3bd67832f85e362e
    79be1013b9f04e683de40f8a9881e4d15e07945dd8d86b7760d3abf01d14ceb6
    36d599cbea7c0bd1855d560c2e171e9b61117f4e69cfe3bac4dcd0b126cb549f
    83edf1951eb564ab86e28a6e5f1c9fdb49788fc42ba43d7d26db9fd8f08cf40d)
set(_cache_patch "${CMAKE_CURRENT_LIST_DIR}/vortex-fmm-cache-shards.diff")
set(_all_clean TRUE)
set(_all_reader_patched TRUE)
set(_all_after TRUE)
foreach(_index RANGE 0 4)
    list(GET _files ${_index} _file)
    list(GET _before ${_index} _old)
    list(GET _reader_patched ${_index} _intermediate)
    list(GET _after ${_index} _new)
    file(SHA256 "${VORTEX_SOURCE_DIR}/${_file}" _actual)
    if(NOT _actual STREQUAL _old)
        set(_all_clean FALSE)
    endif()
    if(NOT _actual STREQUAL _intermediate)
        set(_all_reader_patched FALSE)
    endif()
    if(NOT _actual STREQUAL _new)
        set(_all_after FALSE)
    endif()
endforeach()
if(NOT _all_clean
   AND NOT _all_reader_patched
   AND NOT _all_after)
    message(FATAL_ERROR "Vortex read patch: unknown or partly updated source; preserve and inspect local changes"
    )
endif()

function(_run_vortex_read_patch _path _direction _dry_run)
    set(_arguments --batch ${_direction})
    if(_dry_run)
        list(APPEND _arguments --dry-run)
    endif()
    execute_process(COMMAND patch ${_arguments} -p1 -i "${_path}"
                    WORKING_DIRECTORY "${VORTEX_SOURCE_DIR}"
                    RESULT_VARIABLE _result
                    OUTPUT_VARIABLE _stdout
                    ERROR_VARIABLE _stderr)
    if(NOT _result EQUAL 0)
        message(FATAL_ERROR "Vortex read patch failed: ${_stdout}${_stderr}")
    endif()
endfunction()

# The patches affect disjoint files, so both dry-runs complete before any write.
if(_all_clean)
    _run_vortex_read_patch("${VORTEX_READ_PATCH}" --forward TRUE)
else()
    _run_vortex_read_patch("${VORTEX_READ_PATCH}" --reverse TRUE)
endif()
if(_all_after)
    _run_vortex_read_patch("${_cache_patch}" --reverse TRUE)
else()
    _run_vortex_read_patch("${_cache_patch}" --forward TRUE)
endif()

if(_all_clean)
    _run_vortex_read_patch("${VORTEX_READ_PATCH}" --forward FALSE)
endif()
if(NOT _all_after)
    _run_vortex_read_patch("${_cache_patch}" --forward FALSE)
endif()
foreach(_index RANGE 0 4)
    list(GET _files ${_index} _file)
    list(GET _after ${_index} _expected)
    file(SHA256 "${VORTEX_SOURCE_DIR}/${_file}" _actual)
    if(NOT _actual STREQUAL _expected)
        message(FATAL_ERROR "Vortex read patch output hash differs: ${_file}")
    endif()
endforeach()
