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
set(_base_files
    vortex-ffi/src/lib.rs
    vortex-ffi/src/scan.rs
    vortex-ffi/src/dtype.rs
    vortex-ffi/cinclude/vortex.h
    vortex-ffi/Cargo.toml
    Cargo.lock
    vortex-array/src/executor.rs
    vortex-buffer/src/trusted_len.rs
    vortex-array/src/arrow/executor/list.rs)
set(_base_after
    3fdbba15c951286ea0e565e3e21d75de1d7d4065a358f16aba2d47750e7ace76
    00aba4a0d7339fe8d961fb302572deae58adb9f06eb9b29313aa9503e8352d49
    c6e76821d117533e9b2448bef3842ec566def1384911abae1559410619c71e21
    1ad352c8476bfa41972dd597261fb2753b6903f4e90e9d606b85f9b04ed4c895
    b24d3f3a83933fe4c061b1d8fd8e2ff963f2cf468eecd5e8ef045a7c039fa328
    84d1415d7fa71c080cbd24c198712cf227429cf9fb508698e6c2e58674401410
    2a2a7ff5bd7941899fcb1d9ad4262263bc183920041b56bc9bd328a43d76c9e0
    9779c7a90f839669dd204cd1c3e3fbcace9fd6ca19583ec0deb51d52b51783e7
    56de0a4a7e7dca7e3619790a7058f9109d2b56a77eaa18ea1ec2987cc17d8dfe)
list(LENGTH _base_files _base_count)
math(EXPR _base_last "${_base_count} - 1")
foreach(_index RANGE 0 ${_base_last})
    list(GET _base_files ${_index} _file)
    list(GET _base_after ${_index} _expected)
    file(SHA256 "${VORTEX_SOURCE_DIR}/${_file}" _actual)
    if(NOT _actual STREQUAL _expected)
        message(FATAL_ERROR "Vortex base patch is missing, outdated or modified: ${_file}; use a fresh pinned source checkout"
        )
    endif()
endforeach()

# Accept clean reader sources or the reader patch alone. Also check the cache sources so a
# reused checkout cannot silently retain an obsolete shard override.
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
set(_after
    4bfb5f4bf8f707027702e9157d2d40d1b64445692d766a3a2d9f685998af3806
    093cf553ed52c01a31ff131b96f226157f37ffe67593136d3bd67832f85e362e
    d77641a60e1d4daf84f58f30d5cc0f5f64bc09f30ff1d711862b66606ec9a772
    e75aefbaa363748ab9e75cc21b1ec6d74701e2c973d1eb204e657c4e8076e7f6
    2da8289cbd44c64bcbdf102cdc2be291448fc3aa77a0ca34d2f7efb7dfde2419)
set(_all_clean TRUE)
set(_all_after TRUE)
foreach(_index RANGE 0 4)
    list(GET _files ${_index} _file)
    list(GET _before ${_index} _old)
    list(GET _after ${_index} _new)
    file(SHA256 "${VORTEX_SOURCE_DIR}/${_file}" _actual)
    if(NOT _actual STREQUAL _old)
        set(_all_clean FALSE)
    endif()
    if(NOT _actual STREQUAL _new)
        set(_all_after FALSE)
    endif()
endforeach()
if(NOT _all_clean AND NOT _all_after)
    message(FATAL_ERROR "Vortex read patch: unknown, obsolete or partly updated source; preserve local changes and use a fresh pinned source checkout"
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

# Complete the dry-run before changing any reader source.
if(_all_clean)
    _run_vortex_read_patch("${VORTEX_READ_PATCH}" --forward TRUE)
else()
    _run_vortex_read_patch("${VORTEX_READ_PATCH}" --reverse TRUE)
endif()
if(_all_clean)
    _run_vortex_read_patch("${VORTEX_READ_PATCH}" --forward FALSE)
endif()
foreach(_index RANGE 0 4)
    list(GET _files ${_index} _file)
    list(GET _after ${_index} _expected)
    file(SHA256 "${VORTEX_SOURCE_DIR}/${_file}" _actual)
    if(NOT _actual STREQUAL _expected)
        message(FATAL_ERROR "Vortex read patch output hash differs: ${_file}")
    endif()
endforeach()
