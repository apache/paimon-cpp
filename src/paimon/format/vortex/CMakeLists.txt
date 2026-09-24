# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Vortex file format (Rust FFI, statically linked via paimon_vortex_ffi).

if(PAIMON_ENABLE_VORTEX)
    set(PAIMON_VORTEX_FILE_FORMAT
        vortex_file_batch_reader.cpp
        vortex_file_format.cpp
        vortex_file_format_factory.cpp
        vortex_format_writer.cpp
        vortex_ffi_util.cpp
        vortex_io_callbacks.cpp
        vortex_stats_extractor.cpp
        vortex_writer_builder.cpp)

    add_paimon_lib(paimon_vortex_file_format
                   SOURCES
                   ${PAIMON_VORTEX_FILE_FORMAT}
                   DEPENDENCIES
                   paimon_shared
                   paimon_vortex_ffi
                   STATIC_LINK_LIBS
                   arrow
                   fmt
                   paimon_vortex_ffi
                   Threads::Threads
                   SHARED_LINK_LIBS
                   paimon_shared
                   paimon_vortex_ffi
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    target_link_libraries(paimon_vortex_file_format_objlib PUBLIC paimon_vortex_ffi)

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(vortex_format_test
                        SOURCES
                        vortex_file_format_test.cpp
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        ${PAIMON_VORTEX_FILE_FORMAT_STATIC_LINK_LIBS}
                        ${PAIMON_LOCAL_FILE_SYSTEM_STATIC_LINK_LIBS}
                        paimon_vortex_ffi
                        ${GTEST_LINK_TOOLCHAIN})
    endif()
endif()
