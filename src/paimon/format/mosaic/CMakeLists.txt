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

if(PAIMON_ENABLE_MOSAIC)
    set(PAIMON_MOSAIC_FILE_FORMAT
        mosaic_file_batch_reader.cpp
        mosaic_file_format.cpp
        mosaic_file_format_factory.cpp
        mosaic_format_writer.cpp
        mosaic_stats.cpp
        mosaic_stats_extractor.cpp
        mosaic_stream.cpp
        mosaic_writer_builder.cpp)

    add_paimon_lib(paimon_mosaic_file_format
                   SOURCES
                   ${PAIMON_MOSAIC_FILE_FORMAT}
                   DEPENDENCIES
                   paimon_shared
                   paimon_mosaic_ffi
                   STATIC_LINK_LIBS
                   arrow
                   fmt
                   paimon_mosaic_ffi
                   Threads::Threads
                   SHARED_LINK_LIBS
                   paimon_shared
                   paimon_mosaic_ffi
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    target_link_libraries(paimon_mosaic_file_format_objlib PUBLIC paimon_mosaic_ffi)

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(mosaic_format_test
                        SOURCES
                        mosaic_file_format_test.cpp
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        ${PAIMON_MOSAIC_FILE_FORMAT_STATIC_LINK_LIBS}
                        ${PAIMON_LOCAL_FILE_SYSTEM_STATIC_LINK_LIBS}
                        paimon_mosaic_ffi
                        ${GTEST_LINK_TOOLCHAIN})
    endif()
endif()
