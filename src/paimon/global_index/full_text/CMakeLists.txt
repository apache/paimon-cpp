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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(PAIMON_ENABLE_FULL_TEXT)
    set(PAIMON_FULL_TEXT_INDEX
        full_text_ffi_utils.cpp
        full_text_global_index.cpp
        full_text_global_index_factory.cpp
        full_text_global_index_reader.cpp
        full_text_global_index_writer.cpp)

    add_paimon_lib(paimon_full_text_index
                   SOURCES
                   ${PAIMON_FULL_TEXT_INDEX}
                   DEPENDENCIES
                   paimon_shared
                   paimon_ftindex_ffi
                   STATIC_LINK_LIBS
                   arrow
                   fmt
                   paimon_ftindex_ffi
                   Threads::Threads
                   SHARED_LINK_LIBS
                   paimon_shared
                   paimon_ftindex_ffi
                   SHARED_LINK_FLAGS
                   ${PAIMON_VERSION_SCRIPT_FLAGS})

    target_link_libraries(paimon_full_text_index_objlib PUBLIC paimon_ftindex_ffi)

    if(PAIMON_BUILD_TESTS)
        add_paimon_test(full_text_index_test
                        SOURCES
                        full_text_global_index_test.cpp
                        STATIC_LINK_LIBS
                        paimon_shared
                        test_utils_static
                        ${PAIMON_FULL_TEXT_INDEX_STATIC_LINK_LIBS}
                        ${PAIMON_LOCAL_FILE_SYSTEM_STATIC_LINK_LIBS}
                        paimon_ftindex_ffi
                        ${GTEST_LINK_TOOLCHAIN})
    endif()
endif()
