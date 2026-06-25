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

set(PAIMON_LOCAL_FILE_SYSTEM local_file.cpp local_file_system.cpp
                             local_file_system_factory.cpp)

add_paimon_lib(paimon_local_file_system
               SOURCES
               ${PAIMON_LOCAL_FILE_SYSTEM}
               DEPENDENCIES
               paimon_shared
               STATIC_LINK_LIBS
               fmt
               SHARED_LINK_LIBS
               paimon_shared
               SHARED_LINK_FLAGS
               ${PAIMON_VERSION_SCRIPT_FLAGS})
