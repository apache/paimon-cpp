#!/bin/bash

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

set -e

cd "$(dirname "$0")/../"
repo_root=$PWD
mkdir -p build && cd build
cmake ../ \
    -DCMAKE_BUILD_TYPE=Debug \
    -DPAIMON_BUILD_TESTS=ON \
    -DPAIMON_USE_ASAN=ON \
    -DPAIMON_GENERATE_COVERAGE=ON \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DPAIMON_ENABLE_AVRO=ON \
    -DPAIMON_ENABLE_ORC=ON \
    -DPAIMON_ENABLE_MOSAIC=ON \
    -DPAIMON_ENABLE_LANCE=ON \
    -DPAIMON_ENABLE_VORTEX=ON \
    -DPAIMON_ENABLE_JINDO=ON \
    -DPAIMON_ENABLE_OSS=ON \
    -DPAIMON_ENABLE_S3=ON \
    -DPAIMON_ENABLE_LUMINA=ON \
    -DPAIMON_ENABLE_LUCENE=ON \
    -DPAIMON_ENABLE_FULL_TEXT=ON \
    -DPAIMON_ENABLE_REST=ON
make -j
make test
lcov --capture --directory src/paimon --directory test --output-file coverage.info
# Keep only project implementation code in the report: drop system headers,
# third-party dependencies from the build tree and third_party/, and test
# sources (*_test.cpp, test helpers, mocks).
# Anchor exclusions to repository paths so checkout ancestors cannot match.
# PWD is the build directory; mocks outside testing/ are listed explicitly.
# Exclusion patterns can overlap or be absent, so unused patterns are nonfatal.
lcov --remove coverage.info \
    '/usr/*' \
    "$PWD/*" \
    "$repo_root/third_party/*" \
    "$repo_root/*_test.cpp" \
    "$repo_root/test/*" \
    "$repo_root/src/paimon/testing/*" \
    "$repo_root/src/paimon/rest/mock_rest_server.*" \
    --ignore-errors unused \
    --output-file coverage_filtered.info
mv coverage_filtered.info coverage.info
genhtml coverage.info --output-directory coverage

ip=$(hostname -I | awk '{print $1}')
echo
echo "See coverage files at: $PWD/coverage.info"
echo "See coverage html files at: $PWD/coverage/"
echo "View code coverage at: http://$ip:8000/coverage/index.html"
python3 -m http.server
