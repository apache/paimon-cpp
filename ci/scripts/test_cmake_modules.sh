#!/usr/bin/env bash
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
#
# Runs the CMake module tests under cmake_modules/tests. These tests need no
# compiler or configured build tree, so they stay separate from ctest.

set -euo pipefail

source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

status=0
for test_script in "${source_dir}"/cmake_modules/tests/*_test.cmake; do
    echo "=== ${test_script##*/} ==="
    if ! cmake -P "${test_script}"; then
        status=1
    fi
done

for generator in "Unix Makefiles" "Ninja"; do
    if [[ "${generator}" == "Ninja" ]] && ! command -v ninja >/dev/null; then
        echo "Skipping Ninja patch lifecycle tests: ninja is not installed"
        continue
    fi
    if ! python3 "${source_dir}/cmake_modules/tests/thirdparty_patch_test.py" --generator "${generator}"; then
        status=1
    fi
done

exit "${status}"
