#!/usr/bin/env bash
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

set -euo pipefail

source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
run_test="${source_dir}/build_support/run-test.sh"
cc=${CC:-cc}

work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT

output_root="${work_dir}/output"
binary_dir="${output_root}/debug"
mkdir -p "${binary_dir}" "${work_dir}/test/test_data"

cat > "${work_dir}/core_test.c" <<'EOF'
#include <stdlib.h>
#include <string.h>

int main(void) {
    const char* asan_options = getenv("ASAN_OPTIONS");
    const char* tsan_options = getenv("TSAN_OPTIONS");
    if (asan_options == NULL || tsan_options == NULL ||
        strstr(asan_options, "disable_coredump=0") == NULL ||
        strstr(asan_options, "abort_on_error=1") == NULL ||
        strstr(tsan_options, "disable_coredump=0") == NULL ||
        strstr(tsan_options, "abort_on_error=1") == NULL) {
        return 3;
    }

    abort();
}
EOF

"${cc}" -g -O0 "${work_dir}/core_test.c" -o "${binary_dir}/failing-test"

run_output="${work_dir}/run-test.log"
if "${run_test}" "${output_root}" test "${binary_dir}/failing-test" \
    > "${run_output}" 2>&1; then
    echo "run-test.sh unexpectedly reported that the failing test passed"
    exit 1
fi
cat "${run_output}"

if ! grep -q "Found core dump, printing backtrace" "${run_output}"; then
    echo "run-test.sh did not report the core dump"
    exit 1
fi
if ! grep -q "Program terminated with signal SIGABRT" "${run_output}"; then
    echo "GDB did not report the abort signal"
    exit 1
fi
if ! grep -Eq '#[0-9]+[[:space:]]+.*main' "${run_output}"; then
    echo "GDB did not print the crashing main frame"
    exit 1
fi
if find "${output_root}/build" -name 'core.*' -print -quit | grep -q .; then
    echo "run-test.sh preserved a core dump"
    exit 1
fi
if find "${output_root}/build/test-debug" -type f -print -quit | grep -q .; then
    echo "run-test.sh preserved a binary artifact"
    exit 1
fi

echo "Core dump handling checks passed"
