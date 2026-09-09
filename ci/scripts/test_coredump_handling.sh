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
mkdir -p "${binary_dir}" "${work_dir}/test/test_data" "${work_dir}/bin"

cat > "${work_dir}/sample.c" <<'EOF'
int sample_value(void) {
    return 42;
}
EOF

cat > "${work_dir}/absolute.c" <<'EOF'
int absolute_value(void) {
    return 24;
}
EOF

cat > "${work_dir}/core_test.c" <<'EOF'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int sample_value(void);
int absolute_value(void);

int main(int argc, char** argv) {
    const char* executable = strrchr(argv[0], '/');
    executable = executable == NULL ? argv[0] : executable + 1;

    char core_file[256];
    snprintf(core_file, sizeof(core_file), "core.%s.123", executable);
    FILE* file = fopen(core_file, "w");
    if (file == NULL) {
        return 2;
    }
    fputs("fake core", file);
    fclose(file);

    const char* asan_options = getenv("ASAN_OPTIONS");
    const char* tsan_options = getenv("TSAN_OPTIONS");
    if (sample_value() != 42 || absolute_value() != 24 || asan_options == NULL ||
        tsan_options == NULL || strstr(asan_options, "disable_coredump=0") == NULL ||
        strstr(asan_options, "abort_on_error=1") == NULL ||
        strstr(tsan_options, "disable_coredump=0") == NULL ||
        strstr(tsan_options, "abort_on_error=1") == NULL) {
        return 3;
    }

    return argc > 1 && strcmp(argv[1], "success") == 0 ? 0 : 1;
}
EOF

"${cc}" -fPIC -shared "${work_dir}/sample.c" -o "${binary_dir}/libsample.so"
"${cc}" -fPIC -shared "${work_dir}/absolute.c" -o "${binary_dir}/libabsolute.so"
"${cc}" "${work_dir}/core_test.c" -L"${binary_dir}" -lsample \
    "${binary_dir}/libabsolute.so" -Wl,-rpath,'$ORIGIN' -o "${binary_dir}/failing-test"
cp "${binary_dir}/failing-test" "${binary_dir}/success-test"

# The fake core is sufficient to exercise collection. Avoid depending on GDB
# being installed or asking it to parse deliberately invalid input.
cat > "${work_dir}/bin/gdb" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "${work_dir}/bin/gdb"

if PATH="${work_dir}/bin:${PATH}" \
    "${run_test}" "${output_root}" test "${binary_dir}/failing-test"; then
    echo "run-test.sh unexpectedly reported that the failing test passed"
    exit 1
fi

artifact_dir="${output_root}/build/test-debug/failing-test"
expected_files=(
    "${artifact_dir}/failing-test"
    "${artifact_dir}/failing-test.core.failing-test.123"
    "${artifact_dir}/lib/libabsolute.so"
    "${artifact_dir}/lib/libsample.so"
)
for expected_file in "${expected_files[@]}"; do
    if [[ ! -f "${expected_file}" ]]; then
        echo "Missing core dump artifact: ${expected_file}"
        exit 1
    fi
done

PATH="${work_dir}/bin:${PATH}" \
    "${run_test}" "${output_root}" test "${binary_dir}/success-test" success

if [[ -e "${output_root}/build/test-debug/success-test" ]]; then
    echo "A successful test preserved an expected core dump"
    exit 1
fi
if [[ -e "${output_root}/build/test-work/success-test" ]]; then
    echo "The successful test work directory was not cleaned"
    exit 1
fi

echo "Core dump handling checks passed"
