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

# Argument validation tests for build_paimon.sh. A stub cmake keeps malformed
# input from starting a configure or build if validation regresses.

set -uo pipefail

source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
script="${source_dir}/ci/scripts/build_paimon.sh"

work_dir=$(mktemp -d)
trap 'rm -rf "${work_dir}"' EXIT
mkdir -p "${work_dir}/source" "${work_dir}/bin"

stub_cmake="${work_dir}/bin/cmake"
printf '%s\n' '#!/usr/bin/env bash' 'echo "cmake stub called: $*" >&2' 'exit 99' \
    > "${stub_cmake}"
chmod +x "${stub_cmake}"

stub_ccache="${work_dir}/bin/ccache"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "${stub_ccache}"
chmod +x "${stub_ccache}"

checks=0
failures=0

expect_rejected() {
    local description=$1 expected=$2
    shift 2
    local output status
    checks=$((checks + 1))
    output=$(PATH="${work_dir}/bin:${PATH}" "${script}" "$@" 2>&1)
    status=$?
    if [[ "${status}" -eq 0 ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: expected a non-zero exit"
    elif [[ "${output}" != *"${expected}"* ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: expected '${expected}', got: ${output}"
    elif [[ "${output}" == *"cmake stub called:"* ]]; then
        failures=$((failures + 1))
        echo "FAIL ${description}: validation happened after invoking cmake"
    fi
}

expect_build_type_accepted() {
    local build_type=$1 output status
    checks=$((checks + 1))
    output=$(PATH="${work_dir}/bin:${PATH}" "${script}" --source_dir "${work_dir}/source" \
        --build_type "${build_type}" 2>&1)
    status=$?
    if [[ "${status}" -ne 99 ]]; then
        failures=$((failures + 1))
        echo "FAIL build type ${build_type}: expected cmake stub exit 99, got ${status}: ${output}"
    elif [[ "${output}" != *"-DCMAKE_BUILD_TYPE=${build_type}"* ]]; then
        failures=$((failures + 1))
        echo "FAIL build type ${build_type}: cmake did not receive the expected value: ${output}"
    fi
}

expect_rejected "source directory cannot consume another option" \
    "Missing value for --source_dir" --source_dir --enable_asan
expect_rejected "source directory requires a value" \
    "Missing value for --source_dir" --source_dir
expect_rejected "build type cannot consume another option" \
    "Missing value for --build_type" --source_dir "${work_dir}/source" --build_type --enable_asan
expect_rejected "build type requires a value" \
    "Missing value for --build_type" --source_dir "${work_dir}/source" --build_type
expect_rejected "lint target cannot consume another option" \
    "Missing value for --lint_git_target_commit" --source_dir "${work_dir}/source" \
    --lint_git_target_commit --enable_asan
expect_rejected "lint target requires a value" \
    "Missing value for --lint_git_target_commit" --source_dir "${work_dir}/source" \
    --lint_git_target_commit
expect_rejected "unknown build type is rejected" \
    "Invalid value for --build_type: Profile" --source_dir "${work_dir}/source" \
    --build_type Profile
expect_rejected "source directory is required" \
    "--source_dir is required"
expect_rejected "unknown arguments are rejected" \
    "Unknown argument: --unknown" --unknown
expect_rejected "ASAN and TSAN are mutually exclusive" \
    "ASAN and TSAN cannot be enabled together" --source_dir "${work_dir}/source" \
    --enable_asan --enable_tsan

for build_type in Debug Release RelWithDebInfo MinSizeRel; do
    expect_build_type_accepted "${build_type}"
done

if [[ "${failures}" -ne 0 ]]; then
    echo "${failures}/${checks} checks failed"
    exit 1
fi

echo "All ${checks} build_paimon argument checks passed"
