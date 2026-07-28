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

set -eux

source_dir=${1}
enable_sanitizer=${2:-false}
check_clang_tidy=${3:-false}
build_type=${4:-Debug}
install_smoke=${5:-false}
build_dir="${source_dir}/build"

if [[ -n "${PAIMON_BUILD_JOBS:-}" ]]; then
    build_jobs="${PAIMON_BUILD_JOBS}"
elif command -v nproc >/dev/null 2>&1; then
    build_jobs=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
    build_jobs=$(sysctl -n hw.ncpu)
else
    build_jobs=4
fi

# Display ccache status if available
if command -v ccache &> /dev/null; then
    echo "=== ccache found: $(ccache --version | head -1) ==="
    ccache -p | grep -E "cache_dir|max_size|compression" || true
    ccache -z  # Reset statistics for this build
else
    echo "=== ccache not found, compiling without cache acceleration ==="
fi

mkdir -p "${build_dir}"
pushd "${build_dir}"

ENABLE_LUMINA="ON"
if [[ "${CC:-}" == *"gcc-8"* ]] || [[ "${CXX:-}" == *"g++-8"* ]]; then
    ENABLE_LUMINA="OFF"
fi

CMAKE_ARGS=(
    "-G Ninja"
    "-DCMAKE_BUILD_TYPE=${build_type}"
    "-DPAIMON_BUILD_TESTS=ON"
    "-DPAIMON_ENABLE_JINDO=ON"
    "-DPAIMON_ENABLE_LUMINA=${ENABLE_LUMINA}"
    "-DPAIMON_ENABLE_LUCENE=ON"
)

if [[ "${enable_sanitizer}" == "true" ]]; then
    CMAKE_ARGS+=(
        "-DPAIMON_USE_ASAN=ON"
        "-DPAIMON_USE_UBSAN=ON"
    )
fi

cmake "${CMAKE_ARGS[@]}" "${source_dir}"
cmake --build . -- -j "${build_jobs}"
ctest --output-on-failure -j "${build_jobs}"

if [[ "${check_clang_tidy}" == "true" ]]; then
    cmake --build . --target check-clang-tidy
fi

if [[ "${install_smoke}" == "true" ]]; then
    install_dir="${source_dir}/install-test"
    smoke_build_dir="${source_dir}/build-install-smoke"
    rm -rf "${install_dir}" "${smoke_build_dir}"

    cmake --install . --prefix "${install_dir}"
    cmake -G Ninja \
        -S "${source_dir}/scripts/releasing/install_smoke" \
        -B "${smoke_build_dir}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_PREFIX_PATH="${install_dir};${build_dir}/arrow_ep-install"
    cmake --build "${smoke_build_dir}" -- -j "${build_jobs}"

    runtime_library_path="${install_dir}/lib:${install_dir}/lib64"
    runtime_library_path+=":${build_dir}/arrow_ep-install/lib"
    runtime_library_path+=":${build_dir}/arrow_ep-install/lib64"
    if [[ "$(uname -s)" == "Darwin" ]]; then
        cmake -E env \
            "DYLD_LIBRARY_PATH=${runtime_library_path}" \
            "${smoke_build_dir}/paimon_install_smoke"
    else
        cmake -E env \
            "LD_LIBRARY_PATH=${runtime_library_path}" \
            "${smoke_build_dir}/paimon_install_smoke"
    fi
fi

# Print ccache statistics after build
if command -v ccache &> /dev/null; then
    echo "=== ccache statistics after build ==="
    ccache -s
fi

popd

rm -rf "${build_dir}"
if [[ "${install_smoke}" == "true" ]]; then
    rm -rf "${install_dir}" "${smoke_build_dir}"
fi
