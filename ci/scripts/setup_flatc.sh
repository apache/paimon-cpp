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
# Build and install flatc (the FlatBuffers compiler) at the version Vortex requires, and make sure
# libclang is present.
#
# Vortex compiles its FlatBuffers schemas (.fbs) into Rust at build time, and
# cmake_modules/ThirdpartyToolchain.cmake requires a flatc whose version matches Vortex's
# flatbuffers crate (find_program(... flatc REQUIRED)). Distribution packages are too old, so the
# pinned version is built from source. Separately, Vortex's native dependencies run bindgen in
# their build scripts, which needs libclang at build time; some CI images do not ship it.
# flatc and libclang are build-time tools only; neither is needed at runtime.
#
# The dev container (see .devcontainer/) already has these preinstalled; this script is for the
# GitHub Actions runners and is called before ci/scripts/build_paimon.sh.
#
# Idempotent: a no-op when a matching flatc is already on PATH and libclang is already present.

set -eux

# Must match the flatbuffers crate version pinned by Vortex (see its Cargo.lock).
FLATBUFFERS_VERSION=${FLATBUFFERS_VERSION:-25.12.19}
FLATC_INSTALL_DIR=${FLATC_INSTALL_DIR:-"${HOME}/.local/bin"}

# Skip when a matching flatc is already available.
if command -v flatc >/dev/null 2>&1 &&
    [[ "$(flatc --version 2>/dev/null | awk '{print $NF}')" == "${FLATBUFFERS_VERSION}" ]]; then
    flatc --version
    exit 0
fi

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

git clone --depth 1 --branch "v${FLATBUFFERS_VERSION}" \
    https://github.com/google/flatbuffers.git "${workdir}/flatbuffers"
cmake -S "${workdir}/flatbuffers" -B "${workdir}/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DFLATBUFFERS_BUILD_TESTS=OFF
cmake --build "${workdir}/build" --target flatc -j "$(nproc)"

mkdir -p "${FLATC_INSTALL_DIR}"
install -m 0755 "${workdir}/build/flatc" "${FLATC_INSTALL_DIR}/flatc"

# Make flatc discoverable by later steps: on PATH for find_program, and via FLATC for build_vortex.
export PATH="${FLATC_INSTALL_DIR}:${PATH}"
echo "${FLATC_INSTALL_DIR}" >>"${GITHUB_PATH:-/dev/null}" || true
echo "FLATC=${FLATC_INSTALL_DIR}/flatc" >>"${GITHUB_ENV:-/dev/null}" || true

# Vortex's native dependencies invoke bindgen in their build scripts, which loads libclang at
# build time. Images that only carry GCC (e.g. the gcc8 test container) may lack it.
if ! ldconfig -p 2>/dev/null | grep -q 'libclang'; then
    if [[ "$(id -u)" -ne 0 ]] && command -v sudo >/dev/null 2>&1; then
        apt_prefix="sudo"
    else
        apt_prefix=""
    fi
    ${apt_prefix} apt-get update -y
    ${apt_prefix} apt-get install -y libclang-dev
fi

"${FLATC_INSTALL_DIR}/flatc" --version
