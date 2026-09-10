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
# Install the Rust toolchain used by the Lance, Mosaic, and tantivy-fts FFI builds, plus
# cbindgen required by Lance and tantivy-fts.
#
# The dev container (see .devcontainer/) already has these preinstalled;
# this script is for the GitHub Actions runners and is called before ci/scripts/build_paimon.sh.
#
# Idempotent: a second invocation is a no-op when the tools already exist.

set -eux

RUSTUP_VERSION=${RUSTUP_VERSION:-1.29.0}
# 1.88.0 is the minimum required by transitive crates (e.g. time 0.3.47).
RUST_VERSION=${RUST_VERSION:-1.88.0}
CBINDGEN_VERSION=${CBINDGEN_VERSION:-0.29.2}
PROTOC_VERSION=27.4

# Install rustup + default toolchain if cargo isn't on PATH yet.
if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain "${RUST_VERSION}" --profile minimal --no-modify-path
fi

# Export for the remainder of the CI job.
export PATH="${HOME}/.cargo/bin:${PATH}"
echo "${HOME}/.cargo/bin" >> "${GITHUB_PATH:-/dev/null}" || true

rustup toolchain install "${RUST_VERSION}" --profile minimal
rustup default "${RUST_VERSION}"
rustup component add rustfmt clippy

# Lance's protobuf definitions use proto3 optional fields, which require protoc >= 3.12.
if ! command -v protoc >/dev/null 2>&1 || \
   ! protoc --experimental_allow_proto3_optional --version >/dev/null 2>&1; then
    case "$(uname -m)" in
        x86_64)
            protoc_arch="x86_64"
            protoc_sha256="20a977d023a47a7f27557aa144eb2c06baa6f623784e34d8a13d9abb6f6bc6c0"
            ;;
        aarch64|arm64)
            protoc_arch="aarch_64"
            protoc_sha256="2399fa9e634880e70a8aa760b1b164ea0a8e8acac3bf541e732f9b3ade312cc9"
            ;;
        *)
            echo "Unsupported architecture for protoc: $(uname -m)" >&2
            exit 1
            ;;
    esac
    protoc_home="${HOME}/.local/protoc-${PROTOC_VERSION}"
    protoc_archive="/tmp/protoc-${PROTOC_VERSION}-${protoc_arch}.zip"
    if [[ ! -x "${protoc_home}/bin/protoc" ]]; then
        curl --proto '=https' --tlsv1.2 -fL \
            "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip" \
            -o "${protoc_archive}"
        echo "${protoc_sha256}  ${protoc_archive}" | sha256sum --check
        mkdir -p "${protoc_home}"
        unzip -oq "${protoc_archive}" -d "${protoc_home}"
    fi
    export PATH="${protoc_home}/bin:${PATH}"
    echo "${protoc_home}/bin" >> "${GITHUB_PATH:-/dev/null}" || true
fi

# cbindgen is used by the crate's build.rs to emit the C header that the
# C++ side includes. Corrosion will also run cbindgen at CMake configure
# time; both paths need it available.
if ! command -v cbindgen >/dev/null 2>&1; then
    cargo install cbindgen --version "${CBINDGEN_VERSION}" --locked
fi

rustc --version
cargo --version
cbindgen --version
protoc --version
