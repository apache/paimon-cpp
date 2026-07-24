#!/usr/bin/env bash
#
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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)

RELEASE_VERSION=""
GIT_REF=""
OUTPUT_DIR="${SOURCE_ROOT}/release"
SIGNING_KEY=""

usage() {
    cat <<'EOF'
Create an Apache Paimon C++ source release from an immutable Git ref.

Usage:
  create_source_release.sh --version VERSION --git-ref REF [options]

Required:
  --version VERSION      Release version, for example 0.2.3
  --git-ref REF          Commit or signed RC tag to archive

Options:
  --output-dir DIR       Output directory (default: <repository>/release)
  --signing-key KEY_ID   Create an ASCII-armored detached OpenPGP signature
  -h, --help             Show this help

The script creates:
  apache-paimon-cpp-VERSION-src.tgz
  apache-paimon-cpp-VERSION-src.tgz.sha512
  apache-paimon-cpp-VERSION-src.tgz.asc  (when --signing-key is provided)

Existing artifacts are never overwritten.
EOF
}

fail() {
    echo "Error: $*" >&2
    exit 1
}

calculate_sha512() {
    local file=$1
    if command -v sha512sum >/dev/null 2>&1; then
        sha512sum "${file}" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 512 "${file}" | awk '{print $1}'
    else
        fail "sha512sum or shasum is required"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            [[ $# -ge 2 ]] || fail "--version requires a value"
            RELEASE_VERSION=$2
            shift 2
            ;;
        --git-ref)
            [[ $# -ge 2 ]] || fail "--git-ref requires a value"
            GIT_REF=$2
            shift 2
            ;;
        --output-dir)
            [[ $# -ge 2 ]] || fail "--output-dir requires a value"
            OUTPUT_DIR=$2
            shift 2
            ;;
        --signing-key)
            [[ $# -ge 2 ]] || fail "--signing-key requires a value"
            SIGNING_KEY=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument: $1"
            ;;
    esac
done

[[ -n "${RELEASE_VERSION}" ]] || fail "--version is required"
[[ -n "${GIT_REF}" ]] || fail "--git-ref is required"
[[ "${RELEASE_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
    fail "release version must use MAJOR.MINOR.PATCH format"

git -C "${SOURCE_ROOT}" rev-parse --verify "${GIT_REF}^{commit}" >/dev/null 2>&1 ||
    fail "Git ref does not resolve to a commit: ${GIT_REF}"

for required_file in CMakeLists.txt LICENSE NOTICE; do
    git -C "${SOURCE_ROOT}" cat-file -e "${GIT_REF}:${required_file}" 2>/dev/null ||
        fail "${required_file} is missing from ${GIT_REF}"
done

CMAKE_VERSION=$(
    git -C "${SOURCE_ROOT}" show "${GIT_REF}:CMakeLists.txt" |
        awk '$1 == "VERSION" && $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+$/ { print $2; exit }'
)
[[ "${CMAKE_VERSION}" == "${RELEASE_VERSION}" ]] ||
    fail "CMake version ${CMAKE_VERSION:-<missing>} does not match ${RELEASE_VERSION}"

DOCS_VERSION=$(
    git -C "${SOURCE_ROOT}" show "${GIT_REF}:docs/source/conf.py" |
        sed -n 's/^version = "\([^"]*\)"$/\1/p' |
        head -n 1
)
[[ "${DOCS_VERSION}" == "${RELEASE_VERSION}" ]] ||
    fail "documentation version ${DOCS_VERSION:-<missing>} does not match ${RELEASE_VERSION}"

ARTIFACT_NAME="apache-paimon-cpp-${RELEASE_VERSION}-src.tgz"
ARCHIVE_ROOT="paimon-cpp-${RELEASE_VERSION}"

mkdir -p "${OUTPUT_DIR}"
OUTPUT_DIR=$(cd "${OUTPUT_DIR}" && pwd)

for suffix in "" ".sha512" ".asc"; do
    [[ ! -e "${OUTPUT_DIR}/${ARTIFACT_NAME}${suffix}" ]] ||
        fail "refusing to overwrite ${OUTPUT_DIR}/${ARTIFACT_NAME}${suffix}"
done

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

echo "Creating ${ARTIFACT_NAME} from ${GIT_REF}..."
git -C "${SOURCE_ROOT}" -c tar.umask=0022 archive \
    --format=tar \
    --prefix="${ARCHIVE_ROOT}/" \
    "${GIT_REF}" |
    gzip -n >"${TEMP_DIR}/${ARTIFACT_NAME}"

SHA512=$(calculate_sha512 "${TEMP_DIR}/${ARTIFACT_NAME}")
printf '%s  %s\n' "${SHA512}" "${ARTIFACT_NAME}" \
    >"${TEMP_DIR}/${ARTIFACT_NAME}.sha512"

if [[ -n "${SIGNING_KEY}" ]]; then
    command -v gpg >/dev/null 2>&1 || fail "gpg is required to sign the artifact"
    echo "Signing ${ARTIFACT_NAME} with ${SIGNING_KEY}..."
    gpg --armor \
        --local-user "${SIGNING_KEY}" \
        --detach-sign \
        --output "${TEMP_DIR}/${ARTIFACT_NAME}.asc" \
        "${TEMP_DIR}/${ARTIFACT_NAME}"
fi

mv "${TEMP_DIR}/${ARTIFACT_NAME}" "${OUTPUT_DIR}/"
mv "${TEMP_DIR}/${ARTIFACT_NAME}.sha512" "${OUTPUT_DIR}/"
if [[ -n "${SIGNING_KEY}" ]]; then
    mv "${TEMP_DIR}/${ARTIFACT_NAME}.asc" "${OUTPUT_DIR}/"
fi

echo "Created release artifacts in ${OUTPUT_DIR}:"
echo "  ${ARTIFACT_NAME}"
echo "  ${ARTIFACT_NAME}.sha512"
if [[ -n "${SIGNING_KEY}" ]]; then
    echo "  ${ARTIFACT_NAME}.asc"
else
    echo "  Signature not created; pass --signing-key for an official release candidate."
fi
