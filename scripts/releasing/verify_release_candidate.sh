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

ARTIFACT=""
RAT_JAR=${RAT_JAR:-}
ALLOW_UNSIGNED=false
SKIP_RAT=false
SKIP_BUILD=false

usage() {
    cat <<'EOF'
Verify an Apache Paimon C++ source release candidate.

Usage:
  verify_release_candidate.sh [options] ARTIFACT

Options:
  --rat-jar FILE       Apache RAT executable jar (or set RAT_JAR)
  --allow-unsigned     Allow a missing .asc file for local preparation only
  --skip-rat           Skip Apache RAT for local preparation only
  --skip-build         Skip the release build and test suite
  -h, --help           Show this help

By default the script requires:
  ARTIFACT.sha512
  ARTIFACT.asc
  an Apache RAT jar

It verifies the checksum and signature, inspects and extracts the archive,
checks release metadata, runs Apache RAT, then builds and tests from the
extracted source distribution.
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
        --rat-jar)
            [[ $# -ge 2 ]] || fail "--rat-jar requires a value"
            RAT_JAR=$2
            shift 2
            ;;
        --allow-unsigned)
            ALLOW_UNSIGNED=true
            shift
            ;;
        --skip-rat)
            SKIP_RAT=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            fail "unknown argument: $1"
            ;;
        *)
            [[ -z "${ARTIFACT}" ]] || fail "only one artifact may be specified"
            ARTIFACT=$1
            shift
            ;;
    esac
done

[[ -n "${ARTIFACT}" ]] || fail "an artifact is required"
[[ -f "${ARTIFACT}" ]] || fail "artifact does not exist: ${ARTIFACT}"

ARTIFACT_DIR=$(cd "$(dirname "${ARTIFACT}")" && pwd)
ARTIFACT_NAME=$(basename "${ARTIFACT}")
ARTIFACT="${ARTIFACT_DIR}/${ARTIFACT_NAME}"

if [[ "${ARTIFACT_NAME}" =~ ^apache-paimon-cpp-([0-9]+\.[0-9]+\.[0-9]+)-src\.tgz$ ]]; then
    RELEASE_VERSION=${BASH_REMATCH[1]}
else
    fail "unexpected artifact name: ${ARTIFACT_NAME}"
fi

CHECKSUM_FILE="${ARTIFACT}.sha512"
SIGNATURE_FILE="${ARTIFACT}.asc"
ARCHIVE_ROOT="paimon-cpp-${RELEASE_VERSION}"

[[ -f "${CHECKSUM_FILE}" ]] || fail "missing checksum: ${CHECKSUM_FILE}"
EXPECTED_SHA512=$(awk 'NR == 1 {print $1}' "${CHECKSUM_FILE}")
CHECKSUM_ARTIFACT=$(awk 'NR == 1 {print $2}' "${CHECKSUM_FILE}")
[[ "${EXPECTED_SHA512}" =~ ^[0-9a-fA-F]{128}$ ]] ||
    fail "invalid SHA-512 file: ${CHECKSUM_FILE}"
[[ "${CHECKSUM_ARTIFACT}" == "${ARTIFACT_NAME}" ]] ||
    fail "checksum file names ${CHECKSUM_ARTIFACT:-<missing>}, expected ${ARTIFACT_NAME}"
ACTUAL_SHA512=$(calculate_sha512 "${ARTIFACT}")
EXPECTED_SHA512=$(printf '%s' "${EXPECTED_SHA512}" | tr '[:upper:]' '[:lower:]')
ACTUAL_SHA512=$(printf '%s' "${ACTUAL_SHA512}" | tr '[:upper:]' '[:lower:]')
[[ "${ACTUAL_SHA512}" == "${EXPECTED_SHA512}" ]] ||
    fail "SHA-512 checksum does not match"
echo "SHA-512 checksum: valid"

if [[ -f "${SIGNATURE_FILE}" ]]; then
    command -v gpg >/dev/null 2>&1 || fail "gpg is required to verify the signature"
    gpg --verify "${SIGNATURE_FILE}" "${ARTIFACT}"
    echo "OpenPGP signature: valid"
elif [[ "${ALLOW_UNSIGNED}" == true ]]; then
    echo "OpenPGP signature: skipped for local preparation"
else
    fail "missing signature: ${SIGNATURE_FILE}"
fi

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT
CONTENTS_FILE="${TEMP_DIR}/archive-contents.txt"

tar -tzf "${ARTIFACT}" >"${CONTENTS_FILE}"
[[ -s "${CONTENTS_FILE}" ]] || fail "source archive is empty"

while IFS= read -r entry; do
    [[ "${entry}" != /* ]] || fail "archive contains an absolute path: ${entry}"
    [[ "${entry}" != ".." && "${entry}" != ../* && "${entry}" != */.. &&
        "${entry}" != *"/../"* ]] ||
        fail "archive contains path traversal: ${entry}"
    [[ "${entry}" == "${ARCHIVE_ROOT}" || "${entry}" == "${ARCHIVE_ROOT}/"* ]] ||
        fail "archive entry is outside ${ARCHIVE_ROOT}: ${entry}"
done <"${CONTENTS_FILE}"

tar -xzf "${ARTIFACT}" -C "${TEMP_DIR}"
SOURCE_DIR="${TEMP_DIR}/${ARCHIVE_ROOT}"
[[ -d "${SOURCE_DIR}" ]] || fail "archive root is missing: ${ARCHIVE_ROOT}"

for required_file in LICENSE NOTICE CMakeLists.txt docs/source/conf.py; do
    [[ -f "${SOURCE_DIR}/${required_file}" ]] ||
        fail "required release file is missing: ${required_file}"
done

CMAKE_VERSION=$(
    awk '$1 == "VERSION" && $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+$/ { print $2; exit }' \
        "${SOURCE_DIR}/CMakeLists.txt"
)
[[ "${CMAKE_VERSION}" == "${RELEASE_VERSION}" ]] ||
    fail "CMake version ${CMAKE_VERSION:-<missing>} does not match ${RELEASE_VERSION}"

DOCS_VERSION=$(
    sed -n 's/^version = "\([^"]*\)"$/\1/p' "${SOURCE_DIR}/docs/source/conf.py" |
        head -n 1
)
[[ "${DOCS_VERSION}" == "${RELEASE_VERSION}" ]] ||
    fail "documentation version ${DOCS_VERSION:-<missing>} does not match ${RELEASE_VERSION}"

UNEXPECTED_BINARIES=$(
    find "${SOURCE_DIR}" -type f \
        \( -name '*.a' -o -name '*.class' -o -name '*.dll' -o -name '*.dylib' \
        -o -name '*.exe' -o -name '*.jar' -o -name '*.lib' -o -name '*.o' \
        -o -name '*.pdb' -o -name '*.pyc' -o -name '*.so' \) \
        -print
)
[[ -z "${UNEXPECTED_BINARIES}" ]] ||
    fail "source archive contains unexpected compiled files:${UNEXPECTED_BINARIES}"

UNSAFE_PERMISSIONS=$(
    find "${SOURCE_DIR}" -type f \( -perm -0020 -o -perm -0002 \) -print
)
[[ -z "${UNSAFE_PERMISSIONS}" ]] ||
    fail "source archive contains group- or world-writable files:${UNSAFE_PERMISSIONS}"

echo "Archive layout and release metadata: valid"

if [[ "${SKIP_RAT}" == false ]]; then
    [[ -n "${RAT_JAR}" ]] || fail "--rat-jar or RAT_JAR is required"
    [[ -f "${RAT_JAR}" ]] || fail "Apache RAT jar does not exist: ${RAT_JAR}"

    RAT_REPORT="${TEMP_DIR}/rat-report.txt"
    java -jar "${RAT_JAR}" \
        -E "${SOURCE_DIR}/.github/.rat-excludes" \
        -d "${SOURCE_DIR}" >"${RAT_REPORT}"

    if grep -Eq 'Files with unapproved licenses:[[:space:]]*[1-9]' "${RAT_REPORT}"; then
        cat "${RAT_REPORT}"
        fail "Apache RAT found files with unapproved licenses"
    fi
    echo "Apache RAT: valid"
else
    echo "Apache RAT: skipped for local preparation"
fi

if [[ "${SKIP_BUILD}" == false ]]; then
    "${SOURCE_DIR}/ci/scripts/build_paimon.sh" "${SOURCE_DIR}" false false Release
    echo "Release build and tests: valid"
else
    echo "Release build and tests: skipped"
fi

echo "Release candidate verification completed successfully."
