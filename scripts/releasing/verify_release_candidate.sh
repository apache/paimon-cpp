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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)

ARTIFACT=""
REQUESTED_VERSION=""
RC=""
DIST_DEV_BASE_URL="https://dist.apache.org/repos/dist/dev/paimon"
KEYS_URL=""
KEYS_FILE=""
GIT_REF=""
RAT_JAR=${RAT_JAR:-}
RAT_VERSION="0.16.1"
ALLOW_UNSIGNED=false
SKIP_RAT=false
SKIP_BUILD=false
SKIP_INSTALL=false
JOBS=${PAIMON_BUILD_JOBS:-}

usage() {
    cat <<'EOF'
Verify an Apache Paimon C++ source release candidate.

Usage:
  verify_release_candidate.sh [options] ARTIFACT
  verify_release_candidate.sh --version VERSION --rc RC [options]

Download options:
  --version VERSION      Release version to download
  --rc RC                Release candidate number to download
  --dist-dev-base URL    ASF dist/dev project URL

Trust and reproducibility:
  --keys-url URL         Download KEYS and verify in an isolated GPG home
  --keys-file FILE       Import this KEYS file into an isolated GPG home
  --git-ref REF          Regenerate the archive from REF and compare bytes

Verification options:
  --rat-jar FILE         Apache RAT executable jar (or set RAT_JAR)
  --rat-version VERSION  RAT version to download (default: 0.16.1)
  --jobs JOBS            Parallel build and test jobs
  --allow-unsigned       Allow a missing .asc file for local/CI preparation
  --skip-rat             Skip Apache RAT for local preparation only
  --skip-build           Skip the release build, tests, and install smoke test
  --skip-install         Skip only the install and consumer smoke test
  -h, --help             Show this help

Download mode defaults to https://downloads.apache.org/paimon/KEYS. For a
local artifact, pass --keys-url or --keys-file to avoid trusting the user's
default GPG keyring.
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

download_file() {
    local url=$1
    local destination=$2
    if command -v curl >/dev/null 2>&1; then
        curl --fail --location --show-error --silent \
            --output "${destination}" "${url}"
    elif command -v wget >/dev/null 2>&1; then
        wget --quiet --output-document="${destination}" "${url}"
    else
        fail "curl or wget is required to download release files"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            [[ $# -ge 2 ]] || fail "--version requires a value"
            REQUESTED_VERSION=$2
            shift 2
            ;;
        --rc)
            [[ $# -ge 2 ]] || fail "--rc requires a value"
            RC=$2
            shift 2
            ;;
        --dist-dev-base)
            [[ $# -ge 2 ]] || fail "--dist-dev-base requires a value"
            DIST_DEV_BASE_URL=${2%/}
            shift 2
            ;;
        --keys-url)
            [[ $# -ge 2 ]] || fail "--keys-url requires a value"
            KEYS_URL=$2
            shift 2
            ;;
        --keys-file)
            [[ $# -ge 2 ]] || fail "--keys-file requires a value"
            KEYS_FILE=$2
            shift 2
            ;;
        --git-ref)
            [[ $# -ge 2 ]] || fail "--git-ref requires a value"
            GIT_REF=$2
            shift 2
            ;;
        --rat-jar)
            [[ $# -ge 2 ]] || fail "--rat-jar requires a value"
            RAT_JAR=$2
            shift 2
            ;;
        --rat-version)
            [[ $# -ge 2 ]] || fail "--rat-version requires a value"
            RAT_VERSION=$2
            shift 2
            ;;
        --jobs)
            [[ $# -ge 2 ]] || fail "--jobs requires a value"
            JOBS=$2
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
        --skip-install)
            SKIP_INSTALL=true
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

[[ -z "${KEYS_URL}" || -z "${KEYS_FILE}" ]] ||
    fail "--keys-url and --keys-file are mutually exclusive"
[[ -z "${REQUESTED_VERSION}" ||
    "${REQUESTED_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
    fail "--version must use MAJOR.MINOR.PATCH format"
[[ -z "${RC}" || "${RC}" =~ ^[0-9]+$ ]] ||
    fail "--rc must be a non-negative integer"
[[ -z "${JOBS}" || "${JOBS}" =~ ^[1-9][0-9]*$ ]] ||
    fail "--jobs must be a positive integer"

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

if [[ -z "${ARTIFACT}" ]]; then
    [[ -n "${REQUESTED_VERSION}" && -n "${RC}" ]] ||
        fail "ARTIFACT or both --version and --rc are required"
    ARTIFACT_NAME="apache-paimon-cpp-${REQUESTED_VERSION}-src.tgz"
    RC_URL="${DIST_DEV_BASE_URL}/paimon-cpp-${REQUESTED_VERSION}-rc${RC}"
    DOWNLOAD_DIR="${TEMP_DIR}/download"
    mkdir -p "${DOWNLOAD_DIR}"
    echo "Downloading Apache Paimon C++ ${REQUESTED_VERSION} RC${RC}..."
    download_file "${RC_URL}/${ARTIFACT_NAME}" "${DOWNLOAD_DIR}/${ARTIFACT_NAME}"
    download_file "${RC_URL}/${ARTIFACT_NAME}.sha512" \
        "${DOWNLOAD_DIR}/${ARTIFACT_NAME}.sha512"
    if [[ "${ALLOW_UNSIGNED}" == false ]]; then
        download_file "${RC_URL}/${ARTIFACT_NAME}.asc" \
            "${DOWNLOAD_DIR}/${ARTIFACT_NAME}.asc"
    fi
    ARTIFACT="${DOWNLOAD_DIR}/${ARTIFACT_NAME}"
    KEYS_URL=${KEYS_URL:-"https://downloads.apache.org/paimon/KEYS"}
fi

[[ -f "${ARTIFACT}" ]] || fail "artifact does not exist: ${ARTIFACT}"
ARTIFACT_DIR=$(cd "$(dirname "${ARTIFACT}")" && pwd)
ARTIFACT_NAME=$(basename "${ARTIFACT}")
ARTIFACT="${ARTIFACT_DIR}/${ARTIFACT_NAME}"

if [[ "${ARTIFACT_NAME}" =~ ^apache-paimon-cpp-([0-9]+\.[0-9]+\.[0-9]+)-src\.tgz$ ]]; then
    RELEASE_VERSION=${BASH_REMATCH[1]}
else
    fail "unexpected artifact name: ${ARTIFACT_NAME}"
fi
[[ -z "${REQUESTED_VERSION}" || "${REQUESTED_VERSION}" == "${RELEASE_VERSION}" ]] ||
    fail "artifact version ${RELEASE_VERSION} does not match ${REQUESTED_VERSION}"

CHECKSUM_FILE="${ARTIFACT}.sha512"
SIGNATURE_FILE="${ARTIFACT}.asc"
ARCHIVE_ROOT="paimon-cpp-${RELEASE_VERSION}"

[[ -f "${CHECKSUM_FILE}" ]] || fail "missing checksum: ${CHECKSUM_FILE}"
CHECKSUM_LINE_COUNT=$(awk 'NF { count++ } END { print count + 0 }' "${CHECKSUM_FILE}")
[[ "${CHECKSUM_LINE_COUNT}" == "1" ]] ||
    fail "checksum file must contain exactly one non-empty line"
CHECKSUM_FIELD_COUNT=$(awk 'NF { print NF; exit }' "${CHECKSUM_FILE}")
[[ "${CHECKSUM_FIELD_COUNT}" == "2" ]] ||
    fail "checksum line must contain a digest and a filename"
EXPECTED_SHA512=$(awk 'NF { print $1; exit }' "${CHECKSUM_FILE}")
CHECKSUM_ARTIFACT=$(awk 'NF { print $2; exit }' "${CHECKSUM_FILE}")
[[ "${EXPECTED_SHA512}" =~ ^[0-9a-fA-F]{128}$ ]] ||
    fail "invalid SHA-512 file: ${CHECKSUM_FILE}"
[[ "${CHECKSUM_ARTIFACT}" == "${ARTIFACT_NAME}" ]] ||
    fail "checksum names ${CHECKSUM_ARTIFACT:-<missing>}, expected ${ARTIFACT_NAME}"
ACTUAL_SHA512=$(calculate_sha512 "${ARTIFACT}")
EXPECTED_SHA512=$(printf '%s' "${EXPECTED_SHA512}" | tr '[:upper:]' '[:lower:]')
ACTUAL_SHA512=$(printf '%s' "${ACTUAL_SHA512}" | tr '[:upper:]' '[:lower:]')
[[ "${ACTUAL_SHA512}" == "${EXPECTED_SHA512}" ]] ||
    fail "SHA-512 checksum does not match"
echo "SHA-512 checksum: valid"

if [[ -f "${SIGNATURE_FILE}" ]]; then
    command -v gpg >/dev/null 2>&1 || fail "gpg is required to verify the signature"
    if [[ -n "${KEYS_URL}" || -n "${KEYS_FILE}" ]]; then
        GNUPG_HOME="${TEMP_DIR}/gnupg"
        mkdir -m 700 "${GNUPG_HOME}"
        if [[ -n "${KEYS_URL}" ]]; then
            KEYS_FILE="${TEMP_DIR}/KEYS"
            download_file "${KEYS_URL}" "${KEYS_FILE}"
        else
            KEYS_FILE=$(cd "$(dirname "${KEYS_FILE}")" && pwd)/$(basename "${KEYS_FILE}")
        fi
        [[ -f "${KEYS_FILE}" ]] || fail "KEYS file does not exist: ${KEYS_FILE}"
        gpg --batch --homedir "${GNUPG_HOME}" --import "${KEYS_FILE}" >/dev/null
        gpg --batch --homedir "${GNUPG_HOME}" \
            --verify "${SIGNATURE_FILE}" "${ARTIFACT}"
        echo "OpenPGP signature: valid against ${KEYS_FILE}"
    else
        gpg --verify "${SIGNATURE_FILE}" "${ARTIFACT}"
        echo "OpenPGP signature: valid against the default GPG keyring"
    fi
elif [[ "${ALLOW_UNSIGNED}" == true ]]; then
    echo "OpenPGP signature: skipped for local/CI preparation"
else
    fail "missing signature: ${SIGNATURE_FILE}"
fi

command -v python3 >/dev/null 2>&1 || fail "python3 is required"
python3 "${SCRIPT_DIR}/validate_source_archive.py" \
    --expected-root "${ARCHIVE_ROOT}" "${ARTIFACT}"

tar -xzf "${ARTIFACT}" -C "${TEMP_DIR}"
SOURCE_DIR="${TEMP_DIR}/${ARCHIVE_ROOT}"
[[ -d "${SOURCE_DIR}" ]] || fail "archive root is missing: ${ARCHIVE_ROOT}"

for required_file in \
    LICENSE \
    NOTICE \
    CMakeLists.txt \
    docs/source/conf.py \
    docs/source/_static/versions.json \
    .github/.rat-excludes \
    scripts/releasing/create_source_release.sh; do
    [[ -f "${SOURCE_DIR}/${required_file}" ]] ||
        fail "required release file is missing: ${required_file}"
done

python3 "${SCRIPT_DIR}/bump_version.py" \
    --root "${SOURCE_DIR}" \
    --check "${RELEASE_VERSION}"

UNEXPECTED_BINARIES=$(
    find "${SOURCE_DIR}" -type f \
        \( -name '*.a' -o -name '*.bc' -o -name '*.class' -o -name '*.dll' \
        -o -name '*.dylib' -o -name '*.exe' -o -name '*.jar' -o -name '*.la' \
        -o -name '*.lib' -o -name '*.lo' -o -name '*.o' -o -name '*.obj' \
        -o -name '*.pdb' -o -name '*.pyc' -o -name '*.so' -o -name '*.wasm' \) \
        -print
)
[[ -z "${UNEXPECTED_BINARIES}" ]] ||
    fail "source archive contains unexpected compiled files:${UNEXPECTED_BINARIES}"
echo "Archive layout and release metadata: valid"

if [[ -n "${GIT_REF}" ]]; then
    git -C "${SOURCE_ROOT}" rev-parse --verify "${GIT_REF}^{commit}" >/dev/null 2>&1 ||
        fail "Git ref does not resolve to a commit: ${GIT_REF}"
    if git -C "${SOURCE_ROOT}" rev-parse --verify "${GIT_REF}^{tag}" \
        >/dev/null 2>&1; then
        git -C "${SOURCE_ROOT}" verify-tag "${GIT_REF}"
    fi
    REPRO_DIR="${TEMP_DIR}/reproduced"
    "${SCRIPT_DIR}/create_source_release.sh" \
        --version "${RELEASE_VERSION}" \
        --git-ref "${GIT_REF}" \
        --output-dir "${REPRO_DIR}"
    cmp "${ARTIFACT}" "${REPRO_DIR}/${ARTIFACT_NAME}" >/dev/null ||
        fail "artifact bytes differ from a fresh archive of ${GIT_REF}"
    echo "Git ref reproducibility: valid (${GIT_REF})"
fi

if [[ "${SKIP_RAT}" == false ]]; then
    if [[ -z "${RAT_JAR}" ]]; then
        RAT_JAR="${TEMP_DIR}/apache-rat-${RAT_VERSION}.jar"
        download_file \
            "https://repo.maven.apache.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar" \
            "${RAT_JAR}"
    fi
    [[ -f "${RAT_JAR}" ]] || fail "Apache RAT jar does not exist: ${RAT_JAR}"

    RAT_REPORT="${TEMP_DIR}/rat-report.txt"
    java -jar "${RAT_JAR}" \
        -E "${SOURCE_DIR}/.github/.rat-excludes" \
        -d "${SOURCE_DIR}" >"${RAT_REPORT}"

    UNKNOWN_LICENSE_COUNT=$(
        awk '/^[[:space:]]*[0-9]+[[:space:]]+Unknown Licenses[[:space:]]*$/ {
            print $1
        }' "${RAT_REPORT}"
    )
    if [[ ! "${UNKNOWN_LICENSE_COUNT}" =~ ^[0-9]+$ ]]; then
        cat "${RAT_REPORT}"
        fail "could not determine the Apache RAT unknown license count"
    fi
    if [[ "${UNKNOWN_LICENSE_COUNT}" != "0" ]]; then
        cat "${RAT_REPORT}"
        fail "Apache RAT found ${UNKNOWN_LICENSE_COUNT} files with unknown licenses"
    fi
    echo "Apache RAT: valid"
else
    echo "Apache RAT: skipped for local preparation"
fi

if [[ "${SKIP_BUILD}" == false ]]; then
    INSTALL_SMOKE=true
    if [[ "${SKIP_INSTALL}" == true ]]; then
        INSTALL_SMOKE=false
    fi
    BUILD_ARGS=(
        --source_dir "${SOURCE_DIR}"
        --build_type Release
    )
    if [[ "${INSTALL_SMOKE}" == true ]]; then
        BUILD_ARGS+=(--install_smoke)
    fi
    PAIMON_BUILD_JOBS="${JOBS}" \
        "${SOURCE_DIR}/ci/scripts/build_paimon.sh" "${BUILD_ARGS[@]}"
    echo "Release build and tests: valid"
    if [[ "${INSTALL_SMOKE}" == true ]]; then
        echo "Install and consumer smoke test: valid"
    else
        echo "Install and consumer smoke test: skipped"
    fi
else
    echo "Release build, tests, and install smoke test: skipped"
fi

echo "Release candidate verification completed successfully."
