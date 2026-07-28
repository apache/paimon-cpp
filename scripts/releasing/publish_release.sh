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

VERSION=""
RC=""
SIGNING_KEY=""
REMOTE="origin"
DIST_DEV_BASE_URL="https://dist.apache.org/repos/dist/dev/paimon"
DIST_RELEASE_BASE_URL="https://dist.apache.org/repos/dist/release/paimon"
CONFIRM_VOTE_PASSED=false
SKIP_GITHUB_RELEASE=false
DRY_RUN=false

usage() {
    cat <<'EOF'
Publish an approved Apache Paimon C++ release candidate.

Usage:
  publish_release.sh --version VERSION --rc RC --signing-key KEY_ID \
    --confirm-vote-passed [options]

Required:
  --version VERSION       Approved release version
  --rc RC                 Approved release candidate number
  --signing-key KEY_ID    OpenPGP key used to sign the final Git tag
  --confirm-vote-passed   Explicitly confirm that the PMC vote passed

Options:
  --remote REMOTE         Apache Git remote (default: origin)
  --dist-dev-base URL     ASF dist/dev project URL
  --dist-release-base URL ASF dist/release project URL
  --skip-github-release   Do not create the GitHub Release
  --dry-run               Print the planned publication identifiers and exit
  -h, --help              Show this help

The script is resumable if the final tag, SVN move, or GitHub Release already
completed and still points to the approved RC commit.
EOF
}

fail() {
    echo "Error: $*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

validate_release_directory() {
    local directory=$1
    local -a entries
    local entry
    local name

    shopt -s dotglob nullglob
    entries=("${directory}"/*)
    shopt -u dotglob nullglob
    [[ ${#entries[@]} -eq 3 ]] ||
        fail "${directory} must contain exactly the archive, signature, and checksum"
    for entry in "${entries[@]}"; do
        [[ -f "${entry}" ]] || fail "release contains a non-file entry: ${entry}"
        name=$(basename "${entry}")
        case "${name}" in
            "${ARTIFACT_NAME}" | \
                "${ARTIFACT_NAME}.asc" | \
                "${ARTIFACT_NAME}.sha512")
                ;;
            *)
                fail "release contains an unexpected file: ${name}"
                ;;
        esac
    done
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            [[ $# -ge 2 ]] || fail "--version requires a value"
            VERSION=$2
            shift 2
            ;;
        --rc)
            [[ $# -ge 2 ]] || fail "--rc requires a value"
            RC=$2
            shift 2
            ;;
        --signing-key)
            [[ $# -ge 2 ]] || fail "--signing-key requires a value"
            SIGNING_KEY=$2
            shift 2
            ;;
        --remote)
            [[ $# -ge 2 ]] || fail "--remote requires a value"
            REMOTE=$2
            shift 2
            ;;
        --dist-dev-base)
            [[ $# -ge 2 ]] || fail "--dist-dev-base requires a value"
            DIST_DEV_BASE_URL=${2%/}
            shift 2
            ;;
        --dist-release-base)
            [[ $# -ge 2 ]] || fail "--dist-release-base requires a value"
            DIST_RELEASE_BASE_URL=${2%/}
            shift 2
            ;;
        --confirm-vote-passed)
            CONFIRM_VOTE_PASSED=true
            shift
            ;;
        --skip-github-release)
            SKIP_GITHUB_RELEASE=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
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

[[ "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
    fail "--version must use MAJOR.MINOR.PATCH format"
[[ "${RC}" =~ ^[0-9]+$ ]] || fail "--rc must be a non-negative integer"
[[ -n "${SIGNING_KEY}" ]] || fail "--signing-key is required"
[[ "${CONFIRM_VOTE_PASSED}" == true ]] ||
    fail "--confirm-vote-passed is required"

RC_TAG="v${VERSION}-rc${RC}"
RELEASE_TAG="v${VERSION}"
RC_ID="paimon-cpp-${VERSION}-rc${RC}"
RELEASE_ID="paimon-cpp-${VERSION}"
ARTIFACT_NAME="apache-paimon-cpp-${VERSION}-src.tgz"
RC_URL="${DIST_DEV_BASE_URL}/${RC_ID}"
RELEASE_URL="${DIST_RELEASE_BASE_URL}/${RELEASE_ID}"

if [[ "${DRY_RUN}" == true ]]; then
    cat <<EOF
Approved RC tag:       ${RC_TAG}
Final release tag:     ${RELEASE_TAG}
ASF source:            ${RC_URL}
ASF destination:       ${RELEASE_URL}
Git remote:            ${REMOTE}
Create GitHub Release: $([[ "${SKIP_GITHUB_RELEASE}" == true ]] && echo no || echo yes)
EOF
    exit 0
fi

for command in git gpg svn; do
    require_command "${command}"
done
if [[ "${SKIP_GITHUB_RELEASE}" == false ]]; then
    require_command gh
fi

cd "${SOURCE_ROOT}"
[[ -z "$(git status --porcelain)" ]] ||
    fail "working tree must be clean before publishing a release"

REMOTE_URL=$(git remote get-url "${REMOTE}" 2>/dev/null) ||
    fail "Git remote does not exist: ${REMOTE}"
case "${REMOTE_URL}" in
    git@github.com:apache/paimon-cpp.git | \
        https://github.com/apache/paimon-cpp | \
        https://github.com/apache/paimon-cpp.git | \
        ssh://git@github.com/apache/paimon-cpp.git)
        ;;
    *)
        fail "${REMOTE} must point to apache/paimon-cpp, found ${REMOTE_URL}"
        ;;
esac

git rev-parse --verify "${RC_TAG}^{tag}" >/dev/null 2>&1 ||
    fail "signed RC tag is missing: ${RC_TAG}"
git verify-tag "${RC_TAG}"
RC_COMMIT=$(git rev-parse "${RC_TAG}^{commit}")

RC_PRESENT=false
RELEASE_PRESENT=false
if svn info "${RC_URL}" >/dev/null 2>&1; then
    RC_PRESENT=true
fi
if svn info "${RELEASE_URL}" >/dev/null 2>&1; then
    RELEASE_PRESENT=true
fi
if [[ "${RC_PRESENT}" == true && "${RELEASE_PRESENT}" == true ]]; then
    fail "both RC and final release directories exist; resolve SVN state manually"
fi
if [[ "${RC_PRESENT}" == false && "${RELEASE_PRESENT}" == false ]]; then
    fail "approved artifacts are missing from both dist/dev and dist/release"
fi

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT
RELEASE_FILES_DIR="${TEMP_DIR}/${RELEASE_ID}"
if [[ "${RC_PRESENT}" == true ]]; then
    svn export --quiet "${RC_URL}" "${RELEASE_FILES_DIR}"
else
    svn export --quiet "${RELEASE_URL}" "${RELEASE_FILES_DIR}"
fi
validate_release_directory "${RELEASE_FILES_DIR}"
for suffix in "" ".asc" ".sha512"; do
    [[ -f "${RELEASE_FILES_DIR}/${ARTIFACT_NAME}${suffix}" ]] ||
        fail "approved release is missing ${ARTIFACT_NAME}${suffix}"
done

"${SCRIPT_DIR}/verify_release_candidate.sh" \
    --keys-url "https://downloads.apache.org/paimon/KEYS" \
    --git-ref "${RC_TAG}" \
    --skip-build \
    "${RELEASE_FILES_DIR}/${ARTIFACT_NAME}"

if git rev-parse --verify "${RELEASE_TAG}^{tag}" >/dev/null 2>&1; then
    RELEASE_COMMIT=$(git rev-parse "${RELEASE_TAG}^{commit}")
    [[ "${RELEASE_COMMIT}" == "${RC_COMMIT}" ]] ||
        fail "${RELEASE_TAG} does not point to the approved RC commit"
    git verify-tag "${RELEASE_TAG}"
    echo "Reusing verified final tag ${RELEASE_TAG}."
else
    git tag -s -u "${SIGNING_KEY}" \
        -m "Release Apache Paimon C++ ${VERSION}" \
        "${RELEASE_TAG}" "${RC_COMMIT}"
    git verify-tag "${RELEASE_TAG}"
fi
git push "${REMOTE}" "${RELEASE_TAG}"

if [[ "${RELEASE_PRESENT}" == true ]]; then
    echo "Release artifacts are already present at ${RELEASE_URL}."
else
    svn mv "${RC_URL}" "${RELEASE_URL}" \
        -m "Release Apache Paimon C++ ${VERSION}"
fi

if [[ "${SKIP_GITHUB_RELEASE}" == false ]]; then
    if gh release view "${RELEASE_TAG}" --repo apache/paimon-cpp >/dev/null 2>&1; then
        echo "Checking existing GitHub Release ${RELEASE_TAG}."
        EXISTING_ASSETS_DIR="${TEMP_DIR}/existing-assets"
        mkdir "${EXISTING_ASSETS_DIR}"
        for suffix in "" ".asc" ".sha512"; do
            ASSET_NAME="${ARTIFACT_NAME}${suffix}"
            if gh release view "${RELEASE_TAG}" \
                --repo apache/paimon-cpp \
                --json assets \
                --jq '.assets[].name' |
                grep -Fxq "${ASSET_NAME}"; then
                gh release download "${RELEASE_TAG}" \
                    --repo apache/paimon-cpp \
                    --dir "${EXISTING_ASSETS_DIR}" \
                    --pattern "${ASSET_NAME}"
                cmp "${EXISTING_ASSETS_DIR}/${ASSET_NAME}" \
                    "${RELEASE_FILES_DIR}/${ASSET_NAME}" >/dev/null ||
                    fail "GitHub Release asset differs from ASF release: ${ASSET_NAME}"
            else
                gh release upload "${RELEASE_TAG}" \
                    --repo apache/paimon-cpp \
                    "${RELEASE_FILES_DIR}/${ASSET_NAME}"
            fi
        done
    else
        gh release create "${RELEASE_TAG}" \
            --repo apache/paimon-cpp \
            --verify-tag \
            --generate-notes \
            --title "Apache Paimon C++ ${VERSION}" \
            "${RELEASE_FILES_DIR}/${ARTIFACT_NAME}" \
            "${RELEASE_FILES_DIR}/${ARTIFACT_NAME}.asc" \
            "${RELEASE_FILES_DIR}/${ARTIFACT_NAME}.sha512"
    fi
fi

cat <<EOF

Apache Paimon C++ ${VERSION} has been published.

Next steps:
  1. Verify the release appears under https://downloads.apache.org/paimon/.
  2. Wait at least 24 hours after publication before updating public download
     or documentation pages and announcing the release.
  3. Remove superseded Paimon C++ releases from dist/release after confirming
     they remain available from archive.apache.org.
  4. Update the project download and documentation pages.

After completing the 24-hour wait, send the following announcement in plain
text from an apache.org email address.

To: dev@paimon.apache.org
CC: announce@apache.org
Subject: [ANNOUNCE][C++] Apache Paimon C++ ${VERSION} released

The Apache Paimon community is pleased to announce the release of
Apache Paimon C++ ${VERSION}.

Source release:
https://downloads.apache.org/paimon/${RELEASE_ID}/

Release notes:
https://github.com/apache/paimon-cpp/releases/tag/${RELEASE_TAG}

Thanks to everyone who contributed to this release.
EOF
