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
RELEASE_BRANCH="main"
OUTPUT_DIR=""
DIST_DEV_BASE_URL="https://dist.apache.org/repos/dist/dev/paimon"
PREPARE_ONLY=false
DRY_RUN=false
WORKFLOW_DISCOVERY_TIMEOUT_SECONDS=600
WORKFLOW_RUN_ID=""
TEMP_DIR=""

usage() {
    cat <<'EOF'
Create, verify, and stage an Apache Paimon C++ release candidate.

Usage:
  release_rc.sh --version VERSION --rc RC --signing-key KEY_ID [options]

Required:
  --version VERSION       Release version, for example 0.3.0
  --rc RC                 Release candidate number, for example 1
  --signing-key KEY_ID    OpenPGP key used for the tag and source artifact

Options:
  --remote REMOTE         Apache Git remote (default: origin)
  --release-branch NAME   Remote branch containing the RC commit (default: main)
  --output-dir DIR        Artifact directory (default: release/VERSION-rcRC)
  --dist-dev-base URL     ASF dist/dev project URL
  --prepare-only          Create and verify locally; do not push or upload
  --dry-run               Print the planned release identifiers and exit
  -h, --help              Show this help

For a published RC, GitHub Actions creates and tests the canonical source
archive. This script downloads those exact bytes, signs them locally, and
uploads them to ASF dist/dev. Prepare-only mode creates a local preview.
EOF
}

fail() {
    echo "Error: $*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

validate_release_branch() {
    local remote_branch_ref="refs/remotes/${REMOTE}/${RELEASE_BRANCH}"
    local release_branch_commit

    git check-ref-format "refs/heads/${RELEASE_BRANCH}" >/dev/null 2>&1 ||
        fail "invalid release branch name: ${RELEASE_BRANCH}"

    echo "Fetching ${REMOTE}/${RELEASE_BRANCH} before publishing the RC."
    git fetch --no-tags "${REMOTE}" \
        "refs/heads/${RELEASE_BRANCH}:${remote_branch_ref}"
    release_branch_commit=$(git rev-parse --verify "${remote_branch_ref}^{commit}")
    git merge-base --is-ancestor "${HEAD_COMMIT}" "${release_branch_commit}" ||
        fail "HEAD ${HEAD_COMMIT} is not contained in ${REMOTE}/${RELEASE_BRANCH} (${release_branch_commit})"
}

wait_for_release_artifact_workflow() {
    local deadline=$((SECONDS + WORKFLOW_DISCOVERY_TIMEOUT_SECONDS))
    local run_id=""

    echo "Waiting for the ${RC_TAG} Release Artifact workflow to start."
    while [[ -z "${run_id}" ]]; do
        if ! run_id=$(
            gh run list \
                --repo apache/paimon-cpp \
                --workflow release_artifact.yaml \
                --branch "${RC_TAG}" \
                --commit "${HEAD_COMMIT}" \
                --event push \
                --limit 1 \
                --json databaseId \
                --jq '.[0].databaseId // empty'
        ); then
            fail "unable to query the Release Artifact workflow for ${RC_TAG}"
        fi
        if [[ -n "${run_id}" ]]; then
            break
        fi
        if ((SECONDS >= deadline)); then
            fail "timed out waiting for the Release Artifact workflow for ${RC_TAG}"
        fi
        sleep 10
    done

    echo "Waiting for Release Artifact workflow run ${run_id} to succeed."
    gh run watch "${run_id}" \
        --repo apache/paimon-cpp \
        --compact \
        --exit-status \
        --interval 30 ||
        fail "Release Artifact workflow run ${run_id} failed"
    WORKFLOW_RUN_ID=${run_id}
}

validate_workflow_artifact_directory() {
    local directory=$1
    local -a entries
    local entry
    local name

    shopt -s dotglob nullglob
    entries=("${directory}"/*)
    shopt -u dotglob nullglob
    [[ ${#entries[@]} -eq 2 ]] ||
        fail "workflow artifact must contain exactly the archive and checksum"
    for entry in "${entries[@]}"; do
        [[ -f "${entry}" ]] ||
            fail "workflow artifact contains a non-file entry: ${entry}"
        name=$(basename "${entry}")
        case "${name}" in
            "${ARTIFACT_NAME}" | "${ARTIFACT_NAME}.sha512")
                ;;
            *)
                fail "workflow artifact contains an unexpected file: ${name}"
                ;;
        esac
    done
}

download_and_sign_workflow_artifact() {
    local workflow_dir="${TEMP_DIR}/source-archive"
    local source
    local target
    local suffix

    [[ -n "${WORKFLOW_RUN_ID}" ]] || fail "release workflow run ID is missing"
    mkdir -p "${workflow_dir}"
    gh run download "${WORKFLOW_RUN_ID}" \
        --repo apache/paimon-cpp \
        --name source-archive \
        --dir "${workflow_dir}"
    validate_workflow_artifact_directory "${workflow_dir}"

    mkdir -p "${OUTPUT_DIR}"
    for suffix in "" ".sha512"; do
        source="${workflow_dir}/${ARTIFACT_NAME}${suffix}"
        target="${OUTPUT_DIR}/${ARTIFACT_NAME}${suffix}"
        if [[ -e "${target}" ]]; then
            [[ -f "${target}" ]] || fail "artifact path is not a file: ${target}"
            cmp "${source}" "${target}" >/dev/null ||
                fail "existing ${target} differs from workflow run ${WORKFLOW_RUN_ID}"
        else
            cp -p "${source}" "${target}"
        fi
    done

    ARTIFACT="${OUTPUT_DIR}/${ARTIFACT_NAME}"
    if [[ -e "${ARTIFACT}.asc" ]]; then
        [[ -f "${ARTIFACT}.asc" ]] ||
            fail "artifact signature path is not a file: ${ARTIFACT}.asc"
        echo "Reusing existing source artifact signature."
    else
        echo "Signing workflow source artifact with ${SIGNING_KEY}."
        gpg --armor \
            --local-user "${SIGNING_KEY}" \
            --detach-sign \
            --output "${ARTIFACT}.asc" \
            "${ARTIFACT}"
    fi
}

validate_artifact_directory() {
    local -a entries
    local entry
    local name

    shopt -s dotglob nullglob
    entries=("${OUTPUT_DIR}"/*)
    shopt -u dotglob nullglob
    [[ ${#entries[@]} -eq 3 ]] ||
        fail "${OUTPUT_DIR} must contain exactly the archive, signature, and checksum"
    for entry in "${entries[@]}"; do
        [[ -f "${entry}" ]] ||
            fail "release candidate contains a non-file entry: ${entry}"
        name=$(basename "${entry}")
        case "${name}" in
            "${ARTIFACT_NAME}" | \
                "${ARTIFACT_NAME}.asc" | \
                "${ARTIFACT_NAME}.sha512")
                ;;
            *)
                fail "release candidate contains an unexpected file: ${name}"
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
        --release-branch)
            [[ $# -ge 2 ]] || fail "--release-branch requires a value"
            RELEASE_BRANCH=$2
            shift 2
            ;;
        --output-dir)
            [[ $# -ge 2 ]] || fail "--output-dir requires a value"
            OUTPUT_DIR=$2
            shift 2
            ;;
        --dist-dev-base)
            [[ $# -ge 2 ]] || fail "--dist-dev-base requires a value"
            DIST_DEV_BASE_URL=${2%/}
            shift 2
            ;;
        --prepare-only)
            PREPARE_ONLY=true
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

RC_TAG="v${VERSION}-rc${RC}"
RC_ID="paimon-cpp-${VERSION}-rc${RC}"
ARTIFACT_NAME="apache-paimon-cpp-${VERSION}-src.tgz"
RC_URL="${DIST_DEV_BASE_URL}/${RC_ID}"
OUTPUT_DIR=${OUTPUT_DIR:-"${SOURCE_ROOT}/release/${VERSION}-rc${RC}"}

if [[ "${DRY_RUN}" == true ]]; then
    cat <<EOF
Release candidate tag: ${RC_TAG}
Artifact:             ${ARTIFACT_NAME}
Output directory:     ${OUTPUT_DIR}
ASF staging URL:      ${RC_URL}
Git remote:           ${REMOTE}
Release branch:       ${RELEASE_BRANCH}
Prepare only:         ${PREPARE_ONLY}
EOF
    exit 0
fi

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

for command in git gpg python3; do
    require_command "${command}"
done
if [[ "${PREPARE_ONLY}" == false ]]; then
    require_command gh
    require_command svn
fi

cd "${SOURCE_ROOT}"
[[ -z "$(git status --porcelain)" ]] ||
    fail "working tree must be clean before creating a release candidate"

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

"${SCRIPT_DIR}/bump_version.py" --check "${VERSION}"

HEAD_COMMIT=$(git rev-parse HEAD)
if [[ "${PREPARE_ONLY}" == false ]]; then
    validate_release_branch
fi

if git rev-parse --verify "${RC_TAG}^{tag}" >/dev/null 2>&1; then
    TAG_COMMIT=$(git rev-parse "${RC_TAG}^{commit}")
    [[ "${TAG_COMMIT}" == "${HEAD_COMMIT}" ]] ||
        fail "${RC_TAG} points to ${TAG_COMMIT}, expected HEAD ${HEAD_COMMIT}"
    git verify-tag "${RC_TAG}"
    echo "Reusing verified local tag ${RC_TAG}."
else
    git tag -s -u "${SIGNING_KEY}" -m "Apache Paimon C++ ${VERSION} RC${RC}" \
        "${RC_TAG}"
    git verify-tag "${RC_TAG}"
fi

ARTIFACT="${OUTPUT_DIR}/${ARTIFACT_NAME}"
if [[ "${PREPARE_ONLY}" == true ]]; then
    if [[ -e "${ARTIFACT}" || -e "${ARTIFACT}.asc" || -e "${ARTIFACT}.sha512" ]]; then
        [[ -f "${ARTIFACT}" && -f "${ARTIFACT}.asc" && -f "${ARTIFACT}.sha512" ]] ||
            fail "artifact directory contains an incomplete release candidate"
        echo "Reusing existing preview artifacts in ${OUTPUT_DIR}."
    else
        "${SCRIPT_DIR}/create_source_release.sh" \
            --version "${VERSION}" \
            --git-ref "${RC_TAG}" \
            --output-dir "${OUTPUT_DIR}" \
            --signing-key "${SIGNING_KEY}"
    fi

    "${SCRIPT_DIR}/verify_release_candidate.sh" \
        --git-ref "${RC_TAG}" \
        --keys-url "https://downloads.apache.org/paimon/KEYS" \
        "${ARTIFACT}"
    validate_artifact_directory

    cat <<EOF

Local release preparation completed successfully.

The signed tag and preview source artifacts were created and verified locally.
No tag was pushed and no artifacts were uploaded to ASF dist/dev.
The published workflow artifact is authoritative; do not start a release vote
from this prepare-only run.
EOF
    exit 0
fi

if svn info "${RC_URL}" >/dev/null 2>&1; then
    fail "release candidate already exists in ASF dist/dev: ${RC_URL}"
fi
git push "${REMOTE}" "${RC_TAG}"
wait_for_release_artifact_workflow
download_and_sign_workflow_artifact

"${SCRIPT_DIR}/verify_release_candidate.sh" \
    --git-ref "${RC_TAG}" \
    --keys-url "https://downloads.apache.org/paimon/KEYS" \
    "${ARTIFACT}"
validate_artifact_directory

svn import "${OUTPUT_DIR}" "${RC_URL}" \
    -m "Add Apache Paimon C++ ${VERSION} RC${RC}"

cat <<EOF

Release candidate staged successfully.

To: dev@paimon.apache.org
Subject: [VOTE][C++] Release Apache Paimon C++ ${VERSION} RC${RC}

Hi everyone,

Please review and vote on Apache Paimon C++ ${VERSION} RC${RC}.

The release candidate is based on commit:
${HEAD_COMMIT}

Source artifacts:
${RC_URL}/

Git tag:
https://github.com/apache/paimon-cpp/releases/tag/${RC_TAG}

KEYS:
https://downloads.apache.org/paimon/KEYS

The vote will remain open for at least 72 hours.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not approve, because...
EOF
