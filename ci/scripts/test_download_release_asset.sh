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
set -euo pipefail

script="$(dirname "${BASH_SOURCE[0]}")/download_release_asset.sh"
work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT
export PATH="${work}:${PATH}"
export REQUEST_LOG="${work}/requests"
cat > "${work}/curl" <<'MOCK'
#!/usr/bin/env bash
set -eu
echo request >> "${REQUEST_LOG}"
case "$*" in
    *github.com/org/repo/releases/download/*) exit 22 ;;
    *api.github.com/repos/org/repo/releases/tags/v1*)
        echo '{"assets":[{"name":"archive","url":"https://api.github.com/assets/123"}]}' ;;
    *application/octet-stream*https://api.github.com/assets/123*)
        printf '%s' "${ASSET_CONTENT:-abc}" > "${!#}" ;;
    *) exit 1 ;;
esac
MOCK
chmod +x "${work}/curl"
url=https://github.com/org/repo/releases/download/v1/archive
checksum=ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
fetch() {
    bash "${script}" "${url}" "${work}/archive" "${checksum}"
}

fetch
[[ $(cat "${work}/archive") == abc ]]
fetch
[[ $(wc -l < "${REQUEST_LOG}") -eq 3 ]]
echo corrupt > "${work}/archive"
fetch
[[ $(cat "${work}/archive") == abc ]]
rm "${work}/archive"
if ASSET_CONTENT=invalid fetch; then
    echo "Checksum mismatch accepted" >&2
    exit 1
fi
[[ ! -e "${work}/archive" ]]
