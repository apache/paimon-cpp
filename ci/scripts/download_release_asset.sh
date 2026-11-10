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
# Fetch a pinned GitHub release asset. Use the release API if the browser
# download endpoint times out, and publish a file only after verification.
set -euo pipefail

url=$1
output=$2
sha256=$3

verify() {
    if command -v sha256sum >/dev/null; then
        echo "${sha256}  $1" | sha256sum -c -
    else
        echo "${sha256}  $1" | shasum -a 256 -c -
    fi
}

if [[ -f "${output}" ]] && verify "${output}"; then
    exit 0
fi

mkdir -p "$(dirname "${output}")"
tmp=$(mktemp "${output}.XXXXXX")
trap 'rm -f "${tmp}"' EXIT
curl_args=(--proto '=https' --tlsv1.2 -fsSL --retry 3 --connect-timeout 15 --max-time 180)
if ! curl "${curl_args[@]}" "${url}" -o "${tmp}" || ! verify "${tmp}"; then
    release=${url#https://github.com/}
    repo=${release%%/releases/download/*}
    asset=${release##*/}
    tag=${release#*/releases/download/}
    tag=${tag%/*}
    asset_url=$(curl "${curl_args[@]}" "https://api.github.com/repos/${repo}/releases/tags/${tag}" |
        python3 -c 'import json, sys
print(next(a["url"] for a in json.load(sys.stdin)["assets"] if a["name"] == sys.argv[1]))' "${asset}")
    curl "${curl_args[@]}" -H 'Accept: application/octet-stream' "${asset_url}" -o "${tmp}"
    verify "${tmp}"
fi
mv -f "${tmp}" "${output}"
