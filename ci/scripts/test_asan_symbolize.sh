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

# Tests that build_support/asan_symbolize.py, which every test binary pipes its
# output through, passes bytes that are not valid UTF-8 straight through. A test
# that fails while printing such a byte used to kill the symbolizer, and with it
# the rest of the test log.

set -euo pipefail

source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
symbolizer="${source_dir}/build_support/asan_symbolize.py"
python=${PYTHON:-python3}
# The vendored script emits SyntaxWarnings for its own regexes; they are unrelated noise here.
export PYTHONWARNINGS=ignore

status=0

# Returns non-zero on failure as well as recording it, so that a caller running it in a
# subshell, where the recorded status would not propagate, can still see the result.
check() {
    local name=$1 input=$2 expected=$3 actual_hex expected_hex result=0
    echo "=== ${name} ==="
    # Compare hex dumps so that a mismatch is readable and the shell does not
    # mangle the bytes on the way. A failing symbolizer is a failed check, not a
    # reason to abort the script, so the remaining checks still run.
    if ! actual_hex=$(printf '%b' "${input}" | "${python}" "${symbolizer}" | od -An -tx1 |
        tr -d ' \n'); then
        echo "symbolizer exited non-zero"
        status=1
        result=1
    fi
    expected_hex=$(printf '%b' "${expected}" | od -An -tx1 | tr -d ' \n')
    if [[ "${actual_hex}" != "${expected_hex}" ]]; then
        echo "expected: ${expected_hex}"
        echo "actual:   ${actual_hex}"
        status=1
        result=1
    fi
    return "${result}"
}

# A lone 0xff is what a failing TINYINT literal assertion prints. Every byte,
# valid UTF-8 or not, has to come out unchanged.
# `|| status=1` keeps `set -e` from aborting on the first failed check.
check "invalid utf-8 round trips" 'ok\n\xff\xfe binary\nplain\n' \
    'ok\n\xff\xfe binary\nplain\n' || status=1

# Valid multi byte UTF-8 must not be damaged either: test names contain it.
check "utf-8 round trips" '\xe4\xb8\xad\xe6\x96\x87\n' '\xe4\xb8\xad\xe6\x96\x87\n' || status=1

# The bytes have to survive whatever the environment asks Python to use: an encoding taken
# from PYTHONIOENCODING on one stream and from the locale on the other would re-encode them.
(
    export PYTHONIOENCODING=latin-1
    check "invalid utf-8 round trips under PYTHONIOENCODING" 'ok\n\xff\xfe\n' \
        'ok\n\xff\xfe\n' || status=1
    # status is set in this subshell and does not reach the caller, so report it as the exit
    # code, which stays correct if another check is added here.
    exit "${status}"
) || status=1

# An invalid byte must not stop the lines that follow it from being processed:
# the stack frame below is still rewritten by the symbolizer. Its stderr is dropped
# because addr2line reports the fake binary path there even when the check passes.
echo "=== keeps processing after an invalid byte ==="
if ! output=$(printf '%b' '\xff\n    #0 0x7f6e35cf2e45  (/blah/foo.so+0x11fe45)\ntail\n' |
    "${python}" "${symbolizer}" 2>/dev/null); then
    echo "symbolizer exited non-zero"
    status=1
fi
# "#0" and "tail" alone would also match unprocessed input, so assert that the frame was
# actually rewritten: the symbolized form gains " in" and loses the raw binary path.
if [[ "${output}" != *"#0"* || "${output}" != *"tail"* || "${output}" != *" in"* ||
    "${output}" == *"/blah/foo.so"* ]]; then
    echo "frame was not rewritten after the invalid byte, got:"
    echo "${output}"
    status=1
fi

echo "=== Darwin symbolizer exchanges text with atos ==="
"${python}" - "${symbolizer}" <<'PY' || status=1
import runpy
import subprocess
import sys
from unittest import mock

symbolizer_path = sys.argv[1]
symbols = runpy.run_path(symbolizer_path)
sys.argv = [symbolizer_path]
real_popen = subprocess.Popen


def fake_atos(command, **kwargs):
    assert command[0] == 'atos', command
    return real_popen(
        [sys.executable, '-c',
         "assert input() == '0x123'; "
         "print('foo(int) (in foo.dylib) (source.cc:42)')"], **kwargs)


symbolizer = symbols['DarwinSymbolizer']('0x123456789', '/blah/foo.dylib')
with mock.patch.object(subprocess, 'Popen', side_effect=fake_atos):
    result = symbolizer.symbolize('0x123456789', '/blah/foo.dylib', '0x123')
assert result == ['0x123456789 in foo source.cc:42'], result
assert symbolizer.pipe.wait() == 0
symbolizer.pipe.stdout.close()
symbolizer.pipe.stderr.close()
PY

exit "${status}"
