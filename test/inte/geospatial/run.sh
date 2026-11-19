#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_dir="$(cd "$script_dir/../../.." && pwd)"
test_binary="${1:-$repo_dir/build/debug/paimon-parquet-format-test}"
interop_dir="$(mktemp -d)"
trap 'rm -rf "$interop_dir"' EXIT
mvn -q -f "$script_dir/pom.xml" dependency:build-classpath \
    -Dpaimon.version="${PAIMON_JAVA_VERSION:-2.2-SNAPSHOT}" -Dmdep.outputFile="$interop_dir/classpath"
classpath="$(cat "$interop_dir/classpath")"
javac -cp "$classpath" -d "$interop_dir" "$script_dir/GeospatialInterop.java"
java -cp "$interop_dir:$classpath" GeospatialInterop write "$interop_dir/java.parquet"
PAIMON_GEOSPATIAL_INTEROP_DIR="$interop_dir" "$test_binary" --gtest_filter=GeospatialParquetTest.JavaInteroperability
java -cp "$interop_dir:$classpath" GeospatialInterop check "$interop_dir/cpp.parquet"
