<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lance compatibility fixtures

## Paimon Java table

`append_java_compat.db/append_java_compat` is a real append-only Paimon table,
created through the Java catalog and `BatchTableWrite`/`BatchTableCommit` APIs.
Its schema, two snapshots, Avro manifests and two Lance data files are included
unchanged. It contains four rows (IDs 1, 2, 10, 11) and 25 columns.

`ScanAndReadInteTest.TestLanceJavaCompatibility` loads the table through
`TableScan` and `TableRead`, checking every value, conservative predicate handling,
opt-in exact row filtering, and reordered column projection.
The reader batch size is one to exercise multiple batches.

Coverage: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, CHAR, STRING,
BINARY, BYTES, DATE, TIMESTAMP(0/3/6/9), DECIMAL(1,0)/(18,2)/(19,2)/(38,18),
ARRAY<INT>, ARRAY<ARRAY<INT>>, required ROW, nullable-declared ROW, and
VECTOR<FLOAT,3>. Values include null scalars/lists/vectors, null list elements,
empty lists/strings, negative dates/numbers, and non-null structs with null children.
The producer requests full statistics for `id` and none for the other columns.
The default C++ scan retains both files; the test does not assume Lance min/max pruning.

The producer is Apache Paimon Java commit
`63ffd403648e1cf9848e32bb29a398c1b3b16ab1`, using `lance-core:0.39.0` and
Arrow Java 15.0.0. `GenerateLanceTable.java` verifies the generated table by
reading all rows back through Java and comparing their serialized values.

### Regeneration

Use a checkout of that Java revision, JDK 11, and Maven 3.9.9. Set `JAVA_REPO`
and `CPP_REPO` to the two source directories. Keep generated classes and Maven
classpath files outside the C++ checkout:

```bash
WORK=$(mktemp -d)
mvn -f "$JAVA_REPO/pom.xml" -pl paimon-lance -am install \
  -DskipTests -Dcheckstyle.skip -Drat.skip=true -Dspotless.skip=true
mvn -f "$JAVA_REPO/paimon-lance/pom.xml" dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile="$WORK/classpath"
CP="$JAVA_REPO/paimon-lance/target/classes:$(cat "$WORK/classpath")"
javac -cp "$CP" -d "$WORK" \
  "$CPP_REPO/test/test_data/lance/GenerateLanceTable.java"
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$WORK:$CP" \
  GenerateLanceTable "$WORK/warehouse"
```

Expected: `Verified Java table round trip: 4 rows, 25 columns, 2 commits`.
Replace the entire `append_java_compat.db` directory with the generated one;
do not mix files from different runs. File UUIDs, commit users and timestamps
vary, but schema and row values are reproducible. The generator refuses to
overwrite an existing warehouse. No Java or Python runtime is needed by C++ tests.

```bash
cmake --build build --target paimon-scan-and-read-inte-test -j 4
./build/debug/paimon-scan-and-read-inte-test \
  --gtest_filter=ScanAndReadInteTest.TestLanceJavaCompatibility
```

Configure that build with `PAIMON_ENABLE_LANCE=ON` and `PAIMON_BUILD_TESTS=ON`.

### Type limitations

TIME32 is omitted because the C++ end-to-end table path does not support it.
Java's Lance validator rejects local-zoned timestamps, MAP, MULTISET, VARIANT
and BLOB at this revision. A nullable ROW declaration is accepted, but an actual
null parent does not survive Java's Lance round trip. Reproduce separately:

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$WORK:$CP" \
  GenerateLanceTable "$WORK/null-parent" null-parent
```

This fails with `id=2 expected null parent=true actual null parent=false` and
`AssertionError: Java round trip mismatch for id 2`. The positive fixture keeps
both struct parents valid, including row 2 where both children are null. This
does not claim support for null struct parents or Python-generated tables.

## Raw format fixture

`java_lance_0_39_0.lance` was written with Apache Paimon Java commit
`63ffd403648e1cf9848e32bb29a398c1b3b16ab1` and `com.lancedb:lance-core:0.39.0`.

Its schema is `id INT, name STRING, score DOUBLE`, and its rows are:

```text
1, alpha, 1.25
2, beta, -2.5
3, null, null
4, delta, 4.75
```

The fixture verifies that the C++ reader remains compatible with files written by the Java
implementation. The reciprocal C++-write/Java-read check is run during compatibility validation.

SHA-256 checksum:

```text
b8aae32740ad048c926380d976f6beed5f44326d4553d692b4d81d8ed17d8334  java_lance_0_39_0.lance
```
