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

Table: append_java_compat
Writer: Paimon Java 2.1-SNAPSHOT at commit 63ffd403648e1cf9848e32bb29a398c1b3b16ab1
Lance version: lance-core 0.39.0
Arrow Java version: 15.0.0

Schema:
id INT
f_boolean BOOLEAN
f_tinyint TINYINT
f_smallint SMALLINT
f_bigint BIGINT
f_float FLOAT
f_double DOUBLE
f_char CHAR(8)
f_string STRING
f_binary BINARY(4)
f_varbinary BYTES
f_date DATE
f_ts_0 TIMESTAMP(0)
f_ts_3 TIMESTAMP(3)
f_ts_6 TIMESTAMP(6)
f_ts_9 TIMESTAMP(9)
f_decimal_1_0 DECIMAL(1, 0)
f_decimal_18_2 DECIMAL(18, 2)
f_decimal_19_2 DECIMAL(19, 2)
f_decimal_38_18 DECIMAL(38, 18)
f_array_int ARRAY<INT>
f_array_array_int ARRAY<ARRAY<INT>>
f_struct ROW<number INT, label STRING> NOT NULL
f_nullable_struct ROW<number INT, label STRING>
f_vector VECTOR<FLOAT, 3>

Options:
bucket = -1
fields.id.stats-mode = full
file.format = lance
metadata.stats-mode = none

This unpartitioned append-only table has no primary key. The Java catalog and
BatchTableWrite/BatchTableCommit APIs wrote two rows per commit into two Lance data files,
with schema, Avro manifests and two snapshots. All four rows were read back and verified in Java.
Values below follow schema order; timestamps are UTC and binary values are hexadecimal.

Add: (1, true, -5, -1000, 10000000001, 1.25, -2.5, "char0001", "value-1", 0x62696e31, 0x0001027f, 1969-12-31, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, -3, 1.25, 12345678901234567.89, 12345678901234567890.123456789012345678, [1, NULL, -1], [[1, NULL], NULL, []], (1, "required"), (1, "nullable"), [1.0, -1.5, 0.25])
Add: (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, (NULL, NULL), (NULL, NULL), NULL)

Commit - snapshot 1 (2 rows)

Add: (10, false, -5, -1000, 10000000010, 1.25, -2.5, "char0001", "", 0x62696e31, 0x0001027f, 2024-10-04, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, -3, 1.25, 12345678901234567.89, 12345678901234567890.123456789012345678, [], [[10, NULL], NULL, []], (10, "required"), (10, "nullable"), [10.0, -1.5, 0.25])
Add: (11, true, -5, -1000, 10000000011, 1.25, -2.5, "char0001", "value-11", 0x62696e31, 0x0001027f, 2024-10-04, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, -3, 1.25, 12345678901234567.89, 12345678901234567890.123456789012345678, [11, NULL, -11], [[11, NULL], NULL, []], (11, "required"), (11, "nullable"), [11.0, -1.5, 0.25])

Commit - snapshot 2 (4 rows total)

ScanAndReadInteTest.TestLanceJavaCompatibility checks all values, reordered projection and
opt-in exact predicate filtering with batch size 1. The default scan retains both files;
the test does not assume Lance min/max pruning.

At this Java revision, nullable ROW declarations are accepted but actual null ROW parents
do not survive the Lance round trip (a separate probe read id=2's null parent as non-null).
Both struct parents here are valid, including id=2 where their children are NULL.
TIME32 is omitted because the C++ table path does not support it; Java's Lance validator
rejects local-zoned timestamps, MAP, MULTISET, VARIANT and BLOB. This fixture does not
claim null-parent ROW support or Python compatibility.
