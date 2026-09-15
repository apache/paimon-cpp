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

# Paimon writer compatibility fixtures

The `append_types_compatibility.db` database contains append-only compatibility tables written by
Python, Rust, and Java Paimon. Table names use the writer as a prefix and describe the group of
types in that table; no individual table is claimed to contain every Paimon type.

The writers and Parquet implementations are:

- `python_*`: `pypaimon==2.0.0` with `pyarrow==19.0.1`. The Parquet files contain `ARROW:schema` metadata.
- `rust_*`: Paimon Rust 0.4.0 at commit `ced0c86b4db76265b5b2aedcbc9909a208f2c130`, using `parquet-rs version 58.4.0`. The Parquet files contain `ARROW:schema` metadata.
- `java_*`: the Java batch-write API from `paimon-bundle-2-ali-2.6.jar`, using `parquet-mr version 1.16.0`. The Parquet files do not contain `ARROW:schema` metadata.

## Table groups

Each writer prefix has five tables with the same declared schema and intended logical values:

- `<writer>_types`: 43 columns and 3 rows covering integral and floating types, BOOLEAN, CHAR/VARCHAR/STRING, BINARY/VARBINARY/BYTES, BLOB, DATE, TIMESTAMP and TIMESTAMP_LTZ at precision 0/3/6/9, multiple DECIMAL precisions, VARIANT, ARRAY, MAP, ROW, and deeply nested combinations. Row 2 is null in every nullable column and row 3 exercises empty values.
- `<writer>_vector_types`: VECTOR length 3 for BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, and DOUBLE.
- `<writer>_array_blob_types`: Paimon C++ does not currently support ARRAY&lt;BLOB&gt;.
- `<writer>_map_blob_types`: MAP&lt;STRING, BLOB&gt;, including non-null, null, and empty map
  values. Python and Java store these values in standard separate BLOB files, which Paimon C++
  can read. Rust stores the raw values inline as Parquet `binary`; this is not the Paimon BLOB
  descriptor representation, cannot be read as BLOB by Java, and is intentionally rejected by
  Paimon C++.
- `<writer>_time_types`: TIME declarations at precision 0/3/6/9. Paimon C++ currently rejects `TIME` while parsing the table schema.

The tables are separated because Paimon C++ does not allow VECTOR in a data-evolution table, BLOB requires data evolution, and a schema-level incompatibility must not prevent compatible columns from being tested.

`MULTISET` is absent because the Python batch writer cannot produce it through its PyArrow conversion and Paimon C++ rejects it while parsing the table schema. Paimon Rust 0.4.0 supports `MULTISET` and represents it as an Arrow map from each element to its `INT` count, but it cannot be included in an equivalent three-writer fixture. Consequently, this fixture is a compatibility matrix, not an assertion that every type supported by every Paimon implementation can be represented by one table.

The `f_blob_descriptor` value intentionally points at a nonexistent external URI. Read it with `blob-as-descriptor=true`; it exercises the inline descriptor and Parquet `ARROW:schema` path rather than external blob fetching.

Both PyArrow and parquet-rs persist the original Arrow type in `ARROW:schema`, but the stored types
differ for `f_blob_descriptor`. The Python fixture records the physical Parquet `binary` column as
Arrow `large_binary`, while the Rust fixture records it as Arrow `binary`. The compatibility test
verifies that Paimon C++ can read both representations and preserve the descriptor value.
