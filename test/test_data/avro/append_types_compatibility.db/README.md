# Java Avro append type compatibility data

These unpartitioned append-only tables were written by the Java batch API from
`paimon-bundle-2.2-20260915.220657-5.jar`. They use `bucket = -1`, `file.format = avro`, and Avro
manifests.

## `java_types`

The table has three rows and the following columns:

```text
id INT NOT NULL
f_boolean BOOLEAN
f_tinyint TINYINT
f_smallint SMALLINT
f_int INT
f_bigint BIGINT
f_float FLOAT
f_double DOUBLE
f_char CHAR(8)
f_varchar VARCHAR(64)
f_string STRING
f_binary BINARY(8)
f_varbinary VARBINARY(64)
f_bytes BYTES
f_blob BLOB
f_blob_descriptor BLOB
f_decimal_1_0 DECIMAL(1, 0)
f_decimal_9_2 DECIMAL(9, 2)
f_decimal_18_2 DECIMAL(18, 2)
f_decimal_19_2 DECIMAL(19, 2)
f_decimal_38_18 DECIMAL(38, 18)
f_decimal_38_38 DECIMAL(38, 38)
f_date DATE
f_timestamp_0/3/6 TIMESTAMP(0/3/6)
f_timestamp_ltz_0/3/6 TIMESTAMP_LTZ(0/3/6)
f_array_int ARRAY<INT>
f_map_string_bigint MAP<STRING NOT NULL, BIGINT>
f_row ROW<nested_int INT, nested_string STRING, nested_decimal DECIMAL(19, 4)>
f_array_array_int ARRAY<ARRAY<INT>>
f_array_map ARRAY<MAP<STRING NOT NULL, INT>>
f_map_array MAP<STRING NOT NULL, ARRAY<INT>>
f_array_row ARRAY<ROW<name STRING, score DECIMAL(9, 2)>>
f_map_row MAP<STRING NOT NULL, ROW<enabled BOOLEAN, event_time TIMESTAMP(6)>>
```

Data:

- `id = 1` contains representative non-null values: numeric boundary values; `char`,
  `varchar-中文`, `pypaimon 2.0.0`; fixed and variable binary values including embedded NULs;
  decimals at precisions 1/9/18/19/38; date `2024-02-29`; timestamps
  `2024-02-29 12:34:56[.123[456]]`; and populated nested arrays, maps, and rows.
- `f_blob` contains `ordinary blob payload from pypaimon 2.0.0`. `f_blob_descriptor` describes
  `file:///nonexistent/pypaimon-all-types-external-blob.bin`, offset 7, length 11.
- `id = 2` has null in every nullable column.
- `id = 3` contains empty strings, byte arrays, BLOB, arrays, and maps; the remaining nullable
  columns are null.

`VARIANT` and its nested forms are absent because Java cannot derive an Avro schema for them.
Nanosecond timestamps and the corresponding nested timestamp fields are absent because Java Avro
supports timestamp precision only through 6.

## `java_vector_types`

The schema contains `id INT NOT NULL` plus length-3 VECTOR columns for BOOLEAN, TINYINT,
SMALLINT, INT, BIGINT, FLOAT, and DOUBLE. Java writes each VECTOR as an Avro array; Paimon C++
uses the Paimon table schema to restore the fixed-size VECTOR type.

```text
id  boolean             tinyint     smallint             int                       bigint
1   [true,false,true]   [-1,0,1]    [-1000,0,1000]       [-100000,0,100000]        [-10000000000,0,10000000000]
2   [false,true,false]  [2,3,4]     [2000,3000,4000]     [200000,300000,400000]    [20000000000,30000000000,40000000000]

id  float                 double
1   [1.25,-2.5,3.75]      [1.125,-2.25,3.5]
2   [4.25,5.5,6.75]       [4.125,5.25,6.5]
```

## `java_array_blob_types`

Schema: `id INT NOT NULL, f_array_blob ARRAY<BLOB>`.

```text
(1, ["array-blob-value", null, ""])
(2, null)
(3, [])
```

Paimon C++ is expected to reject this table while `ARRAY<BLOB>` is unsupported.

## `java_map_blob_types`

Schema: `id INT NOT NULL, f_map_blob MAP<STRING NOT NULL, BLOB>`.

```text
(1, {"left": "blob-map-left", "right": null})
(2, null)
(3, {})
```

## `java_time_types`

Schema: `id INT NOT NULL, f_time_0 TIME(0), f_time_3 TIME(3)`.

```text
(1, 12:34:56, 12:34:56.123)
(2, null, null)
(3, null, null)
```

Paimon C++ is expected to reject this table while `TIME` is unsupported. `TIME(6)` and `TIME(9)`
are absent because Java Avro supports time precision only through 3.
