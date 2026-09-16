# Java ORC append type compatibility data

These unpartitioned append-only tables were written by the Java batch API from
`paimon-bundle-2.2-20260915.220657-5.jar`. They use `bucket = -1`, `file.format = orc`, Avro
manifests, and `orc.timestamp-ltz.legacy.type = false`.

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
f_timestamp_0/3/6/9 TIMESTAMP(0/3/6/9)
f_timestamp_ltz_0/3/6/9 TIMESTAMP_LTZ(0/3/6/9)
f_array_int ARRAY<INT>
f_map_string_bigint MAP<STRING NOT NULL, BIGINT>
f_row ROW<nested_int INT, nested_string STRING, nested_decimal DECIMAL(19, 4),
          nested_timestamp TIMESTAMP(9), nested_timestamp_ltz TIMESTAMP_LTZ(9)>
f_array_array_int ARRAY<ARRAY<INT>>
f_array_map ARRAY<MAP<STRING NOT NULL, INT>>
f_map_array MAP<STRING NOT NULL, ARRAY<INT>>
f_array_row ARRAY<ROW<name STRING, score DECIMAL(9, 2)>>
f_map_row MAP<STRING NOT NULL, ROW<enabled BOOLEAN, event_time TIMESTAMP(6)>>
```

Data:

- `id = 1` contains the representative values used by the Parquet Java fixture: numeric boundary
  values; `char`, `varchar-中文`, `pypaimon 2.0.0`; embedded-NUL binary values; decimals at
  precisions 1/9/18/19/38; date `2024-02-29`; timestamps
  `2024-02-29 12:34:56[.123[456]]`; and populated nested arrays, maps, and rows.
- ORC stores `CHAR(8)` values padded to their declared width, so the first and third `f_char`
  values read as `"char    "` and eight spaces respectively.
- `f_blob` contains `ordinary blob payload from pypaimon 2.0.0`. `f_blob_descriptor` describes
  `file:///nonexistent/pypaimon-all-types-external-blob.bin`, offset 7, length 11.
- `id = 2` has null in every nullable column.
- `id = 3` contains empty strings, byte arrays, BLOB, arrays, and maps; the remaining nullable
  columns are null.

`VARIANT`, nested VARIANT fields, and VECTOR are absent because Java's ORC path does not support
them.

## `java_array_blob_types`

Schema: `id INT NOT NULL, f_array_blob ARRAY<BLOB>`.

```text
(1, ["blob-array-left", null, "blob-array-right"])
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

Schema: `id INT NOT NULL, f_time_0 TIME(0), f_time_3 TIME(3), f_time_6 TIME(6),
f_time_9 TIME(9)`.

```text
(1, 12:34:56, 12:34:56.123, 12:34:56.123000, 12:34:56.123000000)
(2, null, null, null, null)
(3, 00:00:00, 00:00:00.000, 00:00:00.000000, 00:00:00.000000000)
```

Paimon C++ is expected to reject this table while `TIME` is unsupported.
