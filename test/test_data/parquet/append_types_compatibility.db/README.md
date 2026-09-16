# Parquet append type compatibility data

This database contains equivalent unpartitioned append-only tables written by three Paimon
implementations. Every table uses `bucket = -1`, `file.format = parquet`, and Avro manifests.

Writers:

- `python_*`: PyPaimon 2.0.0 with PyArrow 19.0.1.
- `rust_*`: Paimon Rust 0.4.0 at commit `ced0c86b4db76265b5b2aedcbc9909a208f2c130`,
  with parquet-rs 58.4.0.
- `java_*`: Java batch writer from `paimon-bundle-2.2-20260915.220657-5.jar`, with parquet-mr
  1.16.0.

The Python and Rust Parquet files contain `ARROW:schema` metadata. The Java files do not.

## `<writer>_types`

The main table has three rows and covers these columns:

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
f_variant VARIANT
f_array_int ARRAY<INT>
f_map_string_bigint MAP<STRING NOT NULL, BIGINT>
f_row ROW<nested_int INT, nested_string STRING, nested_decimal DECIMAL(19, 4),
          nested_timestamp TIMESTAMP(9), nested_timestamp_ltz TIMESTAMP_LTZ(9)>
f_array_array_int ARRAY<ARRAY<INT>>
f_array_map ARRAY<MAP<STRING NOT NULL, INT>>
f_map_array MAP<STRING NOT NULL, ARRAY<INT>>
f_array_row ARRAY<ROW<name STRING, score DECIMAL(9, 2)>>
f_map_row MAP<STRING NOT NULL, ROW<enabled BOOLEAN, event_time TIMESTAMP(6)>>
f_deep_row ROW<children ARRAY<ROW<leaf_id BIGINT, leaf_variant VARIANT>>,
               labels MAP<STRING NOT NULL, STRING>>
f_array_variant ARRAY<VARIANT>
f_map_variant MAP<STRING NOT NULL, VARIANT>
```

Row contents are identical for the three writers:

- `id = 1` contains representative non-null values. Numeric values include `-8`, `1234`,
  `-123456`, `9223372036854770000`, `1.25`, and `-12345.6789`. String and binary values include
  `char`, `varchar-中文`, `pypaimon 2.0.0`, `12345678`, and values containing an embedded NUL.
- Decimal values exercise precisions 1, 9, 18, 19, and 38. The date is `2024-02-29`; timestamp
  values are `2024-02-29 12:34:56[.123[456]]` at precisions 0/3/6/9.
- Container values include `[1, null, 3]`, `{"one": 1, "null": null}`, nested null elements,
  empty nested containers, rows, arrays of rows, and maps of rows.
- The VARIANT values include an object
  `{"name":"variant-object","count":42,"active":true,"items":[null,1,"x"],"nested":{"decimal":12.34}}`
  and an array `[1,"two",false,{"k":"v"}]`. `f_deep_row` contains child IDs 10 and 11 and
  labels `{"language":"python","format":"parquet"}`.
- `f_blob` contains `ordinary blob payload from pypaimon 2.0.0`. `f_blob_descriptor` is the
  inline descriptor for `file:///nonexistent/pypaimon-all-types-external-blob.bin`, offset 7,
  length 11; the URI is intentionally nonexistent.
- `id = 2` has null in every nullable column.
- `id = 3` exercises empty strings, byte arrays, BLOB, arrays, and maps; the remaining nullable
  columns are null.

## `<writer>_vector_types`

The schema contains `id INT NOT NULL` plus length-3 VECTOR columns for BOOLEAN, TINYINT,
SMALLINT, INT, BIGINT, FLOAT, and DOUBLE. It has two rows:

```text
id  boolean             tinyint     smallint             int                       bigint
1   [true,false,true]   [-1,0,1]    [-1000,0,1000]       [-100000,0,100000]        [-10000000000,0,10000000000]
2   [false,true,false]  [2,3,4]     [2000,3000,4000]     [200000,300000,400000]    [20000000000,30000000000,40000000000]

id  float                 double
1   [1.25,-2.5,3.75]      [1.125,-2.25,3.5]
2   [4.25,5.5,6.75]       [4.125,5.25,6.5]
```

## BLOB container tables

`<writer>_array_blob_types` has schema `id INT NOT NULL, f_array_blob ARRAY<BLOB>` and rows for
a populated array containing a null element, a null array, and an empty array. Paimon C++ is
expected to reject this table because it does not support `ARRAY<BLOB>`.

`<writer>_map_blob_types` has schema
`id INT NOT NULL, f_map_blob MAP<STRING NOT NULL, BLOB>` and these logical rows:

```text
(1, {"left": "blob-map-left", "right": null})
(2, null)
(3, {})
```

Python and Java store the map values in Paimon's external BLOB representation. Rust stores its
map values inline as Parquet `binary`; that representation is intentionally rejected as a Paimon
BLOB map by Java and C++.

## `<writer>_time_types`

The schema is `id INT NOT NULL` plus `TIME(0)`, `TIME(3)`, `TIME(6)`, and `TIME(9)` columns. The
three rows exercise a daytime value, nulls, and zero/empty values. Paimon C++ is expected to reject
the table while `TIME` is unsupported.

The tables are separate because VECTOR cannot be combined with data evolution, BLOB requires data
evolution, and an unsupported table-level type must not prevent compatible columns from being read.
`MULTISET` is not included because the Python PyArrow conversion cannot write it and Paimon C++
cannot parse it.
