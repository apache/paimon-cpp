Schema:

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
f_timestamp_0 TIMESTAMP(0)
f_timestamp_3 TIMESTAMP(3)
f_timestamp_6 TIMESTAMP(6)
f_timestamp_9 TIMESTAMP(9)
f_timestamp_ltz_0 TIMESTAMP_LTZ(0)
f_timestamp_ltz_3 TIMESTAMP_LTZ(3)
f_timestamp_ltz_6 TIMESTAMP_LTZ(6)
f_timestamp_ltz_9 TIMESTAMP_LTZ(9)
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

Options:

bucket = -1
file.format = parquet
blob-field = f_blob,f_blob_descriptor
blob-descriptor-field = f_blob_descriptor
data-evolution.enabled = true
row-tracking.enabled = true

The table has no partition key or primary key. Values below are listed as [id=1, id=2, id=3].
Timestamps and local-zoned timestamps are shown in Asia/Shanghai; binary values are hexadecimal.

id: [1, 2, 3]
f_boolean: [true, null, null]
f_tinyint: [-8, null, null]
f_smallint: [1234, null, null]
f_int: [-123456, null, null]
f_bigint: [9223372036854770000, null, null]
f_float: [1.25, null, null]
f_double: [-12345.6789, null, null]
f_char: ["char", null, ""]
f_varchar: ["varchar-中文", null, ""]
f_string: ["pypaimon 2.0.0", null, ""]
f_binary: [0x3132333435363738, null, 0x4142434445464748]
f_varbinary: [0x76617262696e6172790076616c7565, null, 0x]
f_bytes: [0x62797465730076616c7565, null, 0x]
f_blob: ["ordinary blob payload from pypaimon 2.0.0", null, ""]
f_blob_descriptor: [Blob("file:///nonexistent/pypaimon-all-types-external-blob.bin", 7, 11), null, null]
f_decimal_1_0: [9, null, null]
f_decimal_9_2: [1234567.89, null, null]
f_decimal_18_2: [1234567890123456.78, null, null]
f_decimal_19_2: [12345678901234567.89, null, null]
f_decimal_38_18: [12345678901234567890.123456789012345678, null, null]
f_decimal_38_38: [0.12345678901234567890123456789012345678, null, null]
f_date: [2024-02-29, null, null]
f_timestamp_0: [2024-02-29 12:34:56, null, null]
f_timestamp_3: [2024-02-29 12:34:56.123, null, null]
f_timestamp_6: [2024-02-29 12:34:56.123456, null, null]
f_timestamp_9: [2024-02-29 12:34:56.123456000, null, null]
f_timestamp_ltz_0: [2024-02-29 12:34:56, null, null]
f_timestamp_ltz_3: [2024-02-29 12:34:56.123, null, null]
f_timestamp_ltz_6: [2024-02-29 12:34:56.123456, null, null]
f_timestamp_ltz_9: [2024-02-29 12:34:56.123456000, null, null]
f_variant: [{"name":"variant-object","count":42,"active":true,"items":[null,1,"x"],"nested":{"decimal":12.34}}, null, null]
f_array_int: [[1, null, 3], null, []]
f_map_string_bigint: [{"one":1,"null":null}, null, {}]
f_row: [(7,"nested",123456789012345.6789,2024-02-29 12:34:56.123456000,2024-02-29 12:34:56.123456000), null, null]
f_array_array_int: [[[1,2],null,[]], null, []]
f_array_map: [[{"a":1,"b":null},null,{}], null, []]
f_map_array: [{"numbers":[1,null,3],"empty":[]}, null, {}]
f_array_row: [[("alice",99.50),null,("bob",null)], null, []]
f_map_row: [{"first":(true,2024-02-29 12:34:56.123456),"second":null}, null, {}]
f_deep_row: [([{"leaf_id":10,"leaf_variant":[1,"two",false,{"k":"v"}]},{"leaf_id":11,"leaf_variant":null}],{"language":"python","format":"parquet"}), null, null]
f_array_variant: [[{"name":"variant-object","count":42,"active":true,"items":[null,1,"x"],"nested":{"decimal":12.34}},null,[1,"two",false,{"k":"v"}]], null, []]
f_map_variant: [{"object":{"name":"variant-object","count":42,"active":true,"items":[null,1,"x"],"nested":{"decimal":12.34}},"array":[1,"two",false,{"k":"v"}]}, null, {}]
