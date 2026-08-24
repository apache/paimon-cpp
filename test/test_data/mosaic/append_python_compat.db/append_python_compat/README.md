Table: append_python_compat
Writer: Paimon Python at commit 0043a70fd88ac75dcb83a8f2da5e72ce91e22b1f
Mosaic version: 0.2.0

This table was created and committed through Paimon Python's batch table-write pipeline.

Schema:
id INT NOT NULL
f_boolean BOOLEAN
f_tinyint TINYINT
f_smallint SMALLINT
f_bigint BIGINT
f_float FLOAT
f_double DOUBLE
f_char CHAR(8)
f_varchar VARCHAR(64)
f_binary BINARY(8)
f_varbinary VARBINARY(64)
f_date DATE
f_ts_3 TIMESTAMP(3)
f_ts_6 TIMESTAMP(6)
f_ts_9 TIMESTAMP(9)
f_ltz_3 TIMESTAMP(3) WITH LOCAL TIME ZONE
f_ltz_6 TIMESTAMP(6) WITH LOCAL TIME ZONE
f_ltz_9 TIMESTAMP(9) WITH LOCAL TIME ZONE
f_decimal_1_0 DECIMAL(1, 0)
f_decimal_18_2 DECIMAL(18, 2)
f_decimal_19_2 DECIMAL(19, 2)
f_decimal_38_18 DECIMAL(38, 18)
f_array_int ARRAY<INT>
f_map_numeric MAP<TINYINT, SMALLINT>
f_map_string_bigint MAP<STRING, BIGINT>
f_array_array_int ARRAY<ARRAY<INT>>
f_array_map ARRAY<MAP<STRING, INT>>
f_map_array MAP<STRING, ARRAY<INT>>

Options:
bucket = -1
file.block-size = 1 B
file.format = mosaic
manifest.format = avro
mosaic.num-buckets = 4
mosaic.stats-columns = id,f_varchar,f_date,f_ts_3,f_ltz_9,f_decimal_18_2
target-file-size = 64 MB
write.batch-size = 2

Data is written as three row groups with two rows in each row group. Timestamp values are shown in
UTC. The second row has NULL in every nullable field.

Row group 0:
Add: (1, true, -5, -1000, 10000000001, 1.25, 10.5, "char0001", "value-1", "bin00001", 0x010203, 2024-10-04, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, 1970-01-01 00:01:01.321, 1970-01-01 00:01:01.654321, 1970-01-01 00:01:01.321654987, -3, 1.25, 12345678901234567.89, 12345678901234567890.123456789012345678, [1, 2], {0: 0, 10: 1}, {"k0": 1000, "z0": 2000}, [[1, 2], [3]], [{"nested0": 0}, {"nested10": 10}], {"array0": [0, 1]})
Add: (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)

Row group 1:
Add: (10, true, -3, -998, 10000000010, 3.25, 12.5, "char0010", "value-10", "bin00010", 0x0a0b0c, 2024-10-06, 1970-01-01 00:00:10.123, 1970-01-01 00:00:10.123456, 1970-01-01 00:00:10.123456789, 1970-01-01 00:01:10.321, 1970-01-01 00:01:10.654321, 1970-01-01 00:01:10.321654987, -1, 10.25, 12345678901234569.89, 12345678901234567892.123456789012345678, [10, 11], {2: 20, 12: 21}, {"k2": 1002, "z2": 2002}, [[10, 11], [12]], [{"nested2": 2}, {"nested12": 12}], {"array2": [2, 3]})
Add: (11, false, -2, -997, 10000000011, 4.25, 13.5, "char0011", "value-11", "bin00011", 0x0b0c0d, 2024-10-07, 1970-01-01 00:00:11.123, 1970-01-01 00:00:11.123456, 1970-01-01 00:00:11.123456789, 1970-01-01 00:01:11.321, 1970-01-01 00:01:11.654321, 1970-01-01 00:01:11.321654987, 0, 11.25, 12345678901234570.89, 12345678901234567893.123456789012345678, [11, 12], {3: 30, 13: 31}, {"k3": 1003, "z3": 2003}, [[11, 12], [13]], [{"nested3": 3}, {"nested13": 13}], {"array3": [3, 4]})

Row group 2:
Add: (20, true, -1, -996, 10000000020, 5.25, 14.5, "char0020", "value-20", "bin00020", 0x141516, 2024-10-08, 1970-01-01 00:00:20.123, 1970-01-01 00:00:20.123456, 1970-01-01 00:00:20.123456789, 1970-01-01 00:01:20.321, 1970-01-01 00:01:20.654321, 1970-01-01 00:01:20.321654987, 1, 20.25, 12345678901234571.89, 12345678901234567894.123456789012345678, [20, 21], {4: 40, 14: 41}, {"k4": 1004, "z4": 2004}, [[20, 21], [22]], [{"nested4": 4}, {"nested14": 14}], {"array4": [4, 5]})
Add: (21, false, 0, -995, 10000000021, 6.25, 15.5, "char0021", "value-21", "bin00021", 0x151617, 2024-10-09, 1970-01-01 00:00:21.123, 1970-01-01 00:00:21.123456, 1970-01-01 00:00:21.123456789, 1970-01-01 00:01:21.321, 1970-01-01 00:01:21.654321, 1970-01-01 00:01:21.321654987, 2, 21.25, 12345678901234572.89, 12345678901234567895.123456789012345678, [21, 22], {5: 50, 15: 51}, {"k5": 1005, "z5": 2005}, [[21, 22], [23]], [{"nested5": 5}, {"nested15": 15}], {"array5": [5, 6]})

Commit - snapshot 1
