Table: append_python_compat
Writer: Apache Paimon Python at commit 63ffd403648e1cf9848e32bb29a398c1b3b16ab1
Source: https://github.com/apache/paimon/tree/63ffd403648e1cf9848e32bb29a398c1b3b16ab1/paimon-python
Python version: 3.11.15
Lance version: pylance 0.39.0
Arrow Python version: 19.0.1

This unpartitioned append-only table has no primary key. The Python Catalog and batch
table-write/commit APIs wrote two rows per commit into two Lance files, with Avro manifests
and two snapshots. All values were read back in Python after each commit; reordered
projection was also verified after snapshot 2.

Schema (all fields nullable):
id INT
f_boolean BOOLEAN
f_tinyint TINYINT
f_smallint SMALLINT
f_bigint BIGINT
f_float FLOAT
f_double DOUBLE
f_string STRING
f_binary BYTES
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
f_struct ROW<number INT, label STRING>
f_vector VECTOR<FLOAT, 3>

Options:
bucket = -1
file.format = lance
manifest.format = avro
metadata.stats-mode = none

Values below follow schema order; timestamps are UTC and binary values are hexadecimal.
The second row has NULL in every field except id and the valid ROW parent with two NULL children.

Add: (1, true, -5, -1000, 10000000001, 1.25, -2.5, "value-1", 0x0001027f, 1969-12-31, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, -3, 1.25, 12345678901234567.89, 12345678901234567890.123456789012345678, [1, NULL, -1], [[1, NULL], NULL, []], (1, "row-1"), [1.0, -1.5, 0.25])
Add: (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, (NULL, NULL), NULL)

Commit - snapshot 1 (2 rows)

Add: (10, false, -5, -1000, 10000000010, 1.25, -2.5, "", 0x0001027f, 2024-10-04, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, -3, 1.25, 12345678901234567.89, 12345678901234567890.123456789012345678, [], [[10, NULL], NULL, []], (10, "row-10"), [10.0, -1.5, 0.25])
Add: (11, true, -5, -1000, 10000000011, 1.25, -2.5, "value-11", 0x0001027f, 2024-10-04, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, -3, 1.25, 12345678901234567.89, 12345678901234567890.123456789012345678, [11, NULL, -11], [[11, NULL], NULL, []], (11, "row-11"), [11.0, -1.5, 0.25])

Commit - snapshot 2 (4 rows total)

Null and empty lists, null list elements, an empty string,
binary zero bytes, null scalars, null ROW children and a null vector are included.
ROW parents are always valid: this fixture does not claim null-parent ROW compatibility.
MAP, local-zoned timestamps and other types absent from this schema are not covered.
Statistics are disabled; this fixture makes no predicate-pruning claim.
