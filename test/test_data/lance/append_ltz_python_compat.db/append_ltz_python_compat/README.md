Table: append_ltz_python_compat
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
f_ltz_0 TIMESTAMP_LTZ(0)
f_ltz_3 TIMESTAMP_LTZ(3)
f_ltz_6 TIMESTAMP_LTZ(6)
f_ltz_9 TIMESTAMP_LTZ(9)
f_ltz_array ARRAY<TIMESTAMP_LTZ(6)>
f_ltz_struct ROW<number INT, ts TIMESTAMP_LTZ(3)>

Options:
bucket = -1
file.format = lance
manifest.format = avro
metadata.stats-mode = none

Values below follow schema order; timestamps are UTC. They match append_ltz_java_compat.

Add: (1, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, [1970-01-01 00:00:01.123456, NULL, 1969-12-31 23:59:59.999999], (1, 1970-01-01 00:00:01.123))
Add: (2, NULL, NULL, NULL, NULL, NULL, (NULL, NULL))

Commit - snapshot 1 (2 rows)

Add: (10, 1969-12-31 00:00:00, 1969-12-31 23:59:59.999, 1969-12-31 23:59:59.999999, 1969-12-31 23:59:59.999999999, [], (10, 1969-12-31 23:59:59.999))
Add: (11, 2024-10-04 12:34:56, 2024-10-04 12:34:56.123, 2024-10-04 12:34:56.123456, 2024-10-04 12:34:56.123456789, [2024-10-04 12:34:56.123456], (11, 2024-10-04 12:34:56.123))

Commit - snapshot 2 (4 rows total)

Paimon Python maps a pyarrow timestamp with a time zone to TIMESTAMP_LTZ and writes every
TIMESTAMP_LTZ, including the ARRAY element and the ROW child, to Lance as a timestamp with
the "UTC" time zone.
ROW parents are always valid: this fixture does not claim null-parent ROW compatibility.
