Table: append_ltz_java_compat
Writer: Paimon Java 2.2-SNAPSHOT at commit 535a5c944f854cb8b68196d44677e576cf22120b
Lance version: lance-core 0.39.0
Arrow Java version: 15.0.0
JVM default time zone: GMT-10:00

Schema:
id INT
f_ltz_0 TIMESTAMP(0) WITH LOCAL TIME ZONE
f_ltz_3 TIMESTAMP(3) WITH LOCAL TIME ZONE
f_ltz_6 TIMESTAMP(6) WITH LOCAL TIME ZONE
f_ltz_9 TIMESTAMP(9) WITH LOCAL TIME ZONE
f_ltz_array ARRAY<TIMESTAMP(6) WITH LOCAL TIME ZONE>
f_ltz_struct ROW<number INT, ts TIMESTAMP(3) WITH LOCAL TIME ZONE>

Options:
bucket = -1
file.format = lance
metadata.stats-mode = none

This unpartitioned append-only table has no primary key. The Java catalog and
BatchTableWrite/BatchTableCommit APIs wrote two rows per commit into two Lance data files,
with schema, Avro manifests and two snapshots. All four rows were read back and verified in Java.
Values below follow schema order; timestamps are UTC.

Add: (1, 1970-01-01 00:00:01, 1970-01-01 00:00:01.123, 1970-01-01 00:00:01.123456, 1970-01-01 00:00:01.123456789, [1970-01-01 00:00:01.123456, NULL, 1969-12-31 23:59:59.999999], (1, 1970-01-01 00:00:01.123))
Add: (2, NULL, NULL, NULL, NULL, NULL, (NULL, NULL))

Commit - snapshot 1 (2 rows)

Add: (10, 1969-12-31 00:00:00, 1969-12-31 23:59:59.999, 1969-12-31 23:59:59.999999, 1969-12-31 23:59:59.999999999, [], (10, 1969-12-31 23:59:59.999))
Add: (11, 2024-10-04 12:34:56, 2024-10-04 12:34:56.123, 2024-10-04 12:34:56.123456, 2024-10-04 12:34:56.123456789, [2024-10-04 12:34:56.123456], (11, 2024-10-04 12:34:56.123))

Commit - snapshot 2 (4 rows total)

The writer ran with a default time zone containing ':', which Lance 0.39 cannot parse as a
time zone id. Java writes every local-zoned timestamp, including the ARRAY element and the
ROW child, to Lance as a timestamp with the "UTC" time zone, leaving the instants unchanged.
ROW parents are always valid, including id=2 where both children are NULL, because Lance 0.39
does not preserve null ROW parents.
