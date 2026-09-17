Schema:

id INT NOT NULL
f_time_0 TIME(0)
f_time_3 TIME(3)
f_time_6 TIME(6)
f_time_9 TIME(9)

Options: bucket = -1, file.format = parquet. The table is unpartitioned and has no primary
key.

Add: (1, 12:34:56, 12:34:56.123, 12:34:56.123000, 12:34:56.123000000)
Add: (2, null, null, null, null)

Paimon C++ is expected to reject this table while TIME is unsupported. Paimon's internal TIME
value is milliseconds since midnight, so the precision-6 and precision-9 declarations do not add
digits beyond .123 in this fixture.
