Schema:

id INT NOT NULL
f_time_0 TIME(0)
f_time_3 TIME(3)

Options: bucket = -1, file.format = avro. The table is unpartitioned and has no primary
key.

Add: (1, 12:34:56, 12:34:56.123)
Add: (2, null, null)
Add: (3, null, null)

Paimon C++ is expected to reject this table while TIME is unsupported. Java Avro supports only
the precision-0 and precision-3 declarations used here.
