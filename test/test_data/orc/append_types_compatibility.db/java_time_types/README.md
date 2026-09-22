Schema:

id INT NOT NULL
f_time_0 TIME(0)
f_time_3 TIME(3)
f_time_6 TIME(6)
f_time_9 TIME(9)

Options: bucket = -1, file.format = orc, and orc.timestamp-ltz.legacy.type = false. The table
is unpartitioned and has no primary key.

Add: (1, 12:34:56, 12:34:56.123, 12:34:56.123000, 12:34:56.123000000)
Add: (2, null, null, null, null)
Add: (3, 00:00:00, 00:00:00.000, 00:00:00.000000, 00:00:00.000000000)
