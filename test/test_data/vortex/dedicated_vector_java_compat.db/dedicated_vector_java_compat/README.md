Table: dedicated_vector_java_compat
Writer: Paimon Java 2.2-SNAPSHOT (vortex-jni 0.73.0), JDK 17
Java source commit: 7d6d146bbaf33a349dc83c3caf71334bd551e3d5

Schema (all columns are nullable):
id INT
embedding VECTOR<FLOAT, 3>
tag STRING

No partition key or primary key.

Options:
file.format = parquet
vector.file.format = vortex
bucket = -1
row-tracking.enabled = true
data-evolution.enabled = true
write-only = true

Msgs:
snapshot-1 APPEND
Add: (1, [1, 2, 3], "a")
Add: (2, NULL, "b")
Add: (3, [-1, 0, 1], "c")

snapshot-2 APPEND
Add: (4, NULL, "d")
Add: (5, [7, 8, 9], "e")
Add: (6, [10, 11, 12], "f")

Each snapshot adds one Parquet main file (writeCols=[id, tag]) and one Vortex
VECTOR file (writeCols=[embedding]). Each file has three rows. The first pair
covers row ids [0, 2]; the second pair covers [3, 5].

Generated using the Paimon Java batch write API (BatchWriteBuilder /
BatchTableWrite / BatchTableCommit). Create the table with the schema and
options above, then write and commit each three-row batch separately.
write-only prevents compaction, preserving both append commits. Java readback
verified every row and file range before copying the table files unchanged.
