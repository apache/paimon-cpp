Table: dedicated_vector_java_rolling
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
target-file-size = 128 mb
vector.target-file-size = 1 kb
write.batch-size = 100
read.batch-size = 127

Msgs:
snapshot-1 APPEND
Add 3001 rows in ascending id order, from 0 through 3000:
(id, [id, id + 1, id + 2], "row-<id>")
VECTOR elements are FLOAT values. embedding is NULL at ids
0, 999, 1000, 1999, 2000, 2999, and 3000.

Files (inclusive row id ranges):
Parquet main: [0, 3000], 3001 rows, writeCols=[id, tag]
Vortex VECTOR: [0, 999], 1000 rows, writeCols=[embedding]
Vortex VECTOR: [1000, 1999], 1000 rows, writeCols=[embedding]
Vortex VECTOR: [2000, 2999], 1000 rows, writeCols=[embedding]
Vortex VECTOR: [3000, 3000], 1 row, writeCols=[embedding]

Generated using the Paimon Java batch write API (BatchWriteBuilder /
BatchTableWrite / BatchTableCommit). Create the table with the schema and
options above, write all 3001 rows, then prepare and commit once.
write-only prevents compaction. The small write batch flushes Vortex buffers
before Java checks rolling every 1000 rows. The VECTOR files roll while the
main file stays open; vector.target-file-size is a target, not a physical size
limit. NULL vectors cover both sides of file boundaries and the final tail;
read.batch-size=127 also exercises reads across those boundaries. Java readback
verified every row and file range before copying the table files unchanged.
