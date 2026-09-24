Generated with Apache Paimon Java bundle paimon-bundle-2.2-20260923.220659-14.jar
(writer commit 91ee13f838e066c399810ca8160f64f2c204fd01).

Schema:
id INT
array_payloads ARRAY<BLOB>

Options:
bucket = -1
data-evolution.enabled = true
file.format = parquet
row-tracking.enabled = true

Msgs:
snapshot-1
Commit four rows with BatchTableWrite over the full row type. BLOB values below are UTF-8 byte
payloads shown as text; "" is a zero-length BLOB:
id 1: array_payloads is ["array-alpha", null, "", "array-omega"]
id 2: array_payloads is null
id 3: array_payloads is empty
id 4: array_payloads is ["array-single"]

snapshot-2
Create BatchTableWrite with the write type projected to array_payloads. Write
BlobArrayPlaceholder.INSTANCE for row positions 0, 2, and 3, and ["array-updated"] for row
position 1. Set the first row id to 0 before committing. This adds a second sequence layer while
updating id 2 and falling back to snapshot-1 values for the other rows.

snapshot-3
Repeat a projected write with BlobArrayPlaceholder.INSTANCE for all four row positions and set the
first row id to 0. Reading this snapshot falls back to snapshot 2 for id 2 and through both newer
placeholder layers to snapshot 1 for ids 1, 3, and 4.
