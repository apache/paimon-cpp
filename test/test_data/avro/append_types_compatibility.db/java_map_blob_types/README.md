Schema: id INT NOT NULL, f_map_blob MAP<STRING NOT NULL, BLOB>.

Options:

bucket = -1
file.format = avro
blob-field = f_map_blob
data-evolution.enabled = true
row-tracking.enabled = true

The table is unpartitioned and has no primary key.

Add: (1, {"left":"blob-map-left","right":null})
Add: (2, null)
Add: (3, {})

This writer stores map BLOB values in Paimon's external BLOB representation.
