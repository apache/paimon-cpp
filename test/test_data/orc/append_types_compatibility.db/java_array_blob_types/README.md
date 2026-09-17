Schema: id INT NOT NULL, f_array_blob ARRAY<BLOB>.

Options:

bucket = -1
file.format = orc
orc.timestamp-ltz.legacy.type = false
blob-field = f_array_blob
data-evolution.enabled = true
row-tracking.enabled = true

The table is unpartitioned and has no primary key.

Add: (1, ["blob-array-left", null, "blob-array-right"])
Add: (2, null)
Add: (3, [])

Paimon C++ is expected to reject this table while ARRAY<BLOB> is unsupported.
