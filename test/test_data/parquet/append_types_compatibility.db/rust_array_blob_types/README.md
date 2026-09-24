Schema: id INT NOT NULL, f_array_blob ARRAY<BLOB>.

Options:

bucket = -1
file.format = parquet
blob-field = f_array_blob
data-evolution.enabled = true
row-tracking.enabled = true

The table is unpartitioned and has no primary key.

Add: (1, ["blob-array-0", null, "blob-array-2"])
Add: (2, null)
Add: (3, [])

Paimon Rust stores these values inline as Parquet binary; Java and C++ intentionally reject that
representation as a Paimon BLOB array.
