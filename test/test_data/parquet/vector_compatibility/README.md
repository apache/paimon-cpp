# VECTOR Parquet compatibility fixtures

These files pin the two physical Arrow schemas produced by Java and Rust writers for Paimon
VECTOR columns, with and without null vectors.

- `java_vector.parquet` was copied from Apache Paimon Rust commit
  `403a2b2e9bfc4ea66cd7e633619f1460efd18bc8`, path
  `crates/paimon/testdata/pkvector/pk_vector_ivf_flat/bucket-0/data-932a1249-f7e0-4a03-8e1f-ab8c85cbb76f-0.parquet`.
  The fixture documentation records Apache Paimon Java commit `7234e4c34` and
  `PkVectorFixtureGenerator` as its source. Its VECTOR column is exposed as Arrow `list`.
- `java_vector_nullable.parquet` was generated with parquet-mr 1.15.1 (`parquet-avro`
  `AvroParquetWriter` with `parquet.avro.write-old-list-structure=false`, so the column uses the
  standard 3-level `list` / `element` layout Paimon Java writes). The rows are `(1, [1, 2, 3])`,
  `(2, null)` and `(3, [4, 5, 6])`. The file carries no `ARROW:schema` key, so the VECTOR column
  is exposed as Arrow `list`.
- `rust_vector.parquet` was generated with Apache Arrow Rust 58.4.0 using
  `FixedSizeListBuilder<Float32Builder>` and `parquet::arrow::ArrowWriter`, the same Arrow and
  Parquet representation used by Apache Paimon Rust. Its VECTOR column is exposed as Arrow
  `fixed_size_list[3]`. The rows are `(1, [1, 2, 3])`, `(2, [7, 8, 9])`, and
  `(3, [4, 5, 6])`.
- `rust_vector_nullable.parquet` was generated the same way, with the rows `(1, [1, 2, 3])`,
  `(2, null)` and `(3, [4, 5, 6])`.

A file that stores the Arrow schema, as the Rust writer does, is read back as
`fixed_size_list`. Arrow 17 cannot read a null value from such a column, because Parquet stores a
null list slot with no values while `FixedSizeListReader::AssembleArray` in
`parquet/arrow/reader.cc` requires every slot to span exactly `list_size` values. Reading
`rust_vector_nullable.parquet` therefore fails until Arrow is upgraded, which
`ParquetVectorIoTest.ReadNullableRustFixtureIsUnsupported` pins.

SHA-256 checksums:

```text
2b2325cc2266301beaa2c78ec666cb5e0ee62283049de2a7231e3c9ae07bf3ca  java_vector.parquet
42352e11daf5a291e8a8c4cfc8d0f0f6f8c9099cf7dcf24c28d9e159a29e0d8a  java_vector_nullable.parquet
b5ba47e766ad72fca9c8485aa718ad27709c1fb4d34fb3670aa35e2001cbdbb0  rust_vector.parquet
f86058b1bc6cf803003446ca0abb7923e928d455fd39a7288c6d3acdd5fd10e9  rust_vector_nullable.parquet
```
