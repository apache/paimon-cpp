.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

================
Benchmark Usage
================

Paimon C++ provides Google Benchmark based cases at two levels:

``paimon-read-write-benchmark``
   Table-level cases for append-table write/read and primary-key table write/MOR
   read paths.

``paimon-parquet-format-benchmark``
   Format-level cases that drive the Parquet writer and reader directly, without
   catalog lookup, split planning, merge/sort or commit.

Benchmarks are disabled by default.

Build
=====

Enable benchmarks when configuring CMake::

   cmake -S . -B build -DPAIMON_BUILD_BENCHMARKS=ON
   cmake --build build --target paimon-read-write-benchmark
   cmake --build build --target paimon-parquet-format-benchmark

Run all benchmark cases through CTest::

   cmake --build build --target benchmark

Table-level Custom Options
==========================

``paimon-read-write-benchmark`` accepts Google Benchmark options plus the Paimon
specific options below:

``--paimon_source_data_file=<path>``
   Source data file used to build benchmark data. Currently Parquet source files
   are supported.

``--paimon_source_table_path=<path>``
   Read directly from an existing table path for ``BM_Read`` and ``BM_MOR_Read``.
   When set, the source loading and pre-write stage are skipped.

``--paimon_pk_columns=<col1,col2,...>``
   Primary key columns for ``BM_PK_Write`` and ``BM_MOR_Read``. These cases
   explicitly use ``bucket=1`` because benchmark batches are written to bucket 0.

``--paimon_option=<key1>:<value1>;<key2>:<value2>``
   Repeatable table options passed through to Paimon. The default table file
   format is ``parquet``. Benchmark output supports ``parquet`` and, when
   built with ``PAIMON_ENABLE_ORC=ON``, ``orc``; use
   ``--paimon_option file.format:parquet`` or
   ``--paimon_option file.format:orc`` to select one. For ``BM_PK_Write`` and
   ``BM_MOR_Read``, ``bucket`` is forced to ``1``.

Examples
========

Append table write::

   paimon-read-write-benchmark \
       --paimon_source_data_file /path/data.parquet \
       --benchmark_filter=BM_Write

Append table read with four prefetch workers::

   paimon-read-write-benchmark \
       --paimon_source_data_file /path/data.parquet \
       --benchmark_filter=BM_Read/4

Primary-key table write::

   paimon-read-write-benchmark \
       --paimon_source_data_file /path/data.parquet \
       --paimon_pk_columns=id \
       --benchmark_filter=BM_PK_Write

MOR read from an existing table::

   paimon-read-write-benchmark \
       --paimon_source_table_path /path/table \
       --paimon_pk_columns=id \
       --benchmark_filter=BM_MOR_Read/4

Parquet Format Benchmark
========================

``paimon-parquet-format-benchmark`` takes only Google Benchmark options. It
generates its own data and writes it to a temporary directory, so it needs no
source file or table.

Two things shape how the results should be read:

- Every axis is swept on its own rather than as a combined matrix, so each
  case answers one question and a change can be attributed to it.
- Writes go through the local FileSystem into a temporary directory, so
  absolute numbers carry the cost of that path. Comparisons are meaningful
  only under the same environment and methodology - the same machine, build
  configuration and options - which is what makes a before/after comparison
  useful.

Writer cases (``BM_ParquetWrite_*``) cover flat primitives, ``VARCHAR`` at low /
medium / high cardinality with and without file-level dictionary encoding,
already dictionary-encoded ``VARCHAR`` / ``INTEGER`` input arrays against their
flat equivalents, ``DECIMAL`` at precision 9 / 18 / 38, nested ``STRUCT`` /
``LIST`` / ``VECTOR`` / ``MAP``, null density from 0 to 100 percent, rows per
``AddBatch`` call, column count at a fixed row count, row group size, the
writer memory threshold that triggers a byte-based row-group flush, and the
codecs Parquet accepts - ``none``, ``snappy``, ``gzip``, ``brotli``, ``zstd``,
``lz4_raw`` and ``lz4_hadoop``. Note that ``lz4`` is deliberately not among
them: it resolves to Arrow's ``LZ4_FRAME``, which
``parquet::IsCodecSupported`` rejects.

The two dictionary axes are different questions. ``BM_ParquetWrite_String`` and
``BM_ParquetWrite_StringNoDictionary`` vary whether the *file* is dictionary
encoded; ``BM_ParquetWrite_Dictionary*`` vary whether the *input array* already
is, which is what decides whether Arrow can pass indices through to Parquet or
has to materialize them first.

Three of those cases form one comparison, at the same cardinality and over the
same logical column: ``BM_ParquetWrite_String`` writes it flat,
``BM_ParquetWrite_DictionaryStringIntoStringSchema`` writes it as one dictionary
forwarded through the whole file, and
``BM_ParquetWrite_ChangingDictionaryStringIntoStringSchema`` gives every batch
its own dictionary - a rotation of the same values, with the indices shifted the
other way, so the data is unchanged and only the dictionary object differs. A
Parquet column chunk holds one dictionary, so the third case makes the writer
fall back to plain encoding partway through the row group. Reading the second
against the first is what forwarding an encoding buys; the third against the
second is what that fallback costs in time; the third against the first is the
output size a rewrite that materialized and rebuilt would have produced.

These are format-writer microbenchmarks: they measure ``AddBatch`` against a
Parquet file, not a compaction. The compaction time, CPU and peak memory of
``parquet.read.enable-dictionary-passthrough`` on a real table have to be
measured on that table - see the "Dictionary Passthrough" section of
:doc:`../user_guide/compaction`.

Reader cases (``BM_ParquetRead_*``) cover full scan, single-column projection,
predicate-filtered reads at varying selectivity with page-index filtering on and
off, skip-heavy reads driven by a strided selection bitmap, null density,
``DECIMAL`` at precision 9 / 18 / 38, ``DOUBLE``, dictionary-encoded against
plain-encoded files, rows per ``NextBatch`` call, and nested column reads.

Every case reports ``ns_per_row`` next to ``bytes_per_row`` - ``file_bytes`` for
writes, ``read_bytes`` for reads - so a change that trades CPU for size is
visible in both directions. Read cases additionally report ``rows_read``,
``batches``, and ``row_groups`` / ``row_groups_after_filter`` from the reader's
own metrics.

Compare filtered cases on ``ns_per_input_row`` and ``bytes_per_input_row``, not
``ns_per_row`` and ``bytes_per_row``. The latter pair divides by the rows a case
actually materialized, so pruning shrinks numerator and denominator together and
they can rise even as the run gets faster; the ``_input_row`` pair divides by the
rows the file holds, which every setting shares.

``row_groups_after_filter`` counts row groups only. It does not show page-level
pruning: on the ordered ``id`` column both page-index settings usually keep the
same row groups, and the page-index gain shows up in ``rows_read``,
``read_bytes`` and ``ns_per_input_row`` instead.

A case that cannot run - an unsupported codec, a schema the reader rejects -
calls ``SkipWithError`` and makes the process exit non-zero, so ``ctest -L
benchmark`` fails instead of reporting a silent skip. Read cases also assert on
the number of rows they materialized, so a fixture that stopped producing rows
fails rather than looking fast.

Because the benchmark is only compiled under ``PAIMON_BUILD_BENCHMARKS``, the
format-layer assumptions it relies on are covered separately by
``paimon-parquet-format-benchmark-test``, which builds with the normal test
suite.

Each read case scans a file that is generated once on first use and reused for
the rest of the run, so a filtered run only pays to build the fixtures its own
cases need.

All Parquet writer cases::

   paimon-parquet-format-benchmark --benchmark_filter=BM_ParquetWrite

Page-index filtering at 1% selectivity, on and off - compare ``rows_read``,
``read_bytes`` and ``ns_per_input_row`` between the two::

   paimon-parquet-format-benchmark \
       --benchmark_filter='BM_ParquetRead_Filtered/keep_pct:1/'

Read batch size sweep, repeated for a stable comparison::

   paimon-parquet-format-benchmark \
       --benchmark_filter=BM_ParquetRead_BatchSize \
       --benchmark_repetitions=5 \
       --benchmark_report_aggregates_only=true

Null density on both sides, to see what definition levels cost::

   paimon-parquet-format-benchmark --benchmark_filter='Parquet(Write|Read)_Nulls'
