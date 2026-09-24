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

Compaction
==========
Compaction is the process of merging multiple small data files into fewer, larger
files. It is a resource intensive procedure which consumes CPU time and disk IO,
so too frequent compaction may result in slower writes. However, without
compaction, the accumulation of small files degrades query performance. Tuning
compaction is therefore a trade-off between write throughput and read efficiency.

.. note::
   - There can only be one job working on the same partition's compaction,
     otherwise it will cause conflicts.
   - Paimon C++ does not support producing changelog for now.
   - Compaction is disabled when ``write-only`` is set to ``true``, or when the
     table uses dynamic bucketing (``bucket = -1``) for append-only tables.
   - For a complete list of compaction-related configurations, see the
     :ref:`Options API Reference <cpp-api-options>`.

Append-Only Table Compaction
----------------------------
In append-only table, data files are simply appended in sequence order.
Over time, many small files accumulate, which degrades read performance due to the
overhead of opening and scanning numerous files.

Append-only table compaction merges multiple small files into fewer, larger files
to improve read efficiency. The compaction is performed asynchronously and does
not block writes.

.. note::
   Append-only table compaction is only available for fixed-bucket mode
   (``bucket > 0``). Dynamic bucketing (``bucket = -1``) does not support
   compaction. Tables with blob columns also skip compaction.

.. _data-evolution-deletion-vectors-compaction:

.. note::
   A data-evolution table (``data-evolution.enabled = true``) may enable
   ``deletion-vectors.enabled``. Paimon C++ never compacts such a table: auto
   compaction never runs on it, since data evolution requires ``bucket = -1``,
   and ``AppendCompactCoordinator::Run``, the dedicated compaction entry point,
   rejects it outright rather than dropping the deletes. This matches the
   Paimon Java revision this support was ported from, which ends the compaction
   scan as soon as deletion vectors are enabled; later Java releases do compact
   such a table, and Paimon C++ has not caught up.

   Reading such a table is supported; see
   :ref:`data-evolution-deletion-vectors`.

Auto Compaction
~~~~~~~~~~~~~~~
During each flush, the writer triggers a best-effort auto compaction. The
compaction picker scans the file queue ordered by sequence number and selects a
contiguous window of files for merging when the number of candidate files reaches
the ``compaction.min.file-num`` threshold.

Full Compaction
~~~~~~~~~~~~~~~
Full compaction rewrites all eligible files in the bucket. During full
compaction:

- Files whose size is already at or above ``compaction.file-size`` (and have no
  associated deletion vectors) are skipped to avoid unnecessary rewrites.
- When deletion vectors are enabled, all files are always eligible for
  compaction regardless of size, because deletion vectors must be applied.
- When ``compaction.force-rewrite-all-files`` is ``true``, all files are
  rewritten unconditionally.
- Without deletion vectors, full compaction only proceeds when the number of
  small files exceeds the number of large files and the total file count is at
  least 3.

After compaction, if the last output file is still smaller than
``compaction.file-size``, it is placed back into the compaction queue for future
merging.

Dictionary Passthrough
~~~~~~~~~~~~~~~~~~~~~~
An append-only compaction rewrite copies rows into the new file without
inspecting any value, so a Parquet column that an input file already stores
dictionary-encoded is forwarded to the writer still encoded instead of being
expanded to one copy of the value per row and re-encoded. This saves the reader
materializing the values and the writer hashing them again; how much that is
worth depends on the column, and low-cardinality ``STRING`` columns benefit most.
Primary-key compaction merges rows and is not covered.

This is **off by default** and is enabled per table by setting
``parquet.read.enable-dictionary-passthrough`` to ``true``. It is opt-in rather
than automatic because of the output file size trade-off described below, which
depends on the data and is not a win on every table. Measure before turning it
on, and compare output file size as well as compaction time.

Note that this is a *read* option, so it applies to every read of the table and
not only to the compaction rewrite. The values are unchanged, but an eligible
column reaches the consumer as an Arrow ``DictionaryArray`` rather than one value
per row, and a consumer that reads columns through its own accessors has to
unwrap it. Only a consumer that forwards batches without inspecting values -
which is what the rewrite does - gains anything from the encoding.

Once enabled, eligibility is decided per input file: a non-nested ``STRING``
column is forwarded when its data pages are dictionary-encoded throughout every
row group of *that* file, so one input file can be read encoded while the next
one is read as ordinary values, and the writer takes both. A high-cardinality
column that started dictionary-encoded and fell back to plain encoding therefore
does not qualify, even though it still carries a dictionary page. ``BINARY`` is
not forwarded although Parquet stores it in the same physical type and
dictionary-encodes it the same way, because the value accessors cannot read a
``BINARY`` dictionary. The rewrite also overrides the option back to ``false``
when the table writes a format other than Parquet, when
``parquet.enable-dictionary`` is ``false`` because the writer would only expand
the values again, or when variant/map shredding is configured because those
writers reshape each batch against a fixed physical schema. Setting the option
therefore never makes a rewrite fail; at worst it has no effect.

When the option is vetoed, the rewrite also enforces the veto on every input
batch. A format reader can independently hand over dictionary-encoded columns
because of its own lazy-decoding setting, so the rewrite decodes those columns
before handing them to a writer that cannot accept dictionary arrays.

If a file index is configured on a forwarded column, that column alone is
materialized so the index still sees its values; the other columns stay encoded.

Trade-off
^^^^^^^^^
Passthrough changes what the rewrite costs, not what it produces. The rewritten
data is identical either way, but the output file can be **larger**.

A Parquet column chunk can carry only one dictionary. The writer keeps the first
dictionary a column presents in a row group and falls back to plain encoding for
the rest of that row group as soon as a different one arrives. Compaction merges
several input files, each with its own dictionary, and output row groups are cut
by ``parquet.block.size`` and by the writer's memory limit, which are not aligned
to input file boundaries - a boundary may coincide, but nothing arranges for it.
So a rewrite can keep the first input file's dictionary and write the rest of the
row group plain, where materializing and rebuilding would have hashed the values
into a single dictionary for that row group (up to
``parquet.dictionary.page.size``, past which the writer falls back to plain in
either case). Where that happens the output column also becomes ineligible for
passthrough on the next compaction round, since its data pages are then only
partly dictionary-encoded.

How often it happens, and what it costs on a given table, is what the
measurement above is for.

Passthrough is therefore worth enabling when the reduction in compaction CPU
matters more than output size - for example when the same dictionary recurs
across input files, when the columns are wide enough that materializing them
dominates, or when the output is short-lived. Leave it off otherwise.

Append-Only Table Compaction Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 10 10 40

   * - Option
     - Required
     - Default
     - Type
     - Description
   * - ``compaction.min.file-num``
     - No
     - 5
     - Integer
     - The minimum number of files to trigger an auto compaction for
       append-only tables.


Primary Key Table Compaction
----------------------------
Primary key tables use an LSM tree (log-structured merge-tree) for file storage.
When more and more records are written, the number of sorted runs increases.
Because querying an LSM tree requires all sorted runs to be combined, too many
sorted runs will result in poor query performance, or even out of memory.

To limit the number of sorted runs, several sorted runs are merged into one big
sorted run once in a while. Paimon currently adopts a compaction strategy similar
to RocksDB's `universal compaction
<https://github.com/facebook/rocksdb/wiki/Universal-Compaction>`_.

Primary key table compaction solves:

- Reduce Level 0 files to avoid poor query performance.
- Produce deletion vectors for MOW mode.

Full Compaction
~~~~~~~~~~~~~~~
Paimon uses Universal Compaction. By default, when there is too much incremental
data, Full Compaction will be automatically performed. You don't usually have to
worry about it.

Paimon also provides configurations that allow for regular execution of Full
Compaction:

- ``compaction.optimization-interval``: Implying how often to perform an
  optimization full compaction. This configuration is used to ensure the query
  timeliness of the read-optimized system table.
- ``compaction.total-size-threshold``: Full compaction will be constantly triggered
  when total size is smaller than this threshold.
- ``compaction.incremental-size-threshold``: Full compaction will be constantly
  triggered when incremental size is bigger than this threshold.

Lookup Compaction
~~~~~~~~~~~~~~~~~
When a primary key table is configured with ``lookup`` changelog producer or
``first-row`` merge engine or has enabled deletion vectors for MOW mode, Paimon
will use a radical compaction strategy to force compacting level 0 files to
higher levels for every compaction trigger.

Paimon also provides configurations to optimize the frequency of this
compaction:

- ``lookup-compact``: compact mode used for lookup compaction. Possible values:

  * ``radical``: will use ``ForceUpLevel0Compaction`` strategy to radically
    compact new files.
  * ``gentle``: will use ``UniversalCompaction`` strategy to gently compact new
    files.

- ``lookup-compact.max-interval``: The max interval for a forced L0 lookup
  compaction to be triggered in ``gentle`` mode. This option is only valid when
  ``lookup-compact`` mode is ``gentle``.

By configuring ``lookup-compact`` as ``gentle``, new files in L0 will not be
compacted immediately. This may greatly reduce the overall resource usage at the
expense of worse data freshness in certain cases.

Primary Key Table Compaction Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of Sorted Runs to Pause Writing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
When the number of sorted runs is small, Paimon writers will perform compaction
asynchronously in separated threads, so records can be continuously written into
the table. However, to avoid unbounded growth of sorted runs, writers will pause
writing when the number of sorted runs hits the threshold.

.. list-table::
   :header-rows: 1
   :widths: 30 10 10 10 40

   * - Option
     - Required
     - Default
     - Type
     - Description
   * - ``num-sorted-run.stop-trigger``
     - No
     - (none)
     - Integer
     - The number of sorted runs that trigger the stopping of writes. The
       default value is ``num-sorted-run.compaction-trigger + 3``.

Write stalls will become less frequent when ``num-sorted-run.stop-trigger``
becomes larger, thus improving writing performance. However, if this value
becomes too large, more memory and CPU time will be needed when querying the
table.

Number of Sorted Runs to Trigger Compaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Paimon uses LSM tree which supports a large number of updates. LSM organizes
files in several sorted runs. When querying records from an LSM tree, all sorted
runs must be combined to produce a complete view of all records.

One can easily see that too many sorted runs will result in poor query
performance. To keep the number of sorted runs in a reasonable range, Paimon
writers will automatically perform compactions. The following table property
determines the minimum number of sorted runs to trigger a compaction.

.. list-table::
   :header-rows: 1
   :widths: 30 10 10 10 40

   * - Option
     - Required
     - Default
     - Type
     - Description
   * - ``num-sorted-run.compaction-trigger``
     - No
     - 5
     - Integer
     - The sorted run number to trigger compaction. Includes level 0 files (one
       file one sorted run) and high-level runs (one level one sorted run).

Compaction will become less frequent when ``num-sorted-run.compaction-trigger``
becomes larger, thus improving writing performance. However, if this value
becomes too large, more memory and CPU time will be needed when querying the
table. This is a trade-off between writing and query performance.
