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
   Automatic append-only compaction is only available for fixed-bucket mode
   (``bucket > 0``); it never runs under dynamic bucketing (``bucket = -1``)
   or for tables with blob columns. Unaware-bucket append tables
   (``bucket = -1``) are compacted through the dedicated entry point
   ``AppendCompactCoordinator::Run`` instead; for a data-evolution table that
   entry point plans across evolved field groups, described in
   :ref:`data-evolution-compaction`.

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


.. _data-evolution-compaction:

Data-Evolution Table Compaction
-------------------------------
For a data-evolution table (``data-evolution.enabled = true``),
``AppendCompactCoordinator::Run`` plans compaction across evolved field groups
instead of running the plain append rewrite. Files are grouped by row id range:
files covering the exact same rows form one evolved field group, holding
different versions or different subsets of the table columns produced by
partial-column updates. Each compact task merges the field groups of one
contiguous row id run — the newest version of every column wins — and rewrites
them into a single normal file holding every non-dedicated column.

Row ids are never changed by this compaction: ``_ROW_ID`` values stay stable.
The rewritten file carries the merged ``[min, max]`` sequence number range of
its inputs, so per-row ``_SEQUENCE_NUMBER`` values, which derive from the
file-level maximum, may rise to the group's maximum after the compaction.
Blob files are dedicated storage and are not rewritten; their row ranges
remain covered by the compacted data file. Tables holding vector-store files
are rejected until Paimon C++ gains a ``VECTOR`` schema type that lets the
rewrite exclude their columns; Paimon Java instead compacts the normal files
of such tables and leaves the vector files in place.

The normal-file bin-packing rules follow Paimon Java's
``DataEvolutionCompactCoordinator``:

- A file group's weight is ``sum(max(file_size, source.split.open-file-cost))``;
  a bin of groups becomes a task once its weight exceeds ``target-file-size``.
- A single file group heavier than the target is compacted on its own —
  provided it holds at least ``compaction.min.file-num`` files (merging its
  files is still worthwhile) — and is never packed with neighbors.
- A row id gap always cuts the current bin, so tasks stay contiguous.
- A bin becomes a task only when it holds at least ``compaction.min.file-num``
  files.

Deletion Vectors
~~~~~~~~~~~~~~~~
A data-evolution table may enable ``deletion-vectors.enabled`` and still be
compacted. The rewrite never applies the deletions: every input row is written
to the output file and the row ids are preserved, so the deletions stay
logical. Because a deletion vector is keyed by the *anchor file* of its row
range group, the compaction re-keys the vectors of the groups it replaced onto
the file that now holds those rows, and commits the rewritten deletion-vector
index in the same snapshot as the data files. Deleted rows therefore stay
deleted across a compaction, and ``_ROW_ID`` references remain valid.

Only the index files a move actually touches are rewritten, as in Paimon Java.
Which index file owns which vector is taken from the index metadata, so
nothing is deserialized to find out, and an index file no move touches is
never opened however many vectors the partition holds. A touched index file is
replaced whole: the vectors that stay have to move into the file taking over,
so they are read through a single opened stream and rewritten, while one left
with no vector at all is deleted outright. New index files roll at
``deletion-vector.index-file.target-size`` rather than being written as a
single file. Both vector kinds are supported — a table with
``deletion-vectors.bitmap64 = true`` has its moved vectors rebuilt as 64 bit
vectors, since the two kinds serialize differently and cannot be merged into
one another.

One rule differs from Paimon Java: a replaced file's vector is taken away even
when it deletes nothing, where Java leaves such an entry in place. Keeping it
would key a vector by a data file the commit removes, and the commit check
below — which can only see the recorded cardinality, absent altogether in index
metadata written by older Paimon versions — would then have to guess whether
that entry was harmless.

The commit is checked for exactly this migration, against the deletion vectors
the *latest* snapshot holds rather than the one the round was planned against.
Each dropped file is attributed to the rewritten file that took over its rows,
in the same partition and bucket, so the accounting is per compact group rather
than per partition. Four shapes are refused:

- A data file the commit drops whose vector it does not remove. This is what a
  concurrent commit re-keying that vector looks like: the vector belongs to an
  index file the round never saw, and would outlive its data file.
- A data file the commit drops whose rows none of the files it writes covers,
  so its deletions have nowhere to move to.
- A rewritten file given no vector, or one deleting a different number of rows
  than the groups it absorbed. The row ranges of distinct groups are disjoint,
  so the merged vector deletes exactly as many rows as they did together.
- A data file the commit keeps whose vector it removes without writing back one
  deleting as many rows, which happens when an index file also held vectors for
  files outside the compacted groups.

Each would make deleted rows visible again, so the commit fails and the round
is planned afresh instead. Only the index entries that can decide these four
questions are kept from the snapshot's index manifest, so a round holds what
its own work touches rather than the table's whole deletion-vector index.

All four are decided from index metadata alone — which index file holds a
vector and how many rows it deletes — so no vector is read during the commit.
The check therefore catches a vector that went missing or changed size, not one
that deletes the right *number* of wrong rows; positions are moved one by one
by the rewrite itself, and reading every vector back to re-prove that is the
cost this check is built to avoid.

Deletion vectors on a *plain* append table are still not compacted: that
rewrite reorders rows and has no equivalent migration.

Materializing Deletions
~~~~~~~~~~~~~~~~~~~~~~~
Everything above keeps the deletions *logical*. The heavy alternative is to
apply them physically, which
``AppendCompactCoordinator::MaterializeDeletionVectors`` does — the counterpart
of Paimon Java's ``materialize_deletion_vectors`` procedure. It rewrites every
row range that carries deletions with the deleted rows dropped, so the surviving
rows get **new** row ids assigned by the commit, and it drops the vectors rather
than moving them.

That is a much more disruptive operation than the default rewrite, which is why
neither Paimon Java nor Paimon C++ does it automatically:

- Every column of a rewritten range has to be rewritten together, because a
  column left in place would still be addressed by the old row ids.
- ``_ROW_ID`` values change, so anything holding a reference to one — another
  engine's bookkeeping, a materialized view, an external index — has to look it
  up again.
- Global indexes of the touched partitions are invalidated and deleted in the
  same commit by ``DataEvolutionCompactGlobalIndexDropper``, scanned at the
  newest snapshot. The deletions are re-derived from the latest index manifest
  on every commit attempt, so an index another writer commits while this runs is
  either deleted by the attempt that sees it or forces a retry that does. They
  have to be rebuilt afterwards.

Only ranges that actually carry deletions are rewritten; a table with no
deletion vectors is a no-op. Unlike the default rewrite, the output is split by
the table's ordinary rolling rules rather than pinned to one file, since the
input range is not preserved anyway.

The work is split into rounds over the row id space exactly as ``RunAndCommit``
splits compaction, with the same soft target and the same manifest-gap cuts, and
each round commits on its own — so a large table neither holds every live file's
metadata at once nor lands in a single commit. The call returns how many rounds
committed. Rows a round renumbers land above every planned window, so a later
round never re-plans what an earlier one rewrote.

Because the commit assigns new row ids to whole ranges, it also has to fail when
another writer touched those rows while it ran: the commit checks the snapshots
since the one the round planned against and rejects any file added over a range
it rewrites, whatever columns that file wrote. An update landing on a range this
run does not touch commits normally.

Compared with Paimon Java, two things differ. A row range covered by a
dedicated blob or vector-store file is **rejected**: Paimon C++ does not rewrite
those, so their row ids cannot be reassigned in step with the rows they belong
to, and refusing is the only safe answer — Java rewrites blob files instead, and
rejects vector-store files as this does. And the planning is simpler: Java packs
runs into bins weighted by their estimated size after the deletions are applied,
while Paimon C++ emits one task per contiguous row id run, which streams through
the reader at bounded memory either way.

Bounded Rounds and Committing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``AppendCompactCoordinator::Run`` plans the whole snapshot in one pass and
returns the commit messages for the caller to commit. For a large
data-evolution table that means holding every live file's metadata at once and
landing everything in a single commit.

``AppendCompactCoordinator::RunAndCommit`` avoids both. It splits the table's
row id space into rounds targeting ``candidate_files_per_round`` files
(default 100,000, matching Paimon Java's candidate batch size) and commits each
round on its own, so only one round's metadata is live at a time. The count is
a soft target rather than a bound: because a cut needs a gap between two
manifests' coverage, a single large manifest — or manifests whose row id
ranges overlap — keeps all of its files in one round however many they are.
The split is
derived from the row id ranges and file counts already recorded on the
snapshot's data manifests, so it costs no manifest-entry reads; a cut is only
placed where one manifest's coverage ends before the next one begins, which
keeps every file inside exactly one round. Each round re-reads the snapshot so
it observes the rounds committed before it, but not most of the manifest files:
an ADD-bearing manifest carrying usable row id statistics is pruned by the
round's row id window and so is read for exactly one round. A manifest holding
only DELETE entries is left out of the split — it contributes no candidate
file — and if it also lacks row id statistics the window cannot place it, so
every round reads it; its stray entries cancel nothing in the rounds they do
not belong to.
When a partition filter is given, manifests it rules out are left out of the
split as well, so compacting one partition does not produce rounds covering
only other partitions' rows. A manifest without usable row id statistics
disables the split — the run logs that and falls back to a single round.
Rounds are independent — if one fails, the rounds already committed stay
committed and a later call re-plans whatever is left. A plain append table has
no row id space to split on, so it runs as a single round and only gains the
commit.

.. note::
   Auto compaction never runs on data-evolution tables, since data evolution
   requires ``bucket = -1``.

   Compared with Paimon Java, the following are not ported yet: the
   concurrent-MERGE rebase retry of the Spark integration, and the
   ``blob-compaction.enabled`` blob pack rewriting — that option has no effect
   here, blob packs are never rewritten whatever it is set to. Java also plans
   its batches by reading manifests with a projection, while Paimon C++ derives
   them from manifest-level row id statistics.

   Two further differences are intentional. The planning scan drops manifest
   statistics only when ``manifest.delete-file-drop-stats`` asks for it, while
   Paimon Java always drops them for this scan, so the DELETE entries this
   compaction commits may carry statistics Java would have left out; the plans
   themselves are identical. A table holding vector-store files is rejected
   outright rather than compacted for its normal files, as described above.

   Reading a data-evolution table with deletion vectors is described in
   :ref:`data-evolution-deletion-vectors`.

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
