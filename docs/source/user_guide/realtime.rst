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

Real-time Write and Read
========================

Real-time mode makes writes visible to queries in the same process before a
snapshot commit. A shared ``RealtimeContext`` owns the uncommitted stores;
queries combine an immutable view of those stores with committed table files.
Readers without that context see only committed files.

Real-time storage is pluggable through the ``RealtimeStore`` interface. The
default implementation uses Apache Arrow to buffer rows and serve reads.
Applications can provide their own ``RealtimeStoreFactory`` through
``RealtimeContext::Create(factory)`` to customize storage and retrieval while
keeping Paimon's real-time write, read, and commit lifecycle.

This is not a streaming scan, a write-ahead log, or a cross-process cache.
Durability still comes from snapshot commits. Upstream must retain replayable
input and provide monotonically increasing offsets for each partition-bucket.
Real-time operations currently support only the main branch. Use a separate
context for each table, and coordinate a single active writer for each
partition-bucket.

Modes and Capabilities
----------------------

The table schema and options select the mode; there is no separate
``realtime.mode`` option.

.. list-table::
   :header-rows: 1
   :widths: 18 25 27 30

   * - Mode
     - Selection
     - Uncommitted state
     - Query semantics
   * - Append
     - No primary key; no real-time deduplicate key
     - Arrow batches in write order
     - Concatenate committed files and the visible memory tail
   * - PK MOR
     - Primary key; ``deletion-vectors.enabled=false``
     - Batches sorted by primary key and sequence number
     - Merge disk runs, sealed batches, and building batches by primary key
   * - PK DV
     - Primary key; ``deletion-vectors.enabled=true``
     - The same sorted PK store as PK MOR
     - Apply committed DVs, then merge disk and memory by primary key
   * - Append deduplicate
     - Append table with ``realtime.deduplicate-key-fields`` set and
       ``deletion-vectors.enabled=true``
     - Mark superseded rows in an in-memory DV during writes; persist DV files
       at commit
     - Full predicate pushdown; apply the in-memory DV to disk and memory reads
       without MOR

All four modes support read-before-prepare, sealed-but-uncommitted reads,
partition-bucket isolation, offset-aware commit and recovery, immutable read
views, projection, and optional sealed-segment spill with the default store.
The shared read pipeline also handles nested projection, selected MAP keys,
and supported VARIANT access over physical store fields. Predicate pruning
and exact residual filtering follow the mode-specific rules below.

Append accepts ``INSERT`` only. PK and append deduplicate also accept
``UPDATE_AFTER``, ``DELETE``, and ``UPDATE_BEFORE``. PK supports composite
primary keys, but its real-time merge engine is currently limited to
``merge-engine=deduplicate``. Append deduplicate currently accepts exactly one
non-null, non-nested key field and is experimental: its persisted index format
may change.

PK DV and append deduplicate are different designs. PK DV retains primary-key
merge semantics; append deduplicate uses a persisted stable row offset and
does not turn the table into a primary-key table.

Configuration
-------------

Common Options
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 33 15 52

   * - Option
     - Default
     - Meaning
   * - ``realtime.enabled``
     - ``false``
     - Enables real-time operations. The builders must also receive a shared
       ``RealtimeContext`` to select process-local writing and union reading.
   * - ``realtime.spill-enabled``
     - ``false``
     - The default store synchronously spills frozen Arrow data to temporary
       IPC files during explicit ``Seal``. Prepare does not trigger spill.
   * - ``realtime.read-view-ttl``
     - ``5 min``
     - Positive lifetime of a scan's read-view ticket before reader creation.
   * - ``realtime.store.stats-mode``
     - ``none``
     - ``none`` or ``full``. Full mode collects per-batch min/max and null counts
       for conservative predicate pruning, including retained spill metadata.
   * - ``realtime.deduplicate-key-fields``
     - Unset
     - Selects append deduplicate mode. Although expressed as a comma-separated
       list, currently exactly one field is accepted.
   * - ``bucket``
     - Must be set
     - Real-time mode requires fixed buckets: ``bucket > 0``.
   * - ``read.batch-size``
     - ``1024``
     - Maximum raw batch size returned by the default store's query readers;
       not the number of visible rows after bitmap filtering.
   * - ``spill-compression``
     - ``zstd``
     - Compression used by temporary spill files.
   * - ``spill-compression.zstd-level``
     - ``1``
     - Compression level when using Zstandard.

Set ``WriteContextBuilder::WithTempDirectory()`` when enabling spill. The
default factory rejects spill without a non-empty temporary directory or a
file system. Spill is explicit at seal boundaries; the normal write-buffer
memory manager does not automatically bound real-time store memory.

Setting ``realtime.enabled=true`` alone does not attach an in-process store.
This permits a separate ordinary writer to compact committed files without
joining the real-time context. It does not permit ordinary row writes into an
append deduplicate table.

Mode-specific Setup
~~~~~~~~~~~~~~~~~~~

For append mode, create a table without primary keys and use:

.. code-block:: text

   bucket = 4
   realtime.enabled = true

For PK MOR, define the table's primary keys and use:

.. code-block:: text

   bucket = 4
   realtime.enabled = true
   merge-engine = deduplicate
   deletion-vectors.enabled = false

For PK DV, define primary keys and enable DVs instead:

.. code-block:: text

   bucket = 4
   realtime.enabled = true
   merge-engine = deduplicate
   deletion-vectors.enabled = true

Both PK modes use the same real-time writer and store. PK DV uses the normal
PK deletion-vector and compaction infrastructure; it does not require the
append deduplicate ``.offset`` sidecar or an offset bitmap file index.

For append deduplicate, create a table with no primary keys. Its schema must
contain ``_REALTIME_OFFSET BIGINT NOT NULL`` and a non-null key, for example
``id BIGINT NOT NULL``. Keep all versions of a key in the same partition and
bucket, for example with ``bucket-key=id`` on an unpartitioned table:

.. code-block:: text

   bucket = 4
   bucket-key = id
   realtime.enabled = true
   realtime.deduplicate-key-fields = id
   deletion-vectors.enabled = true
   deletion-vectors.bitmap64 = false
   file-index.read.enabled = true
   file-index.bitmap.columns = _REALTIME_OFFSET

The two indexes serve different purposes:

* The ``.offset`` sidecar looks up existing offsets by deduplicate key during
  writes, so superseded rows can be marked in the in-memory DV.
* ``file-index.bitmap.columns=_REALTIME_OFFSET`` builds an offset index used
  during queries to quickly map in-memory DV offsets to exact file row
  positions and filter those rows.

The offset column must have an index supporting exact BIGINT ``IN`` and
``NOT_IN`` row-position matching. Supported choices are ``bitmap``, ``bsi``,
and ``range-bitmap``; for example, replace the bitmap declaration above with
``file-index.bsi.columns=_REALTIME_OFFSET`` or
``file-index.range-bitmap.columns=_REALTIME_OFFSET``. Indexes may be embedded
in file metadata or stored in an external file-index file. Approximate indexes
such as Bloom filters are insufficient.

Index capabilities are checked when creating a table and loading its schema,
not for each file during reads. Applications must keep the index configuration
consistent with existing files and ensure that those files have compatible
exact indexes.

The bitmap64 prohibition concerns persisted file DVs; logical deleted offsets
are still represented by ``RoaringBitmap64`` in memory.

The ``.offset`` sidecar lookup uses ``cache-page-size``,
``lookup.cache-max-memory-size``, and
``lookup.cache.high-priority-pool-ratio``. These configure its lookup cache,
not a total memory limit for the real-time store.

Input Schema and Offsets
------------------------

Offsets are application-assigned, non-negative ``int64`` values scoped to one
logical partition-bucket. They must be strictly increasing within each input
batch and across subsequent batches. Gaps are allowed; reuse or backward
movement is rejected. ``INT64_MAX`` cannot be an input offset because the
exclusive end must also fit in ``int64``.

.. list-table::
   :header-rows: 1
   :widths: 20 45 35

   * - Mode
     - ``FileStoreWrite::Write`` input layout
     - Persisted offset
   * - Append
     - ``[_REALTIME_OFFSET, table fields...]``
     - Internal prefix removed before writing data files
   * - PK MOR / PK DV
     - ``[_REALTIME_OFFSET, table fields...]``
     - Internal prefix removed before writing data files
   * - Append deduplicate
     - Table schema in its declared field order, including ``_REALTIME_OFFSET``
     - Normal table column retained in data files and compaction output

Do not add the internal offset prefix to the formal table schema in ordinary
append or PK mode. In append deduplicate mode, do not add a second prefix: the
offset is already a formal field and may occupy any declared position.

Construct Arrow batches in the required layout and set their partition and
bucket with ``RecordBatchBuilder::SetPartition()`` and ``SetBucket()``. Routing
and offset assignment belong to the application. Deduplication never crosses
partition-bucket boundaries.

For PK writes, the framework adds ``_SEQUENCE_NUMBER`` and ``_VALUE_KIND`` and
sorts each stored batch by primary key, then sequence number. Input offsets
are validated before sorting. The sequence number, not the numeric offset,
resolves PK versions; application-defined ``sequence.field`` is not supported
by real-time PK mode.

An ``OffsetRange`` is half-open: ``[begin, end)``. It is an event-progress
envelope, not a row count. For example, input offsets ``10, 12, 15`` cover
``[10, 16)``. Deletes consume offsets even when they produce no data-file row,
so a delete-only prepare can advance progress with an empty data increment.

API Lifecycle
-------------

Use the same ``RealtimeContext`` for writing, planning, and reading. Enable
streaming mode on the writer for progress-aware checkpoints; real-time union
queries themselves use batch scans.

The snippets below run inside a ``Status`` or ``Result``-returning function
using the ``paimon`` namespace. They assume an existing native table,
``table_path``, a stable ``commit_user``, the mode-specific ``options`` map,
and an input ``batch`` built as described above.

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeContext> realtime,
                          RealtimeContext::Create());

   WriteContextBuilder write_builder(table_path, commit_user);
   write_builder.SetOptions(options)
       .WithStreamingMode(true)
       .WithRealtimeContext(realtime);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context,
                          write_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreWrite> writer,
                          FileStoreWrite::Create(std::move(write_context)));
   PAIMON_RETURN_NOT_OK(writer->Write(std::move(batch)));

To query before preparing or committing:

.. code-block:: cpp

   ScanContextBuilder scan_builder(table_path);
   scan_builder.SetOptions(options).WithRealtimeContext(realtime);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> scan_context,
                          scan_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableScan> scan,
                          TableScan::Create(std::move(scan_context)));
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Plan> plan, scan->CreatePlan());

   ReadContextBuilder read_builder(table_path);
   read_builder.SetOptions(options).WithRealtimeContext(realtime);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> read_context,
                          read_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> read,
                          TableRead::Create(std::move(read_context)));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                          read->CreateReader(plan->Splits()));
   // Consume NextBatch() or NextBatchWithBitmap(), then Close() the reader.

Apply the same query predicate to scan and read. If exact residual filtering
must happen inside the library, call
``ReadContextBuilder::EnablePredicateFilter(true)``. File and batch statistics
are conservative pruning, not a substitute for evaluating the user predicate.

Checkpoint with progress-aware APIs:

.. code-block:: cpp

   const int64_t checkpoint_id = 1;
   PAIMON_ASSIGN_OR_RAISE(std::vector<RealtimeCommitProgress> progress,
                          writer->PrepareCommitWithProgress(checkpoint_id));

   CommitContextBuilder commit_builder(table_path, commit_user);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<CommitContext> commit_context,
                          commit_builder.SetOptions(options).Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreCommit> commit,
                          FileStoreCommit::Create(std::move(commit_context)));
   PAIMON_ASSIGN_OR_RAISE(
       int64_t snapshot_id,
       commit->CommitWithProgress(progress, checkpoint_id, /*watermark=*/std::nullopt));
   PAIMON_RETURN_NOT_OK(writer->RefreshCommittedSnapshot(snapshot_id));
   PAIMON_RETURN_NOT_OK(writer->Close());

The operations have different responsibilities:

1. ``Write`` publishes rows to the process-local building store.
2. Optional ``Seal`` freezes a segment, opens a new building segment, and may
   spill. It does not create a committed snapshot or prepare data-file messages.
3. ``PrepareCommitWithProgress`` freezes the current building segment without
   spilling and writes it together with earlier sealed segments to table data
   files, returning their offset ranges together with commit messages. It does
   not publish a snapshot or reclaim the real-time data.
4. ``CommitWithProgress`` atomically publishes files, indexes, and the updated
   partition-bucket offset map. The snapshot's ``realtime.offsets`` property
   references a metadata file containing the durable progress.
5. ``RefreshCommittedSnapshot`` loads the committed progress and reclaims
   fully covered sealed segments. Deduplicate refresh also replaces the active
   file-key lookup atomically with memory/DV advancement.

Keep every prepared-but-uncommitted ``RealtimeCommitProgress`` entry. If a later
checkpoint subsumes earlier prepares, pass the complete outstanding prefix to
commit, not just the latest prepare result. Offset gaps make omitted prepares
indistinguishable from valid sparse input, and skipping one can cause data loss.

``CommitMessage`` serialization does not contain the progress range or the
deduplicate ``deleted_offsets`` bitmap. Preserve the complete progress when
coordinating checkpoints. Snapshot conflicts are retried internally against
the latest snapshot; commit identifiers must follow the streaming commit
protocol. With a version-managed catalog, configure the writer and committer
with the same ``WithCatalog()`` catalog and identifier.

Read Views and Consistency
--------------------------

Planning first pins immutable memory views and then plans committed disk data.
For each active partition-bucket it captures two exclusive bounds:

.. code-block:: text

   disk:   progress covered by the selected snapshot
   memory: [committed_end_offset, memory_end_offset)

Filtering memory by these bounds prevents duplicate reads after a commit and
excludes writes after the captured memory view. Existing views remain valid
across later writes, seals, spill publication, and refresh-driven reclamation.

Ordinary append scans keep earlier disk splits independently schedulable and
attach the memory view to a tail split. PK and append deduplicate group a
partition-bucket's disk splits with its memory view so that merge or pending
deletion semantics cover the whole bucket.

A real-time split carries an opaque read-view ticket. Resolve it with the same
context in the same process. Successful reader creation consumes the ticket;
failed creation retains it until its TTL expires. Once a reader is created,
the reader pins its view independently of ticket expiry. Replan if a ticket
expires, or if a previously consumed split needs to be read again.

An explicit snapshot id is accepted only if its progress is not behind the
context's reclaimed progress for relevant active stores. It does not restore
an old in-memory state and is not unrestricted historical time travel. For a
disk-only historical query, do not attach the real-time context.

Implementation by Mode
----------------------

Append
~~~~~~

``RealtimeAppendOnlyWriter`` validates offsets and writes to an
``APPEND_ONLY`` store. The default ``ArrowRealtimeStore`` retains building
batches and immutable sealed segments in write order. Query readers project
the requested fields, optionally prune stored batches using statistics, and
apply the visible offset range before removing the hidden offset column.

Preparing reads sealed segments through the ordinary ``AppendOnlyWriter``,
removing the internal offset prefix before writing table files. Queries do not
need key lookup or version merge; disk and memory readers are concatenated.

Primary Key MOR
~~~~~~~~~~~~~~~

``RealtimePrimaryKeyWriter`` materializes PK transport batches with sequence
number, row kind, and offset. Each sorted batch is an independent merge input;
multiple stored batches do not form a single globally sorted stream.

Query construction merges sorted disk sections and the memory readers, applies
deduplicate merge semantics, and drops final delete records. It collects all
disk splits belonging to the active partition-bucket, rather than merging
memory against only one disk split. Preparing similarly merges the sealed
streams before handing them to ``MergeTreeWriter`` to write normal PK files.

Only primary-key predicates are safe to push into unresolved versions. A
predicate on a value column must be evaluated after version merge: filtering
out the newest value first could incorrectly expose an older matching value.

Primary Key DV
~~~~~~~~~~~~~~

PK DV uses the same real-time PK write and sequence-number logic. During a
union read, committed file DVs remove already-invalid physical rows, but L0
runs and memory may still contain duplicate keys and delete records. The final
reader therefore still performs a primary-key merge across disk and memory.

For real-time scans, L0 is retained instead of being skipped as in the normal
DV batch scan. Predicate handling distinguishes storage levels:

* High-level files (``level > 0``) can use the full predicate for statistics and
  file-index pruning under committed DV semantics.
* L0 and memory receive only primary-key predicates before merge.
* The full residual predicate is applied after merge when explicitly enabled
  in the read context.

Enabling DVs does not make uncommitted PK data append-only and does not make
arbitrary value-predicate pushdown safe for L0 or memory.

Append Deduplicate / Offset DV
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This mode uses two independent lookup directions:

.. code-block:: text

   key -> logical offset(s)                  memory key indexes / .offset sidecars
   logical offset -> physical row positions  exact _REALTIME_OFFSET file index

For each write, ``RealtimeDeduplicateWriter`` looks up existing keys across
committed files, sealed segments, and the building segment. Matching old offsets
are marked deleted, and within-batch duplicates are resolved in row order.
Add rows are appended to the store; delete events update only the key/DV state,
without persisting a delete row.

Key indexes and DV bitmaps use copy-on-write snapshots, so later writes do not
change the state held by existing lookups, read views, or seal checkpoints.

Memory reads use the read view's DV to exclude rows whose offsets are marked
deleted.

Disk reads apply both committed file DVs and pending offset deletions. Pending
deletions become per-file ``_REALTIME_OFFSET NOT IN (...)`` predicates, trimmed
using reliable min/max statistics or left untrimmed when statistics are missing
or ambiguous. The exact file index enforces these predicates independently of
optional residual filtering. The offset is omitted from the final result unless
the user selected it.

Prepare retains each seal's logical deletion checkpoint and writes surviving
add rows with their original offsets. Each output file gets a ``.offset``
sidecar mapping key fingerprints to offsets, separate from the exact offset
file index.

Each commit attempt resolves deleted offsets to row positions in the latest
snapshot's files and the prepared new files, then merges them into the committed
file DVs. Repeating this resolution on retries handles file changes caused by
conflicting commits or compaction.

Spill and Memory Accounting
---------------------------

The default store freezes the current building data and opens a new building
segment under a short lock. Explicit ``Seal`` writes the frozen segment to an
Arrow IPC spill file outside the write lock when spill is enabled. Prepare
freezes without spilling and writes directly to table data files, reading any
earlier spilled segments as needed. There is no background spill worker: the
seal/prepare thread waits for its I/O, while another thread can write to the
new building segment.

Deduplicate uses two-phase seal to freeze data, key index, offset range, and DV
checkpoint at the same boundary. The frozen segment remains queryable and its
keys remain discoverable while spill is in progress. Only after writing and
closing the file successfully is its stored representation replaced. A spill
failure keeps the in-memory segment and cleans up the incomplete file, but is
still terminal for the owning writer/context.

Old views can retain the memory representation after spill or reclamation;
new views use the published representation. Spill files are deleted when the
store and all remaining handles/readers have released them. They are temporary
storage, not a recoverable log after process restart. Deduplicate key indexes
and DVs remain in memory; full-mode statistics also remain resident after spill.

``RealtimeContext::GetMetrics()`` and real-time writer metrics expose:

* ``realtimeBuildingMemoryBytes``, ``realtimeSealedMemoryBytes``,
  ``realtimeTotalMemoryBytes``;
* ``realtimeBuildingRowCount``, ``realtimeSealedRowCount``,
  ``realtimeTotalRowCount``.

These are current store accounting, not process RSS or a visible-row count.
Rows include stored versions hidden by merge/DV filtering. Memory counts include
Arrow buffers and retained statistics; deduplicate also counts retained key-map
buffers, row-offset bitmaps, and distinct deletion checkpoints.

Recovery, Schema Changes, and Compaction
----------------------------------------

Recovery and Failure Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Read durable progress with ``RealtimeSnapshotOffsets::ReadAllOffsets()`` or
``ReadOffset()`` from a committed snapshot. Replay starting at the returned
offset. If ``ReadOffset()`` returns ``-1`` for a partition-bucket, use the
upstream's initial position.

A new context restores committed progress, but uncommitted data requires
upstream replay. Reusing a healthy context preserves its uncommitted tail;
retain any outstanding prepared progress when replacing the writer.

After a write/flush, seal, prepare, or terminal progress-commit failure, discard
the writer and context and recover from the latest committed progress. Closing
with unprepared data also invalidates the context. Prepare alone is not durable:
retain prepared progress until commit, even after the writer closes.

Schema Changes
~~~~~~~~~~~~~~

Reusing a live store requires the same physical schema and mode. After a schema
change, recreate the context and writer and replay uncommitted input.
``data-evolution.enabled`` is not supported in real-time mode.

Deduplicate reads and lookups support historical file schemas, which must remain
available together with their key options. Key renames and field reordering are
supported when key field ID, type, and non-nullability stay compatible.
``_REALTIME_OFFSET`` must retain its name, field ID, and non-null int64 type.

Compaction and Destructive Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The real-time writer does not compact files automatically or support explicit
``Compact``. Use a separate ordinary compaction writer without
``WithRealtimeContext()``, then refresh to the resulting snapshot.

Append deduplicate compaction preserves surviving offsets, rebuilds indexes,
and remaps the latest deletions to the output files. External rewrites must not
drop the required offsets or indexes.

Overwrite, truncate, partition drop, and rollback may reset progress and are not
automatically coordinated with active real-time writers. Stop affected writers,
recreate their contexts, and reset the upstream recovery position before resuming.

Extension Points and Current Limits
-----------------------------------

``RealtimeContext::Create(factory)`` accepts a custom ``RealtimeStoreFactory``.
Custom stores manage buffering, read views, and spill while following the
``RealtimeStore`` contracts. Paimon manages table files, indexes, merge semantics,
and commits. Append deduplicate also keeps key indexes and DVs in the framework;
custom delegates need ``RealtimeStoreTwoPhaseSeal`` to enable its spill.

Current restrictions include:

* Native tables on the main branch with fixed positive bucket counts and full
  write schemas only.
* Process-local batch union scans only. Streaming scans, limit pushdown,
  global-index result splits, vector/full-text search, and count-only real-time
  readers are unsupported.
* PK mode supports the deduplicate merge engine with default delete handling,
  ascending sequence order, and ``changelog-producer=none``. Custom
  ``sequence.field``, floating-point keys, PK global indexes, lookup without DVs,
  and read-optimized union scans are unsupported.
* Append deduplicate requires a single non-null scalar key, a persisted offset
  column, exact offset file-index reads, and non-bitmap64 DVs. Existing ordinary
  append files are not migrated automatically.
* Spill covers Arrow data, not key indexes or DVs, and provides neither a hard
  memory limit nor crash recovery.

Code Map
--------

* ``include/paimon/realtime/``: public context, store, progress, and recovery APIs.
* ``src/paimon/core/realtime/``: mode-specific writers, default Arrow store,
  deduplicate wrapper/indexes, schema layout, and memory-read pipeline.
* ``src/paimon/core/table/source/realtime_table_scan.cpp``: memory pinning,
  snapshot-progress validation, and partition-bucket split grouping.
* ``src/paimon/core/table/source/append_only_table_read.cpp``: append union reads
  and per-file pending offset-DV predicates.
* ``src/paimon/core/operation/merge_file_split_read.cpp``: PK disk/memory merge
  and level-aware predicate pushdown.
* ``src/paimon/core/operation/commit/realtime_*``: durable progress, logical-DV
  materialization, commit retry, and compaction DV remapping.
* ``test/inte/realtime_write_inte_test.cpp``: lifecycle and mode-specific
  integration coverage; store/reader unit tests live next to their implementation.
