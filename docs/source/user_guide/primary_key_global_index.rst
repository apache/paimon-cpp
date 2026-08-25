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

Primary Key Global Index
========================

Paimon 2.0 primary-key tables support *source-backed* global scalar indexes
(``pk-btree`` / ``pk-bitmap``). Unlike the Data Evolution global indexes described in
:doc:`global_index`, which address rows by a table-wide row id, a source-backed payload
covers the complete active source set of one positive data level of one bucket, and its
results are group ordinals that are localized back to per-file physical row positions.

paimon-cpp supports the BTree build, maintenance, and read lifecycle of this protocol for
fixed-bucket primary-key tables. Compaction automatically builds one immutable payload for
each indexed field and positive data level. Ordinary batch scans evaluate the indexed part
of the predicate against validated payload groups and narrow covered files to indexed
splits carrying file-local row ranges. No dedicated build or query API is required.

Table requirements
------------------

The definitions follow the Java table options:

- ``'pk-btree.index.columns' = 'price'`` with optional
  ``'fields.price.pk-btree.index.options' = '{"block-size":"64 kb"}'``
- fixed bucket (``bucket > 0``) for automatic C++ maintenance; Java-compatible postpone
  bucket schemas remain readable, but the C++ postpone writer does not build payloads
- ``'deletion-vectors.enabled' = 'true'`` and ``'deletion-vectors.merge-on-read' = 'false'``

Semantics
---------

- A payload is only used when it provably covers the current active source set of its
  data level: exactly one payload per level, source file names / order / row counts
  identical to the active COMPACT files of that level, matching index type and field id,
  and a row range of exactly ``[0, total source rows - 1]``. Anything else is treated as
  uncovered and scanned normally.
- Index construction reads every physical source row without applying deletion vectors,
  orders source files by file name, and externally sorts ``(value, group row id)``. Missing,
  duplicate, malformed, or stale payloads cause their complete level to be rebuilt. Data
  files and the corresponding index ADD / DELETE entries are committed in the same
  snapshot.
- The builder uses the existing write-buffer and spill settings. A write context needs a
  temporary directory when a level exceeds the in-memory write buffer and spill is enabled.
- If payload construction fails, the data-file transition is still committed and the affected
  level remains uncovered; scans fall back to the data files and a later maintenance attempt can
  rebuild the payload. Structural commit-increment errors are still rejected.
- Snapshot expiration retains payloads referenced by the snapshots in its retention set and
  current-branch live tags, and removes retired payloads before their index manifests, including
  payloads on an external index path. Orphan cleanup covers table-local index manifests and
  payloads; it does not enumerate a potentially shared global-index external path.
- ``AND`` predicates narrow with any safely evaluable indexed child; ``OR`` predicates
  only use the index when every branch is evaluable. Files whose evaluation fails, whose
  positions are out of range, or whose result needs more than 4096 ranges fall back to a
  normal scan individually.
- Indexed splits keep their deletion files aligned with the data file; the reader still
  applies deletion vectors and the complete original predicate, so index results never
  change visibility semantics.
- ``'global-index.enabled' = 'false'`` disables the planner.

Current scope
-------------

- The BTree payload reader is wired up. ``pk-bitmap`` (and vector / full-text)
  definitions are recognized for validation, but their evaluation conservatively falls
  back to a normal scan until their dedicated payload readers are supported.
- The read path targets the Java release-2.0.0 layout and scan semantics (source metadata
  v1, ``GlobalIndexMeta`` with ``_SOURCE_META``, commit message v12). Source-file names
  currently use the existing C++ length-prefixed UTF-8 streams; ASCII and non-null BMP
  names are compatible with Java ``writeUTF``, while complete modified UTF-8 support for
  supplementary code points will be handled by a shared stream-level change.
- Automatic maintenance is synchronous during prepare-commit. Java's asynchronous build
  scheduling, retries, fairness metrics, and manual rebuild actions are not part of the C++
  API. Realtime and postpone-bucket writers do not build source-backed payloads.
