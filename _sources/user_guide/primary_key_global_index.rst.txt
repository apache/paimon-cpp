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
covers an immutable ordered source group from one positive data level of one bucket, and
its results are group ordinals that are localized back to per-file physical row positions.

paimon-cpp supports the read path of this protocol: ordinary batch scans of a
primary-key table with scalar index definitions automatically evaluate the part of the
scan predicate that touches indexed fields against the validated payload groups of the
scanned snapshot, and narrow covered files to indexed splits carrying file-local row
ranges. No dedicated query API is required.

Table requirements
------------------

The definitions follow the Java table options:

- ``'pk-btree.index.columns' = 'price'`` with optional
  ``'fields.price.pk-btree.index.options' = '{"block-size":"64 kb"}'``
- fixed bucket (``bucket > 0``) or postpone bucket mode
- ``'deletion-vectors.enabled' = 'true'`` and ``'deletion-vectors.merge-on-read' = 'false'``

Semantics
---------

- A payload retains its complete ordered source list as the group-ordinal namespace. If
  part of that list is not in the current scan because it was retired or safely pruned,
  the payload can still cover the remaining files. There is at most one accepted payload
  per data level. A payload is rejected if it has no active source at its metadata-declared
  level, an active source's row count differs, its metadata or row range is invalid, or
  another payload exists for that level. Active files without accepted coverage are scanned
  normally.
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
- ``PkSortedIndexFile::Build`` can build one payload for an ordered source group from
  value-sorted input, which supports tooling and tests; automatic build and maintenance
  during compaction is not included yet.
