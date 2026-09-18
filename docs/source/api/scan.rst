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

===========
Scan
===========

.. _cpp-api-scan:

Bucket pruning
==============

For fixed-bucket append and primary-key tables, an equality predicate on every bucket key lets
the scan derive the target bucket using the table's bucket function. Other buckets
are excluded from the scan plan without requiring an explicit bucket ID from the
caller. An explicit bucket filter takes precedence. Queries that do not constrain
all bucket keys with equality, and bucket-unaware tables, keep the existing scan
behavior. Both scan types use a shared selector that computes the bucket with
each manifest entry's total bucket count, so rescaled files are not filtered using
the current table's bucket count. Historical schemas remain eligible when their ordered bucket-key field IDs,
types and bucket function match. Changes to unrelated columns do not disable
pruning. Incompatible bucket schemas and nonpositive total bucket counts retain
the existing filtering behavior.

This inference prunes data files at the manifest-entry level. Manifest min/max-bucket
skipping requires an explicit bucket filter. When the snapshot live-manifest-entry
cache is enabled, inferred scans cache candidates by bucket, current bucket count
and current schema ID. Files with other bucket counts or schema IDs remain in the
cached candidates and are filtered after lookup. This permits cache reuse without
discarding files that require a different bucket calculation or schema fallback.

Decimal literals are rescaled to the bucket field's type only when the conversion
is exact. NaN literals and decimals that cannot be represented exactly disable
inferred bucket pruning.

Sharing table metadata
======================

``TableScanResources`` retains schema metadata across scans of the same managed table and
branch. Create it once with a file system and pass it to each ``ScanContextBuilder``:

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(
       std::shared_ptr<paimon::TableScanResources> resources,
       paimon::TableScanResources::Create(table_path, file_system, "main"));

   // Repeat for each query, reusing resources.
   paimon::ScanContextBuilder builder(table_path);
   builder.WithTableResources(resources).SetPredicate(predicate);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ScanContext> context, builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableScan> scan,
                          paimon::TableScan::Create(std::move(context)));

Include ``paimon/table/source/table_scan_resources.h`` to create the resource object.
At ``Finish()``, the resources supply the context's file system when none was explicitly set.
An explicit ``WithFileSystem()`` must point to the same instance as the resources' file system;
a different instance is rejected regardless of builder call order. The resources also supply
the default branch; conflicting explicit branches are rejected. The physical table path must match,
including when scanning its ``$ro`` or ``$audit_log`` system table. Format tables and global system tables do not use these resources.
``Finish()`` resets the builder's resource setting, like ``WithCache()`` and ``WithExecutor()``.

Every new scan still checks for the latest schema ID. Previously loaded schema versions, Arrow
schemas, and partition and primary-key field information are reused. Existing scans retain their
original schema. Snapshot selection, filters, streaming progress, executors and
scan metrics remain independent. ``SetTableSchema()`` keeps its existing
behavior: on main it bypasses the shared schema cache; on other branches it is ignored.

Successfully loaded snapshots are cached by their full file paths, with an LRU limit of 20 entries
per resource object. The whole snapshot cache is replaced on the first access after it reaches
30 minutes of age. Reads and writes do not extend this deadline; recently added entries are
discarded along with older ones. There is no per-entry TTL or background refresh thread.
Snapshot caching is enabled by these resources; ordinary snapshot managers have no cache unless
one is explicitly supplied.
Latest and earliest snapshot discovery and existence checks still use the catalog or file system
as appropriate. Read or parse failures are not cached.

Snapshot deletion through a manager replaces its entire injected snapshot cache before and after
the deletion attempt. Explicit invalidation also discards the whole cache. In-flight loads can
finish against the old cache but cannot populate the replacement used by subsequent callers.
Historical commit lookup, timestamp searches, and retained-snapshot publication checks read files
directly so cached metadata cannot hide missing or replaced files. Other clients' deletions do not
immediately invalidate this process's cache. A cached snapshot can remain available after its
metadata file expires; a cache hit does not establish that the snapshot or its data is still readable.

The resources can be shared by concurrent scans when the supplied file system supports concurrent
use. Schema versions and schema-derived resources have no entry-count limit and are retained for
the resource object's lifetime. Eviction releases the cache's references; active scans retain
the metadata they need.
The snapshot cache limit bounds entry counts, not bytes or metadata held by active scans.
There is no background refresh.
The caller must recreate the resources after fast-forward, deleting and recreating a table or
branch, or changing file system access configuration.
Fast-forward can replace schema and snapshot contents under existing IDs; discovering the latest ID
does not refresh their cached contents. After fast-forward completes, create a new
``TableScanResources`` and use it for subsequent scans; cache hits in existing resources may still
return the old contents, and existing scans retain their original metadata. This follows
Paimon's `fast-forward cache refresh requirement
<https://paimon.apache.org/docs/1.3/maintenance/manage-branches/#fast-forward>`_.
A metadata cache does not pin snapshots or prevent their data files from expiring.

Interface
=========

.. doxygenclass:: paimon::TableScan
   :members:
   :undoc-members:

.. doxygenclass:: paimon::TableScanResources
   :members:

.. doxygenclass:: paimon::ScanContextBuilder
   :members:
   :undoc-members:

.. doxygenclass:: paimon::ScanContext
   :members:
   :undoc-members:

.. doxygenclass:: paimon::Plan
   :members:
   :undoc-members:

.. doxygenclass:: paimon::Split
   :members:
   :undoc-members:

.. doxygenclass:: paimon::DataSplit
   :members:
   :undoc-members:

.. doxygenclass:: paimon::ScanFilter
   :members:
   :undoc-members:
