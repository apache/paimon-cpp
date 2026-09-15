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
       paimon::TableScanResources::Create(table_path, file_system, "main", paimon::GetDefaultPool()));

   // Repeat for each query, reusing resources.
   paimon::ScanContextBuilder builder(table_path);
   builder.WithTableResources(resources).SetPredicate(predicate);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ScanContext> context, builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableScan> scan,
                          paimon::TableScan::Create(std::move(context)));

Include ``paimon/table/source/table_scan_resources.h`` to create the resource object.
The resources supply the file system and the default branch; conflicting explicit settings
are rejected. The physical table path must match, including when scanning its ``$ro`` or
``$audit_log`` system table. Format tables and global system tables do not use these resources.
``Finish()`` resets the builder's resource setting, like ``WithCache()`` and ``WithExecutor()``.

Every new scan still checks for the latest schema ID. Previously loaded schema versions, Arrow
schemas, partition and primary-key field information, and statistics evolution objects are
reused. Existing scans retain their original schema. Snapshot selection, filters, streaming
progress, executors and scan metrics remain independent. ``SetTableSchema()`` keeps its existing
behavior: on main it bypasses the shared schema cache; on other branches it is ignored.

The resources can be shared by concurrent scans when the supplied file system and metadata
memory pool support concurrent use. The resource object retains its metadata pool independently
of each scan's memory pool. Cached schema versions are retained for the resource object's lifetime;
there is no automatic eviction or background refresh. The caller must recreate the resources after
fast-forward, deleting and recreating a table or branch, or changing file system access configuration.
Fast-forward can replace schema contents under existing IDs, so discovering the latest ID does not
refresh a cached schema. After fast-forward completes, create a new ``TableScanResources`` and use it
for subsequent scans; existing resources and scans retain their cached metadata. This follows
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
