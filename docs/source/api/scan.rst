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

Cooperative batch planning cancellation
=======================================

Attach a query-owned ``std::shared_ptr<paimon::ScanCancellationToken>`` using
``ScanContextBuilder::WithCancellationToken``. Another thread may call ``Cancel()``
while the single planning caller runs ``TableScan::Create`` or ``CreatePlan()``.
The scan retains the token after its context is consumed. Cancellation is one-way;
use a new token for a new query. A recognized request returns the typed
``Status::Cancelled`` rather than a partial or empty successful plan. An operation
that already returned a native error retains that error. Hint failures remain best
effort while a query is live; if the failed hint read also requests cancellation,
that original error terminates planning instead of starting a retry.

This opt-in path supports managed append-only tables with fixed positive or
unaware buckets and Avro manifest metadata. Data files may use another supported
format. Batch startup may be latest, latest-full, from-snapshot, or
from-snapshot-full; the latter two require a numeric snapshot ID. Primary-key,
data-evolution, deletion-vector, streaming, real-time, system-table, format-table,
timestamp/tag-startup, supplied global-index results, and active embedded-index
evaluation paths are refused rather than silently ignoring cancellation. Active
embedded indexes are detected while reading matching manifest entries; disabling
file-index reading avoids that evaluation path.

Manifest work runs synchronously on the caller, without submitting to or shutting
down its executor. Both shared manifest caches are bypassed for this query; existing
cache contents and other queries are unchanged. Calls without a token keep their
ordinary planning behavior. Cancellation does not interrupt blocking filesystem
calls, one decoder operation, or CPU sorting. It provides neither a latency bound
nor a planning-memory quota, and does not cancel subsequent data-file reading.
Rebuild the library and paired native consumers together: the added context state
and optional constructor argument preserve source usage, not binary ABI.
Arrow import/export error paths also release acquired roots for calls without a token.

Interface
=========

.. doxygenclass:: paimon::TableScan
   :members:
   :undoc-members:

.. doxygenclass:: paimon::ScanContextBuilder
   :members:
   :undoc-members:

.. doxygenclass:: paimon::ScanContext
   :members:
   :undoc-members:

.. doxygenclass:: paimon::ScanCancellationToken
   :members:

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
