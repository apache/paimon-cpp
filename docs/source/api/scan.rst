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
the current table's bucket count. Files with an older schema ID or a nonpositive
total bucket count retain the existing filtering behavior.

This inference prunes data files at the manifest-entry level. It does not enable
manifest min/max-bucket skipping or the bucket-specific live-manifest-entry cache,
which require an explicit bucket filter.

Decimal literals are rescaled to the bucket field's type only when the conversion
is exact. NaN literals and decimals that cannot be represented exactly disable
inferred bucket pruning.

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
