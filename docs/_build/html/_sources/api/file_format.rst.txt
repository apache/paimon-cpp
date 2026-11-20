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
File Format
===========

.. _cpp-api-file-format:

Interface
=========

.. doxygenclass:: paimon::FileFormat
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FormatWriter
   :members:
   :undoc-members:

.. doxygenclass:: paimon::WriterBuilder
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FileBatchReader
   :members:
   :undoc-members:

.. doxygenclass:: paimon::ReaderBuilder
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FileFormatFactory
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FormatStatsExtractor
   :members:
   :undoc-members:

.. doxygenclass:: paimon::WrittenFileStatsProvider
   :members:
   :undoc-members:


.. _cpp-api-vortex-read-options:

Vortex Read Options
===================

Pass Vortex read options through ``ReadContextBuilder::AddOption`` at runtime.
Read options override table options for each key; a key absent from both uses
its default. Changes take effect when a new reader is created and do not change
existing readers.

Predicate Pushdown
------------------

``vortex.read.in-pushdown.max-values`` sets the maximum list length eligible for
native IN/NOT IN pushdown. It accepts a decimal integer from ``0`` to
``4294967295`` (``UINT32_MAX``), with a default of ``32``. Negative, malformed or
out-of-range values are rejected. The limit applies independently to each IN or
NOT IN predicate and counts the original list entries, including duplicates.

A positive value permits lists of up to that length, subject to type and value
safety checks. ``0`` disables only IN/NOT IN pushdown. A list exceeding the limit
is not pushed down and is never truncated. Vortex also skips native pushdown for
empty IN/NOT IN lists. ``TableRead`` rejects IN/NOT IN predicates containing a
``NULL`` literal during predicate validation.

An ``AND`` expression can push down its eligible children. An ``OR`` expression
can be pushed down when each direct child yields a native candidate expression.
For example, Vortex can use ``A OR C`` as a candidate filter for
``(A AND B) OR C`` when ``A`` and ``C`` are eligible and ``B`` requires residual
filtering.

Increasing the membership limit does not bypass these fallback conditions:

- ``FLOAT``/``DOUBLE`` comparisons and IN/NOT IN are not pushed down, including
  predicates with finite constants, because native NaN ordering differs from
  Paimon's comparisons.
- Timestamp constants must convert to the file's timestamp unit without precision
  loss or integer overflow and remain within the supported native temporal range.
  This checks the predicate constant; stored values are not truncated.
- Timestamp value predicates require an empty stored timezone or the exact
  timezone label ``UTC``. Other stored labels fall back, independently of the
  timezone requested by the reader.
- Decimal constants must normalize to the file's precision and scale without
  rounding or overflow.

``IS NULL`` and ``IS NOT NULL`` remain eligible for floating-point and timestamp
fields. If the file contains a top-level user column named ``_row_idx``, however,
the reader disables all native predicate filtering, even when that column is not
projected. Projection and bitmap selection remain available.

The Vortex adapter does not deduplicate lists or compress consecutive integer
values into ranges. The query engine is responsible for these optimizations.
To allow up to 128 entries per IN/NOT IN predicate:

.. code-block:: cpp

    read_context_builder.AddOption("vortex.read.in-pushdown.max-values", "128");

Native pushdown may return candidate rows requiring further filtering. For exact
results, retain the full residual predicate: enable ``EnablePredicateFilter(true)``
with ``TableRead``, or apply equivalent filtering in the caller. This also applies
when an IN/NOT IN predicate exceeds the configured limit.

I/O Coalescing
--------------

``vortex.read.io-coalesce.max-gap`` and ``vortex.read.io-coalesce.max-size``
accept decimal byte counts without unit suffixes, up to
``9223372036854775807`` (``INT64_MAX``). Their defaults are ``1048576``
(1 MiB) and ``16777216`` (16 MiB), respectively. ``max-gap`` must be non-negative,
``max-size`` must be positive, and ``max-gap`` must not exceed ``max-size``.
The limits are validated when the reader opens its data source.

``max-size`` is a threshold for merging requested ranges. An individual original
request may exceed it, so it does not impose a hard read-size or memory limit.
For example, configure a 64 KiB gap and a 4 MiB merging threshold:

.. code-block:: cpp

    read_context_builder.AddOption("vortex.read.io-coalesce.max-gap", "65536");
    read_context_builder.AddOption("vortex.read.io-coalesce.max-size", "4194304");

Vortex Timestamp Storage
========================

For newly written ``TIMESTAMP_LTZ`` fields, the writer normalizes non-empty
timezone metadata to ``UTC``, including timestamps nested in supported struct,
list and fixed-size-list types. Epoch values and timestamp units are unchanged;
the represented instants are not shifted. Timestamp fields without a timezone
retain empty timezone metadata. This changes the observable stored file schema;
existing files are not rewritten. Value predicates on existing timestamp fields
with other stored timezone labels follow the fallback rules above.
