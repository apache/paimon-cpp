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
is not pushed down and is never truncated. Other eligible predicates may still
be pushed down; an OR expression requires every branch to be convertible.

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

``max-size`` is a threshold for merging requested ranges. An individual original
request may exceed it, so it does not impose a hard read-size or memory limit.
For example, configure a 64 KiB gap and a 4 MiB merging threshold:

.. code-block:: cpp

    read_context_builder.AddOption("vortex.read.io-coalesce.max-gap", "65536");
    read_context_builder.AddOption("vortex.read.io-coalesce.max-size", "4194304");
