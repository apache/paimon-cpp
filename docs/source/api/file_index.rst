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
File Index
===========

.. _cpp-api-file-index:

Full-text predicates and root TopK
=================================

``PredicateBuilder::FullTextMatch`` creates a ``LeafPredicate`` for a full-text
condition without TopK truncation. It holds a shared ``FullTextMatch`` request,
whose query uses the same JSON DSL as ``FullTextSearch``. Do not modify a request
after passing it to a predicate or read context.

Compose full-text and scalar predicates with the existing
``PredicateBuilder::And`` and ``PredicateBuilder::Or`` factories. Every field
referenced by this tree must appear in the read schema; field indices refer to
positions in that schema. For example, the following constructs
``body matches "apple" OR price < 10``:

.. code-block:: cpp

   auto request = std::make_shared<paimon::FullTextMatch>(
       "body", R"({"match":{"query":"apple"}})");
   PAIMON_ASSIGN_OR_RAISE(
       std::shared_ptr<paimon::Predicate> text,
       paimon::PredicateBuilder::FullTextMatch(0, request));
   auto price = paimon::PredicateBuilder::LessThan(
       1, "price", paimon::FieldType::INT, paimon::Literal(10));
   PAIMON_ASSIGN_OR_RAISE(
       std::shared_ptr<paimon::Predicate> filter,
       paimon::PredicateBuilder::Or({text, price}));
   paimon::ReadContextBuilder builder(table_path);
   builder.SetReadFieldNames({"body", "price"}).SetPredicate(filter);

``SetVectorSearch`` and ``SetFullTextSearch`` configure a separate root TopK
request. They are mutually exclusive and require a positive limit. The intended
semantics are to filter by the complete predicate before ranking; the TopK search
field does not need to be projected. A full-text match inside a predicate never
has a TopK limit.

Score output is requested only through an explicit ``_<field>_INDEX_SCORE``
projection. ``FullTextMatch``, ``SetVectorSearch`` and ``SetFullTextSearch`` have
no separate score switch. For example, request the score of a full-text
predicate by projecting ``_body_INDEX_SCORE``:

.. code-block:: cpp

   auto request = std::make_shared<paimon::FullTextMatch>(
       "body", R"({"match":{"query":"apple"}})");
   PAIMON_ASSIGN_OR_RAISE(
       std::shared_ptr<paimon::Predicate> text,
       paimon::PredicateBuilder::FullTextMatch(0, request));
   paimon::ReadContextBuilder builder(table_path);
   builder.SetReadFieldNames({"body", "_body_INDEX_SCORE"}).SetPredicate(text);

Score fields have type ``float32`` and preserve the source field's exact name,
including case. Select them with ``SetReadFieldNames`` or ``SetReadSchema``;
they have no persistent field IDs for ``SetReadFieldIds``. Reading all table
fields does not implicitly add score fields.

Root TopK searches always need scores internally to rank and merge results
across files, regardless of the projection. A full-text predicate without TopK
needs scores only when its score field is projected.

Each projected score must have exactly one search condition as its source.
The source may be a root TopK request or a full-text predicate. A full-text
predicate whose score is projected cannot have an OR ancestor, and its field
cannot occur in another search condition, including the root TopK request.
An ordinary scalar predicate on the same field is allowed. For example, when
projecting the score of ``match``, ``match AND (p OR q)`` is valid, while
``match OR p`` is rejected. Without that score projection, both OR branches and
repeated full-text search fields are allowed. These checks use the effective
read projection, after resolving the read schema.

Internally, the read context binds each score output position to a search type
and the original table field name. The binding identifies a logical source,
not a request object's identity. Execution must preserve this source when
copying requests for per-file field mapping or candidate filtering.
``NeedsScores`` derives execution requirements from the root TopK request and
these bindings; it does not add another user configuration.

This first stage implements predicate construction, validation, field mapping,
and read-context configuration. Executing a full-text predicate, including using
it for scan pruning, returns ``NotImplemented``. Valid per-field score
projections also return ``NotImplemented`` when reading. Existing scalar and root
search execution retain their current capabilities and limitations.

The next stage will implement exact mixed-predicate execution, per-file index
reader reuse, and selectivity-based AND ordering with candidate propagation.
Missing full-text or vector indexes must fail; scalar conditions may fall back
to exact data scanning. TopK must execute after the complete filter, with scores
and per-file results combined by the read layer. These execution changes are
not part of the predicate interface stage.

Interface
=========

.. doxygenstruct:: paimon::FullTextMatch
   :members:

.. doxygenclass:: paimon::FileIndexer
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FileIndexerFactory
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FileIndexWriter
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FileIndexReader
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FileIndexFormat
   :members:
   :undoc-members:

.. doxygenclass:: paimon::FileIndexResult
   :members:
   :undoc-members:

.. doxygenclass:: paimon::BitmapIndexResult
   :members:
   :undoc-members:
