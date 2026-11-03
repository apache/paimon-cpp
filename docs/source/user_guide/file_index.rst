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

File Index
==========

Full-text predicates and root TopK
---------------------------------

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

Score output is requested through an explicit ``_<field>_INDEX_SCORE``
projection. For example, request the score of a full-text
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

Each projected score must have exactly one search condition as its source.
The source may be a root TopK request or a full-text predicate. A full-text
predicate whose score is projected cannot have an OR ancestor, and its field
cannot occur in another search condition, including the root TopK request.
An ordinary scalar predicate on the same field is allowed. For example, when
projecting the score of ``match``, ``match AND (p OR q)`` is valid, while
``match OR p`` is rejected. Without that score projection, both OR branches and
repeated full-text search fields are allowed. These checks use the effective
read projection, after resolving the read schema.
