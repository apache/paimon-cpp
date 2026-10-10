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

Global Index
============

Global Index is a powerful indexing mechanism for append-only tables.
It enables efficient row-level lookups and filtering without full-table scans.
Paimon C++ supports the following global index types:

- **BTree Index**: An efficient index based on multi-level SST files for scalar column lookups.
- **Range Bitmap Index**: A range bitmap index optimized for range predicates on ordered scalar columns. Extends the bitmap approach by encoding value ordering, enabling efficient less-than, greater-than, and range conditions.
- **Lucene Index**: A full-text search index powered by Lucene++. Supports tokenized text search with the JSON queries of the full-text index: match, multi-match, phrase and boolean queries.
- **Full-Text Index (experimental)**: A full-text search index powered by the native
  ``paimon-full-text-index`` engine. It uses the same index file format as Java Paimon's
  ``full-text`` index.
- **Vector Index (Lumina)**: An approximate nearest neighbor (ANN) index powered by Lumina for vector similarity search with configurable distance metrics.

Global indexes work on top of Data Evolution tables. To use global indexes, your table must have:

- ``'bucket' = '-1'`` (unaware-bucket mode)
- ``'row-tracking.enabled' = 'true'``
- ``'data-evolution.enabled' = 'true'``

Bitmap Index Compatibility
--------------------------

The current Paimon C++ version does not support bitmap global indexes.

BTree Index
-----------

BTree is an efficient index based on multi-level SST files, supporting rich predicate pushdown, block cache, file-level min/max key pruning, lazy loading, and block compression.

**Special Configuration:**

- **Option**: ``btree-index.read-buffer-size``

  - **Description**: Optional. Specifies the read buffer size for the B-tree index. This setting can be tuned based on query patterns:

    - For **range queries** (e.g., ``VisitLessThan``, ``VisitGreaterOrEqual``), increasing the buffer size (e.g., to 1MB) may improve I/O bandwidth and sequential read performance.
    - For **point queries** (e.g., ``VisitEqual``), buffering can introduce negative effects due to read amplification; it is recommended to leave this option unset.

Range Bitmap Index
------------------

A range bitmap index optimized for range predicates on ordered scalar columns. It extends the
bitmap approach by encoding value ordering information, enabling efficient evaluation of
less-than, greater-than, and range conditions without scanning all bitmaps.


Lucene Index
------------

A full-text search index powered by Lucene++. It uses ``FullTextSearch`` and the JSON DSL
described in `Full-Text Index (Experimental)`_, translating queries into Lucene queries on the
indexed field:

- ``match``: one term query per analyzed term, combined with OR, or with AND for
  ``"operator": "And"``. ``boost`` sets the boost of the query.
- ``multi_match``: one ``match`` query per column, combined with OR. Every column must be the
  indexed field.
- ``match_phrase`` (alias ``phrase``): a phrase query with an optional ``slop``.
- ``boolean``: a boolean query of the translated ``must``, ``should``, ``must_not`` and
  ``queries`` clauses.

``boost`` queries and fuzzy matching (a ``fuzziness`` other than 0) are rejected with a
``NotImplemented`` status, and a ``column`` other than the indexed field is rejected. The query
text is analyzed with the Jieba tokenizer of the index. Each index shard returns at most ``limit``
rows with the highest Lucene relevance scores.

Duplicate known fields, raw NUL bytes in the JSON text and boosts that do not round to a finite
positive float are rejected, while unknown fields are ignored. Leading and trailing Unicode
whitespace in operators, boolean occurrences and column names is trimmed as in the native DSL, and
a column name containing only such whitespace selects the indexed field.

**Special Configuration:**

- **Option**: ``lucene-fts.write.tmp.directory``

  - **Description**: Specifies the temporary directory used during Lucene index writing. No default value; must be explicitly set.

- **Environment Variable**: ``PAIMON_JIEBA_DICT_DIR``

  - **Description**: Specifies the directory containing Jieba dictionary files for Chinese text tokenization. At runtime, the system first checks this environment variable; if not set, it falls back to the compile-time ``JIEBA_TEST_DICT_DIR`` macro (only available in test builds). If neither is available, will fail with an error.

Full-Text Index (Experimental)
------------------------------

A full-text search index powered by the native
`paimon-full-text-index <https://github.com/apache/paimon-full-text>`_ engine (0.1.0, built on
Tantivy 0.26). It uses the same index type ``full-text``, option prefix and index file format as
Java Paimon, so index files written by either side are meant to be readable by the other when both
use the same engine version: a reader rejects index files written with another Tantivy version.
Cross-reading is currently verified with index files written by the engine's Python binding
(``paimon-ftindex``), which wraps the same engine as Java Paimon, but not yet with index files
written by Java Paimon itself. Enable it at build time with ``-DPAIMON_ENABLE_FULL_TEXT=ON``; see
:doc:`../building`.

The indexed field must be a ``STRING``, ``CHAR`` or ``VARCHAR`` column. Null values are not
indexed. Unlike in Java Paimon, values that contain NUL characters are rejected, because the native
C API takes NUL-terminated strings; index option keys and values and queries must not contain NUL
characters either. A writer that receives no rows writes no index file.

This index replaces the former experimental ``tantivy-fulltext`` index, whose files cannot be read
by it. Rebuild such indexes with the ``full-text`` index type.

**Queries:**

``FullTextSearch`` is aligned with Java Paimon: it takes the field name, a JSON query, a positive
``limit`` and optional ``include_row_ids``. ``FullTextSearch::query`` is passed to the engine
unchanged and must be a JSON query, for example:

- ``{"match": {"query": "paimon lake"}}``: rows that contain any of the analyzed terms. Add
  ``"operator": "And"`` to require all terms. ``boost``, ``fuzziness``, ``max_expansions`` and
  ``prefix_length`` are optional.
- ``{"multi_match": {"query": "paimon lake", "columns": ["text"]}}``: a ``match`` query on the
  default native index field, with optional per-column ``boosts``.
- ``{"match_phrase": {"query": "data lake"}}``: rows that contain the terms as a phrase. An
  optional ``"slop"`` allows other terms between them.
- ``{"boolean": {"must": [...], "should": [...], "must_not": [...]}}``: combines other queries.
- ``{"boost": {"positive": {...}, "negative": {...}, "negative_boost": 0.5}}``: lowers the score
  of rows that also match the negative query.

The C++ writer indexes one table column. ``FullTextSearch::field_name`` selects that table column,
while ``column`` and ``columns`` in the JSON query name fields inside the native index. The native
field defaults to ``text`` and can be renamed with ``full-text.text-field``. These names do not
enable searches across multiple table columns. For ``lucene-fts``, DSL column names must instead
match the indexed table column.

The query text is analyzed with the analyzer stored in the index file, and an invalid query is
reported as an error status. A search always returns a ``ScoredGlobalIndexResult``. Within each
index shard, the engine ranks matching rows by BM25 score and returns at most ``limit`` rows with
the highest scores.
``include_row_ids`` holds global row ids and restricts the rows that are ranked; an empty set
matches no rows.

For both ``full-text`` and ``lucene-fts``, searches across multiple shards currently return the
union of the shard candidates, which can exceed ``limit``. Applying a final global top-k is tracked
in `issue #402 <https://github.com/apache/paimon-cpp/issues/402>`_.

**Configuration:**

Table options with the ``full-text.`` prefix are passed to the engine with the prefix removed.
They are only used when writing an index: the analyzer configuration is stored in every index file
and readers use that copy.

.. list-table::
   :header-rows: 1
   :widths: 30 10 60

   * - Option
     - Default
     - Description
   * - ``full-text.tokenizer``
     - ``default``
     - Tokenizer: ``default`` or ``simple`` (split on non-alphanumeric characters),
       ``whitespace``, ``raw`` (no splitting), ``ngram`` (n-grams of the whole text) or ``jieba``
       (Chinese segmentation with a built-in dictionary).
   * - ``full-text.ngram.min-gram``
     - 3
     - Minimum n-gram length of the ``ngram`` tokenizer.
   * - ``full-text.ngram.max-gram``
     - 3
     - Maximum n-gram length of the ``ngram`` tokenizer.
   * - ``full-text.ngram.prefix-only``
     - false
     - Whether the ``ngram`` tokenizer only emits the n-grams that start at the beginning of the
       text.
   * - ``full-text.jieba.search-mode``
     - true
     - Whether the ``jieba`` tokenizer also emits the shorter words contained in long words.
   * - ``full-text.jieba.ordinal-position``
     - true
     - Whether the ``jieba`` tokenizer assigns consecutive token positions.
   * - ``full-text.lower-case``
     - true
     - Whether tokens are lower-cased.
   * - ``full-text.max-token-length``
     - 40
     - Tokens of this many bytes or more are dropped.
   * - ``full-text.ascii-folding``
     - true
     - Whether non-ASCII characters are folded to their ASCII equivalents.
   * - ``full-text.stem``
     - true
     - Whether tokens are stemmed, so that ``run`` matches ``running``.
   * - ``full-text.language``
     - ``english``
     - Language used for stemming and stop words.
   * - ``full-text.remove-stop-words``
     - true
     - Whether the built-in stop words of ``full-text.language`` and ``full-text.stop-words`` are
       removed.
   * - ``full-text.stop-words``
     - (empty)
     - Additional stop words, separated by ``;``. Requires ``full-text.remove-stop-words=true``.
   * - ``full-text.with-position``
     - true
     - Whether token positions are indexed. Phrase queries require positions.

The prefix-stripped options are also stored as a flat JSON object in the metadata of each index
file, as Java Paimon does.

Vector Index (Lumina)
---------------------

An approximate nearest neighbor (ANN) index powered by Lumina for vector similarity search.
It supports high-dimensional vector search with configurable distance metrics and encoding strategies.
For more configurations, refer to the ``docs/reference`` directory in the Lumina release package.
