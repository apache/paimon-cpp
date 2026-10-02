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
- **Lucene Index**: A full-text search index powered by Lucene++. Supports tokenized text search with multiple modes including match-all, match-any, phrase, prefix, and wildcard queries.
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

A full-text search index powered by Lucene++. It supports tokenized text search with multiple
search modes including match-all, match-any, phrase, prefix, and wildcard queries.

**Supported search types:**

- ``MATCH_ALL``: All terms in the query must be present (AND semantics).
- ``MATCH_ANY``: Any term in the query can match (OR semantics).
- ``PHRASE``: Matches the exact sequence of words (with proximity).
- ``PREFIX``: Matches terms starting with the given string (e.g., "run*" → running, runner). The
  query is not tokenized. The original prefix is retained, and a pure ASCII alphanumeric prefix
  is also matched using the lowercase case-normalization applied to pure ASCII terms at indexing
  time. This preserves matches for mixed terms such as ``B超`` while allowing ``THIS`` to match
  terms indexed as ``this...``.
- ``WILDCARD``: Supports wildcards ``*`` and ``?`` (e.g., "ap*e", "app?e" → "apple"). The query
  is not tokenized, and wildcard operators are preserved. Both the original pattern and an
  alternative with each ASCII alphanumeric fragment lowercased are matched, covering pure ASCII
  and mixed ASCII/non-ASCII terms.

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

``FullTextSearch::query`` is passed to the engine unchanged and must be a JSON query, for example:

- ``{"match": {"query": "paimon lake"}}``: rows that contain any of the analyzed terms. Add
  ``"operator": "And"`` to require all terms.
- ``{"match_phrase": {"query": "data lake"}}``: rows that contain the terms as a phrase. An
  optional ``"slop"`` allows other terms between them.
- ``{"boolean": {"must": [...], "should": [...], "must_not": [...]}}``: combines other queries.

The query text is analyzed with the analyzer stored in the index file. ``search_type`` is ignored.
A positive ``limit`` is required. The engine always ranks the matching rows by BM25 score and
returns the ``limit`` rows with the highest scores; ``pre_filter`` restricts the rows that are
ranked. ``min_score`` is applied to those rows afterwards and drops rows whose score is not greater
than it, so a search can return fewer than ``limit`` rows. ``with_score`` only selects whether the
scores are returned; scores are computed either way.

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
