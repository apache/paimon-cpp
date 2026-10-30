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

- **Bitmap Index**: An index that maps each distinct scalar value to its matching row IDs. The
  file format is compatible with the Java bitmap global index.
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

Cleaning Up Build Checkpoints
-----------------------------

Checkpoint-enabled index builds remove their checkpoints after a successful build. If a process
crashes, use ``GlobalIndexWriteTask::DeleteCheckpoint`` to remove all checkpoints left by that
task once it is no longer running or being retried:

.. code-block:: cpp

   PAIMON_RETURN_NOT_OK(paimon::GlobalIndexWriteTask::DeleteCheckpoint(
       table_path, "build-task-1", options, file_system));

Checkpoints are stored in ``<table_path>/index/checkpoint/<task_id>`` by default. When
``global-index.external-path`` is configured, they are stored in
``<global-index.external-path>/checkpoint/<task_id>`` instead.

Pass the same table path, task id, and ``global-index.external-path`` configuration used in
``WriteIndex``. Cleanup reads the configuration from the table schema and applies any supplied
options as overrides. Task ids must be unique among builds sharing the same global index directory,
including builds for different tables sharing an external path. The cleanup recursively removes
the whole task directory, other tasks' directories and completed index files are preserved.

Bitmap Index
------------

Bitmap maps each distinct scalar value to a compressed row-ID bitmap. Input keys must be written
in ascending order. Point predicates use direct dictionary lookup, while range and non-prefix
string predicates can scan selected dictionary files within a configurable fallback budget.

**Special Configuration:**

- ``bitmap-index.dictionary-block-size``: Target dictionary block size. Default: ``16KB``.
- ``bitmap-index.compression``: Dictionary block compression codec. Default: ``none``.
- ``bitmap-index.compression-level``: Dictionary block compression level. Default: ``1``.
- ``bitmap-index.fallback-scan-max-size``: Maximum total selected index file size for fallback
  scans. Default: ``256MB``; set to ``0`` to disable fallback scans.

BTree Index
-----------

BTree is an efficient index based on multi-level SST files, supporting rich predicate pushdown, block cache, file-level min/max key pruning, lazy loading, and block compression.

**Special Configuration:**

- **Option**: ``btree-index.bloom-filter.enabled``

  - **Default**: ``false``
  - **Description**: Enables a Bloom filter for BTree point lookups such as equality and ``IN``
    predicates. This can avoid reading data blocks for missing keys.

- **Option**: ``btree-index.read-buffer-size``

  - **Description**: Optional. Specifies the read buffer size for the B-tree index. This setting can be tuned based on query patterns:

    - For **range queries** (e.g., ``VisitLessThan``, ``VisitGreaterOrEqual``), increasing the buffer size (e.g., to 1MB) may improve I/O bandwidth and sequential read performance.
    - For **point queries** (e.g., ``VisitEqual``), buffering can introduce negative effects due to read amplification; it is recommended to leave this option unset.

- **Option**: ``btree-index.fallback-scan-max-size``

  - **Default**: ``256MB``
  - **Description**: Limits the total selected BTree index file size for predicates that require
    an index scan, such as range predicates and non-prefix string predicates. Set it to ``0`` to
    disable fallback scans. Point lookups and prefix lookups are not affected.

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
positive float are rejected, while unknown fields are ignored. As in the native DSL, a query may
nest at most 127 JSON arrays and objects, not counting unknown fields. Leading and trailing Unicode
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
the highest scores. ``include_row_ids`` holds global row ids and restricts the rows that are
ranked; an empty set matches no rows.

For both ``full-text`` and ``lucene-fts``, a search across multiple index shards keeps the
``limit`` highest-scoring rows returned by the shards, breaking ties by ascending row id. Which rows
tied at a shard's cutoff score the shard returns is up to its engine. A row in overlapping index
ranges is ranked only by the range that starts first, or ends first if both start at the same row.
A shard ignores the ``include_row_ids`` outside the rows it ranks and is not searched if none
remain. The result iterates rows by ascending row id, not by score.

When ``deletion-vectors.enabled`` and ``row-tracking.enabled`` are set, readers created by
``GlobalIndexScan`` rank only the rows that are live in the snapshot the scan reads, as Java Paimon
does, so rows deleted by deletion vectors do not take ``limit`` slots. BM25 statistics still come
from the whole index. A later snapshot can delete some of the matched rows, so pass one snapshot id
to both the index scan and the data scan:

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(
       std::unique_ptr<paimon::GlobalIndexScan> index_scan,
       paimon::GlobalIndexScan::Create(table_path, snapshot_id, /*partitions=*/std::nullopt,
                                       /*options=*/{}, /*file_system=*/nullptr,
                                       /*executor=*/nullptr, pool));
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::GlobalIndexReader> reader,
                          index_scan->CreateReader("f0", "full-text",
                                                   /*row_range_index=*/std::nullopt));
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::ScoredGlobalIndexResult> result,
                          reader->VisitFullTextSearch(std::make_shared<paimon::FullTextSearch>(
                              "f0", R"({"match":{"query":"paimon lake"}})", /*limit=*/10)));

   paimon::ScanContextBuilder scan_builder(table_path);
   scan_builder.AddOption(paimon::Options::SCAN_MODE, "from-snapshot")
       .AddOption(paimon::Options::SCAN_SNAPSHOT_ID, std::to_string(snapshot_id))
       .SetGlobalIndexResult(result);

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

Table-Level Full-Text Search
----------------------------

``FullTextSearchBuilder`` (``paimon/table/source/full_text_search_builder.h``) searches the global
full-text indexes of one column of a data-evolution table, as Java Paimon's
``FullTextSearchBuilder`` does, so a caller does not create index readers or choose index types and
shard ranges. It searches the index files whose indexer supports full-text search
(``GlobalIndexer::SupportsFullTextSearch()``): ``full-text`` and ``lucene-fts``. Only the rows that
an index shard covers are searched, as in the default ``fast`` ``full-text-index.search-mode`` of
Java Paimon, so rows written since the index was last built are not found until it is built for
them. A search whose ``full-text-index.search-mode``, or else ``global-index.search-mode``, is
``full`` or ``detail``, which also search the rows that no index covers, fails with
``NotImplemented``. Row filters, which Java Paimon evaluates with scalar indexes before ranking, are
not supported yet.

.. code-block:: cpp

   paimon::FullTextSearchBuilder search_builder(table_path);
   search_builder.SetQuery("content", R"({"match":{"query":"paimon lake"}})").SetLimit(10);
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::ScoredGlobalIndexResult> result,
                          search_builder.ExecuteLocal());

   // Read the rows found, with their scores.
   paimon::ScanContextBuilder scan_builder(table_path);
   scan_builder.SetGlobalIndexResult(result);
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ScanContext> scan_context,
                          scan_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableScan> scan,
                          paimon::TableScan::Create(std::move(scan_context)));
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Plan> data_plan, scan->CreatePlan());

   paimon::ReadContextBuilder read_builder(table_path);
   read_builder.SetReadFieldNames({"id", "content", "_INDEX_SCORE"});
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ReadContext> read_context,
                          read_builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableRead> read,
                          paimon::TableRead::Create(std::move(read_context)));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::BatchReader> reader,
                          read->CreateReader(data_plan->Splits()));

The result holds the ``limit`` highest-scoring rows among the candidates of all index shards, and
a read of the rows returns their scores in the ``_INDEX_SCORE`` field. Each shard returns at most
``limit`` candidates, chosen by its index, so ties are broken by ascending row id among the
candidates only: which of the rows tied at the ``limit``-th score of a shard become candidates is up
to its index.

- ``SetPartitionFilter()`` restricts the search to some partitions, as in ``ScanContextBuilder``.
- The search reads the snapshot that the time-travel options select, as Java Paimon does: snapshot
  ``scan.snapshot-id``, the snapshot of tag ``scan.tag-name``, the latest snapshot at or before
  ``scan.timestamp`` or ``scan.timestamp-millis``, or else the latest snapshot. At most one of these
  options may be set, and it must agree with an explicit ``scan.mode``: ``from-snapshot`` needs
  ``scan.snapshot-id`` or ``scan.tag-name``, ``from-snapshot-full`` needs ``scan.snapshot-id``,
  ``from-timestamp`` needs ``scan.timestamp`` or ``scan.timestamp-millis``, and ``latest-full`` and
  ``latest`` take none of them. The search still uses the latest schema of the table, not the
  schema of that snapshot.
- A shard whose indexed field is the column or that carries it as an extra field is searched.
  When shards overlap, each row is ranked by one of them: an index whose indexed field is the
  column comes first, then the order of the index type and fields, then the start and end of the
  shard.
- With deletion vectors, only the rows live in that snapshot are ranked, as described for
  ``GlobalIndexScan`` above.
- The shards are searched in parallel on the executor given to ``WithExecutor()``, or else on one
  with ``global-index.thread-num`` threads, and each returns at most ``limit`` rows.
- If Paimon C++ is built without the backend of a ``full-text`` or ``lucene-fts`` index, the
  search fails instead of skipping the index and leaving its rows out of the result.
- A column with a primary-key full-text index (``pk-full-text.index.columns``) cannot be searched
  yet.

The row ids of the result refer to the snapshot the search was planned from. If the table may
change before the rows are read, plan with ``NewFullTextScan()`` and pin the data scan to the
snapshot of the plan, setting ``scan.mode`` too so that a ``scan.mode`` of the table cannot override
it. For a search of ``scan.tag-name``, pin the data scan with the same tag instead, as the tag may
outlive the snapshot.

.. code-block:: cpp

   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FullTextScan> search_scan,
                          search_builder.NewFullTextScan());
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::FullTextScan::Plan> plan,
                          search_scan->CreatePlan());
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FullTextRead> search_read,
                          search_builder.NewFullTextRead());
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::ScoredGlobalIndexResult> result,
                          search_read->Read(*plan));

   paimon::ScanContextBuilder scan_builder(table_path);
   scan_builder.SetGlobalIndexResult(result);
   if (plan->SnapshotId()) {
       scan_builder.AddOption(paimon::Options::SCAN_MODE, "from-snapshot")
           .AddOption(paimon::Options::SCAN_SNAPSHOT_ID, std::to_string(*plan->SnapshotId()));
   }

``ExecuteLocal()`` plans and searches in one process. To plan in one process and search in others,
plan with ``NewFullTextScan()``, ship the splits with ``FullTextSearchSplit::Serialize()`` and
``FullTextSearchSplit::Deserialize()``, and search them with the ``NewFullTextRead()`` of a builder
for the same table, column and query. The serialized form is versioned and specific to Paimon C++:
Java Paimon serializes its splits with Java serialization, so splits cannot be exchanged with it.
Each split records the snapshot it was planned from, and the read ranks the rows live in that
snapshot. The read rejects a split whose index files do not index the column of the latest schema,
such as after the column was replaced. A read returns the top rows of the splits it is given, so
keep the ``limit`` highest-scoring rows of all reads if the splits are searched by several of them.

.. code-block:: cpp

   // Planning process.
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FullTextScan> scan,
                          search_builder.NewFullTextScan());
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::FullTextScan::Plan> plan, scan->CreatePlan());
   for (const auto& split : plan->Splits()) {
       PAIMON_ASSIGN_OR_RAISE(std::string bytes,
                              paimon::FullTextSearchSplit::Serialize(split, pool));
       // Send `bytes` to a searching process.
   }

   // Searching process.
   std::vector<std::shared_ptr<paimon::FullTextSearchSplit>> splits;
   PAIMON_ASSIGN_OR_RAISE(
       std::shared_ptr<paimon::FullTextSearchSplit> split,
       paimon::FullTextSearchSplit::Deserialize(bytes.data(), bytes.size(), pool));
   splits.push_back(std::move(split));
   PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FullTextRead> read,
                          search_builder.NewFullTextRead());
   PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::ScoredGlobalIndexResult> result,
                          read->Read(splits));

Vector Index (Lumina)
---------------------

An approximate nearest neighbor (ANN) index powered by Lumina for vector similarity search.
It supports high-dimensional vector search with configurable distance metrics and encoding strategies.
For more configurations, refer to the ``docs/reference`` directory in the Lumina release package.
