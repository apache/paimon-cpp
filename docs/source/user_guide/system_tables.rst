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

System Tables
=============

Paimon C++ supports reading system tables by appending a system table suffix to
the data table path. For example, ``/warehouse/db.db/orders$snapshots`` reads
the snapshots system table for ``orders``.

Branch-qualified paths are also supported. For example,
``/warehouse/db.db/orders$branch_audit$files`` reads the files system table from
the ``audit`` branch.

Table-Level System Tables
-------------------------

These suffixes are recognized on a data table path. Matching is
case-insensitive, and an unrecognized suffix fails with
``unsupported system table: <name>`` instead of falling back to the base table.

* ``$options``: the latest base table options as key/value rows.
* ``$snapshots``: snapshot commit history.
* ``$schemas``: schema evolution history.
* ``$tags``: tags and the snapshots they reference.
* ``$branches``: table branches, including ``main``.
* ``$consumers``: persisted streaming consumer offsets.
* ``$manifests``: data manifest metadata in the latest snapshot.
* ``$files``: data file metadata in the latest snapshot.
* ``$audit_log``: row-level changelog records with rowkind.
* ``$binlog``: changelog records with list-wrapped data columns.
* ``$ro``: read-optimized data, described in the next section.

Global System Tables
--------------------

Four system tables are not bound to a single data table. They live under the
reserved ``sys`` database, and three of them enumerate across all databases in
the warehouse:

* ``sys.catalog_options``: catalog-level configuration as key/value rows.
* ``sys.all_table_options``: all table options as
  ``(database_name, table_name, key, value)`` rows.
* ``sys.tables``: metadata for all tables, including record counts and file
  statistics.
* ``sys.partitions``: partition-level file statistics for all tables.

``sys.all_table_options``, ``sys.tables`` and ``sys.partitions`` are always
available. ``sys.catalog_options`` is disabled by default and has to be turned
on with the catalog option ``catalog-options-table.enabled``; a disabled one is
reported as not existing.

Those three enumerate databases and tables, so they need a catalog: loading a
bare ``<warehouse>/sys/<name>`` path fails with ``global system table requires
catalog context: <name>``. ``sys.catalog_options`` enumerates nothing, and is
the one a bare path serves.

.. note::

   No public read path serves those three yet. A catalog lists them and
   returns their schema, but does not hand back a readable table:
   ``Catalog::GetTable(Identifier("sys", "tables"))`` is rejected by a
   file-system catalog with ``do not support checking TableSchemaExists for
   system table``, and a REST catalog returns one whose path is left unset,
   which ``TableScan`` and ``TableRead`` cannot dispatch to the global system
   table loader.

Read-Optimized System Table
---------------------------

The read-optimized system table is addressed by the ``$ro`` suffix:

.. code-block:: text

   /warehouse/db.db/orders$ro

For primary-key tables, ``$ro`` only plans data files from the highest LSM
level, which is the level produced by full compaction. This avoids merging data
from multiple LSM levels during query planning and reading. The tradeoff is that
the result may lag behind the latest committed data until a full compaction
publishes the newest records into the highest level.

This stale view is not guaranteed to correspond to any single historical table
snapshot. Full compaction may finish independently for different buckets. When
``$ro`` scans the latest snapshot, the selected highest-level files can
therefore combine bucket states produced by full-compaction commits at different
snapshot IDs.

For primary-key tables, ``$ro`` also enables value-stats filtering, so file-level
pruning of the reader predicate can be more aggressive than the base table.

For append-only tables, ``$ro`` has the same read behavior as the base table,
including streaming reads.

Limitations
~~~~~~~~~~~

- Primary-key ``$ro`` scans are batch-only. Streaming scans are not supported.
- Freshness depends on full compaction frequency, and the result may not match
  any single historical snapshot across buckets.
- Primary-key tables in bucket-unaware mode are not supported by the current C++
  scan path.

Typical Usage
~~~~~~~~~~~~~

Use ``$ro`` for OLAP or batch workloads that can tolerate stale results and
prefer reading compacted files directly. Use the base table path when the query
must see the latest committed data.
