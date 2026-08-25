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

.. Ported from the Paimon documentation:
.. https://github.com/apache/paimon/blob/master/docs/docs/concepts/rest/tables.mdx

.. default-domain:: cpp
.. highlight:: cpp

Format Table
============
A format table is a directory that holds multiple files of the same format. It carries no
snapshots and no manifests: the files in the directory are the table, so reading it lists
directories and writing to it adds files. A table is a format table when its ``type`` option is
``format-table``; ``file.format`` then names the format of every file in it, which here is
``parquet`` or ``orc``.

A partitioned format table uses the standard Hive directory layout, and its partitions are
discovered from that layout rather than from metadata. By default a partition directory is named
``key=value``; setting ``format-table.partition-path-only-value`` names it by the value alone.

Because a directory of plain files records no row identity, a format table only accepts inserts.
Reads still carry the leading ``_VALUE_KIND`` field every ``BatchReader`` promises, so an engine
that reads a batch by field index sees the same layout it does for a managed table; every row of a
format table is an insert.

Reading and writing
-------------------
A format table is not served through :cpp:func:`Catalog::GetTable`, which describes a managed
table; use :cpp:func:`Catalog::GetFormatTable` instead.

Reading and writing go through the entry points every other table uses: ``TableScan::Create``,
``TableRead::Create``, ``FileStoreWrite::Create`` and ``FileStoreCommit::Create``, each from its
usual context builder. Each reads the table's schema - from under the table path, or from the one
the context carries - and dispatches to a format table when its ``type`` says so, which is what
Java Paimon does through ``FormatTable.newReadBuilder()`` and ``newBatchWriteBuilder()``.

``FormatTable`` is the only format-table type in the public API. The classes behind it -
``FormatTableScan``, ``FormatTableRead``, ``FormatTableWrite``, ``FormatTableCommit``,
``FormatDataSplit`` and ``FormatCommitMessage`` - are implementation details under ``src`` and are
named here only to describe what happens. A caller never needs them: a plan comes back as
``Plan``, a split as ``Split``, and a commit message as ``CommitMessage``.

A catalog that can load a format table itself overrides ``Catalog::LoadFormatTable()``, the
protected hook :cpp:func:`Catalog::GetFormatTable` calls. The file system catalog uses it to say
its metadata lives under the table location, and the REST catalog to take the location and the
schema from one response instead of two that could disagree. A catalog that does not override it
still serves format tables, by reading the location and the schema through the virtuals every
catalog has.

``TableScan::ListPartitions()`` lists the partitions a scan can see. A format table answers it by
listing directories; every other table type returns ``NotImplemented`` for now.

Not all of the generic interfaces fit. ``FileStoreCommit`` is mostly about snapshots and manifests -
expiring them, rolling back to one, filtering by a commit identifier recorded in one - and a format
table keeps none of that state, so those calls are refused rather than quietly doing nothing.
``FileStoreWrite::Compact()`` is refused for the same reason, and both write and commit take batch
writes only, since there is no snapshot to record a commit identifier or a watermark in.
``TableScan`` takes a partition filter and a limit; a predicate or a bucket filter is refused. Java
refuses a predicate from ``FormatTableScan.withFilter`` too, but ``FormatReadBuilder.newScan()``
splits one first and hands the partition half to the scan, so there a predicate over partition
columns still prunes directories. See the limits below.

Options given at the call win over the ones the schema stored, as they do for every other table -
except ``type``, which is structural and is read from the schema alone, so one read or write
cannot decide what kind of table this is. The merged result is validated, not the schema's own
options, so an option a format table refuses - ``metastore.partitioned-table``, a file format
nothing here can read - is refused wherever it comes from rather than dropped in silence.

A setting the format path cannot act on is refused by name rather than quietly dropped:

* ``ReadContextBuilder::SetReadSchema()``. A projected read schema can rename a column, prune a
  nested one and give it metadata of its own; a format table's projection is a list of top-level
  names, so name the columns instead.
* ``WithStreamingMode()`` on a scan or a write, a global index result on a scan, and a real-time
  context on a scan, a read or a write: a format table has no snapshots, no real-time store and no
  index.
* ``WriteContextBuilder::WithWriteSchema()``, which names a subset of the columns to write.
* ``WriteContextBuilder::WithWriteId()``, which prefixes a postpone-bucket writer's files so one
  compaction reader can put them back in order; a format table has no buckets.
* ``CommitContextBuilder::IgnoreEmptyCommit(false)``, ``UseRESTCatalogCommit(true)`` and
  ``AppendCommitCheckConflict(true)``. Keeping an empty commit means writing a snapshot that adds
  no files, a rest-catalog commit sends that snapshot to a catalog, and the conflict check reads
  the manifests of concurrent commits - none of which exist here. Each is refused only when set
  away from its default, so an ordinary commit is unaffected.
* A scan predicate or bucket filter, as above, and more than one partition filter: a scan descends
  one directory layout, so it takes the values of a single partition rather than a set of them.

What a data file is opened with is not one of the refusals. ``EnablePrefetch()``, the read-ahead
cache and its ``CacheConfig``, and the ``Cache`` a read carries all apply, because a format table
opens its files through the same component the managed table path opens its own with. What differs
between the two paths is which files there are and how a row is put back together, not how a file
is read.

Some settings are not refused because they describe machinery the format path never reaches, and
refusing them would refuse the defaults: ``EnableMultiThreadRowToBatch()`` on a read, a write's
temporary directory and spill configuration, and ``WithIgnoreNumBucketCheck()`` and
``WithIgnorePreviousFiles()`` on a write. They have no effect here: a format read hands out the
batches parquet or orc already produced rather than assembling them from rows, a format write
buffers in memory and never spills, and a table with no buckets has no bucket count to check and
no previous files to read back.

A write is two-phase, since a directory has no metadata to switch atomically: a file is written
into a ``_temporary`` directory beside where it will end up, under a hidden name of its own, and
only the commit renames it into place. That is the layout Java Paimon's
``RenamingTwoPhaseOutputStream`` stages under. The directory and the name are both hidden, the
convention a Hive-style directory uses for output that is not committed table data, and what a scan
of this table skips. The ``_temporary`` directory is shared with every other writer
of the same table and is left behind after a commit.

A plan is in-memory only. ``FormatDataSplit`` has no serialized form - ``Split::Serialize()``
refuses it - and neither has ``FormatCommitMessage``: a format table's plan has no cross-runtime
encoding, so plan, read and commit within one process.

A ``FormatTableWrite`` and a ``FormatTableCommit`` are each driven by one thread, but separate
ones may fill and add to a table at once: each write stages its files under a uuid of its own, and
each commit publishes only the files its own messages name. Two *overwriting* commits over the
same directory race, since an overwrite clears what is committed there before publishing anything.
A ``FormatTableScan`` may be shared, since planning leaves it as it was.

``TableRead::CreateCountReader()`` is not implemented for a format table, so counting its rows
means reading them. That is a gap here rather than something the layout forces: ``parquet`` and
``orc`` both record a row count in their own footer.

A writer starts a new file once the one it is filling reaches ``target-file-row-num`` rows or
``target-file-size`` bytes. Both are checked between batches rather than between rows, because a
batch is the unit this API writes in, so a file may pass either target by up to one batch. Java
checks the row count on every row and the size every thousand rows, and its files therefore sit
closer to the target.

Aborting a write
----------------
``FormatTableWrite::Abort()`` removes the files the write staged. It is the one call still allowed
after ``PrepareCommit()``, and that is what it is for: a write dropped *before* preparing clears
its staged files from its own destructor, so only a commit that is prepared and then abandoned
needs it.

Path containment is checked on the path text, which stops a ``..`` from leaving the table but not
a symbolic link pointing out of it - the same as Java's own local file system behaviour.

``FormatTableCommit::Abort()`` does the same for the messages a commit was given. **Neither undoes
a commit that succeeded**: once a file has been renamed into place it is no longer staged, and
nothing here will take it back. Java's committer removes the published path as well as the staged
one, which matters there because a commit publishes file by file with nothing watching; here a
commit that fails part way takes its own published files back before it returns, so an abort is
left with the staged files alone. Both are best effort and never fail, so a warning in the log is
the only signal that a file could not be removed.

Give ``FormatTableCommit`` only the messages this job's own writers produced. A message names a
staged file by path, and a commit can tell that the path belongs to this table, sits in the
partition the message declares, and is staged rather than already published - not whose staged file
it is. A well-formed message from somewhere else is published, or discarded by ``Abort()``, like
any other.

Relationship to Java Paimon
---------------------------
Java serves format tables from a Hive or REST catalog, which holds the schema. This implementation
also serves them from a file system catalog, which keeps the schema under the table directory - an
extension Java does not have. Only for such a table are the ``schema`` and ``branch`` directories
below the location treated as metadata rather than as data.

A file system catalog keeps a table's schema in ``schema`` and its branches in ``branch`` below
the table location, so under ``format-table.partition-path-only-value`` the first partition value
may not be ``schema`` or ``branch``: the directory a write would use is the one holding the
table's own metadata. Such a write is refused, as is an overwrite naming that partition - which
would otherwise delete the schema. A table served from a REST or Hive catalog keeps its schema
elsewhere, so there these are ordinary partition values and are read and written like any other.

Under that same layout a partition value may not start with ``_`` or ``.`` either, whichever
catalog serves the table: the value is the whole directory name, and a scan skips every hidden
name. Java writes such a directory and then cannot read it back; here the write is refused
instead. The one exception is the value standing for a null partition, ``partition.default-name``,
which the scan reads at a partition level by design. Under the ``key=value`` layout the question
does not arise, since the key in front of the value keeps the directory name visible.

A few smaller differences come from this library's own conventions:

* a write takes one partition per batch: the batch declares it through
  ``RecordBatch::SetPartition()``, every row is checked against that declaration, and a batch
  mixing partitions is refused. Java routes row by row, so one write call there may land in any
  number of partitions;
* a write takes its partition from ``RecordBatch::SetPartition()`` rather than from the rows, so
  the values arrive as text. They are still read into their column types and rendered back out
  before anything is named after them - the round trip Java's writer makes when it renders a
  partition out of the row it is writing, through the partition computer its
  ``FileStorePathFactory`` holds. The table therefore decides the directory name and the commit
  message, not the spelling the caller used: with ``partition.legacy-name`` on, its default, a
  ``DATE`` partition is written as its day count whether the caller wrote ``19723`` or
  ``2024-01-01``, and as ``YYYY-MM-DD`` when the option is off. A value that cannot be read into
  its column type is refused. ``FormatTableCommit``'s static partition is *not* put through that
  round trip and is used as given, which is what Java's ``FormatTableCommit.buildPartitionPath``
  does with it too;
* a commit message carries the partition its file belongs to, and a commit checks that it agrees
  with the directory the file sits in. Java's message carries none and derives the partition from
  the committer's target path, so the two cannot disagree there. A message here is a public struct
  a caller may have built itself, so the value is checked rather than trusted;
* a projection that names the same column twice is rejected when the read is built. Java reads
  such a column once per entry;
* a row limit is the caller's: ``FormatTableScan`` takes one so that a plan can drop splits it
  cannot need, but ``FormatTableRead`` does not bound the reader it hands out, and the caller stops
  calling ``NextBatch()`` once it has enough. Java wraps its reader in a ``LimitRecordReader``.

Current limits
--------------
Compared with Java Paimon, this implementation does not yet support:

* the ``csv``, ``json``, ``text`` and ``mosaic`` file formats, leaving ``parquet`` and ``orc``.
  All four are line-delimited text in Java, which shares one line-reading layer between them;
  this library has no text file format at all, so the first of them to be added has to bring that
  layer with it;
* cutting one large data file into byte ranges so that several readers share it. Java does this
  only for its line-delimited text formats, which are the ones missing here; ``parquet`` and
  ``orc`` each record where their own row groups and stripes begin, and a reader handed a byte
  range of one would have to find that out for itself;
* ``metastore.partitioned-table``, which moves partition visibility into the catalog, and the
  Hive partition sync that goes with it;
* partition filters beyond equality on partition values, where Java accepts a full predicate.
  Partition discovery here also lists one directory level at a time and applies the filter to each
  name, while Java turns a leading run of equality constraints into a path and starts listing
  below it; a table with many partitions therefore costs more listings here than in Java;
* ``scan.ignore-corrupt-files`` and ``scan.ignore-lost-files``, which are not implemented: a
  corrupt or missing data file fails the read rather than being skipped;
* ``dynamic-partition-overwrite``. An overwrite here always replaces the partitions the commit
  actually writes to, which is what Java does under that option's default of ``true``. Java also
  has the other mode: with it off, and always for an unpartitioned table, an overwrite empties
  everything the table holds - so a statement whose query returns nothing still clears the table.
  A commit with no messages therefore clears nothing here where Java would;
* ``format-table.commit-hive-sync-url``, which registers committed partitions with a Hive
  metastore;
* column default values. Java replaces a null in a column whose schema field declares a default
  with that default as it writes; here the null is written as it came;
* a table every one of whose columns is a partition column. Java projects the partition columns
  out of what it writes, leaving files that carry nothing but a row count; here such a schema is
  refused when the table is created and when it is opened;
* ``TIMESTAMP``, ``DECIMAL``, ``FLOAT`` and ``DOUBLE`` partition columns, which Java allows. This
  is a restriction of the whole library rather than of format tables. The types that do work are
  ``BOOLEAN``, ``TINYINT``, ``SMALLINT``, ``INT``, ``BIGINT``, ``STRING`` and ``DATE`` - the set
  the managed table path reads and writes partitions in. Any other, ``BINARY`` among them, is
  refused when the table is created and when it is opened, rather than at the first read or write:
  validation asks by building the partition computer that does the round trip, so there is one
  answer rather than a list of types that could fall out of step with it.

``data-file.path-directory`` has no effect here, and none in Java either: Java's format table
writer builds its paths from the table root rather than from that directory.
``format-table.implementation`` is honoured by the engines rather than by the table - in Java
Spark it selects between Paimon's own implementation and the engine's ``FileTable`` - so it has
no meaning inside this library.

Validation
----------
A table Java can serve and this library cannot is refused at creation rather than accepted and
then found unopenable, whichever catalog it is created through. It can still reach a catalog
another way - written by Java, or by an older client - so the same checks run again when the
table is opened.

Whitespace in a partition value is judged by ASCII rules here, while Java uses
``Character.isWhitespace``; a value made only of non-ASCII whitespace therefore lands in a
partition of its own rather than in the default one.
