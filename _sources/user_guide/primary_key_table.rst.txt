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
.. https://github.com/apache/paimon/blob/master/docs/content/primary-key-table/overview.md

Primary Key Table
=================
If you define a table with primary key, you can insert, update or delete records
in the table.

Primary keys consist of a set of columns that contain unique values for each
record. Paimon enforces data ordering by sorting the primary key within each
bucket, allowing users to achieve high performance by applying filtering
conditions on the primary key.


Bucket
-------
Unpartitioned tables, or partitions in partitioned tables, are sub-divided into
buckets, to provide extra structure to the data that may be used for more
efficient querying.

Each bucket directory contains an LSM tree and its changelog files.

.. note::
   Changelog is not supported yet for Paimon C++ primary key table write.

The range for a bucket is determined by the hash value of one or more columns in
the records. Users can specify bucketing columns by providing the bucket-key option.
If no bucket-key option is specified, the primary key (if defined) or the complete
record will be used as the bucket key.

A bucket is the smallest storage unit for reads and writes, so the number of
buckets limits the maximum processing parallelism. This number should not be too
big, though, as it will result in lots of small files and low read performance.
In general, the recommended data size in each bucket is about 200MB - 1GB.

Also, see rescale bucket if you want to adjust the number of buckets after a
table is created.


LSM Trees
-------------
Paimon adopts the LSM tree (log-structured merge-tree) as the data structure for
file storage. This documentation briefly introduces the concepts about LSM trees.

Sorted Runs
~~~~~~~~~~~~~~
LSM tree organizes files into several sorted runs. A sorted run consists of one
or multiple data files and each data file belongs to exactly one sorted run.

Records within a data file are sorted by their primary keys. Within a sorted run,
ranges of primary keys of data files never overlap.

.. image:: ../_static/sorted-runs.png
   :alt: Sorted Runs
   :align: center
   :width: 100%

As you can see, different sorted runs may have overlapped primary key ranges,
and may even contain the same primary key. When querying the LSM tree, all
sorted runs must be combined and all records with the same primary key must be
merged according to the user-specified merge engine and the timestamp of each record.

New records written into the LSM tree will be first buffered in memory. When the
memory buffer is full, all records in memory will be sorted and flushed to disk.
A new sorted run is now created.


Merge Engines
-------------
When Paimon sink receives two or more records with the same primary key, it
merges them into one record to keep the primary key unique. The
``merge-engine`` table option decides how. Paimon C++ supports ``deduplicate``
(the default, which keeps the last record), ``partial-update``, ``first-row``
and ``aggregation``.

Aggregation
~~~~~~~~~~~~~~
The ``aggregation`` merge engine aggregates each value field across the records
sharing a primary key, using the function configured for that field. Fields
without a configured function fall back to
``fields.default-aggregate-function``, and then to ``last_non_null_value``.

.. code-block:: text

   merge-engine = aggregation
   fields.<field-name>.aggregate-function = <function>
   fields.default-aggregate-function = <function>
   fields.<field-name>.ignore-retract = true

The available functions are ``sum``, ``product``, ``min``, ``max``,
``first_value``, ``last_value``, ``first_non_null_value``,
``last_non_null_value``, ``bool_and``, ``bool_or``, ``listagg``, ``collect``,
``merge_map``, ``nested_update``, ``hll_sketch`` and ``theta_sketch``. Each one
accepts only the field types it is defined for. An unknown function, or a
function configured on an unsupported field type, is rejected when aggregation
logic is initialized. Read paths that bypass aggregation may skip this
validation.

Not every function can process a retraction, that is a record whose row kind is
``DELETE`` or ``UPDATE_BEFORE``. A function that cannot returns an error, unless
``fields.<field-name>.ignore-retract`` is set, which drops the retraction
instead.

product
^^^^^^^^^^^^^^
Multiplies the values of a field. It accepts ``TINYINT``, ``SMALLINT``,
``INT``, ``BIGINT``, ``FLOAT``, ``DOUBLE`` and ``DECIMAL``, and it supports
retraction by dividing the accumulated value.

.. code-block:: text

   merge-engine = aggregation
   fields.price.aggregate-function = product

Null values are skipped, so aggregating a null into a field leaves the field
unchanged, and the first non-null value seeds the product. Retracting into a
field that is still null leaves it null rather than producing a reciprocal.

Integer arithmetic is exact. A product or a quotient outside the range of the
field type, a division by zero, and dividing the smallest value of the type by
``-1`` all fail with an error instead of wrapping around. An integer quotient
truncates towards zero, so retracting ``3`` from ``10`` yields ``3``.

``FLOAT`` and ``DOUBLE`` follow IEEE 754 instead of reporting an error.
Retracting a zero from a finite non-zero value yields an infinity carrying the
sign of both operands, and retracting a zero from a zero yields ``NaN``.

``DECIMAL`` has two boundary behaviors worth knowing, both matching Paimon
Java:

- The product and the quotient are rounded half up to the scale of the field.
  A result that no longer fits the precision of the field aggregates to
  ``NULL`` rather than failing, so a ``DECIMAL(4, 2)`` field holding
  ``99.99 * 99.99`` becomes ``NULL``.
- Retraction only accepts a quotient with a finite decimal expansion, and
  returns an error otherwise. ``1.00 / 8.00`` is accepted and rounds to
  ``0.13``, while ``2.00 / 3.00`` is rejected.
