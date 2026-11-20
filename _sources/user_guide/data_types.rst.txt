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

Data Types
==========

A data type describes the logical type of a value in the table ecosystem. It can
be used to declare input and/or output types of operations.

All data types by Java Paimon are as follows, Paimon C++ uses Apache Arrow as
its schema representation. The following table shows the mapping between `Java
Paimon DataTypes <https://paimon.apache.org/docs/master/concepts/data-types/>`_
and `Arrow DataTypes <https://arrow.apache.org/docs/format/Columnar.html#data-types>`_:

.. list-table::
   :header-rows: 1
   :widths: 10 10 30

   * - Java Paimon DataType
     - Arrow DataType
     - Description

   * - ``BOOLEAN``
     - Boolean
     - Data type of a boolean with a (possibly) three-valued logic of TRUE, FALSE, and UNKNOWN.

   * - ``CHAR``

       ``CHAR(n)``
     - Utf8
     - Data type of a fixed-length character string.

       The type can be declared using ``CHAR(n)`` where n is the number of code
       points. n must have a value between 1 and 2,147,483,647 (both inclusive).
       If no length is specified, n is equal to 1.

       **Note:** Apache Arrow has no fixed-length string type, so the declared
       length ``n`` is not enforced; values are mapped to a variable-length ``Utf8``.

   * - ``VARCHAR``

       ``VARCHAR(n)``

     - Utf8
     - Data type of a variable-length character string.

       The type can be declared using ``VARCHAR(n)`` where n is the maximum
       number of code points. n must have a value between 1 and 2,147,483,647
       (both inclusive). If no length is specified, n is equal to 1.

       **Note:** The declared maximum length ``n`` is not enforced.

   * - ``STRING``
     - Utf8
     - Data type of a variable-length character string. ``STRING`` is a synonym for ``VARCHAR(2147483647)``.

   * - ``BINARY``

       ``BINARY(n)``
     - Binary
     - Data type of a fixed-length binary string (=a sequence of bytes).

       The type can be declared using ``BINARY(n)`` where n is the number of
       bytes. n must have a value between 1 and 2,147,483,647 (both inclusive).
       If no length is specified, n is equal to 1.

       **Note:** Apache Arrow has no fixed-length binary type here, so the declared
       length ``n`` is not enforced; values are mapped to a variable-length ``Binary``.

   * - ``VARBINARY``

       ``VARBINARY(n)``
     - Binary
     - Data type of a variable-length binary string (=a sequence of bytes).

       The type can be declared using ``VARBINARY(n)`` where n is the maximum
       number of bytes. n must have a value between 1 and 2,147,483,647
       (both inclusive). If no length is specified, n is equal to 1.

       **Note:** The declared maximum length ``n`` is not enforced.

   * - ``BYTES``
     - Binary
     - ``BYTES`` is a synonym for ``VARBINARY(2147483647)``.

   * - ``DECIMAL``

       ``DECIMAL(p)``

       ``DECIMAL(p, s)``
     - Decimal128
     - Data type of a decimal number with fixed precision and scale.

       The type can be declared using ``DECIMAL(p, s)`` where p is the number of
       digits in a number (precision) and s is the number of digits to the right
       of the decimal point in a number (scale). p must have a value between 1
       and 38 (both inclusive). s must have a value between 0 and p
       (both inclusive). The default value for p is 10. The default value for s is 0.

   * - ``TINYINT``
     - Int8
     - Data type of a 1-byte signed integer with values from -128 to 127.

   * - ``SMALLINT``
     - Int16
     - Data type of a 2-byte signed integer with values from -32,768 to 32,767.

   * - ``INT``
     - Int32
     - Data type of a 4-byte signed integer with values from -2,147,483,648 to 2,147,483,647.

   * - ``BIGINT``
     - Int64
     - Data type of an 8-byte signed integer with values from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

   * - ``FLOAT``
     - Float
     - Data type of a 4-byte single precision floating point number.

       Compared to the SQL standard, the type does not take parameters.

   * - ``DOUBLE``
     - Double
     - Data type of an 8-byte double precision floating point number.

   * - ``DATE``
     - Date32
     - Data type of a date consisting of year-month-day with values ranging from 0000-01-01 to 9999-12-31.

       Compared to the SQL standard, the range starts at year 0000.

   * - ``TIME``

       ``TIME(p)``
     - Time32
     - Data type of a time without time zone consisting of hour:minute:second[.fractional] with up to nanosecond precision and values ranging from 00:00:00.000000000 to 23:59:59.999999999.

       The type can be declared using ``TIME(p)`` where p is the number of digits of
       fractional seconds (precision). p must have a value between 0 and 9
       (both inclusive). If no precision is specified, p is equal to 0.

       Paimon C++ maps every ``TIME(p)`` to an Arrow ``Time32`` in milliseconds
       (``time32[ms]``) and accepts no other Arrow time type. The declared
       precision p is kept in the ``paimon.time.precision`` field metadata; a
       ``Time32`` field without it is ``TIME(0)``. As in Paimon Java, values are
       stored as milliseconds of the day whatever p is, so ``TIME(6)`` and
       ``TIME(9)`` values keep only millisecond precision.

       Both append-only and primary-key tables support TIME columns, as value
       columns, primary keys, partition keys and bucket keys:

       - A TIME partition value is written like Paimon Java: ``HH:mm:ss``
         followed by at most p fractional digits with trailing zeros dropped,
         except that a whole second keeps one digit when p is greater than 0.
         For example ``12:34:56.123`` and ``01:02:03.0`` for ``TIME(3)``, and
         ``01:02:03`` for ``TIME(0)``. With ``partition.legacy-name`` set to
         true, it is the milliseconds of the day.
       - A TIME bucket key is hashed as its int32 milliseconds of the day, like
         Paimon Java.
       - Predicates can reference TIME columns. A TIME literal is created with
         ``Literal(FieldType::TIME, millis)``, where ``millis`` is the
         milliseconds of the day and must be in ``[0, 86400000)``. A query auth
         row filter of a REST catalog table reads a TIME literal like Paimon
         Java ``LocalTime.parse``, such as ``12:34:56.123``.

       An Arrow time column of any unit other than milliseconds is rejected by
       predicates, by partition and bucket keys, and when writing ORC and Avro
       files. ``variant_get`` rejects such a target type too, including one
       nested in a ROW, ARRAY or MAP. Reading ORC and Avro files rejects it
       too, including a read schema such as ``time32[s]`` passed to
       ``SetReadSchema``, so a value is never written or read in another unit.

       TIME statistics are int statistics. A format stats extractor returns an
       ``IntColumnStats`` for a TIME column, and manifest files keep its int32
       values and read them back with the TIME type of the table schema. The
       following table shows the TIME support of each data file format:

       .. list-table::
          :header-rows: 1

          * - Format
            - Physical type, same as Paimon Java
            - Statistics
            - Tested in Paimon C++
            - Verified against Paimon Java
          * - Parquet
            - ``INT32`` annotated as ``TIME(MILLIS)``
            - min and max
            - append-only and primary-key tables
            - read only
          * - ORC
            - ``int``
            - min and max
            - append-only and primary-key tables
            - read only
          * - Avro
            - ``int`` annotated as ``time-millis``; ``TIME(p)`` with p greater
              than 3 is rejected, like Paimon Java
            - empty
            - append-only and primary-key tables
            - read only
          * - Lance
            - ``time32[ms]``
            - empty
            - append-only and primary-key tables
            - no
          * - Vortex
            - ``time32[ms]``
            - empty
            - append-only and primary-key tables, without deletion vectors
            - no
          * - Mosaic
            - ``time32[ms]``
            - min and max of the columns in ``mosaic.stats-columns``, decoded as
              big-endian int32 like Paimon Java
            - append-only and primary-key tables
            - no

       The tests of Paimon C++ cover:

       - Append-only tables: TIME partition keys read with predicates that
         combine the partition key, a TIME value column and ``IS [NOT] NULL``.
       - Primary-key tables: TIME partition, primary and bucket keys with
         several buckets, updates, deletes, full compaction, bucket pruning
         and deletion vectors. Key statistics pruning is tested for Parquet and
         ORC only.

       Lance, Vortex and Mosaic are only tested when Paimon C++ is built with
       them enabled.

       Across languages, "read only" means that Paimon C++ reads append-only
       tables with TIME columns that Paimon Java writes. The following is
       implemented following Paimon Java but not verified against it yet:

       - Paimon Java reading tables that Paimon C++ writes.
       - Paimon C++ reading primary-key tables that Paimon Java writes,
         including TIME partitions, buckets, updates, deletes and compaction.
       - TIME partition values and bucket ids computed by Paimon Java. The tests
         of Paimon C++ compute the expected bucket ids with Paimon C++ itself.

       A TIME string, such as a partition value, is parsed like Paimon Java
       ``BinaryStringUtils.toTime``: an integer number of milliseconds, or
       ``H[H][:m[m][:s[s][.f...]]]`` whose fraction is rounded half up to
       milliseconds, optionally followed by an offset ``[+|-]hh[:mm]`` that is
       added to the time, so ``12:34:56+01:00`` is ``13:34:56``. A string whose
       components are all in range gives the same value as in Paimon Java.
       Unlike Paimon Java, which may return an out-of-range value, Paimon C++
       rejects an hour above 23, a minute, second or offset minute above 59, an
       offset hour above 23, a non-digit in the fraction and a result outside
       the day. It also accepts an ``hh:mm`` offset after ``H`` or ``H:m``,
       such as ``12:34+01:00``, which Paimon Java rejects.

       As in Paimon Java, schema evolution can change a column between TIME
       and ``STRING``, ``TIMESTAMP`` or ``TIMESTAMP WITH LOCAL TIME ZONE``, and
       reading older files casts their values:

       - A string is parsed as described above, and a TIME value is formatted
         as a partition value is, with the precision of the TIME type it was
         written with.
       - A timestamp gives its time of day, and a TIME value becomes that time
         on 1970-01-01, both in the local time zone for ``TIMESTAMP WITH LOCAL
         TIME ZONE``.

       System tables, such as ``$files``, format TIME values in the same way as
       a cast to ``STRING``.

       **Note:** The following are not supported for TIME yet:

       - Column default values (``fields.<name>.default-value``). Paimon C++
         does not validate or apply default values for any type yet.
       - File or global indexes on TIME columns.

   * - ``TIMESTAMP``

       ``TIMESTAMP(p)``
     - Timestamp
     - Data type of a timestamp without time zone consisting of year-month-day hour:minute:second[.fractional] with up to nanosecond precision and values ranging from 0000-01-01 00:00:00.000000000 to 9999-12-31 23:59:59.999999999.

       The type can be declared using ``TIMESTAMP(p)`` where p is the number of
       digits of fractional seconds (precision). Paimon C++ supports precision
       values 0 (seconds), 3 (milliseconds), 6 (microseconds), and 9
       (nanoseconds). If no precision is specified, p is equal to 6.

   * - ``TIMESTAMP WITH LOCAL TIME ZONE``

       ``TIMESTAMP(p) WITH LOCAL TIME ZONE``
     - Timestamp
     - Data type of a timestamp with local time zone consisting of year-month-day hour:minute:second[.fractional] zone with up to nanosecond precision and values ranging from 0000-01-01 00:00:00.000000000 +14:59 to 9999-12-31 23:59:59.999999999 -14:59.

       This type fills the gap between time zone free and time zone mandatory
       timestamp types by allowing the interpretation of UTC timestamps according
       to the configured session time zone. A conversion from and to int describes
       the number of seconds since epoch. A conversion from and to long describes the number of milliseconds since epoch.

       Paimon C++ supports precision values 0 (seconds), 3 (milliseconds), 6
       (microseconds), and 9 (nanoseconds).

       As in Paimon Java, Lance data files store this type as an Arrow
       timestamp with the ``UTC`` time zone whatever the local time zone is,
       and reads return it in the local time zone. Only the time zone of the
       Arrow type changes; the values since the epoch are kept as they are.

   * - ``ARRAY<t>``
     - List
     - Data type of an array of elements with same subtype.

       Compared to the SQL standard, the maximum cardinality of an array cannot be specified but is fixed at 2,147,483,647. Also, any valid type is supported as a subtype.

       The type can be declared using ``ARRAY<t>`` where t is the data type of the contained elements.

   * - ``VECTOR<t, n>``
     - FixedSizeList
     - Data type of a dense vector containing exactly ``n`` elements of type ``t``.

       ``n`` must be positive. ``t`` can be ``BOOLEAN``, ``TINYINT``,
       ``SMALLINT``, ``INT``, ``BIGINT``, ``FLOAT``, or ``DOUBLE``. A VECTOR
       value may be NULL, but its elements cannot be NULL.

       Paimon C++ supports VECTOR value columns in append-only and primary-key
       tables backed by Parquet data files. They use the standard Parquet LIST
       representation on disk and are restored as Arrow ``FixedSizeList``
       values on read. With Vortex enabled at build time, Vortex data files also
       support VECTOR value columns. Row-tracking append-only Parquet tables
       with data evolution support full-row and partial-column VECTOR writes, including reads across
       schema versions that add or drop VECTOR columns. Changing a VECTOR's
       dimension or element type is not supported. VECTOR columns cannot be
       primary, partition, or bucket keys, nor comparator-based
       ordering fields such as sequence and sequence-group fields.

       ``vector.file.format`` stores a single top-level VECTOR column in dedicated
       ``.vector.<format>`` files. Supported formats are ``parquet``, ``lance``,
       and ``vortex``; Lance and Vortex must be enabled at build time.
       This mode requires ``file.format=parquet``, ``bucket=-1``,
       ``row-tracking.enabled=true``, and ``data-evolution.enabled=true``, plus at
       least one ordinary non-partition column. It supports full-row inserts into append-only
       tables. Custom write schemas, partial writes, multiple or nested VECTOR
       columns, and BLOB columns are not supported in this mode.

       VECTOR files roll independently at ``vector.target-file-size``, which
       defaults to ``target-file-size``. Main files roll at ``target-file-size``
       or ``target-file-row-num`` and also close the current VECTOR file. One
       main file can therefore cover the combined row range of several VECTOR files.

       Paimon C++ also reads Parquet files written by Paimon Rust or Python whose
       embedded Arrow schema restores VECTOR columns as ``FixedSizeList``,
       including NULL vector values.

   * - ``MAP<kt, vt>``
     - Map
     - Data type of an associative array that maps keys (including NULL) to values (including NULL). A map cannot contain duplicate keys; each key can map to at most one value.

       There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

       The type can be declared using ``MAP<kt, vt>`` where kt is the data type of the key elements and vt is the data type of the value elements.

       **Note:** Paimon C++ accepts nullable map key declarations for schema compatibility.
       A query fails only if the data actually contains a NULL map key.

   * - ``MULTISET<t>``
     - Not Supported
     - Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its elements with a common subtype. Each unique value (including NULL) is mapped to some multiplicity.

       There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

       The type can be declared using ``MULTISET<t>`` where t is the data type of the contained elements.

   * - ``ROW<n0 t0, n1 t1, ...>``

       ``ROW<n0 t0 'd0', n1 t1 'd1', ...>``
     - Struct
     - Data type of a sequence of fields.

       A field consists of a field name, field type, and an optional description.
       The most specific type of a row of a table is a row type. In this case,
       each column of the row corresponds to the field of the row type that has
       the same ordinal position as the column.

       Compared to the SQL standard, an optional field description simplifies
       the handling with complex structures.

       A row type is similar to the ``STRUCT`` type known from other non-standard-compliant frameworks.

       The type can be declared using ``ROW<n0 t0 'd0', n1 t1 'd1', ...>`` where n
       is the unique name of a field, t is the logical type of a field, d is the description of a field.

   * - ``VARIANT``
     - Struct
     - Data type of semi-structured data (e.g. JSON). A variant value contains one of:
       a primitive (e.g. integer, string), an array of variant values, or an object
       mapping string keys to variant values.

       Variant values are encoded with two binaries following the parquet-format
       `Variant Binary Encoding <https://github.com/apache/parquet-format/blob/master/VariantEncoding.md>`_
       specification (compatible with the Java Paimon / Spark implementation). In
       Paimon C++, a variant field is represented in an Arrow schema as
       ``Struct{value: Binary NOT NULL, metadata: Binary NOT NULL}`` marked with
       Paimon-specific field metadata; use ``paimon::Variant::ArrowField`` to
       construct such a field, and ``paimon::Variant`` (``FromJson``/``ToJson``/
       ``VariantGet``) to build and inspect values.

       Only the parquet file format supports VARIANT columns. VARIANT cannot be
       used as a primary key, partition key or bucket key, and no predicate
       pushdown applies to it.

       When writing, variant columns can optionally be *shredded* into typed
       parquet columns per the parquet-format
       `Variant Shredding <https://github.com/apache/parquet-format/blob/master/VariantShredding.md>`_
       specification by setting ``variant.shreddingSchema`` to a ROW type JSON
       whose fields map top-level variant column names to their shredding
       types. Explicit field IDs are preserved. If every ROW field, including
       nested fields, omits ``id``, IDs are assigned in preorder starting at 0,
       as in Java Paimon. Partially specified IDs are rejected.
       Alternatively, setting ``variant.inferShreddingSchema`` to
       ``true`` infers a shredding schema per file from the first written rows
       (tuned by ``variant.shredding.maxSchemaWidth``, which bounds the total
       number of shredded fields across all variant columns of the schema,
       ``variant.shredding.maxSchemaDepth``,
       ``variant.shredding.minFieldCardinalityRatio`` and
       ``variant.shredding.maxInferBufferRow``). Inference also covers variant
       columns nested inside ROW columns (variants inside arrays or maps stay
       unshredded, as in Java Paimon). Readers reassemble shredded columns
       transparently.

       When reading, instead of the full variant, specific paths can be
       extracted by replacing the variant column in the read schema with a
       projection built by ``paimon::VariantAccessBuilder`` (e.g. ``$.a.b`` as
       BIGINT). For shredded files only the required typed sub-columns are
       read.

   * - ``BLOB``

       ``ARRAY<BLOB>``

       ``MAP<kt, BLOB>``
     - LargeBinary

       List<LargeBinary>

       Map<kt, LargeBinary>
     - Data type of a binary large object, such as an image or a video, stored
       in dedicated ``.blob`` files instead of the normal data files. A BLOB
       column listed in ``blob-descriptor-field`` or ``blob-view-field`` instead
       keeps a serialized reference in the data file, so the referenced data
       must remain available.

       A BLOB field is a ``LargeBinary`` field marked with Paimon-specific
       field metadata; use ``paimon::Blob::ArrowField`` to construct such a
       field. An ``ARRAY<BLOB>`` field is a top-level ``List`` field whose
       element field is a BLOB field, which stores an ordered collection of
       objects (e.g. the frames of one sample) in a single blob entry. Both the
       array and its elements may be null. A ``MAP<kt, BLOB>`` field is a
       top-level ``Map`` field whose values are ``LargeBinary``, which stores
       keyed objects (e.g. the views of one sample) in a single blob entry. Both
       the map and its values may be null, while its keys must be unique and
       non-null. ``kt`` must be BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, DATE,
       DECIMAL, CHAR, VARCHAR, BINARY or VARBINARY.

       Tables with BLOB, ``ARRAY<BLOB>`` or ``MAP<kt, BLOB>`` columns must
       enable ``row-tracking.enabled`` and ``data-evolution.enabled``, and must
       have at least one non-BLOB column. These columns cannot be partition
       keys. ``ARRAY<BLOB>`` and ``MAP<kt, BLOB>`` cannot be nested inside other
       types or listed in ``blob-descriptor-field`` or ``blob-view-field``.
       Paimon C++ does not support compacting these tables yet. See :doc:`write`
       for writing BLOB columns.
