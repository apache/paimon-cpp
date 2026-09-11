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

============
Format Table
============

.. _cpp-api-format-table:

Table
=====

.. doxygenclass:: paimon::FormatTable
   :members:
   :undoc-members:

Reading and writing a format table go through the generic entry points every other table uses:
:cpp:class:`paimon::TableScan`, :cpp:class:`paimon::TableRead`, :cpp:class:`paimon::FileStoreWrite`
and :cpp:class:`paimon::FileStoreCommit`. See :doc:`../user_guide/format_table`.

Contexts
========

Each of those entry points builds its context from a table path, and the schema under that path
says what kind of table it is. A caller that already holds a ``FormatTable`` hands it over instead,
through the constructor each context builder has for one:

* :cpp:class:`paimon::ScanContextBuilder`\ ``(const std::shared_ptr<FormatTable>&)``
* :cpp:class:`paimon::ReadContextBuilder`\ ``(const std::shared_ptr<FormatTable>&)``
* :cpp:class:`paimon::WriteContextBuilder`\ ``(const std::shared_ptr<FormatTable>&)``
* :cpp:class:`paimon::CommitContextBuilder`\ ``(const std::shared_ptr<FormatTable>&)``

This is the only way to reach a format table whose schema does not live under its own location,
such as one a REST catalog serves: nothing under the location says that it is a format table, nor
that what sits below it is data rather than metadata. The table knows both, so a setting that would
answer either question again - ``SetTableSchema()``, ``WithFileSystem()`` and a branch among them -
is refused by these builders rather than quietly ignored. Options given at the call still win over
the ones the table carries, as they do everywhere else.
