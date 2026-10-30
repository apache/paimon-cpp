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

Each of those entry points builds its context the way it does for a data table: from the table's
location, through the builder's usual constructor:

* :cpp:class:`paimon::ScanContextBuilder`\ ``(const std::string&)``
* :cpp:class:`paimon::ReadContextBuilder`\ ``(const std::string&)``
* :cpp:class:`paimon::WriteContextBuilder`\ ``(const std::string&)``
* :cpp:class:`paimon::CommitContextBuilder`\ ``(const std::string&)``

The context then goes to the table's own operator - ``NewScan()``, ``NewRead()``, ``NewWrite()`` or
``NewCommit()`` - which serves a table whose schema does not live under its own location as well,
such as one a REST catalog serves: the table carries its schema and the file system it was loaded
through, so nothing under the location has to answer for them. Options given at the call still win
over the ones the table carries, as they do everywhere else.
