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
.. https://github.com/apache/paimon/blob/master/docs/content/append-table/overview.md

Append Only Table
=================
If a table does not have a primary key defined, it is an append table.
Ordinary append writes accept ``INSERT`` rows only and do not apply upserts
or deletes by key.

The experimental real-time append deduplicate mode is an exception. With
``realtime.deduplicate-key-fields`` configured, it also accepts ``UPDATE_AFTER``,
``DELETE`` and ``UPDATE_BEFORE``, using deletion vectors to hide superseded
rows. The table still has no primary key; deduplication is scoped to each
partition-bucket. See :doc:`realtime` for the required schema, options and
shared context.
