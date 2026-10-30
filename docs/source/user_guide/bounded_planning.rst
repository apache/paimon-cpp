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

Bounded metadata planning
=========================

``TableScan::CreateWithPlanningBudget`` opts into synchronous, finite-byte metadata
planning. ``TableScan::Create`` retains its existing behavior. This API admits
allocations before requesting storage, including allocations outside ``MemoryPool``.
It does not plan first and compare a memory estimate afterwards.

Budget contract
---------------

A ``PlanningBudget`` holds a cumulative requested-byte limit. Every admitted
allocation consumes bytes permanently; freeing storage does not replenish the
budget. Consequently temporary input, parser, decompression and output storage
cannot each reuse the whole limit. This conservative policy bounds their combined
peak and the total admitted allocation work; it can reject a query whose peak live
storage would fit. It measures requested storage, including allocator bookkeeping
added by this planner, rather than process RSS, malloc metadata, alignment padding,
stack storage or filesystem page cache.

The budget covers retained caller-provided path/schema/options, scan and plan
objects, metadata file buffers, RapidJSON DOM and parser stacks, Avro schema lookup
and decoded records, Zstandard stream workspace and output, duplicate detection,
file metadata, split vectors, strings and shared ownership control blocks.
Zstandard frames with unknown decoded length grow output only after admission.
Compressed and decoded storage coexist within the same budget.

Create the budget once per query. Scans may share it; atomic admission never exceeds
the shared limit. Admission does not serialize upstream ``Malloc``/``Free`` calls.
Overlapping scans or destruction of escaped plans/splits require a thread-safe
upstream, such as ``GetDefaultPool()``, or caller synchronization covering both
operations and cleanup. Individual scan objects must not be called concurrently.
The first allocation failure makes the budget terminal.
Exhaustion returns ``CapacityError``; upstream allocation failure returns
``OutOfMemory``. Malformed inputs return an error and unsupported profiles return
``NotImplemented``. No failure returns a partial plan or invokes the unrestricted
planner. A failed scan returns its original failure on subsequent calls; a
successful batch scan produces one plan and then returns end of scan.

Callers create and validate the ``ScanContext`` and retained schema before invoking
this API. Their construction, the fixed budget control object, fixed error Status
messages, and process-owned immutable empty row/statistics constants are outside
the variable planning payload. Existing shared executor, allocator and cache
services supplied through the context are also outside it; this planner schedules
no work on them and reads/writes no shared metadata cache. Creating or growing
those services is not covered. Data reading, metrics retrieval, split serialization
and user copies made after planning require their own allocation policy.

Successful plans and separately retained nonempty splits retain the budget and its
upstream allocator through file-metadata shared owners. Destroying the scan or plan
therefore cannot invalidate an escaped split. Failed planning releases all temporary
storage, including JSON children whose parent allocation failed. Freed storage
still counts in ``BytesAdmitted()``.

Admitted profile
----------------

The first implementation supports Linux with libstdc++ 9 or later, its C++11
string ABI, and statically linked Zstandard with matching headers and static
workspace APIs (including the default bundled Zstandard). Other builds return
``NotImplemented`` from the bounded factory and retain ordinary scan support.
It supports local filesystem tables with:

* An explicitly retained schema and positive numeric ``scan.snapshot-id``.
* Unpartitioned append-only data, ``bucket=-1``, main branch and matching schema ID.
* Avro OCF manifest lists and version-2 ADD-only entries, null or Zstandard codec.
* Parquet or ORC data files without external paths, extra files, embedded indexes,
  deletion vectors or data-evolution metadata.
* No streaming, planning predicates, partition/bucket filters, limit, realtime
  context, catalog/format table or global-index result.

DELETE entries, duplicate ADD identities, different file schemas, unsupported scan
selectors and unsupported codecs fail explicitly. Missing/expired snapshots fail.
Snapshot and schema IDs must match the bound request. Metadata names cannot escape
one filesystem component. Inputs must remain available and immutable throughout
planning, as with a retained snapshot read; this API does not pin snapshot retention
or provide an atomic filesystem snapshot.

Planning emits one ordinary native split per file and omits pruning statistics.
The normal native data reader consumes these splits. This profile deliberately
avoids generic Avro/Arrow decoding, schema discovery, asynchronous planning and
shared caches, because those paths do not route all variable allocations through
an admission allocator. Enabling this opt-in control can change planning latency,
I/O and split grouping; it is not a performance-equivalent switch for the cached
unrestricted planner. It does not change normal scans.

Allocation qualification
------------------------

Parser and decompressor buffers use a PMR resource backed by the supplied budget.
Shared metadata and plan blocks use ``allocate_shared`` with that allocator.
Ordinary SDK publication constructors still require their existing STL types.
Before invoking them the planner reserves GNU C++11 ABI string forward-range/copy
storage (at most length + 1), exact-size vector element storage, the split object
and its GNU shared-pointer control block. Small-string optimization may cause
conservative over-admission. No approximate per-row multiplier is used.

This qualification relies on the GNU implementations of ``basic_string::_M_construct``,
``vector(size_type)`` and ``shared_ptr(Y*)``, not on a portable standard guarantee
about capacities. The Linux static test target links libstdc++ statically so that its out-of-line
string constructors are included in the ``operator new`` hook. A long-string
calibration proves observation and failure injection. The test observes upstream
pool requests during the complete public factory and planning call, and consumes
all retained-input admission before observing publication allocations so those
credits cannot mask a missing reservation. A new standard-library implementation or ABI needs requalification
before expanding this support gate.

Usage
-----

.. code-block:: cpp

   #include "paimon/memory/planning_budget.h"
   #include "paimon/scan_context.h"
   #include "paimon/table/source/table_scan.h"

   // schema_json is retained, validated table schema JSON bound by the caller.
   auto pool = paimon::GetDefaultPool();
   PAIMON_ASSIGN_OR_RAISE(auto budget,
                          paimon::PlanningBudget::Create(32 * 1024 * 1024, pool));
   paimon::ScanContextBuilder builder(local_table_path);
   builder.SetTableSchema(schema_json)
       .WithMemoryPool(pool)
       .AddOption("scan.snapshot-id", "1");
   PAIMON_ASSIGN_OR_RAISE(auto context, builder.Finish());
   PAIMON_ASSIGN_OR_RAISE(auto scan,
                          paimon::TableScan::CreateWithPlanningBudget(
                              std::move(context), budget));
   PAIMON_ASSIGN_OR_RAISE(auto plan, scan->CreatePlan());
   // Feed plan->Splits() to the normal TableRead. Data-reader bytes are separate.

Tests
-----

``paimon-bounded-table-scan-test`` exercises real Parquet full-field and ORC
``id``-projection plan/read parity (existing ORC TIME decoding is outside this change),
exact and insufficient quotas, shared concurrent admission, allocation failures at
every arena and publication allocation, escaped-split ownership, malformed metadata,
wrong writer field types, duplicate fields/files and unsupported profile rejection.
The GNU static target additionally observes actual allocation requests. The existing
core tests exercise the unchanged ordinary scan path.
