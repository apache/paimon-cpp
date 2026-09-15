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

Metrics reference
=================

``Metrics`` contains counters, gauges, and histogram snapshots. A counter is a non-negative
integer, a gauge represents current state, and a histogram records a distribution of observed
values. The scan and prefetch-reader metrics described below are returned as point-in-time
snapshots. Modifying a returned snapshot does not modify the component that produced it.

Scan planning
-------------

The names below are declared by ``ScanMetrics``. ``last*`` counters are replaced after each
successful ``CreatePlan()`` call. Histograms and the cache hit/miss counters accumulate for the
lifetime of the scan. They are unrelated to ``SetReadSchema()`` and ``ReadAheadCache::Reset()``.
The first six names match Java ``ScanMetrics``; the remaining names are C++-only.

.. csv-table::
   :header: "Name", "Type", "Unit", "Meaning"
   :widths: 34, 12, 12, 52

   "lastScanDuration", "counter", "milliseconds", "Duration of the last successful plan"
   "scanDuration", "histogram", "milliseconds", "Distribution of successful plan durations"
   "lastScannedSnapshotId", "counter", "snapshot ID", "Snapshot used by the last plan, or 0"
   "lastScannedManifests", "counter", "files", "Manifest files selected by the last plan"
   "lastScanSkippedTableFiles", "counter", "files", "Table files skipped by the last plan"
   "lastScanResultedTableFiles", "counter", "files", "Table files returned by the last plan"
   "lastManifestReadDuration", "counter", "milliseconds", "Manifest-list and entry read time for the last plan"
   "manifestReadDuration", "histogram", "milliseconds", "Distribution of manifest read times"
   "lastSnapshotCacheEnabled", "counter", "boolean", "Whether snapshot manifest-entry cache was eligible"
   "lastSnapshotCacheHit", "counter", "boolean", "Whether the last eligible lookup hit"
   "snapshotCacheHits", "counter", "lookups", "Cumulative exact-snapshot cache hits"
   "snapshotCacheMisses", "counter", "lookups", "Cumulative eligible cache misses"
   "lastSnapshotCacheLoadDuration", "counter", "milliseconds", "Cache load time for the last plan"
   "snapshotCacheLoadDuration", "histogram", "milliseconds", "Distribution of cache load times"
   "lastSnapshotCacheStoreDuration", "counter", "milliseconds", "Cache store time for the last plan; 0 when not stored"
   "snapshotCacheStoreDuration", "histogram", "milliseconds", "Distribution of cache store times"
   "lastLazyDecodeScannedRows", "counter", "manifest rows", "Candidate manifest rows inspected by the last plan"
   "lastLazyDecodeMaterializedRows", "counter", "manifest rows", "Manifest rows retained after lazy filtering"

Prefetch reader
---------------

The names below are declared by ``PrefetchMetrics``. Counters and histograms accumulate for the
lifetime of the prefetch reader, including across ``SetReadSchema()``. ``enabled`` and
``parallelism`` describe the most recently initialized schema. ``queue-depth`` is reset by
``SetReadSchema()`` and ``Close()``; ``queue-depth.max`` remains the lifetime maximum.
These metrics are C++-only and have no counterparts in Java Paimon.

.. csv-table::
   :header: "Name", "Type", "Unit", "Meaning"
   :widths: 38, 12, 12, 48

   "prefetch.enabled", "gauge", "boolean", "Whether the most recently initialized schema selected prefetch"
   "prefetch.parallelism", "gauge", "readers", "Effective reader parallelism"
   "prefetch.read-ranges.total", "counter", "ranges", "Generated ranges before bitmap filtering"
   "prefetch.read-ranges.after-bitmap", "counter", "ranges", "Ranges retained after bitmap filtering"
   "prefetch.seek.count", "counter", "operations", "Underlying reader seek operations"
   "prefetch.produced-batches", "counter", "batches", "Data batches placed into prefetch queues"
   "prefetch.consumed-batches", "counter", "batches", "Data batches returned to the consumer"
   "prefetch.discarded-batches", "counter", "batches", "Data batches released without consumption, plus EOF entries released during cleanup"
   "prefetch.errors", "counter", "errors", "Errors recorded by the background prefetch loop"
   "prefetch.adaptive-disabled-count", "counter", "decisions", "Times adaptive strategy disabled prefetch"
   "prefetch.queue-full-count", "counter", "events", "Times production found a full queue"
   "prefetch.queue-depth", "gauge", "queue entries", "Current queued entries, including retained EOF markers"
   "prefetch.queue-depth.max", "gauge", "queue entries", "Maximum queued entries in the reader lifetime"
   "prefetch.reader-read-latency-us", "histogram", "microseconds", "Underlying reader batch latency"
   "prefetch.consumer-wait-latency-us", "histogram", "microseconds", "Consumer wait latency per returned batch or EOF"

Prefetch I/O
------------

``PrefetchIoMetrics`` describes only I/O that passes through the prefetch reader's instrumented
input streams. It is not a whole-query or whole-table I/O total. All counters accumulate for the
reader lifetime and are retained across ``SetReadSchema()`` and cache reset. Latency uses relaxed
atomic count and sum counters instead of per-I/O histograms to reduce hot-path cost. Collection is
disabled by default; set ``prefetch.io-metrics.enabled`` to ``true`` in the read options to enable
it. When disabled, these per-I/O metrics are absent and the input streams have no metrics
instrumentation.
``io.async.pending`` is current state and returns to zero when all callbacks complete.
These metrics are C++-only and have no counterparts in Java Paimon.

.. csv-table::
   :header: "Name", "Type", "Unit", "Meaning"
   :widths: 34, 12, 12, 52

   "io.read.requests", "counter", "requests", "Synchronous read requests"
   "io.read.requested-bytes", "counter", "bytes", "Bytes requested by synchronous reads"
   "io.read.physical-bytes", "counter", "bytes", "Bytes returned by successful synchronous reads"
   "io.read.failed", "counter", "requests", "Failed synchronous reads"
   "io.read.latency.count", "counter", "requests", "Completed synchronous read latency samples"
   "io.read.latency.sum-us", "counter", "microseconds", "Sum of synchronous read latency"
   "io.async.requests", "counter", "requests", "Asynchronous read requests"
   "io.async.requested-bytes", "counter", "bytes", "Bytes requested by asynchronous reads"
   "io.async.physical-bytes", "counter", "bytes", "Bytes attributed to successful asynchronous reads"
   "io.async.completed", "counter", "requests", "Successful asynchronous reads"
   "io.async.failed", "counter", "requests", "Failed asynchronous reads"
   "io.async.pending", "gauge", "requests", "Asynchronous callbacks not yet completed"
   "io.async.latency.count", "counter", "requests", "Completed asynchronous callback latency samples"
   "io.async.latency.sum-us", "counter", "microseconds", "Sum of asynchronous callback latency"
