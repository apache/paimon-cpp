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



Parquet Data Range Cache
========================

Repeated reads of immutable Parquet files can reuse asynchronous pre-buffer
ranges through the caller's cache. This is opt-in and does not cache query
results, snapshot discovery, decoded columns, or mutable reader objects.

Configuration
-------------

Set ``parquet.read.enable-data-cache=true`` in the read options and pass a
bounded cache through ``ReadContextBuilder::WithCache()``. Pre-buffering must
also be enabled for these asynchronous reads to use the data cache.

``parquet.read.data-cache.max-range-bytes`` defaults to ``4194304`` (4 MiB)
and must be positive. Larger individual ranges bypass the cache. The caller's
cache controls total resident capacity; this range limit also bounds the extra
copy used when admitting a successfully read range. Concurrent reads and buffers
still retained by readers may use memory beyond the cache's resident capacity.

Only enable this option when a stream URI uniquely identifies immutable content,
as required by ``InputStream::GetUri()``. Reusing the same URI for changed bytes
is incompatible with caching. Paimon's immutable data-file paths satisfy this
requirement; callers reading external files must ensure it themselves.

Cache Integration
-----------------

The provided ``LruCache`` supports non-loading ``Cache::GetIfPresent()`` lookups.
A miss starts the ordinary filesystem asynchronous read and admits the bytes
only after successful completion. Admission failure does not fail the read.
Read errors are never cached. Cache hits retain their byte owner so eviction
cannot invalidate a buffer already returned to a reader.

Custom cache implementations may override ``GetIfPresent()`` to support this
feature. The default returns ``NotImplemented`` and causes the reader to bypass
data caching. Implementations must return a null value for a miss and must not
invoke a loader or wait for storage I/O. Adding this virtual method requires
rebuilding applications and custom cache implementations against the updated
headers and library.

Keys use ``CacheKind::DEFAULT`` and identify the URI, byte offset, and byte count.
Custom routing caches can give data and metadata different capacity budgets.
A single ``LruCache`` shares its configured capacity across all supplied cache
kinds. Exact-range reuse depends on the selected columns and pages; a warm query
with different ranges can still miss. Measure storage bytes, latency, throughput,
and memory under representative cold and warm workloads before enabling it.

Metrics
-------

``GetReaderMetrics()`` exposes cumulative per-reader counters under
``parquet.read.data-cache.``: ``hits``, ``misses``, ``bypasses``, ``hit-bytes``,
``admission-bytes`` and ``admission-failures``. A miss can still fail at storage;
failed reads do not admit bytes. Bypasses include disabled caching, unsupported
custom caches and ineligible ranges. Admission bytes count attempted inserts,
not resident cache size (an oversized insertion may be silently declined).
Counters survive reader close and asynchronous completions retain their owner.
Use ``parquet.read.storage-read-bytes`` alongside hit bytes to quantify avoided
storage I/O. Exact-range misses can occur even when some overlapping bytes are
already cached.
