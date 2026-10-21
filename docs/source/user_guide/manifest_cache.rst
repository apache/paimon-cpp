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

Manifest Cache
==============

Overview
--------

Paimon C++ caches complete, schema-aligned data manifests as indexed binary rows
in ``ManifestFile``. Rows are grouped by partition, bucket, and total bucket count,
with ordinals preserving the original ADD/DELETE order. Manifest lists and index
manifests continue to use Arrow IPC streams in ``ObjectsFile<T>``. All three use
the public ``Cache`` abstraction, enabled through ``ScanContextBuilder::WithCache()``.

The immutable manifest file path identifies each cache value. Different snapshots
and scan filters can therefore share the same file. A hit skips filesystem reads
and source-format decoding. Partition and bucket selection skips entire groups
before constructing ``ManifestEntry`` or ``DataFileMeta`` objects; row-range
selection can further prune the selected rows. Ordinary, bucket, and row-range
reads share one complete, query-independent cache entry.

A cold load reads the complete manifest and builds its index. The loading reader
consumes the original Arrow batches without a cache serialization round trip.
Cold bucket reads may therefore read more metadata than uncached selective reads.
Without a cache, reads retain the existing source-format reader path. Cache
serialization or admission failures do not prevent reading the source. Obsolete
or malformed data-manifest cache envelopes are rebuilt; predicate errors propagate
without retry. This private cache format does not change persisted manifest files.

Configuration
-------------

Manifest caching is disabled by default. Embedding applications that need it can
provide a custom ``Cache`` implementation and inject it through ``WithCache``.
Manifest reads create cache keys with ``CacheKind::MANIFEST`` internally, so
callers do not need to pass the cache kind through scan or read contexts. The
same cache instance can be reused across multiple scan or read contexts when
process-local sharing is desired.

Example:

.. code-block:: cpp

   class RoutingCache : public paimon::Cache {
    public:
     RoutingCache(std::shared_ptr<paimon::Cache> default_cache,
                  std::shared_ptr<paimon::Cache> manifest_cache)
         : default_cache_(std::move(default_cache)),
           manifest_cache_(std::move(manifest_cache)) {}

     paimon::Result<std::shared_ptr<paimon::CacheValue>> Get(
         const std::shared_ptr<paimon::CacheKey>& key,
         std::function<paimon::Result<std::shared_ptr<paimon::CacheValue>>(
             const std::shared_ptr<paimon::CacheKey>&)> supplier) override {
       return Select(key)->Get(key, std::move(supplier));
     }

     // Put(), Invalidate(), InvalidateAll(), and Size() route in the same way.

    private:
     std::shared_ptr<paimon::Cache> Select(
         const std::shared_ptr<paimon::CacheKey>& key) const {
       return key && key->GetKind() == paimon::CacheKind::MANIFEST
                  ? manifest_cache_
                  : default_cache_;
     }

     std::shared_ptr<paimon::Cache> default_cache_;
     std::shared_ptr<paimon::Cache> manifest_cache_;
   };

   auto cache = std::make_shared<RoutingCache>(
       std::make_shared<MyDefaultCache>(),
       std::make_shared<MyManifestCache>());

   paimon::ScanContextBuilder scan_builder(table_path);
   scan_builder.WithCache(cache);

Passing ``nullptr`` or omitting ``ScanContextBuilder::WithCache()`` leaves
manifest caching disabled.

Cache Implementation Responsibilities
-------------------------------------

Embedding applications can implement hit/miss and eviction statistics in their
``Cache`` implementation. Coordination of concurrent loads for the same key
also belongs to that implementation; the manifest readers do not deduplicate
concurrent cache misses.

Cache implementations should use ``CacheValue::GetMemoryUsage()`` for admission
and eviction accounting. Indexed data manifests charge the serialized bytes and
the retained ``Bytes`` object. Manifest-list and index-manifest IPC values charge
the retained Arrow buffer capacity, including unused space from growth.
``GetSegment().Size()`` describes the serialized payload length. The built-in
``LruCache`` uses the memory usage value; existing cache values without an explicit
allocation size continue to charge their segment length.
