/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <optional>

#include "paimon/format/read_hints.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/type_fwd.h"

namespace paimon {
class Cache;

/// Create a file batch reader based on an input stream. Allows you to specify memory pool.
class PAIMON_EXPORT ReaderBuilder {
 public:
    virtual ~ReaderBuilder() = default;

    /// Set memory pool to use.
    virtual ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) = 0;

    /// Inject a cache for reader-specific immutable metadata.
    virtual ReaderBuilder* WithCache(const std::shared_ptr<Cache>& cache) {
        (void)cache;
        return this;
    }

    /// Inject runtime read state from the framework layer, so the format can adapt
    /// its internal behavior accordingly. When present, the hints describe the
    /// authoritative runtime state of this read; when absent, the format should fall
    /// back to its own options.
    virtual ReaderBuilder* WithReadHints(const std::optional<ReadHints>& hints) {
        (void)hints;
        return this;
    }

    /// Build a file batch reader based on the created `InputStream`.
    ///
    /// Every non-EOF ArrowArray returned by the reader must retain all allocator and plugin
    /// resources needed by its release callback. The array must remain releasable after the
    /// reader has been destroyed.
    virtual Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& path) const = 0;
};

}  // namespace paimon
