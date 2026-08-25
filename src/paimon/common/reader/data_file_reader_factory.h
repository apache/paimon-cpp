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

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "paimon/read_context.h"
#include "paimon/result.h"

namespace paimon {

class Cache;
class Executor;
class FileBatchReader;
class FileSystem;
class MemoryPool;
class ReaderBuilder;

/// Settings applied when a data file is opened, gathered from the read context and the table
/// options. The defaults are a plain read: no cache, no prefetch.
struct DataFileReadOptions {
    /// Block cache the format reader may put what it reads into, or null for none.
    std::shared_ptr<Cache> cache;
    /// Rows a batch holds.
    int32_t read_batch_size = 0;
    /// Whether files are read ahead of the batches being asked for.
    bool prefetch_enabled = false;
    uint32_t prefetch_max_parallel_num = 0;
    uint32_t prefetch_batch_count = 0;
    bool adaptive_prefetch_strategy = false;
    /// Whether the shared read-ahead cache takes over reading ahead from the format itself.
    bool read_ahead_cache_enabled = true;
    CacheConfig cache_config;
};

/// Opens one data file as a `FileBatchReader`.
///
/// Both table paths come through here, so a file is opened the same way whichever of them reached
/// it: a managed table through `AbstractSplitRead`, a format table through `FormatTableRead`. Each
/// arrives with its own files and its own schema, but which format reads a file, what it may
/// cache and whether it is read ahead are decided in one place rather than two that can drift.
class DataFileReaderFactory {
 public:
    DataFileReaderFactory() = delete;
    ~DataFileReaderFactory() = delete;

    /// Builds the reader builder a format reads with. It is kept out of `Open()` so that a split
    /// builds one and reuses it for every file.
    ///
    /// @param format_options The table's options, which the format reads its own settings from.
    ///        Internal blob placeholder options are dropped: only the internal read path may
    ///        enable those, through `extra_format_options`.
    /// @param extra_format_options Options only the caller knows, applied over `format_options`.
    static Result<std::unique_ptr<ReaderBuilder>> CreateReaderBuilder(
        const std::string& format_identifier,
        const std::map<std::string, std::string>& format_options,
        const std::map<std::string, std::string>& extra_format_options,
        const DataFileReadOptions& read_options, const std::shared_ptr<MemoryPool>& pool);

    /// Opens `file_path`, reading ahead when the options ask for it and the format can.
    ///
    /// @param file_size Size of the file in bytes. It is trusted: an object-store read is issued
    ///        against it, so a caller holding a size it does not vouch for should check it against
    ///        the file system first.
    static Result<std::unique_ptr<FileBatchReader>> Open(
        const std::string& format_identifier, const std::string& file_path, int64_t file_size,
        const ReaderBuilder* reader_builder, const DataFileReadOptions& read_options,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<Executor>& executor,
        const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
