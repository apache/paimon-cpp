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

#include "paimon/common/reader/data_file_reader_factory.h"

#include <utility>

#include "paimon/common/data/blob_defs.h"
#include "paimon/common/reader/delegating_prefetch_reader.h"
#include "paimon/common/reader/prefetch_file_batch_reader_impl.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/read_hints.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"

namespace paimon {

namespace {

/// Formats the prefetching reader cannot drive: `blob` is read whole rather than in batches, and
/// `avro` is row-oriented, so neither has the batch boundaries it reads ahead to.
bool FormatSupportsPrefetch(const std::string& format_identifier) {
    return format_identifier != "blob" && format_identifier != "avro";
}

}  // namespace

Result<std::unique_ptr<ReaderBuilder>> DataFileReaderFactory::CreateReaderBuilder(
    const std::string& format_identifier, const std::map<std::string, std::string>& format_options,
    const std::map<std::string, std::string>& extra_format_options,
    const DataFileReadOptions& read_options, const std::shared_ptr<MemoryPool>& pool) {
    std::map<std::string, std::string> options = format_options;
    // The blob placeholder channels are internal: a table option must not enable them, only
    // `extra_format_options` may.
    BlobDefs::EraseInternalPlaceholderOptions(&options);
    for (const auto& [key, value] : extra_format_options) {
        options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> file_format,
                           FileFormatFactory::Get(format_identifier, options));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                           file_format->CreateReaderBuilder(read_options.read_batch_size));
    reader_builder->WithMemoryPool(pool);
    reader_builder->WithCache(read_options.cache);
    // Runtime read state rather than more format options, so each format can adapt: parquet turns
    // its own pre-buffering off when the shared read-ahead cache is reading ahead.
    ReadHints read_hints;
    read_hints.prefetch_enabled = read_options.prefetch_enabled;
    read_hints.read_ahead_cache_enabled = read_options.read_ahead_cache_enabled;
    reader_builder->WithReadHints(read_hints);
    return reader_builder;
}

Result<std::unique_ptr<FileBatchReader>> DataFileReaderFactory::Open(
    const std::string& format_identifier, const std::string& file_path, int64_t file_size,
    const ReaderBuilder* reader_builder, const DataFileReadOptions& read_options,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<Executor>& executor,
    const std::shared_ptr<MemoryPool>& pool) {
    if (read_options.prefetch_enabled && FormatSupportsPrefetch(format_identifier)) {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<PrefetchFileBatchReaderImpl> prefetch_reader,
            PrefetchFileBatchReaderImpl::Create(
                file_path, file_size, reader_builder, file_system,
                read_options.prefetch_max_parallel_num, read_options.read_batch_size,
                read_options.prefetch_batch_count, read_options.adaptive_prefetch_strategy,
                executor,
                /*initialize_read_ranges=*/false, read_options.read_ahead_cache_enabled,
                read_options.cache_config, pool));
        return std::make_unique<DelegatingPrefetchReader>(std::move(prefetch_reader));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                           file_system->Open(FileStatus(file_path, file_size)));
    return reader_builder->Build(input_stream);
}

}  // namespace paimon
