/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {
class SortMergeReader;
/// Builds one source-backed primary-key index payload for an ordered set of physical data
/// files of a single data level.
///
/// The caller provides all indexed values of the source group as one array sorted by
/// value, together with each value's zero-based ordinal in the ordered source group. The
/// builder writes exactly one payload file and returns its metadata carrying the
/// serialized `PrimaryKeyIndexSourceMeta`, so the payload can later be validated against
/// the active source set of its level.
class PkSortedIndexFile {
 public:
    PkSortedIndexFile() = delete;
    ~PkSortedIndexFile() = delete;

    /// @param field The indexed field.
    /// @param index_type The index algorithm identifier, e.g. "btree".
    /// @param options The resolved index options (algorithm-prefixed keys included).
    /// @param data_level The positive data level covered by the payload.
    /// @param source_files The level's active source files ordered by file name.
    /// @param sorted_values All indexed values of the source group sorted by value; nulls
    ///        may appear anywhere.
    /// @param sorted_ordinals The group ordinal of each value, aligned with
    ///        `sorted_values`; every ordinal in `[0, total source rows)` must appear
    ///        exactly once.
    /// @param file_writer The index-directory file writer of the payload's bucket.
    /// @param is_external_path Whether `file_writer` resolves to an external index path.
    /// @param pool The memory pool used for metadata and index construction.
    /// @return Metadata for the single payload file written by the index builder.
    static Result<std::shared_ptr<IndexFileMeta>> Build(
        const DataField& field, const std::string& index_type,
        const std::map<std::string, std::string>& options, int32_t data_level,
        const std::vector<PrimaryKeyIndexSourceFile>& source_files,
        const std::shared_ptr<arrow::Array>& sorted_values, std::vector<int64_t> sorted_ordinals,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer, bool is_external_path,
        const std::shared_ptr<MemoryPool>& pool);

    /// Builds a payload from a bounded, globally sorted stream. The stream's key contains the
    /// indexed field and its sequence number contains the source-group physical row id.
    static Result<std::shared_ptr<IndexFileMeta>> BuildFromSortedReader(
        const DataField& field, const std::string& index_type,
        const std::map<std::string, std::string>& options, int32_t data_level,
        const std::vector<PrimaryKeyIndexSourceFile>& source_files,
        std::unique_ptr<SortMergeReader>&& sorted_reader,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer, bool is_external_path,
        int32_t write_batch_size, const std::shared_ptr<MemoryPool>& pool);
};

}  // namespace paimon
