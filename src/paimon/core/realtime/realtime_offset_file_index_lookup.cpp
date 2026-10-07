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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/realtime_offset_file_index_lookup.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"

namespace paimon {

Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>> RealtimeOffsetFileIndexLookup::Create(
    const std::shared_ptr<arrow::Schema>& data_schema, int64_t data_schema_id,
    const std::shared_ptr<arrow::Field>& deduplicate_key_field,
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& memory_pool,
    const CoreOptions& options) {
    if (!data_schema || data_schema_id < 0 || !deduplicate_key_field || !path_factory ||
        !file_system || !memory_pool) {
        return Status::Invalid("realtime offset file-index lookup is missing a dependency");
    }
    // TODO(xinyu.lxy): Bitmap is the mandatory first implementation. Select a more suitable
    // high-cardinality offset index (for example BSI) after its exact row-position API is ready.
    for (const std::shared_ptr<DataFileMeta>& file : data_files) {
        if (!file || file->row_count < 0 || file->row_count > RoaringBitmap32::MAX_VALUE) {
            return Status::Invalid("invalid data file for realtime offset lookup");
        }
    }
    return std::shared_ptr<RealtimeOffsetFileIndexLookup>(
        new RealtimeOffsetFileIndexLookup(data_schema, data_schema_id, deduplicate_key_field,
                                          std::vector<std::shared_ptr<DataFileMeta>>(data_files),
                                          path_factory, file_system, memory_pool, options.ToMap()));
}

RealtimeOffsetFileIndexLookup::RealtimeOffsetFileIndexLookup(
    std::shared_ptr<arrow::Schema> data_schema, int64_t data_schema_id,
    std::shared_ptr<arrow::Field> deduplicate_key_field,
    std::vector<std::shared_ptr<DataFileMeta>> data_files,
    std::shared_ptr<DataFilePathFactory> path_factory, std::shared_ptr<FileSystem> file_system,
    std::shared_ptr<MemoryPool> memory_pool, std::map<std::string, std::string> options)
    : data_schema_(std::move(data_schema)),
      data_schema_id_(data_schema_id),
      deduplicate_key_field_(std::move(deduplicate_key_field)),
      data_files_(std::move(data_files)),
      path_factory_(std::move(path_factory)),
      file_system_(std::move(file_system)),
      memory_pool_(std::move(memory_pool)),
      options_(std::move(options)) {}

Result<std::map<std::string, RoaringBitmap32>> RealtimeOffsetFileIndexLookup::LookupFilePositions(
    const RoaringBitmap64& offsets) const {
    std::map<std::string, RoaringBitmap32> result;
    if (offsets.IsEmpty()) {
        return result;
    }
    const std::string& offset_name = SpecialFields::RealtimeOffset().Name();
    const int32_t offset_position = data_schema_->GetFieldIndex(offset_name);
    for (const std::shared_ptr<DataFileMeta>& file : data_files_) {
        std::optional<int32_t> stats_offset_position;
        if (file->value_stats_cols) {
            const auto iter = std::find(file->value_stats_cols->begin(),
                                        file->value_stats_cols->end(), offset_name);
            if (iter != file->value_stats_cols->end()) {
                stats_offset_position =
                    static_cast<int32_t>(iter - file->value_stats_cols->begin());
            }
        } else if (file->schema_id == data_schema_id_) {
            stats_offset_position = offset_position;
        }
        // TODO(xinyu.lxy): Resolve `file->schema_id` to its write-time schema before using stats
        // from an evolved schema. `write_cols` belongs to data evolution, which real-time
        // deduplicate does not support yet.

        const BinaryRow& min_values = file->value_stats.MinValues();
        const BinaryRow& max_values = file->value_stats.MaxValues();
        const bool has_offset_stats = stats_offset_position && *stats_offset_position >= 0 &&
                                      *stats_offset_position < min_values.GetFieldCount() &&
                                      *stats_offset_position < max_values.GetFieldCount() &&
                                      !min_values.IsNullAt(*stats_offset_position) &&
                                      !max_values.IsNullAt(*stats_offset_position) &&
                                      min_values.GetLong(*stats_offset_position) <=
                                          max_values.GetLong(*stats_offset_position);

        std::vector<Literal> literals;
        if (has_offset_stats) {
            const int64_t min_offset = min_values.GetLong(*stats_offset_position);
            const int64_t max_offset = max_values.GetLong(*stats_offset_position);
            for (RoaringBitmap64::Iterator iter = offsets.EqualOrLarger(min_offset);
                 iter != offsets.End() && *iter <= max_offset; ++iter) {
                literals.emplace_back(*iter);
            }
        } else {
            literals.reserve(offsets.Cardinality());
            for (RoaringBitmap64::Iterator iter = offsets.Begin(); iter != offsets.End(); ++iter) {
                literals.emplace_back(*iter);
            }
        }
        if (literals.empty()) {
            continue;
        }

        std::shared_ptr<Predicate> predicate =
            PredicateBuilder::In(offset_position, offset_name, FieldType::BIGINT, literals);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> index_result,
                               FileIndexEvaluator::Evaluate(data_schema_, predicate, path_factory_,
                                                            file, file_system_, memory_pool_));
        PAIMON_ASSIGN_OR_RAISE(bool remains, index_result->IsRemain());
        if (!remains) {
            continue;
        }
        std::shared_ptr<BitmapIndexResult> bitmap =
            std::dynamic_pointer_cast<BitmapIndexResult>(index_result);
        if (!bitmap) {
            return Status::Invalid(
                fmt::format("_REALTIME_OFFSET index for data file {} is not an exact bitmap index",
                            file->file_name));
        }
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* positions, bitmap->GetBitmap());
        if (!positions->IsEmpty()) {
            result.emplace(file->file_name, *positions);
        }
    }
    return result;
}

Result<std::shared_ptr<const KeyOffsetLookup>>
RealtimeOffsetFileIndexLookup::CreateKeyOffsetLookup() const {
    std::vector<std::shared_ptr<const KeyOffsetLookup>> readers;
    readers.reserve(data_files_.size());
    for (const std::shared_ptr<DataFileMeta>& file : data_files_) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
            DataFileKeyOffsetIndexReader::Create(deduplicate_key_field_, file, path_factory_,
                                                 file_system_, memory_pool_, options_));
        readers.push_back(std::move(reader));
    }
    return std::shared_ptr<const KeyOffsetLookup>(new CompositeKeyOffsetLookup(std::move(readers)));
}

Result<std::shared_ptr<RealtimeOffsetFileIndexLookup>> RealtimeOffsetFileIndexLookup::WithDataFiles(
    const std::vector<std::shared_ptr<DataFileMeta>>& data_files) const {
    for (const std::shared_ptr<DataFileMeta>& file : data_files) {
        if (!file || file->row_count < 0 || file->row_count > RoaringBitmap32::MAX_VALUE) {
            return Status::Invalid("invalid data file for realtime offset lookup");
        }
    }
    return std::shared_ptr<RealtimeOffsetFileIndexLookup>(
        new RealtimeOffsetFileIndexLookup(data_schema_, data_schema_id_, deduplicate_key_field_,
                                          std::vector<std::shared_ptr<DataFileMeta>>(data_files),
                                          path_factory_, file_system_, memory_pool_, options_));
}

}  // namespace paimon
