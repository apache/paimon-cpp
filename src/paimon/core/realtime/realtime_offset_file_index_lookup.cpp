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

#include <utility>

#include "arrow/api.h"
#include "fmt/format.h"
#include "paimon/common/io/cache/cache_manager.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/core/realtime/realtime_file_schema.h"
#include "paimon/core/realtime/realtime_key_offset_index.h"
#include "paimon/core/realtime/realtime_utils.h"
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
    const std::shared_ptr<FileSystem>& file_system, const CoreOptions& options,
    const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!data_schema || data_schema_id < 0 || !deduplicate_key_field || !path_factory ||
        !file_system || !memory_pool) {
        return Status::Invalid("realtime offset file-index lookup is missing a dependency");
    }
    PAIMON_RETURN_NOT_OK(RealtimeUtils::ValidateOffsetField(data_schema));
    for (const std::shared_ptr<DataFileMeta>& file : data_files) {
        if (!file || file->row_count < 0 || file->row_count > RoaringBitmap32::MAX_VALUE) {
            return Status::Invalid("invalid data file for realtime offset lookup");
        }
    }
    if (options.GetCachePageSize() <= 0 || options.GetLookupCacheMaxMemory() < 0) {
        return Status::Invalid("invalid realtime key-offset cache options");
    }
    auto cache_manager = std::make_shared<CacheManager>(options.GetLookupCacheMaxMemory(),
                                                        options.GetLookupCacheHighPrioPoolRatio());
    return std::shared_ptr<RealtimeOffsetFileIndexLookup>(new RealtimeOffsetFileIndexLookup(
        deduplicate_key_field, std::vector<std::shared_ptr<DataFileMeta>>(data_files), path_factory,
        file_system, options.ToMap(), options.GetCachePageSize(), std::move(cache_manager),
        std::make_shared<RealtimeFileSchema>(data_schema, data_schema_id, schema_manager),
        memory_pool));
}

RealtimeOffsetFileIndexLookup::RealtimeOffsetFileIndexLookup(
    std::shared_ptr<arrow::Field> deduplicate_key_field,
    std::vector<std::shared_ptr<DataFileMeta>> data_files,
    std::shared_ptr<DataFilePathFactory> path_factory, std::shared_ptr<FileSystem> file_system,
    std::map<std::string, std::string> options, uint32_t cache_page_size,
    std::shared_ptr<CacheManager> cache_manager, std::shared_ptr<RealtimeFileSchema> file_schema,
    std::shared_ptr<MemoryPool> memory_pool)
    : deduplicate_key_field_(std::move(deduplicate_key_field)),
      data_files_(std::move(data_files)),
      path_factory_(std::move(path_factory)),
      file_system_(std::move(file_system)),
      memory_pool_(std::move(memory_pool)),
      options_(std::move(options)),
      cache_page_size_(cache_page_size),
      cache_manager_(std::move(cache_manager)),
      file_schema_(std::move(file_schema)) {}

Result<RealtimeOffsetFileIndexLookup::FilePositions>
RealtimeOffsetFileIndexLookup::LookupFilePositions(const RoaringBitmap64& offsets) const {
    FilePositions result;
    if (offsets.IsEmpty()) {
        return result;
    }
    const std::string& offset_name = SpecialFields::RealtimeOffset().Name();
    for (const std::shared_ptr<DataFileMeta>& file : data_files_) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema,
                               file_schema_->GetFileSchema(*file));
        PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> literals,
                               file_schema_->SelectOffsets(*file, offsets));
        if (literals.empty()) {
            continue;
        }

        std::shared_ptr<Predicate> predicate = PredicateBuilder::In(
            schema->GetFieldIndex(offset_name), offset_name, FieldType::BIGINT, literals);
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileIndexResult> index_result,
            FileIndexEvaluator::Evaluate(schema, options_, predicate, path_factory_, file,
                                         file_system_, memory_pool_));
        PAIMON_ASSIGN_OR_RAISE(bool remains, index_result->IsRemain());
        if (!remains) {
            continue;
        }
        std::shared_ptr<BitmapIndexResult> bitmap =
            std::dynamic_pointer_cast<BitmapIndexResult>(index_result);
        if (!bitmap) {
            return Status::Invalid(fmt::format(
                "_REALTIME_OFFSET index for data file {} did not return exact row positions",
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
    std::lock_guard<std::mutex> lock(key_readers_mutex_);
    std::vector<std::shared_ptr<const KeyOffsetLookup>> readers;
    readers.reserve(data_files_.size());
    for (const std::shared_ptr<DataFileMeta>& file : data_files_) {
        const std::string path = path_factory_->ToPath(file);
        auto iter = key_readers_.find(path);
        if (iter == key_readers_.end()) {
            PAIMON_RETURN_NOT_OK(file_schema_->ValidateKey(*file, deduplicate_key_field_));
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFileKeyOffsetIndexReader> reader,
                                   DataFileKeyOffsetIndexReader::Create(
                                       cache_page_size_, cache_manager_, deduplicate_key_field_,
                                       file, path_factory_, file_system_, memory_pool_));
            iter = key_readers_.emplace(path, KeyReader{file, std::move(reader)}).first;
        }
        readers.push_back(iter->second.reader);
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
    auto next = std::shared_ptr<RealtimeOffsetFileIndexLookup>(new RealtimeOffsetFileIndexLookup(
        deduplicate_key_field_, std::vector<std::shared_ptr<DataFileMeta>>(data_files),
        path_factory_, file_system_, options_, cache_page_size_, cache_manager_, file_schema_,
        memory_pool_));
    {
        std::lock_guard<std::mutex> lock(key_readers_mutex_);
        for (const std::shared_ptr<DataFileMeta>& file : data_files) {
            const std::string path = path_factory_->ToPath(file);
            auto iter = key_readers_.find(path);
            if (iter != key_readers_.end() && *iter->second.file == *file) {
                next->key_readers_.emplace(path, KeyReader{file, iter->second.reader});
            }
        }
    }
    return next;
}

}  // namespace paimon
