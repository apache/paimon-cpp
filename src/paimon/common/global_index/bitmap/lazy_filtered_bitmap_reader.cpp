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

#include "paimon/common/global_index/bitmap/lazy_filtered_bitmap_reader.h"

#include <utility>

#include "paimon/common/global_index/bitmap/bitmap_index_reader.h"
#include "paimon/common/global_index/key_serializer.h"

namespace paimon {

Result<std::shared_ptr<LazyFilteredBitmapReader>> LazyFilteredBitmapReader::Create(
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<arrow::DataType>& key_type,
    int64_t fallback_scan_max_size, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor) {
    if (file_reader == nullptr || key_type == nullptr || pool == nullptr) {
        return Status::Invalid(
            "Cannot create LazyFilteredBitmapReader without file reader, key type, and memory "
            "pool.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> key_serializer,
                           KeySerializer::Create(key_type, pool));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortedFileMetaSelector> file_selector,
                           SortedFileMetaSelector::Create(files, key_serializer));
    return std::shared_ptr<LazyFilteredBitmapReader>(
        new LazyFilteredBitmapReader(file_reader, std::move(file_selector), key_serializer,
                                     fallback_scan_max_size, pool, executor));
}

LazyFilteredBitmapReader::LazyFilteredBitmapReader(
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    std::unique_ptr<SortedFileMetaSelector> file_selector,
    const std::shared_ptr<KeySerializer>& key_serializer, int64_t fallback_scan_max_size,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor)
    : SortedFileGlobalIndexReader(std::move(file_selector), fallback_scan_max_size, executor),
      file_reader_(file_reader),
      key_serializer_(key_serializer),
      pool_(pool) {}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LazyFilteredBitmapReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return Status::Invalid("LazyFilteredBitmapReader does not support vector search.");
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBitmapReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Status::Invalid("LazyFilteredBitmapReader does not support full text search.");
}

Result<std::shared_ptr<GlobalIndexReader>> LazyFilteredBitmapReader::OpenReader(
    const GlobalIndexIOMeta& meta) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BitmapIndexReader> reader,
                           BitmapIndexReader::Create(key_serializer_, file_reader_, meta, pool_));
    return std::shared_ptr<GlobalIndexReader>(std::move(reader));
}

}  // namespace paimon
