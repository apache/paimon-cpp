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

#include "paimon/common/global_index/btree/lazy_filtered_btree_reader.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_global_index_reader.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/global_index/sorted_index_file_meta.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/sst/block_cache.h"
#include "paimon/common/sst/sst_file_reader.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/io/buffered_input_stream.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
namespace {

using FullRangeBounds = std::optional<std::pair<Literal, Literal>>;

Result<FullRangeBounds> GetFullRangeBounds(const std::vector<GlobalIndexIOMeta>& files,
                                           int64_t total_row_count,
                                           const std::shared_ptr<KeySerializer>& key_serializer,
                                           MemoryPool* pool) {
    if (total_row_count <= 0 || files.empty()) {
        return std::optional<std::pair<Literal, Literal>>();
    }
    int64_t remaining = total_row_count;
    std::optional<Literal> min_key;
    std::optional<Literal> max_key;
    for (const GlobalIndexIOMeta& file : files) {
        // A file's min/max does not identify its row IDs. Only skip the entire reader when its
        // scalar entries cover the complete local row-ID domain without gaps.
        if (file.row_count < 0 || file.row_count > remaining) {
            return std::optional<std::pair<Literal, Literal>>();
        }
        remaining -= file.row_count;
        if (file.metadata == nullptr) {
            return std::optional<std::pair<Literal, Literal>>();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SortedIndexFileMeta> meta,
                               SortedIndexFileMeta::Deserialize(file.metadata, pool));
        if (meta->HasNulls() || meta->FirstKey() == nullptr || meta->LastKey() == nullptr) {
            return std::optional<std::pair<Literal, Literal>>();
        }
        PAIMON_ASSIGN_OR_RAISE(Literal first,
                               key_serializer->Deserialize(MemorySlice::Wrap(meta->FirstKey())));
        PAIMON_ASSIGN_OR_RAISE(Literal last,
                               key_serializer->Deserialize(MemorySlice::Wrap(meta->LastKey())));
        if (!min_key) {
            min_key = first;
        } else {
            PAIMON_ASSIGN_OR_RAISE(int32_t compare, first.CompareTo(min_key.value()));
            if (compare < 0) {
                min_key = first;
            }
        }
        if (!max_key) {
            max_key = last;
        } else {
            PAIMON_ASSIGN_OR_RAISE(int32_t compare, last.CompareTo(max_key.value()));
            if (compare > 0) {
                max_key = last;
            }
        }
    }
    if (remaining != 0) {
        return std::optional<std::pair<Literal, Literal>>();
    }
    return std::optional<std::pair<Literal, Literal>>(
        std::make_pair(std::move(min_key.value()), std::move(max_key.value())));
}

std::optional<RoaringBitmap64> CreateRowIdFilter(
    const std::optional<std::vector<Range>>& row_ranges) {
    if (!row_ranges) {
        return std::nullopt;
    }
    RoaringBitmap64 filter;
    for (const Range& range : row_ranges.value()) {
        filter.AddRange(range.from, range.to + 1);
    }
    return filter;
}

}  // namespace

Result<std::shared_ptr<LazyFilteredBTreeReader>> LazyFilteredBTreeReader::Create(
    std::optional<int32_t> read_buffer_size, const std::vector<GlobalIndexIOMeta>& files,
    const std::shared_ptr<arrow::DataType>& key_type,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::shared_ptr<CacheManager>& cache_manager, int64_t fallback_scan_max_size,
    int64_t total_row_count, const std::optional<std::vector<Range>>& row_ranges,
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Executor>& executor) {
    if (total_row_count < 0) {
        return Status::Invalid("Total row count must be non-negative.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeySerializer> key_serializer,
                           KeySerializer::Create(key_type, pool));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortedFileMetaSelector> file_selector,
                           SortedFileMetaSelector::Create(files, key_serializer));
    PAIMON_ASSIGN_OR_RAISE(FullRangeBounds full_range_bounds,
                           GetFullRangeBounds(files, total_row_count, key_serializer, pool.get()));
    return std::shared_ptr<LazyFilteredBTreeReader>(new LazyFilteredBTreeReader(
        read_buffer_size, std::move(file_selector), key_serializer, file_reader, cache_manager,
        fallback_scan_max_size, total_row_count, std::move(full_range_bounds),
        CreateRowIdFilter(row_ranges), pool, executor));
}

LazyFilteredBTreeReader::LazyFilteredBTreeReader(
    std::optional<int32_t> read_buffer_size, std::unique_ptr<SortedFileMetaSelector> file_selector,
    const std::shared_ptr<KeySerializer>& key_serializer,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::shared_ptr<CacheManager>& cache_manager, int64_t fallback_scan_max_size,
    int64_t total_row_count, std::optional<std::pair<Literal, Literal>> full_range_bounds,
    std::optional<RoaringBitmap64> row_id_filter, const std::shared_ptr<MemoryPool>& pool,
    const std::shared_ptr<Executor>& executor)
    : SortedFileGlobalIndexReader(std::move(file_selector), fallback_scan_max_size, total_row_count,
                                  executor),
      read_buffer_size_(read_buffer_size),
      pool_(pool),
      key_serializer_(key_serializer),
      file_reader_(file_reader),
      cache_manager_(cache_manager),
      total_row_count_(total_row_count),
      full_range_bounds_(std::move(full_range_bounds)),
      row_id_filter_(std::move(row_id_filter)) {}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(bool all_match,
                           AllKeysMatch([&literal](const Literal& key) -> Result<bool> {
                               if (literal.IsNull()) {
                                   return false;
                               }
                               PAIMON_ASSIGN_OR_RAISE(int32_t compare, key.CompareTo(literal));
                               return compare == 0;
                           }));
    return all_match ? FullResult() : SortedFileGlobalIndexReader::VisitEqual(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitLessThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(bool all_match,
                           AllKeysMatch([&literal](const Literal& key) -> Result<bool> {
                               if (literal.IsNull()) {
                                   return false;
                               }
                               PAIMON_ASSIGN_OR_RAISE(int32_t compare, key.CompareTo(literal));
                               return compare < 0;
                           }));
    return all_match ? FullResult() : SortedFileGlobalIndexReader::VisitLessThan(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitLessOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(bool all_match,
                           AllKeysMatch([&literal](const Literal& key) -> Result<bool> {
                               if (literal.IsNull()) {
                                   return false;
                               }
                               PAIMON_ASSIGN_OR_RAISE(int32_t compare, key.CompareTo(literal));
                               return compare <= 0;
                           }));
    return all_match ? FullResult() : SortedFileGlobalIndexReader::VisitLessOrEqual(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitGreaterThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(bool all_match,
                           AllKeysMatch([&literal](const Literal& key) -> Result<bool> {
                               if (literal.IsNull()) {
                                   return false;
                               }
                               PAIMON_ASSIGN_OR_RAISE(int32_t compare, key.CompareTo(literal));
                               return compare > 0;
                           }));
    return all_match ? FullResult() : SortedFileGlobalIndexReader::VisitGreaterThan(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::VisitGreaterOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(bool all_match,
                           AllKeysMatch([&literal](const Literal& key) -> Result<bool> {
                               if (literal.IsNull()) {
                                   return false;
                               }
                               PAIMON_ASSIGN_OR_RAISE(int32_t compare, key.CompareTo(literal));
                               return compare >= 0;
                           }));
    return all_match ? FullResult() : SortedFileGlobalIndexReader::VisitGreaterOrEqual(literal);
}

Result<std::shared_ptr<GlobalIndexResult>> LazyFilteredBTreeReader::FullResult() const {
    if (total_row_count_ <= 0) {
        return BitmapGlobalIndexResult::FromRanges({});
    }
    if (row_id_filter_) {
        RoaringBitmap64 filter = row_id_filter_.value();
        return std::make_shared<BitmapGlobalIndexResult>(
            [filter = std::move(filter)]() -> Result<RoaringBitmap64> { return filter; });
    }
    return BitmapGlobalIndexResult::FromRanges({Range(0, total_row_count_ - 1)});
}

Result<bool> LazyFilteredBTreeReader::AllKeysMatch(
    const std::function<Result<bool>(const Literal&)>& predicate) const {
    if (!full_range_bounds_) {
        return false;
    }
    PAIMON_ASSIGN_OR_RAISE(bool min_matches, predicate(full_range_bounds_->first));
    if (!min_matches) {
        return false;
    }
    return predicate(full_range_bounds_->second);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LazyFilteredBTreeReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return Status::Invalid("LazyFilteredBTreeReader does not support vector search");
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LazyFilteredBTreeReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Status::Invalid("LazyFilteredBTreeReader does not support full text search");
}

Result<std::shared_ptr<GlobalIndexReader>> LazyFilteredBTreeReader::OpenReader(
    const GlobalIndexIOMeta& meta) {
    // Create comparator based on field type
    MemorySlice::SliceComparator comparator = key_serializer_->CreateComparator();

    // Get min/max key slices from meta data (keep as slices; Create() will deserialize)
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<SortedIndexFileMeta> index_meta,
                           SortedIndexFileMeta::Deserialize(meta.metadata, pool_.get()));
    std::optional<MemorySlice> min_key_slice;
    std::optional<MemorySlice> max_key_slice;
    if (index_meta->FirstKey()) {
        min_key_slice = MemorySlice::Wrap(index_meta->FirstKey());
    }
    if (index_meta->LastKey()) {
        max_key_slice = MemorySlice::Wrap(index_meta->LastKey());
    }

    // Open input stream and create block cache
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                           file_reader_->GetInputStream(meta.file_path));
    if (read_buffer_size_) {
        input_stream = std::make_shared<BufferedInputStream>(
            input_stream, read_buffer_size_.value(), pool_.get());
    }

    auto block_cache =
        std::make_shared<BlockCache>(meta.file_path, input_stream, cache_manager_, pool_);

    // Read footer
    PAIMON_ASSIGN_OR_RAISE(MemorySegment footer_segment,
                           block_cache->GetBlock(meta.file_size - BTreeFileFooter::kEncodingLength,
                                                 BTreeFileFooter::kEncodingLength, true,
                                                 /*decompress_func=*/nullptr));
    MemorySlice footer_slice = MemorySlice::Wrap(footer_segment);
    MemorySliceInput footer_input = footer_slice.ToInput();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BTreeFileFooter> footer,
                           BTreeFileFooter::Read(&footer_input));

    // Read null bitmap
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 null_bitmap,
                           ReadNullBitmap(block_cache, footer->GetNullBitmapHandle()));

    // Create SST file reader
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<SstFileReader> sst_file_reader,
        SstFileReader::Create(footer->GetIndexBlockHandle(), footer->GetBloomFilterHandle(),
                              comparator, block_cache, pool_));

    return BTreeGlobalIndexReader::Create(sst_file_reader, std::move(null_bitmap), min_key_slice,
                                          max_key_slice, key_serializer_, row_id_filter_);
}

Result<RoaringBitmap64> LazyFilteredBTreeReader::ReadNullBitmap(
    const std::shared_ptr<BlockCache>& cache, const std::optional<BlockHandle>& block_handle) {
    RoaringBitmap64 null_bitmap;
    if (!block_handle.has_value()) {
        return null_bitmap;
    }

    // Read bytes and CRC value
    PAIMON_ASSIGN_OR_RAISE(
        MemorySegment segment,
        cache->GetBlock(block_handle->Offset(), block_handle->Size() + 4, /*is_index=*/false,
                        /*decompress_func=*/nullptr));

    MemorySlice slice = MemorySlice::Wrap(segment);
    MemorySliceInput slice_input = slice.ToInput();

    // Read null bitmap data
    std::shared_ptr<Bytes> null_bitmap_bytes =
        slice_input.ReadSliceView(block_handle->Size()).CopyBytes(pool_.get());

    // Calculate and verify CRC32C checksum
    uint32_t calculated_crc =
        CRC32C::calculate(null_bitmap_bytes->data(), null_bitmap_bytes->size());
    int32_t expected_crc = slice_input.ReadInt();
    if (calculated_crc != static_cast<uint32_t>(expected_crc)) {
        return Status::Invalid(fmt::format(
            "CRC check failure during decoding null bitmap. Expected: {}, Calculated: {}",
            expected_crc, calculated_crc));
    }

    // Deserialize null bitmap
    PAIMON_RETURN_NOT_OK(
        null_bitmap.Deserialize(null_bitmap_bytes->data(), null_bitmap_bytes->size()));
    return null_bitmap;
}

}  // namespace paimon
