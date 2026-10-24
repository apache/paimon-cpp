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

#include "paimon/table/source/full_text_search_split.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_section.h"
#include "paimon/common/data/serializer/binary_row_serializer.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/core/index/index_file_meta_serializer.h"
#include "paimon/core/table/source/data_evolution_full_text_scan.h"
#include "paimon/core/table/source/index_full_text_search_split_impl.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_segment.h"
#include "paimon/status.h"

namespace paimon {
namespace {
constexpr int64_t kSearchRowRangeBytes = 2 * sizeof(int64_t);
/// A serialized index file is a binary row that starts with its int32 length.
constexpr int64_t kMinIndexFileBytes = sizeof(int32_t);

/// Checks that `count` entries of at least `min_entry_bytes` bytes each fit in the rest of `in`,
/// so a corrupt count fails before anything is allocated for it.
Status CheckEntryCount(const DataInputStream& in, int32_t count, int64_t min_entry_bytes,
                       const char* entries) {
    if (count < 0) {
        return Status::Invalid(
            fmt::format("invalid full-text search split, {} {}", count, entries));
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t length, in.Length());
    if (count > (length - pos) / min_entry_bytes) {
        return Status::Invalid(
            fmt::format("invalid full-text search split, {} {} exceed the remaining {} bytes",
                        count, entries, length - pos));
    }
    return Status::OK();
}

Status CheckBinaryRowLayout(const MemorySegment& segment, int64_t offset, int64_t size,
                            const arrow::StructType& type);

Status CheckBinaryArrayLayout(const MemorySegment& segment, int64_t offset, int64_t size,
                              const arrow::DataType& element_type);

/// Checks the value of `type` in the 8-byte slot at `slot` of the binary row or array that spans
/// [`base`, `base` + `base_size`): an inline string or binary must fit in the slot, and any other
/// variable-length value must lie within the row or array.
Status CheckSlotLayout(const MemorySegment& segment, int64_t base, int64_t base_size, int64_t slot,
                       const arrow::DataType& type) {
    switch (type.id()) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::DATE32:
        case arrow::Type::INT64:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
            return Status::OK();
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        case arrow::Type::STRUCT:
        case arrow::Type::LIST:
            break;
        default:
            return Status::NotImplemented(fmt::format(
                "cannot check the layout of {} in a full-text search split", type.ToString()));
    }
    auto offset_and_size = segment.GetValue<int64_t>(static_cast<int32_t>(slot));
    bool bytes_type = type.id() == arrow::Type::STRING || type.id() == arrow::Type::BINARY;
    if (bytes_type && (offset_and_size & BinarySection::HIGHEST_FIRST_BIT) != 0) {
        int64_t length = (offset_and_size & BinarySection::HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56;
        if (length >= static_cast<int64_t>(sizeof(int64_t))) {
            return Status::Invalid(fmt::format(
                "invalid full-text search split, inline {} of {} bytes", type.ToString(), length));
        }
        return Status::OK();
    }
    int64_t value_offset = offset_and_size >> 32;
    int64_t value_size = static_cast<int32_t>(offset_and_size);
    if (value_offset < 0 || value_size < 0 || value_offset + value_size > base_size) {
        return Status::Invalid(fmt::format(
            "invalid full-text search split, {} of {} bytes at offset {} exceeds {} bytes",
            type.ToString(), value_size, value_offset, base_size));
    }
    if (type.id() == arrow::Type::STRUCT) {
        return CheckBinaryRowLayout(segment, base + value_offset, value_size,
                                    static_cast<const arrow::StructType&>(type));
    }
    if (type.id() == arrow::Type::LIST) {
        return CheckBinaryArrayLayout(segment, base + value_offset, value_size,
                                      *static_cast<const arrow::ListType&>(type).value_type());
    }
    return Status::OK();
}

/// Checks that the binary row of `type` at [`offset`, `offset` + `size`) of `segment` has its
/// fixed-length part and that its variable-length values lie within it, so that decoding a
/// corrupt row fails instead of reading out of bounds.
Status CheckBinaryRowLayout(const MemorySegment& segment, int64_t offset, int64_t size,
                            const arrow::StructType& type) {
    int32_t arity = type.num_fields();
    int32_t fixed_part_size = BinaryRow::CalculateFixPartSizeInBytes(arity);
    if (size < fixed_part_size) {
        return Status::Invalid(fmt::format(
            "invalid full-text search split, binary row of {} bytes without its {}-byte "
            "fixed-length part",
            size, fixed_part_size));
    }
    int64_t slots = offset + BinaryRow::CalculateBitSetWidthInBytes(arity);
    for (int32_t i = 0; i < arity; ++i) {
        const std::shared_ptr<arrow::Field>& field = type.field(i);
        // Readers check a nullable field for null before reading it.
        if (field->nullable() && MemorySegmentUtils::BitGet(segment, static_cast<int32_t>(offset),
                                                            i + BinaryRow::HEADER_SIZE_IN_BITS)) {
            continue;
        }
        PAIMON_RETURN_NOT_OK(CheckSlotLayout(segment, offset, size,
                                             slots + static_cast<int64_t>(i) * 8, *field->type()));
    }
    return Status::OK();
}

/// Checks the binary array of `element_type` at [`offset`, `offset` + `size`) of `segment` as
/// `CheckBinaryRowLayout()` checks a row.
Status CheckBinaryArrayLayout(const MemorySegment& segment, int64_t offset, int64_t size,
                              const arrow::DataType& element_type) {
    constexpr int64_t kCountBytes = sizeof(int32_t);
    if (size < kCountBytes) {
        return Status::Invalid(
            fmt::format("invalid full-text search split, binary array of {} bytes", size));
    }
    auto count = segment.GetValue<int32_t>(static_cast<int32_t>(offset));
    int32_t element_size = BinaryArrayWriter::GetElementSize(element_type.id());
    // `BinaryArray::CalculateHeaderInBytes()`, in int64 so that a corrupt count cannot overflow.
    int64_t header_size = kCountBytes + (static_cast<int64_t>(count) + 31) / 32 * 4;
    if (count < 0 || header_size + static_cast<int64_t>(count) * element_size > size) {
        return Status::Invalid(
            fmt::format("invalid full-text search split, binary array of {} elements in {} bytes",
                        count, size));
    }
    // Readers such as `RowArrayDataToDvRanges()` read every element without checking for null.
    for (int32_t i = 0; i < count; ++i) {
        PAIMON_RETURN_NOT_OK(CheckSlotLayout(
            segment, offset, size, offset + header_size + static_cast<int64_t>(i) * element_size,
            element_type));
    }
    return Status::OK();
}

/// Reads the index files that `IndexFileMetaSerializer::SerializeList()` wrote. Unlike
/// `DeserializeList()`, it checks the count, and the length and layout of each file, before
/// decoding anything.
Result<std::vector<std::shared_ptr<IndexFileMeta>>> ReadIndexFiles(
    DataInputStream* in, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t count, in->ReadValue<int32_t>());
    PAIMON_RETURN_NOT_OK(CheckEntryCount(*in, count, kMinIndexFileBytes, "index files"));
    const auto& file_type = static_cast<const arrow::StructType&>(*IndexFileMeta::DataType());
    BinaryRowSerializer row_serializer(file_type.num_fields(), pool);
    IndexFileMetaSerializer serializer(pool);
    std::vector<std::shared_ptr<IndexFileMeta>> files;
    files.reserve(count);
    for (int32_t i = 0; i < count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int64_t pos, in->GetPos());
        PAIMON_RETURN_NOT_OK(BinaryRowSerializer::Skip(in));
        PAIMON_RETURN_NOT_OK(in->Seek(pos));
        PAIMON_ASSIGN_OR_RAISE(BinaryRow row, row_serializer.Deserialize(in));
        PAIMON_RETURN_NOT_OK(CheckBinaryRowLayout(row.GetSegment(), row.GetOffset(),
                                                  row.GetSizeInBytes(), file_type));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFileMeta> file, serializer.FromRow(row));
        files.push_back(std::move(file));
    }
    return files;
}
}  // namespace

Result<std::shared_ptr<IndexFullTextSearchSplitImpl>> IndexFullTextSearchSplitImpl::Create(
    int64_t snapshot_id, const std::string& column_name, int64_t row_range_start,
    int64_t row_range_end, std::vector<std::shared_ptr<IndexFileMeta>> full_text_index_files,
    std::vector<Range> search_row_ranges) {
    if (column_name.empty()) {
        return Status::Invalid("full-text search split requires a column name");
    }
    // `Serialize()` writes the column name with a 16-bit length.
    if (column_name.size() > std::numeric_limits<uint16_t>::max()) {
        return Status::Invalid(fmt::format(
            "full-text search split requires a column name of at most {} bytes, but got {} bytes",
            std::numeric_limits<uint16_t>::max(), column_name.size()));
    }
    // The row count of the range must fit in int64, as reads take its `Range::Count()`.
    if (row_range_start < 0 || row_range_start > row_range_end ||
        row_range_end - row_range_start == std::numeric_limits<int64_t>::max()) {
        return Status::Invalid(
            fmt::format("invalid physical index range [{}, {}]", row_range_start, row_range_end));
    }
    if (full_text_index_files.empty()) {
        return Status::Invalid("full-text search split requires at least one index file");
    }
    const std::shared_ptr<IndexFileMeta>& first_file = full_text_index_files.front();
    std::string identity;
    for (const auto& file : full_text_index_files) {
        if (!file || !file->GetGlobalIndexMeta()) {
            return Status::Invalid("full-text search split requires global index files");
        }
        const GlobalIndexMeta& meta = file->GetGlobalIndexMeta().value();
        if (meta.row_range_start != row_range_start || meta.row_range_end != row_range_end) {
            return Status::Invalid(
                fmt::format("index file {} covers [{}, {}], not the physical index range [{}, {}]",
                            file->FileName(), meta.row_range_start, meta.row_range_end,
                            row_range_start, row_range_end));
        }
        // The files of a split share the identity that the scan groups them by.
        std::string file_identity = DataEvolutionFullTextScan::IndexIdentity(*file);
        if (identity.empty()) {
            identity = std::move(file_identity);
        } else if (file_identity != identity) {
            return Status::Invalid(
                fmt::format("index files {} and {} of one full-text search split belong to "
                            "different indexes",
                            first_file->FileName(), file->FileName()));
        }
    }
    if (search_row_ranges.empty()) {
        return Status::Invalid("full-text search split requires at least one search row range");
    }
    for (size_t i = 0; i < search_row_ranges.size(); ++i) {
        const Range& range = search_row_ranges[i];
        if (range.from > range.to || range.from < row_range_start || range.to > row_range_end) {
            return Status::Invalid(
                fmt::format("search range {} is outside physical index range [{}, {}]",
                            range.ToString(), row_range_start, row_range_end));
        }
        if (i == 0) {
            continue;
        }
        // Adjacent ranges are merged, so a range starts after the row following the previous one.
        const Range& previous = search_row_ranges[i - 1];
        if (range.from <= previous.to || range.from - 1 == previous.to) {
            return Status::Invalid(
                fmt::format("search ranges {} and {} are not sorted, disjoint and non-adjacent",
                            previous.ToString(), range.ToString()));
        }
    }
    return std::shared_ptr<IndexFullTextSearchSplitImpl>(new IndexFullTextSearchSplitImpl(
        snapshot_id, column_name, row_range_start, row_range_end, std::move(full_text_index_files),
        std::move(search_row_ranges)));
}

bool IndexFullTextSearchSplitImpl::operator==(const IndexFullTextSearchSplitImpl& other) const {
    if (this == &other) {
        return true;
    }
    if (snapshot_id_ != other.snapshot_id_ || column_name_ != other.column_name_ ||
        row_range_start_ != other.row_range_start_ || row_range_end_ != other.row_range_end_ ||
        search_row_ranges_ != other.search_row_ranges_ ||
        full_text_index_files_.size() != other.full_text_index_files_.size()) {
        return false;
    }
    for (size_t i = 0; i < full_text_index_files_.size(); ++i) {
        if (!(*full_text_index_files_[i] == *other.full_text_index_files_[i])) {
            return false;
        }
    }
    return true;
}

std::string IndexFullTextSearchSplitImpl::ToString() const {
    std::vector<std::string> ranges;
    ranges.reserve(search_row_ranges_.size());
    for (const auto& range : search_row_ranges_) {
        ranges.push_back(range.ToString());
    }
    std::vector<std::string> files;
    files.reserve(full_text_index_files_.size());
    for (const auto& file : full_text_index_files_) {
        files.push_back(file->ToString());
    }
    return fmt::format(
        "IndexFullTextSearchSplit{{snapshotId={}, columnName={}, rowRangeStart={}, "
        "rowRangeEnd={}, searchRowRanges=[{}], fullTextIndexFiles=[{}]}}",
        snapshot_id_, column_name_, row_range_start_, row_range_end_, fmt::join(ranges, ", "),
        fmt::join(files, ", "));
}

Result<std::string> FullTextSearchSplit::Serialize(
    const std::shared_ptr<FullTextSearchSplit>& split, const std::shared_ptr<MemoryPool>& pool) {
    auto index_split = std::dynamic_pointer_cast<IndexFullTextSearchSplitImpl>(split);
    if (!index_split) {
        return Status::Invalid("invalid full-text search split, cannot cast to an index split");
    }
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int64_t>(IndexFullTextSearchSplitImpl::MAGIC);
    out.WriteValue<int32_t>(IndexFullTextSearchSplitImpl::VERSION);
    out.WriteValue<int64_t>(index_split->SnapshotId());
    out.WriteString(index_split->ColumnName());
    out.WriteValue<int64_t>(index_split->RowRangeStart());
    out.WriteValue<int64_t>(index_split->RowRangeEnd());
    const std::vector<Range>& search_row_ranges = index_split->SearchRowRanges();
    out.WriteValue<int32_t>(static_cast<int32_t>(search_row_ranges.size()));
    for (const auto& range : search_row_ranges) {
        out.WriteValue<int64_t>(range.from);
        out.WriteValue<int64_t>(range.to);
    }
    IndexFileMetaSerializer serializer(pool);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(index_split->FullTextIndexFiles(), &out));
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

Result<std::shared_ptr<FullTextSearchSplit>> FullTextSearchSplit::Deserialize(
    const char* buffer, size_t length, const std::shared_ptr<MemoryPool>& pool) {
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream in(input_stream);
    PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
    if (magic != IndexFullTextSearchSplitImpl::MAGIC) {
        return Status::Invalid("invalid full-text search split, unknown magic number");
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t version, in.ReadValue<int32_t>());
    if (version < 1 || version > IndexFullTextSearchSplitImpl::VERSION) {
        return Status::Invalid(
            fmt::format("unsupported IndexFullTextSearchSplit version: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t snapshot_id, in.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(std::string column_name, in.ReadString());
    PAIMON_ASSIGN_OR_RAISE(int64_t row_range_start, in.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t row_range_end, in.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t range_count, in.ReadValue<int32_t>());
    PAIMON_RETURN_NOT_OK(
        CheckEntryCount(in, range_count, kSearchRowRangeBytes, "search row ranges"));
    std::vector<Range> search_row_ranges;
    search_row_ranges.reserve(range_count);
    for (int32_t i = 0; i < range_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(int64_t from, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t to, in.ReadValue<int64_t>());
        if (from > to) {
            return Status::Invalid(fmt::format(
                "invalid full-text search split, search row range [{}, {}] ends before it starts",
                from, to));
        }
        search_row_ranges.emplace_back(from, to);
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<IndexFileMeta>> full_text_index_files,
                           ReadIndexFiles(&in, pool));
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
    if (pos != stream_length) {
        return Status::Invalid(
            fmt::format("invalid full-text search split, remaining {} bytes after deserializing",
                        stream_length - pos));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexFullTextSearchSplitImpl> split,
                           IndexFullTextSearchSplitImpl::Create(
                               snapshot_id, column_name, row_range_start, row_range_end,
                               std::move(full_text_index_files), std::move(search_row_ranges)));
    return std::shared_ptr<FullTextSearchSplit>(std::move(split));
}

}  // namespace paimon
