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
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_section.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_file_meta_serializer.h"
#include "paimon/core/table/source/index_full_text_search_split_impl.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {
namespace {
std::shared_ptr<IndexFileMeta> NewIndexFile(
    const std::string& file_name, const std::string& index_type, const Range& range,
    int32_t index_field_id, const std::optional<std::vector<int32_t>>& extra_field_ids,
    const std::shared_ptr<MemoryPool>& pool) {
    GlobalIndexMeta meta(range.from, range.to, index_field_id, extra_field_ids,
                         std::make_shared<Bytes>(R"({"tokenizer":"default"})", pool.get()));
    return std::make_shared<IndexFileMeta>(index_type, file_name, /*file_size=*/128,
                                           /*row_count=*/range.Count(), /*dv_ranges=*/std::nullopt,
                                           /*external_path=*/std::nullopt, meta);
}

std::string ToBytes(MemorySegmentOutputStream* out, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out->Segments(), 0, out->CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

std::string FromHex(const std::string& hex) {
    std::string bytes;
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        bytes.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
    }
    return bytes;
}

std::string SplitBytes(const std::function<void(MemorySegmentOutputStream*)>& write_body,
                       const std::shared_ptr<MemoryPool>& pool) {
    const int64_t snapshot_id = 1;
    const int64_t row_range_start = 0;
    const int64_t row_range_end = 9;
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int64_t>(IndexFullTextSearchSplitImpl::MAGIC);
    out.WriteValue<int32_t>(IndexFullTextSearchSplitImpl::VERSION);
    out.WriteValue(snapshot_id);
    out.WriteString("f0");
    out.WriteValue(row_range_start);
    out.WriteValue(row_range_end);
    write_body(&out);
    return ToBytes(&out, pool);
}
}  // namespace

class FullTextSearchSplitTest : public ::testing::Test {
 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(FullTextSearchSplitTest, TestSerializeRoundTrip) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {
        NewIndexFile("full-text-0.index", "full-text", Range(100, 199), /*index_field_id=*/2,
                     /*extra_field_ids=*/std::vector<int32_t>({1, 3}), pool_),
        NewIndexFile("full-text-1.index", "full-text", Range(100, 199), /*index_field_id=*/2,
                     /*extra_field_ids=*/std::vector<int32_t>({1, 3}), pool_)};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<IndexFullTextSearchSplitImpl> split,
                         IndexFullTextSearchSplitImpl::Create(
                             /*snapshot_id=*/7, "content", /*row_range_start=*/100,
                             /*row_range_end=*/199, files, {Range(100, 119), Range(150, 199)}));

    ASSERT_OK_AND_ASSIGN(std::string bytes, FullTextSearchSplit::Serialize(split, pool_));
    const std::string version_1_index_files = FromHex(
        "00000002"
        "000000C8"
        "0030000000000000090000004000000011000000500000008000000000000000"
        "6400000000000000000000000000000000000000000000006000000068000000"
        "66756C6C2D7465787400000000000000"
        "66756C6C2D746578742D302E696E64657800000000000000"
        "00200000000000006400000000000000C7000000000000000200000000000000"
        "100000003800000017000000480000000000000000000000"
        "02000000000000000100000003000000"
        "7B22746F6B656E697A6572223A2264656661756C74227D00"
        "000000C8"
        "0030000000000000090000004000000011000000500000008000000000000000"
        "6400000000000000000000000000000000000000000000006000000068000000"
        "66756C6C2D7465787400000000000000"
        "66756C6C2D746578742D312E696E64657800000000000000"
        "00200000000000006400000000000000C7000000000000000200000000000000"
        "100000003800000017000000480000000000000000000000"
        "02000000000000000100000003000000"
        "7B22746F6B656E697A6572223A2264656661756C74227D00");
    std::string version_1_bytes = "PFTSPLT1" +
                                  FromHex(
                                      "00000001"
                                      "0000000000000007"
                                      "0007") +
                                  "content" +
                                  FromHex(
                                      "0000000000000064"
                                      "00000000000000C7"
                                      "00000002"
                                      "0000000000000064"
                                      "0000000000000077"
                                      "0000000000000096"
                                      "00000000000000C7") +
                                  version_1_index_files;
    ASSERT_EQ(bytes, version_1_bytes);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FullTextSearchSplit> result,
                         FullTextSearchSplit::Deserialize(bytes.data(), bytes.size(), pool_));
    auto index_split = std::dynamic_pointer_cast<IndexFullTextSearchSplit>(result);
    ASSERT_TRUE(index_split);
    ASSERT_EQ(index_split->SnapshotId(), 7);
    ASSERT_EQ(index_split->ColumnName(), "content");
    ASSERT_EQ(index_split->RowRangeStart(), 100);
    ASSERT_EQ(index_split->RowRangeEnd(), 199);
    ASSERT_EQ(index_split->SearchRowRanges(),
              std::vector<Range>({Range(100, 119), Range(150, 199)}));
    auto impl = std::dynamic_pointer_cast<IndexFullTextSearchSplitImpl>(result);
    ASSERT_TRUE(impl);
    ASSERT_EQ(*impl, *split) << impl->ToString() << " vs " << split->ToString();
    ASSERT_EQ(impl->FullTextIndexFiles().size(), 2u);

    ASSERT_OK_AND_ASSIGN(std::string bytes_again, FullTextSearchSplit::Serialize(result, pool_));
    ASSERT_EQ(bytes_again, bytes);

    std::string longest_column(std::numeric_limits<uint16_t>::max(), 'c');
    ASSERT_OK_AND_ASSIGN(split, IndexFullTextSearchSplitImpl::Create(
                                    /*snapshot_id=*/7, longest_column, /*row_range_start=*/100,
                                    /*row_range_end=*/199, files, {Range(100, 199)}));
    ASSERT_OK_AND_ASSIGN(bytes, FullTextSearchSplit::Serialize(split, pool_));
    ASSERT_OK_AND_ASSIGN(result,
                         FullTextSearchSplit::Deserialize(bytes.data(), bytes.size(), pool_));
    index_split = std::dynamic_pointer_cast<IndexFullTextSearchSplit>(result);
    ASSERT_TRUE(index_split);
    ASSERT_EQ(index_split->ColumnName(), longest_column);
}

TEST_F(FullTextSearchSplitTest, TestDeserializeRejectsInvalidBytes) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {NewIndexFile(
        "full-text-0.index", "full-text", Range(0, 9), /*index_field_id=*/1, std::nullopt, pool_)};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<IndexFullTextSearchSplitImpl> split,
        IndexFullTextSearchSplitImpl::Create(/*snapshot_id=*/1, "f0", 0, 9, files, {Range(0, 9)}));
    ASSERT_OK_AND_ASSIGN(std::string bytes, FullTextSearchSplit::Serialize(split, pool_));

    std::string trailing = bytes + std::string(1, '\0');
    ASSERT_NOK_WITH_MSG(FullTextSearchSplit::Deserialize(trailing.data(), trailing.size(), pool_),
                        "remaining 1 bytes after deserializing");
    for (size_t size = 0; size < bytes.size(); ++size) {
        ASSERT_NOK(FullTextSearchSplit::Deserialize(bytes.data(), size, pool_)) << size;
    }
    ASSERT_NOK_WITH_MSG(FullTextSearchSplit::Deserialize(nullptr, 0, pool_), "null buffer");
    ASSERT_NOK_WITH_MSG(
        FullTextSearchSplit::Deserialize(bytes.data(), std::numeric_limits<size_t>::max(), pool_),
        "exceed the supported maximum");
    {
        MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
        out.WriteValue<int64_t>(12345);
        std::string unknown_magic = ToBytes(&out, pool_);
        ASSERT_NOK_WITH_MSG(
            FullTextSearchSplit::Deserialize(unknown_magic.data(), unknown_magic.size(), pool_),
            "unknown magic number");
    }
    {
        MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
        out.WriteValue<int64_t>(IndexFullTextSearchSplitImpl::MAGIC);
        out.WriteValue<int32_t>(IndexFullTextSearchSplitImpl::VERSION + 1);
        std::string newer_version = ToBytes(&out, pool_);
        ASSERT_NOK_WITH_MSG(
            FullTextSearchSplit::Deserialize(newer_version.data(), newer_version.size(), pool_),
            "unsupported IndexFullTextSearchSplit version: 2");
    }
    ASSERT_NOK_WITH_MSG(FullTextSearchSplit::Serialize(nullptr, pool_),
                        "cannot cast to an index split");
}

TEST_F(FullTextSearchSplitTest, TestDeserializeRejectsCorruptFileLayout) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {
        NewIndexFile("full-text-0.index", "full-text", Range(0, 9), /*index_field_id=*/2,
                     /*extra_field_ids=*/std::vector<int32_t>({1}), pool_)};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<IndexFullTextSearchSplitImpl> split,
        IndexFullTextSearchSplitImpl::Create(/*snapshot_id=*/1, "f0", 0, 9, files, {Range(0, 9)}));
    ASSERT_OK_AND_ASSIGN(std::string bytes, FullTextSearchSplit::Serialize(split, pool_));
    ASSERT_OK(FullTextSearchSplit::Deserialize(bytes.data(), bytes.size(), pool_));

    IndexFileMetaSerializer file_serializer(pool_);
    ASSERT_OK_AND_ASSIGN(BinaryRow file_row, file_serializer.ToRow(files[0]));
    auto slot_of = [](size_t row_start, int32_t arity, size_t field) {
        return row_start + BinaryRow::CalculateBitSetWidthInBytes(arity) + field * 8;
    };
    auto offset_in_slot = [&bytes](size_t slot) {
        int64_t offset_and_size = 0;
        std::memcpy(&offset_and_size, bytes.data() + slot, sizeof(offset_and_size));
        return static_cast<size_t>(offset_and_size >> 32);
    };
    auto deserialize_with = [&](size_t position, const auto& value) {
        std::string corrupt = bytes;
        std::memcpy(corrupt.data() + position, &value, sizeof(value));
        return FullTextSearchSplit::Deserialize(corrupt.data(), corrupt.size(), pool_);
    };
    const size_t index_type_field = 0;
    const size_t global_index_field = 6;
    const size_t extra_field_ids_field = 3;
    const size_t file_start = bytes.size() - file_row.GetSizeInBytes();
    const size_t index_type_slot = slot_of(file_start, file_row.GetFieldCount(), index_type_field);
    const size_t global_index_slot =
        slot_of(file_start, file_row.GetFieldCount(), global_index_field);

    const int64_t outside_row = (static_cast<int64_t>(1000) << 32) | 9;
    ASSERT_NOK_WITH_MSG(deserialize_with(index_type_slot, outside_row),
                        "string of 9 bytes at offset 1000 exceeds");
    const int64_t long_inline =
        BinarySection::HIGHEST_FIRST_BIT | (static_cast<int64_t>(100) << 56);
    ASSERT_NOK_WITH_MSG(deserialize_with(index_type_slot, long_inline),
                        "inline string of 100 bytes");
    ASSERT_NOK_WITH_MSG(deserialize_with(global_index_slot, outside_row),
                        "of 9 bytes at offset 1000 exceeds");

    const size_t global_index_start = file_start + offset_in_slot(global_index_slot);
    const size_t extra_field_ids_slot =
        slot_of(global_index_start, GlobalIndexMeta::NUM_FIELDS, extra_field_ids_field);
    const size_t extra_field_ids_offset = offset_in_slot(extra_field_ids_slot);
    const int64_t short_array = (static_cast<int64_t>(extra_field_ids_offset) << 32) | 2;
    ASSERT_NOK_WITH_MSG(deserialize_with(extra_field_ids_slot, short_array),
                        "binary array of 2 bytes");
    const size_t extra_field_ids_start = global_index_start + extra_field_ids_offset;
    const int32_t element_count = 1000;
    ASSERT_NOK_WITH_MSG(deserialize_with(extra_field_ids_start, element_count),
                        "binary array of 1000 elements");
}

TEST_F(FullTextSearchSplitTest, TestDeserializeRejectsCorruptEntries) {
    auto deserialize = [this](const std::function<void(MemorySegmentOutputStream*)>& write_body) {
        std::string bytes = SplitBytes(write_body, pool_);
        return FullTextSearchSplit::Deserialize(bytes.data(), bytes.size(), pool_);
    };
    auto write_one_range = [](MemorySegmentOutputStream* out) {
        const int32_t range_count = 1;
        const int64_t from = 0;
        const int64_t to = 9;
        out->WriteValue(range_count);
        out->WriteValue(from);
        out->WriteValue(to);
    };

    auto negative_ranges = [](MemorySegmentOutputStream* out) { out->WriteValue<int32_t>(-1); };
    ASSERT_NOK_WITH_MSG(deserialize(negative_ranges), "-1 search row ranges");
    auto too_many_ranges = [](MemorySegmentOutputStream* out) { out->WriteValue<int32_t>(1000); };
    ASSERT_NOK_WITH_MSG(deserialize(too_many_ranges),
                        "1000 search row ranges exceed the remaining 0 bytes");
    auto reversed_range = [](MemorySegmentOutputStream* out) {
        const int32_t range_count = 1;
        const int64_t from = 9;
        const int64_t to = 0;
        out->WriteValue(range_count);
        out->WriteValue(from);
        out->WriteValue(to);
    };
    ASSERT_NOK_WITH_MSG(deserialize(reversed_range),
                        "search row range [9, 0] ends before it starts");

    auto negative_files = [&](MemorySegmentOutputStream* out) {
        write_one_range(out);
        out->WriteValue<int32_t>(-1);
    };
    ASSERT_NOK_WITH_MSG(deserialize(negative_files), "-1 index files");
    auto too_many_files = [&](MemorySegmentOutputStream* out) {
        write_one_range(out);
        out->WriteValue(std::numeric_limits<int32_t>::max());
    };
    ASSERT_NOK_WITH_MSG(deserialize(too_many_files),
                        "2147483647 index files exceed the remaining 0 bytes");
    auto negative_file_length = [&](MemorySegmentOutputStream* out) {
        write_one_range(out);
        out->WriteValue<int32_t>(1);
        out->WriteValue<int32_t>(-1);
    };
    ASSERT_NOK_WITH_MSG(deserialize(negative_file_length), "invalid binary row length -1");
    auto empty_file = [&](MemorySegmentOutputStream* out) {
        write_one_range(out);
        out->WriteValue<int32_t>(1);
        out->WriteValue<int32_t>(0);
    };
    ASSERT_NOK_WITH_MSG(deserialize(empty_file), "binary row of 0 bytes without its");
}

TEST_F(FullTextSearchSplitTest, TestCreateValidatesArguments) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {
        NewIndexFile("full-text-0.index", "full-text", Range(10, 19), /*index_field_id=*/1,
                     std::nullopt, pool_)};
    auto create = [&](const std::vector<std::shared_ptr<IndexFileMeta>>& index_files,
                      const std::vector<Range>& search_row_ranges) {
        return IndexFullTextSearchSplitImpl::Create(/*snapshot_id=*/1, "f0", 10, 19, index_files,
                                                    search_row_ranges);
    };
    ASSERT_OK(create(files, {Range(10, 11), Range(13, 19)}));
    ASSERT_NOK_WITH_MSG(create({}, {Range(10, 19)}), "at least one index file");
    ASSERT_NOK_WITH_MSG(create(files, {}), "at least one search row range");
    ASSERT_NOK_WITH_MSG(create(files, {Range(9, 19)}), "outside physical index range [10, 19]");
    ASSERT_NOK_WITH_MSG(create(files, {Range(10, 20)}), "outside physical index range [10, 19]");
    ASSERT_NOK_WITH_MSG(create(files, {Range(15, 19), Range(10, 12)}),
                        "not sorted, disjoint and non-adjacent");
    ASSERT_NOK_WITH_MSG(create(files, {Range(10, 12), Range(13, 19)}),
                        "not sorted, disjoint and non-adjacent");
    ASSERT_NOK_WITH_MSG(create(files, {Range(10, 15), Range(15, 19)}),
                        "not sorted, disjoint and non-adjacent");

    auto other_range = NewIndexFile("full-text-1.index", "full-text", Range(10, 29),
                                    /*index_field_id=*/1, std::nullopt, pool_);
    ASSERT_NOK_WITH_MSG(create({files[0], other_range}, {Range(10, 19)}),
                        "not the physical index range");
    auto other_type = NewIndexFile("lucene-0.index", "lucene-fts", Range(10, 19),
                                   /*index_field_id=*/1, std::nullopt, pool_);
    ASSERT_NOK_WITH_MSG(create({files[0], other_type}, {Range(10, 19)}), "different indexes");
    auto empty_extra_fields = NewIndexFile("full-text-4.index", "full-text", Range(10, 19),
                                           /*index_field_id=*/1, std::vector<int32_t>(), pool_);
    ASSERT_OK(create({files[0], empty_extra_fields}, {Range(10, 19)}));
    auto other_fields = NewIndexFile("full-text-2.index", "full-text", Range(10, 19),
                                     /*index_field_id=*/1, std::vector<int32_t>({2}), pool_);
    ASSERT_NOK_WITH_MSG(create({files[0], other_fields}, {Range(10, 19)}), "different indexes");
    auto no_global_meta = std::make_shared<IndexFileMeta>(
        "full-text", "full-text-3.index", /*file_size=*/128, /*row_count=*/10,
        /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
    ASSERT_NOK_WITH_MSG(create({no_global_meta}, {Range(10, 19)}), "global index files");
    ASSERT_NOK_WITH_MSG(
        IndexFullTextSearchSplitImpl::Create(/*snapshot_id=*/1, "", 10, 19, files, {Range(10, 19)}),
        "requires a column name");
    std::string too_long_column(std::numeric_limits<uint16_t>::max() + 1, 'c');
    ASSERT_NOK_WITH_MSG(IndexFullTextSearchSplitImpl::Create(/*snapshot_id=*/1, too_long_column, 10,
                                                             19, files, {Range(10, 19)}),
                        "column name of at most 65535 bytes, but got 65536 bytes");
    std::vector<std::pair<int64_t, int64_t>> invalid_ranges = {
        {-1, 19}, {19, 10}, {0, std::numeric_limits<int64_t>::max()}};
    for (const auto& [start, end] : invalid_ranges) {
        ASSERT_NOK_WITH_MSG(IndexFullTextSearchSplitImpl::Create(/*snapshot_id=*/1, "f0", start,
                                                                 end, files, {Range(10, 19)}),
                            "invalid physical index range");
    }
}

}  // namespace paimon::test
