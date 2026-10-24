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

#include "paimon/core/table/source/data_evolution_full_text_scan.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {
namespace {
constexpr int32_t kTextFieldId = 1;

std::shared_ptr<IndexFileMeta> NewIndexFile(
    const std::string& file_name, const std::string& index_type, const Range& range,
    int32_t index_field_id,
    const std::optional<std::vector<int32_t>>& extra_field_ids = std::nullopt) {
    GlobalIndexMeta meta(range.from, range.to, index_field_id, extra_field_ids,
                         /*index_meta=*/nullptr);
    return std::make_shared<IndexFileMeta>(index_type, file_name, /*file_size=*/128,
                                           /*row_count=*/range.Count(), /*dv_ranges=*/std::nullopt,
                                           /*external_path=*/std::nullopt, meta);
}

struct Selection {
    Range file_range;
    std::vector<std::string> file_names;
    std::vector<Range> search_ranges;

    bool operator==(const Selection& other) const {
        return file_range == other.file_range && file_names == other.file_names &&
               search_ranges == other.search_ranges;
    }
};

std::ostream& operator<<(std::ostream& os, const Selection& selection) {
    os << "{range=" << selection.file_range.ToString() << ", files=[";
    for (const auto& name : selection.file_names) {
        os << name << ",";
    }
    os << "], search=[";
    for (const auto& range : selection.search_ranges) {
        os << range.ToString() << ",";
    }
    return os << "]}";
}

std::vector<Selection> Choose(const std::vector<std::shared_ptr<IndexFileMeta>>& files) {
    std::vector<Selection> selections;
    for (const auto& selection :
         DataEvolutionFullTextScan::ChooseIndexRanges(files, kTextFieldId)) {
        std::vector<std::string> file_names;
        for (const auto& file : selection.files) {
            file_names.push_back(file->FileName());
        }
        selections.push_back(Selection{selection.file_range, file_names, selection.search_ranges});
    }
    return selections;
}
}  // namespace

TEST(DataEvolutionFullTextScanTest, TestIndexIdentityAndIndexedFields) {
    auto dedicated = NewIndexFile("a", "full-text", Range(0, 9), kTextFieldId);
    auto multi_field = NewIndexFile("b", "lumina", Range(0, 9), /*index_field_id=*/2,
                                    std::vector<int32_t>({1, 3}));
    ASSERT_EQ(DataEvolutionFullTextScan::IndexIdentity(*dedicated), "full-text|1|[]");
    ASSERT_EQ(DataEvolutionFullTextScan::IndexIdentity(*multi_field), "lumina|2|[1, 3]");
    auto empty_extra_fields =
        NewIndexFile("c", "full-text", Range(0, 9), kTextFieldId, std::vector<int32_t>());
    ASSERT_EQ(DataEvolutionFullTextScan::IndexIdentity(*empty_extra_fields), "full-text|1|[]");

    const GlobalIndexMeta& meta = multi_field->GetGlobalIndexMeta().value();
    ASSERT_TRUE(DataEvolutionFullTextScan::IndexesField(meta, 1));
    ASSERT_TRUE(DataEvolutionFullTextScan::IndexesField(meta, 2));
    ASSERT_TRUE(DataEvolutionFullTextScan::IndexesField(meta, 3));
    ASSERT_FALSE(DataEvolutionFullTextScan::IndexesField(meta, 4));
}

TEST(DataEvolutionFullTextScanTest, TestDisjointShards) {
    const int64_t max_row_id = std::numeric_limits<int64_t>::max();
    std::vector<std::shared_ptr<IndexFileMeta>> files = {
        NewIndexFile("shard-1", "full-text", Range(100, 199), kTextFieldId),
        NewIndexFile("shard-0-a", "full-text", Range(0, 99), kTextFieldId),
        NewIndexFile("shard-0-b", "full-text", Range(0, 99), kTextFieldId),
        NewIndexFile("shard-3", "full-text", Range(300, 399), kTextFieldId),
        NewIndexFile("shard-4", "full-text", Range(400, max_row_id), kTextFieldId),
        NewIndexFile("other", "full-text", Range(0, 399), /*index_field_id=*/5)};
    std::vector<Selection> expected = {
        {Range(0, 99), {"shard-0-a", "shard-0-b"}, {Range(0, 99)}},
        {Range(100, 199), {"shard-1"}, {Range(100, 199)}},
        {Range(300, 399), {"shard-3"}, {Range(300, 399)}},
        {Range(400, max_row_id), {"shard-4"}, {Range(400, max_row_id)}}};
    ASSERT_EQ(Choose(files), expected);
    ASSERT_TRUE(Choose({}).empty());
}

TEST(DataEvolutionFullTextScanTest, TestOverlappingShardsOfOneIndex) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {
        NewIndexFile("0-2", "full-text", Range(0, 2), kTextFieldId),
        NewIndexFile("1-1", "full-text", Range(1, 1), kTextFieldId),
        NewIndexFile("1-3", "full-text", Range(1, 3), kTextFieldId)};
    std::vector<Selection> expected = {{Range(0, 2), {"0-2"}, {Range(0, 2)}},
                                       {Range(1, 3), {"1-3"}, {Range(3, 3)}}};
    ASSERT_EQ(Choose(files), expected);
}

TEST(DataEvolutionFullTextScanTest, TestDedicatedIndexBeforeExtraField) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {
        NewIndexFile("multi", "test-full-text", Range(0, 99), /*index_field_id=*/2,
                     std::vector<int32_t>({kTextFieldId})),
        NewIndexFile("dedicated", "test-full-text", Range(50, 149), kTextFieldId)};
    std::vector<Selection> expected = {{Range(0, 99), {"multi"}, {Range(0, 49)}},
                                       {Range(50, 149), {"dedicated"}, {Range(50, 149)}}};
    ASSERT_EQ(Choose(files), expected);

    ASSERT_EQ(Choose({files[0]}),
              std::vector<Selection>({{Range(0, 99), {"multi"}, {Range(0, 99)}}}));
}

TEST(DataEvolutionFullTextScanTest, TestIdentityOrderBreaksTies) {
    std::vector<std::shared_ptr<IndexFileMeta>> same_range = {
        NewIndexFile("lucene", "lucene-fts", Range(0, 9), kTextFieldId),
        NewIndexFile("full-text", "full-text", Range(0, 9), kTextFieldId)};
    ASSERT_EQ(Choose(same_range),
              std::vector<Selection>({{Range(0, 9), {"full-text"}, {Range(0, 9)}}}));

    std::vector<std::shared_ptr<IndexFileMeta>> lucene_inside = {
        NewIndexFile("full-text", "full-text", Range(0, 99), kTextFieldId),
        NewIndexFile("lucene", "lucene-fts", Range(50, 59), kTextFieldId)};
    ASSERT_EQ(Choose(lucene_inside),
              std::vector<Selection>({{Range(0, 99), {"full-text"}, {Range(0, 99)}}}));

    std::vector<std::shared_ptr<IndexFileMeta>> full_text_inside = {
        NewIndexFile("lucene", "lucene-fts", Range(0, 99), kTextFieldId),
        NewIndexFile("full-text", "full-text", Range(40, 59), kTextFieldId)};
    std::vector<Selection> expected = {{Range(0, 99), {"lucene"}, {Range(0, 39), Range(60, 99)}},
                                       {Range(40, 59), {"full-text"}, {Range(40, 59)}}};
    ASSERT_EQ(Choose(full_text_inside), expected);
}

TEST(DataEvolutionFullTextScanTest, TestServesFullTextSearch) {
    for (const std::string index_type : {"full-text", "lucene-fts"}) {
        ASSERT_NOK_WITH_MSG(
            DataEvolutionFullTextScan::ServesFullTextSearch(index_type, /*indexer=*/nullptr,
                                                            "/warehouse/foo.db/bar",
                                                            /*snapshot_id=*/3),
            "cannot search the " + index_type +
                " index of table /warehouse/foo.db/bar in snapshot 3: Paimon C++ is built without "
                "its global indexer");
    }
    ASSERT_OK_AND_ASSIGN(bool served, DataEvolutionFullTextScan::ServesFullTextSearch(
                                          "no-such-index", /*indexer=*/nullptr,
                                          "/warehouse/foo.db/bar", /*snapshot_id=*/3));
    ASSERT_FALSE(served);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexer> bitmap,
                         GlobalIndexerFactory::Get("bitmap", /*options=*/{}));
    ASSERT_TRUE(bitmap);
    ASSERT_OK_AND_ASSIGN(served, DataEvolutionFullTextScan::ServesFullTextSearch(
                                     "bitmap", bitmap.get(), "/warehouse/foo.db/bar",
                                     /*snapshot_id=*/3));
    ASSERT_FALSE(served);
}

}  // namespace paimon::test
