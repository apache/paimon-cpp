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

#include "paimon/core/global_index/data_evolution_global_index_coverage.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/index/global_index_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {
namespace {
std::shared_ptr<IndexFileMeta> NewIndexFile(
    const std::string& file_name, const Range& range, int32_t index_field_id,
    const std::optional<std::vector<int32_t>>& extra_field_ids = std::nullopt) {
    GlobalIndexMeta meta(range.from, range.to, index_field_id, extra_field_ids, nullptr);
    return std::make_shared<IndexFileMeta>("btree", file_name, 128, range.Count(), std::nullopt,
                                           std::nullopt, meta);
}

DataEvolutionGlobalIndexCoverage::DataRangesSupplier Ranges(const std::vector<Range>& ranges) {
    return [ranges]() -> Result<std::vector<Range>> { return ranges; };
}

DataEvolutionGlobalIndexCoverage::DataRangesSupplier Unused() {
    return []() -> Result<std::vector<Range>> {
        return Status::Invalid("data ranges are not needed");
    };
}
}  // namespace

TEST(DataEvolutionGlobalIndexCoverageTest, TestIndexedRangesOfOneField) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {
        NewIndexFile("a", Range(10, 19), 1),
        NewIndexFile("b", Range(0, 9), 1),
        NewIndexFile("overlap", Range(5, 14), 1),
        NewIndexFile("c", Range(30, 39), 1),
        NewIndexFile("other", Range(20, 29), 2),
        std::make_shared<IndexFileMeta>("deletion-vector", "dv", 128, 10, std::nullopt,
                                        std::nullopt)};
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(files, {1}),
              std::vector<Range>({Range(0, 19), Range(30, 39)}));
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(files, {3}), std::vector<Range>());
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(files, {}), std::vector<Range>());
}

TEST(DataEvolutionGlobalIndexCoverageTest, TestIndexedRangesIntersectFields) {
    std::vector<std::shared_ptr<IndexFileMeta>> files = {NewIndexFile("disjoint", Range(50, 59), 0),
                                                         NewIndexFile("a", Range(0, 49), 1),
                                                         NewIndexFile("b", Range(20, 79), 2)};
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(files, {1, 2}),
              std::vector<Range>({Range(20, 49)}));
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(files, {2}),
              std::vector<Range>({Range(20, 79)}));
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(files, {1, 4}), std::vector<Range>());
    ASSERT_TRUE(DataEvolutionGlobalIndexCoverage::IndexedRanges(files, {0, 1, 2}).empty());
}

TEST(DataEvolutionGlobalIndexCoverageTest, TestMultiFieldCoverageMatchesJava) {
    const std::vector<std::shared_ptr<IndexFileMeta>> multi_field_files = {
        NewIndexFile("multi", Range(0, 9), 1, std::vector<int32_t>({2}))};
    for (const std::set<int32_t>& field_ids :
         {std::set<int32_t>{1}, std::set<int32_t>{2}, std::set<int32_t>{1, 2}}) {
        SCOPED_TRACE(::testing::PrintToString(field_ids));
        const std::vector<Range> indexed =
            DataEvolutionGlobalIndexCoverage::IndexedRanges(multi_field_files, field_ids);
        ASSERT_TRUE(indexed.empty());
        ASSERT_OK_AND_ASSIGN(std::vector<Range> full_ranges,
                             DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                                 GlobalIndexSearchMode::FULL, 10, indexed, Unused()));
        ASSERT_EQ(full_ranges, std::vector<Range>({Range(0, 9)}));
        ASSERT_OK_AND_ASSIGN(
            std::vector<Range> detail_ranges,
            DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                GlobalIndexSearchMode::DETAIL, 10, indexed, Ranges({Range(2, 3), Range(7, 9)})));
        ASSERT_EQ(detail_ranges, std::vector<Range>({Range(2, 3), Range(7, 9)}));
    }

    std::vector<std::shared_ptr<IndexFileMeta>> mixed_files = multi_field_files;
    mixed_files.push_back(NewIndexFile("primary", Range(0, 5), 1));
    mixed_files.push_back(NewIndexFile("extra", Range(3, 9), 2, std::vector<int32_t>()));
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(mixed_files, {1}),
              std::vector<Range>({Range(0, 5)}));
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(mixed_files, {2}),
              std::vector<Range>({Range(3, 9)}));
    ASSERT_EQ(DataEvolutionGlobalIndexCoverage::IndexedRanges(mixed_files, {1, 2}),
              std::vector<Range>({Range(3, 5)}));
}

TEST(DataEvolutionGlobalIndexCoverageTest, TestFullExcludesIndexedRangesFromNextRowId) {
    ASSERT_OK_AND_ASSIGN(
        std::vector<Range> ranges,
        DataEvolutionGlobalIndexCoverage::UnindexedRanges(
            GlobalIndexSearchMode::FULL, 100,
            {Range(50, 59), Range(0, 9), Range(10, 19), Range(95, 120)}, Unused()));
    ASSERT_EQ(ranges, std::vector<Range>({Range(20, 49), Range(60, 94)}));

    ASSERT_OK_AND_ASSIGN(ranges, DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                                     GlobalIndexSearchMode::FULL, 100, {Range(0, 99)}, Unused()));
    ASSERT_TRUE(ranges.empty());

    ASSERT_OK_AND_ASSIGN(ranges, DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                                     GlobalIndexSearchMode::FULL, 100, {}, Unused()));
    ASSERT_EQ(ranges, std::vector<Range>({Range(0, 99)}));
}

TEST(DataEvolutionGlobalIndexCoverageTest, TestDetailExcludesIndexedRangesFromDataFiles) {
    ASSERT_OK_AND_ASSIGN(std::vector<Range> ranges,
                         DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                             GlobalIndexSearchMode::DETAIL, 100, {Range(0, 14)},
                             Ranges({Range(60, 69), Range(0, 9), Range(10, 19), Range(65, 79)})));
    ASSERT_EQ(ranges, std::vector<Range>({Range(15, 19), Range(60, 79)}));

    ASSERT_OK_AND_ASSIGN(ranges, DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                                     GlobalIndexSearchMode::DETAIL, 100, {}, Ranges({})));
    ASSERT_TRUE(ranges.empty());
    ASSERT_OK_AND_ASSIGN(ranges, DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                                     GlobalIndexSearchMode::DETAIL, 100, {Range(0, 99)},
                                     Ranges({Range(0, 9), Range(60, 79)})));
    ASSERT_TRUE(ranges.empty());

    ASSERT_NOK_WITH_MSG(DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                            GlobalIndexSearchMode::DETAIL, 100, {}, Unused()),
                        "data ranges are not needed");
}

TEST(DataEvolutionGlobalIndexCoverageTest, TestSkipUnindexedRanges) {
    ASSERT_OK_AND_ASSIGN(std::vector<Range> ranges,
                         DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                             GlobalIndexSearchMode::FAST, 100, {}, Unused()));
    ASSERT_TRUE(ranges.empty());
    for (const std::optional<int64_t>& next_row_id :
         {std::optional<int64_t>(), std::optional<int64_t>(0), std::optional<int64_t>(-1)}) {
        for (GlobalIndexSearchMode mode :
             {GlobalIndexSearchMode::FULL, GlobalIndexSearchMode::DETAIL}) {
            ASSERT_OK_AND_ASSIGN(ranges, DataEvolutionGlobalIndexCoverage::UnindexedRanges(
                                             mode, next_row_id, {}, Unused()));
            ASSERT_TRUE(ranges.empty());
        }
    }
}

}  // namespace paimon::test
