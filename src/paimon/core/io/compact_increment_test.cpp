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

#include "paimon/core/io/compact_increment.h"

#include <optional>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/result.h"

namespace paimon::test {

class CompactIncrementTest : public ::testing::Test {
 public:
    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name) {
        return DataFileMeta::ForAppend(file_name, 100, 100, SimpleStats::EmptyStats(), 0, 100, 0,
                                       FileSource::Append(), std::nullopt, std::nullopt,
                                       std::nullopt, std::nullopt)
            .value();
    }
};

TEST_F(CompactIncrementTest, TestCompactBefore) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    std::vector<std::shared_ptr<DataFileMeta>> compact_before = {file1, file2};
    std::vector<std::shared_ptr<DataFileMeta>> compact_after = {file3};
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files = {file4};

    CompactIncrement increment(std::move(compact_before), std::move(compact_after),
                               std::move(changelog_files));

    ASSERT_TRUE(ObjectUtils::Equal(increment.CompactBefore(), {file1, file2}));
}

TEST_F(CompactIncrementTest, TestCompactAfter) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    std::vector<std::shared_ptr<DataFileMeta>> compact_before = {file1};
    std::vector<std::shared_ptr<DataFileMeta>> compact_after = {file2, file3};
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files = {file4};

    CompactIncrement increment(std::move(compact_before), std::move(compact_after),
                               std::move(changelog_files));
    ASSERT_TRUE(ObjectUtils::Equal(increment.CompactAfter(), {file2, file3}));
}

TEST_F(CompactIncrementTest, TestChangelogFiles) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    std::vector<std::shared_ptr<DataFileMeta>> compact_before = {file1};
    std::vector<std::shared_ptr<DataFileMeta>> compact_after = {file2};
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files = {file3, file4};

    CompactIncrement increment(std::move(compact_before), std::move(compact_after),
                               std::move(changelog_files));
    ASSERT_TRUE(ObjectUtils::Equal(increment.ChangelogFiles(), {file3, file4}));
}

TEST_F(CompactIncrementTest, TestIsEmpty) {
    CompactIncrement increment({}, {}, {});
    ASSERT_TRUE(increment.IsEmpty());
}

TEST_F(CompactIncrementTest, TestEqualityOperator) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    std::vector<std::shared_ptr<DataFileMeta>> compact_before1 = {file1};
    std::vector<std::shared_ptr<DataFileMeta>> compact_after1 = {file2};
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files1 = {file3};

    std::vector<std::shared_ptr<DataFileMeta>> compact_before2 = {file1};
    std::vector<std::shared_ptr<DataFileMeta>> compact_after2 = {file2};
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files2 = {file3};

    CompactIncrement increment1(std::move(compact_before1), std::move(compact_after1),
                                std::move(changelog_files1));
    CompactIncrement increment2(std::move(compact_before2), std::move(compact_after2),
                                std::move(changelog_files2));
    ASSERT_EQ(increment1, increment2);
}

TEST_F(CompactIncrementTest, TestToString) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    std::vector<std::shared_ptr<DataFileMeta>> compact_before = {file1, file2};
    std::vector<std::shared_ptr<DataFileMeta>> compact_after = {file3};
    std::vector<std::shared_ptr<DataFileMeta>> changelog_files = {file4};

    CompactIncrement increment(std::move(compact_before), std::move(compact_after),
                               std::move(changelog_files));
    std::string expected =
        "CompactIncrement {compactBefore = file1, file2, compactAfter = file3, changelogFiles = "
        "file4, newIndexFiles = , deletedIndexFiles = }";
    ASSERT_EQ(increment.ToString(), expected);
}

}  // namespace paimon::test
