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

#include "paimon/core/io/data_increment.h"

#include <optional>

#include "gtest/gtest.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/result.h"

namespace paimon::test {

class DataIncrementTest : public ::testing::Test {
 public:
    std::shared_ptr<DataFileMeta> CreateDataFileMeta(const std::string& file_name) {
        return DataFileMeta::ForAppend(file_name, 100, 100, SimpleStats::EmptyStats(), 0, 100, 0,
                                       FileSource::Append(), std::nullopt, std::nullopt,
                                       std::nullopt, std::nullopt)
            .value();
    }
};

TEST_F(DataIncrementTest, TestNewFiles) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");

    DataIncrement increment({file1, file2}, {}, {});
    ASSERT_EQ(increment.NewFiles().size(), 2);
    ASSERT_EQ(increment.NewFiles()[0]->file_name, "file1");
    ASSERT_EQ(increment.NewFiles()[1]->file_name, "file2");
}

TEST_F(DataIncrementTest, TestDeletedFiles) {
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");

    DataIncrement increment({}, {file3}, {});
    ASSERT_EQ(increment.DeletedFiles().size(), 1);
    ASSERT_EQ(increment.DeletedFiles()[0]->file_name, "file3");
}

TEST_F(DataIncrementTest, TestChangelogFiles) {
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    DataIncrement increment({}, {}, {file4});
    ASSERT_EQ(increment.ChangelogFiles().size(), 1);
    ASSERT_EQ(increment.ChangelogFiles()[0]->file_name, "file4");
}

TEST_F(DataIncrementTest, TestIsEmpty) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    DataIncrement increment1({}, {}, {});
    ASSERT_TRUE(increment1.IsEmpty());

    DataIncrement increment2({file1}, {}, {});
    ASSERT_FALSE(increment2.IsEmpty());

    DataIncrement increment3({}, {}, {file4});
    ASSERT_FALSE(increment3.IsEmpty());
}

TEST_F(DataIncrementTest, TestEqualityOperator) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    DataIncrement increment1({file1}, {file3}, {file4});
    DataIncrement increment2({file1}, {file3}, {file4});
    DataIncrement increment3({file2}, {file3}, {file4});

    ASSERT_TRUE(increment1 == increment2);
    ASSERT_FALSE(increment1 == increment3);
}

TEST_F(DataIncrementTest, TestToString) {
    std::shared_ptr<DataFileMeta> file1 = CreateDataFileMeta("file1");
    std::shared_ptr<DataFileMeta> file2 = CreateDataFileMeta("file2");
    std::shared_ptr<DataFileMeta> file3 = CreateDataFileMeta("file3");
    std::shared_ptr<DataFileMeta> file4 = CreateDataFileMeta("file4");

    DataIncrement increment({file1, file2}, {file3}, {file4});
    std::string expected =
        "DataIncrement {newFiles = file1, file2, deletedFiles = file3, changelogFiles = file4, "
        "newIndexFiles = , deletedIndexFiles = }";
    ASSERT_EQ(increment.ToString(), expected);
}

}  // namespace paimon::test
