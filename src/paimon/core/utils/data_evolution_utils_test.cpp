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

#include "paimon/core/utils/data_evolution_utils.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
std::shared_ptr<DataFileMeta> CreateFile(const std::string& file_name,
                                         int64_t max_sequence_number) {
    return DataFileMeta::ForAppend(file_name, /*file_size=*/1, /*row_count=*/1,
                                   /*row_stats=*/SimpleStats::EmptyStats(),
                                   /*min_sequence_number=*/0, max_sequence_number,
                                   /*schema_id=*/0, FileSource::Append(),
                                   /*value_stats_cols=*/std::nullopt,
                                   /*external_path=*/std::nullopt, /*first_row_id=*/0,
                                   /*write_cols=*/std::nullopt)
        .value();
}
}  // namespace

TEST(DataEvolutionUtilsTest, TestRetrieveAnchorFileSkipsBlobAndVectorStoreFiles) {
    auto blob_file = CreateFile("blob-file.blob", 1);
    auto vector_store_file = CreateFile("vector-store.vector.parquet", 2);
    auto oldest_normal_file = CreateFile("oldest-normal.parquet", 3);
    auto newest_normal_file = CreateFile("newest-normal.parquet", 4);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> anchor,
        DataEvolutionUtils::RetrieveAnchorFile(
            {blob_file, vector_store_file, newest_normal_file, oldest_normal_file}));
    ASSERT_EQ(anchor, oldest_normal_file);

    // the same rule a caller applies to tell whether a group can be anchored at all
    ASSERT_TRUE(DataEvolutionUtils::IsNormalFile(oldest_normal_file->file_name));
    ASSERT_FALSE(DataEvolutionUtils::IsNormalFile(blob_file->file_name));
    ASSERT_FALSE(DataEvolutionUtils::IsNormalFile(vector_store_file->file_name));
}

TEST(DataEvolutionUtilsTest, TestRetrieveAnchorFileFailsWithoutNormalFile) {
    auto blob_file_1 = CreateFile("blob-file-1.blob", 1);
    auto blob_file_2 = CreateFile("blob-file-2.blob", 2);
    auto vector_store_file = CreateFile("vector-store.vector.parquet", 3);

    ASSERT_NOK_WITH_MSG(
        DataEvolutionUtils::RetrieveAnchorFile({blob_file_1, blob_file_2, vector_store_file}),
        "should have a normal anchor file in each row range group");
    ASSERT_NOK_WITH_MSG(DataEvolutionUtils::RetrieveAnchorFile({}),
                        "should have a normal anchor file in each row range group");
}

TEST(DataEvolutionUtilsTest, TestRetrieveAnchorFileTieBreaksWithFileName) {
    auto larger_file_name = CreateFile("normal-2.parquet", 1);
    auto smaller_file_name = CreateFile("normal-1.parquet", 1);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<DataFileMeta> anchor,
        DataEvolutionUtils::RetrieveAnchorFile({larger_file_name, smaller_file_name}));
    ASSERT_EQ(anchor, smaller_file_name);
}

}  // namespace paimon::test
