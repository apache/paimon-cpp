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
#include "paimon/core/manifest/manifest_file_meta_serializer.h"

#include <optional>
#include <string>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/core/manifest/manifest_file_meta.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
TEST(ManifestFileMetaSerializerTest, TestToFromRow) {
    ManifestFileMeta meta1(/*file_name=*/"meta1", /*file_size=*/10, /*num_added_files=*/15,
                           /*num_deleted_files=*/20, SimpleStats::EmptyStats(), /*schema_id=*/0,
                           /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                           /*min_level=*/std::nullopt, /*max_level=*/std::nullopt,
                           /*min_row_id=*/std::nullopt, /*max_row_id=*/std::nullopt);
    ManifestFileMeta meta2(/*file_name=*/"meta2", /*file_size=*/25, /*num_added_files=*/30,
                           /*num_deleted_files=*/35, SimpleStats::EmptyStats(), /*schema_id=*/1,
                           /*min_bucket=*/std::nullopt, /*max_bucket=*/std::nullopt,
                           /*min_level=*/std::nullopt, /*max_level=*/std::nullopt,
                           /*min_row_id=*/200, /*max_row_id=*/233);

    std::vector<ManifestFileMeta> metas = {meta1, meta2};
    ManifestFileMetaSerializer serializer(GetDefaultPool());
    for (const auto& meta : metas) {
        ASSERT_OK_AND_ASSIGN(auto row, serializer.ToRow(meta));
        ASSERT_OK_AND_ASSIGN(auto result_meta, serializer.FromRow(row));
        ASSERT_EQ(result_meta, meta);
        ASSERT_EQ(result_meta.ToString(), meta.ToString());
    }
}

TEST(ManifestFileMetaSerializerTest, TestInvalidCase) {
    auto pool = GetDefaultPool();

    {
        GenericRow row(12);
        row.SetField(0, BinaryString::FromString("meta1", pool.get()));
        row.SetField(1, 10L);
        row.SetField(2, 20L);
        row.SetField(3, 30L);
        auto simple_stats = BinaryRowGenerator::GenerateStats(
            {"Alex", 10, 0, 12.1}, {"Emily", 10, 0, 16.1}, {0, 0, 0, 0}, pool.get());
        auto simple_stats_row = std::make_shared<BinaryRow>(simple_stats.ToRow());
        row.SetField(4, simple_stats_row);
        row.SetField(5, 2L);
        row.SetField(6, NullType());
        row.SetField(7, NullType());
        row.SetField(8, NullType());
        row.SetField(9, NullType());
        row.SetField(10, NullType());
        row.SetField(11, NullType());

        ManifestFileMetaSerializer serializer(pool);
        ASSERT_NOK_WITH_MSG(serializer.ConvertFrom(/*version=*/1, row),
                            "The current version 2 is not compatible with the version 1, please "
                            "recreate the table.");
        ASSERT_NOK_WITH_MSG(serializer.ConvertFrom(/*version=*/3, row), "Unsupported version: 3");
        ASSERT_OK(serializer.ConvertFrom(/*version=*/2, row));
    }
    {
        GenericRow row(12);
        row.SetField(0, BinaryString::FromString("meta1", pool.get()));
        row.SetField(1, 10L);
        row.SetField(2, 20L);
        row.SetField(3, 30L);
        row.SetField(4, std::shared_ptr<BinaryRow>());
        row.SetField(5, 2L);
        row.SetField(6, NullType());
        row.SetField(7, NullType());
        row.SetField(8, NullType());
        row.SetField(9, NullType());
        row.SetField(10, NullType());
        row.SetField(11, NullType());

        ManifestFileMetaSerializer serializer(pool);
        ASSERT_NOK_WITH_MSG(serializer.ConvertFrom(/*version=*/2, row),
                            "ManifestFileMeta convert from row failed, with null partition stats");
    }
}

}  // namespace paimon::test
