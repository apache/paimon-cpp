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

#include "paimon/core/utils/partition_utils.h"

#include <map>
#include <memory>
#include <string>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(PartitionUtilsTest, MatchNormalizedPartitionSpec) {
    const std::map<std::string, std::string> partition = {{"dt", "2026-08-21"}, {"region", "cn"}};

    ASSERT_TRUE(PartitionUtils::MatchNormalizedPartitionSpec(partition, /*partition_spec=*/{}));
    ASSERT_TRUE(PartitionUtils::MatchNormalizedPartitionSpec(partition, {{"dt", "2026-08-21"}}));
    ASSERT_TRUE(PartitionUtils::MatchNormalizedPartitionSpec(
        partition, {{"dt", "2026-08-21"}, {"region", "cn"}}));
    ASSERT_FALSE(PartitionUtils::MatchNormalizedPartitionSpec(partition, {{"dt", "2026-08-22"}}));
    ASSERT_FALSE(PartitionUtils::MatchNormalizedPartitionSpec(partition, {{"hour", "12"}}));
}

TEST(PartitionUtilsTest, MatchPartitionSpecNormalizesPartialSpec) {
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("dt", arrow::date32()), arrow::field("region", arrow::utf8())});
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BinaryRowPartitionComputer> partition_computer,
                         BinaryRowPartitionComputer::Create(
                             /*partition_keys=*/{"dt", "region"}, schema,
                             /*default_part_value=*/"__DEFAULT_PARTITION__",
                             /*legacy_partition_name_enabled=*/true, GetDefaultPool()));
    const std::map<std::string, std::string> partition = {{"dt", "19723"}, {"region", "cn"}};
    ASSERT_OK_AND_ASSIGN(
        bool raw_spec_matches,
        PartitionUtils::MatchPartitionSpec(partition, {{"dt", "2024-01-01"}}, *partition_computer));
    ASSERT_TRUE(raw_spec_matches);

    ASSERT_OK_AND_ASSIGN(
        bool normalized_spec_matches,
        PartitionUtils::MatchPartitionSpec(partition, {{"dt", "19723"}}, *partition_computer));
    ASSERT_TRUE(normalized_spec_matches);

    ASSERT_OK_AND_ASSIGN(bool unknown_key_matches,
                         PartitionUtils::MatchPartitionSpec(
                             partition, {{"unknown_partition_key", "value"}}, *partition_computer));
    ASSERT_FALSE(unknown_key_matches);
}

}  // namespace paimon::test
