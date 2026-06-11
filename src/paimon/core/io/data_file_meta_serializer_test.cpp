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

#include "paimon/core/io/data_file_meta_serializer.h"

#include <cstdint>
#include <optional>
#include <string>

#include "arrow/api.h"
#include "arrow/array/builder_base.h"
#include "gtest/gtest.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::test {
class DataFileMetaSerializerTest : public testing::Test {
 public:
    void SetUp() override {
        memory_pool_ = GetDefaultPool();
    }

 private:
    std::shared_ptr<DataFileMeta> GetDataFileMeta() {
        return std::make_shared<DataFileMeta>(
            "some_file_name", 1024, 8, DataFileMeta::EmptyMinKey(), DataFileMeta::EmptyMaxKey(),
            SimpleStats::EmptyStats(), SimpleStats::EmptyStats(), /*min_seq_no=*/16,
            /*max_seq_no=*/32,
            /*schema_id=*/1, /*level=*/2, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0), /*delete_row_count=*/3,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::optional<std::string>(),
            /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt);
    }

    const int32_t TRIES = 100;

    std::shared_ptr<MemoryPool> memory_pool_;
};

TEST_F(DataFileMetaSerializerTest, TestToFromRow) {
    DataFileMetaSerializer serializer(memory_pool_);
    auto expected = GetDataFileMeta();
    for (int32_t i = 0; i < TRIES; i++) {
        ASSERT_OK_AND_ASSIGN(auto row, serializer.ToRow(expected));
        ASSERT_OK_AND_ASSIGN(auto actual, serializer.FromRow(row));
        ASSERT_EQ(expected->ToString(), actual->ToString());
    }
}

TEST_F(DataFileMetaSerializerTest, TestSerialize) {
    DataFileMetaSerializer serializer(memory_pool_);
    auto expected = GetDataFileMeta();
    for (int32_t i = 0; i < TRIES; i++) {
        MemorySegmentOutputStream out(1024, memory_pool_);
        ASSERT_OK(serializer.Serialize(expected, &out));
        PAIMON_UNIQUE_PTR<Bytes> bytes = MemorySegmentUtils::CopyToBytes(
            out.Segments(), 0, out.CurrentSize(), memory_pool_.get());
        auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
        DataInputStream in(input_stream);
        ASSERT_OK_AND_ASSIGN(auto actual, serializer.Deserialize(&in));
        ASSERT_EQ(expected->ToString(), actual->ToString());
    }
}
}  // namespace paimon::test
