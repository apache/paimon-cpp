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

#include "paimon/common/utils/serialization_utils.h"

#include "gtest/gtest.h"

namespace paimon::test {

class SerializationUtilsTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SerializationUtilsTest, TestSerializeBinaryRow) {
    std::shared_ptr<MemoryPool> memory_pool = GetDefaultPool();
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, memory_pool);
    BinaryRow row = BinaryRow::EmptyRow();
    std::shared_ptr<Bytes> bytes = SerializationUtils::SerializeBinaryRow(row, memory_pool.get());
    ASSERT_TRUE(bytes);
}

}  // namespace paimon::test
