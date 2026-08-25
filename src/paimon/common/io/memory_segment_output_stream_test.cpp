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

#include "paimon/common/io/memory_segment_output_stream.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {

class MemorySegmentOutputStreamTest : public ::testing::Test,
                                      public ::testing::WithParamInterface<ByteOrder> {};

INSTANTIATE_TEST_SUITE_P(ByteOrder, MemorySegmentOutputStreamTest,
                         ::testing::Values(ByteOrder::PAIMON_BIG_ENDIAN,
                                           ByteOrder::PAIMON_LITTLE_ENDIAN));

TEST_P(MemorySegmentOutputStreamTest, TestSimple) {
    ByteOrder byte_order = GetParam();
    auto pool = GetDefaultPool();
    MemorySegmentOutputStream out(/*segment_size=*/8, pool);
    out.SetOrder(byte_order);
    out.WriteValue(static_cast<char>(127));            // 1 byte
    out.WriteValue(static_cast<int16_t>(32067));       // 2 bytes
    out.WriteValue(static_cast<int32_t>(2147403647));  // 4 bytes
    // move to next segment
    out.WriteValue(static_cast<int64_t>(9223372036854775805));  // 8 bytes
    out.WriteValue(true);                                       // 1 byte
    std::string str1 = "This is a very very very long sentence.";
    out.WriteString(str1);  // 39 bytes + 2 bytes len
    ASSERT_EQ(out.CurrentSize(), 1 + 1 + 2 + 4 + 8 + 41);
    std::string str2 = "yes";
    out.WriteString(str2);  // 3 bytes + 2 bytes len
    std::string str3 = "I have a dream.";
    out.WriteString(str3);  // 15 bytes + 2 bytes len
    std::string str4 = "hello";
    out.WriteValue<int32_t>(str4.size());  // 4 bytes
    out.Write(str4.data(), str4.size());   // 5 bytes

    ASSERT_EQ(out.CurrentSize(), 1 + 1 + 2 + 4 + 8 + 41 + 5 + 17 + 4 + 5);

    auto bytes = MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());

    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream in(input_stream);
    in.SetOrder(byte_order);
    ASSERT_EQ(127, in.ReadValue<char>().value());
    ASSERT_EQ(32067, in.ReadValue<int16_t>().value());
    ASSERT_EQ(2147403647, in.ReadValue<int32_t>().value());
    ASSERT_EQ(9223372036854775805l, in.ReadValue<int64_t>().value());
    ASSERT_EQ(true, in.ReadValue<bool>().value());
    ASSERT_EQ(str1, in.ReadString().value());
    ASSERT_EQ(str2, in.ReadString().value());
    ASSERT_EQ(str3, in.ReadString().value());
    ASSERT_EQ(str4.size(), in.ReadValue<int32_t>().value());
    std::string read_str4(str4.size(), '\0');
    ASSERT_OK(in.Read(read_str4.data(), str4.size()));
    ASSERT_EQ(read_str4, str4);
    ASSERT_EQ(out.CurrentSize(), input_stream->GetPos().value());
}

TEST(MemorySegmentOutputStreamTest, TestRawWriteDoesNotAllocateTemporaryBuffer) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    MemorySegmentOutputStream out(/*segment_size=*/8, pool);
    uint64_t allocated_before_write = pool->CurrentUsage();

    out.Write("abc", 3);

    ASSERT_EQ(allocated_before_write, pool->CurrentUsage());
    ASSERT_EQ(pool->CurrentUsage(), pool->MaxMemoryUsage());
    ASSERT_EQ(3, out.CurrentSize());
}

}  // namespace paimon::test
