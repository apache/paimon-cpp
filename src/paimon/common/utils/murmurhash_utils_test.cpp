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

#include "paimon/common/utils/murmurhash_utils.h"

#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {
TEST(MurmurHashUtilsTest, TestCompatibleWithJava) {
    {
        std::vector<uint8_t> bytes_value = {3,  10, 20, 30, 40, 50, 67, 89,  111, 51,
                                            33, 67, 70, 25, 48, 10, 54, 100, 43,  21};
        int32_t num_bytes = bytes_value.size();
        uint32_t expect = 0xb39f33e6;
        auto pool = GetDefaultPool();
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(num_bytes, pool.get());
        memcpy(bytes->data(), bytes_value.data(), num_bytes);
        MemorySegment segment = MemorySegment::Wrap(bytes);
        ASSERT_EQ(expect,
                  MurmurHashUtils::HashUnsafeBytesByWords(bytes_value.data(), 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashUnsafeBytes(bytes_value.data(), 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashBytesByWords(segment, 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashBytes(segment, 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashBytes(bytes));
    }
    {
        std::vector<uint8_t> bytes_value = {3,   10, 20, 30, 40, 50, 67, 89,
                                            111, 51, 33, 67, 70, 25, 48, 10};
        int32_t num_bytes = bytes_value.size();
        uint32_t expect = 0x46bdab5b;
        auto pool = GetDefaultPool();
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(num_bytes, pool.get());
        memcpy(bytes->data(), bytes_value.data(), num_bytes);
        MemorySegment segment = MemorySegment::Wrap(bytes);
        ASSERT_EQ(expect,
                  MurmurHashUtils::HashUnsafeBytesByWords(bytes_value.data(), 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashUnsafeBytes(bytes_value.data(), 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashBytesByWords(segment, 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashBytes(segment, 0, num_bytes));
        ASSERT_EQ(expect, MurmurHashUtils::HashBytes(bytes));
    }
    {
        // test multi segments with MemorySegmentUtil
        std::vector<uint8_t> bytes_value1 = {3, 10, 20, 30, 40, 50, 67, 89, 111, 51, 33, 67};
        std::vector<uint8_t> bytes_value2 = {70, 25, 48, 10};
        int32_t num_bytes = bytes_value1.size() + bytes_value2.size();
        uint32_t expect = 0x46bdab5b;
        auto pool = GetDefaultPool();
        std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes(bytes_value1.size(), pool.get());
        memcpy(bytes1->data(), bytes_value1.data(), bytes_value1.size());
        MemorySegment segment1 = MemorySegment::Wrap(bytes1);
        std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes(bytes_value2.size(), pool.get());
        memcpy(bytes2->data(), bytes_value2.data(), bytes_value2.size());
        MemorySegment segment2 = MemorySegment::Wrap(bytes2);
        ASSERT_EQ(expect,
                  MemorySegmentUtils::HashByWords({segment1, segment2}, 0, num_bytes, pool.get()));
        ASSERT_EQ(expect, MemorySegmentUtils::Hash({segment1, segment2}, 0, num_bytes, pool.get()));
    }
    {
        // test not aligned bytes (not aligned with 4 bytes)
        std::vector<uint8_t> bytes_value = {3,  10,  120, 130, 240, 50,  167, 189, 211,
                                            51, 233, 167, 170, 25,  148, 10,  226, 19};
        int32_t num_bytes = bytes_value.size();
        uint32_t expect_bytes = 0x94a92b77;
        auto pool = GetDefaultPool();
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(num_bytes, pool.get());
        memcpy(bytes->data(), bytes_value.data(), num_bytes);
        MemorySegment segment = MemorySegment::Wrap(bytes);

        ASSERT_EQ(expect_bytes, MurmurHashUtils::HashUnsafeBytes(bytes_value.data(), 0, num_bytes));
        ASSERT_EQ(expect_bytes, MurmurHashUtils::HashBytes(segment, 0, num_bytes));
        ASSERT_EQ(expect_bytes, MurmurHashUtils::HashBytes(bytes));
    }
    {
        // test multi segments with MemorySegmentUtil
        std::vector<uint8_t> bytes_value1 = {3, 10, 120, 130, 240, 50, 167, 189, 211, 51, 233, 167};
        std::vector<uint8_t> bytes_value2 = {170, 25, 148, 10, 226, 19};
        int32_t num_bytes = bytes_value1.size() + bytes_value2.size();
        uint32_t expect_bytes = 0x94a92b77;

        auto pool = GetDefaultPool();
        std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes(bytes_value1.size(), pool.get());
        memcpy(bytes1->data(), bytes_value1.data(), bytes_value1.size());
        MemorySegment segment1 = MemorySegment::Wrap(bytes1);
        std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes(bytes_value2.size(), pool.get());
        memcpy(bytes2->data(), bytes_value2.data(), bytes_value2.size());
        MemorySegment segment2 = MemorySegment::Wrap(bytes2);

        ASSERT_EQ(expect_bytes,
                  MemorySegmentUtils::Hash({segment1, segment2}, 0, num_bytes, pool.get()));
    }
}
}  // namespace paimon::test
