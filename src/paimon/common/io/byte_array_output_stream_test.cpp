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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/common/io/byte_array_output_stream.h"

#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ByteArrayOutputStreamTest, TestWriteAndFinish) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    auto output = std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/2, pool);
    std::shared_ptr<ByteArrayOutputStream> stream =
        std::make_shared<ByteArrayOutputStream>(std::move(output));
    ASSERT_GT(pool->CurrentUsage(), 0);
    ASSERT_OK_AND_ASSIGN(int64_t first_write, stream->Write("ab", 2));
    ASSERT_EQ(2, first_write);
    ASSERT_OK_AND_ASSIGN(int64_t second_write, stream->Write("cdef", 4));
    ASSERT_EQ(4, second_write);
    ASSERT_OK_AND_ASSIGN(int64_t position, stream->GetPos());
    ASSERT_EQ(6, position);
    ASSERT_EQ(pool->CurrentUsage(), pool->MaxMemoryUsage());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> result, stream->Finish(pool.get()));
    ASSERT_EQ("abcdef", std::string(result->data(), result->size()));
    ASSERT_NOK_WITH_MSG(stream->Write("x", 1), "closed");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> repeated, stream->Finish(pool.get()));
    ASSERT_EQ(result, repeated);
    stream.reset();
    ASSERT_EQ(6, pool->CurrentUsage());
}

TEST(ByteArrayOutputStreamTest, TestWriteValidation) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    auto output = std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/8, pool);
    std::shared_ptr<ByteArrayOutputStream> stream =
        std::make_shared<ByteArrayOutputStream>(std::move(output));
    ASSERT_NOK(stream->Write(nullptr, 1));
    ASSERT_NOK(stream->Write("", -1));
    ASSERT_OK_AND_ASSIGN(int64_t written, stream->Write(nullptr, 0));
    ASSERT_EQ(0, written);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> result, stream->Finish(pool.get()));
    ASSERT_EQ(0, result->size());
}

TEST(ByteArrayOutputStreamTest, TestCallerKeepsMemoryPoolAlive) {
    std::shared_ptr<MemoryPool> pool = GetMemoryPool();
    auto output = std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/8, pool);
    std::shared_ptr<ByteArrayOutputStream> stream =
        std::make_shared<ByteArrayOutputStream>(std::move(output));
    ASSERT_OK_AND_ASSIGN(int64_t written, stream->Write("data", 4));
    ASSERT_EQ(4, written);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> result, stream->Finish(pool.get()));

    stream.reset();
    ASSERT_GT(pool->CurrentUsage(), 0);
    ASSERT_EQ("data", std::string(result->data(), result->size()));

    result.reset();
    ASSERT_EQ(0, pool->CurrentUsage());
}

}  // namespace paimon::test
