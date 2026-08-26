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

#include <cstdint>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

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

TEST_F(SerializationUtilsTest, TestDeserializeBinaryRowFromStream) {
    std::shared_ptr<MemoryPool> memory_pool = GetDefaultPool();
    // a row with mixed field types, including negative integers and a string
    BinaryRow row(3);
    BinaryRowWriter writer(&row, 0, memory_pool.get());
    writer.WriteInt(0, -123456);
    writer.WriteLong(1, static_cast<int64_t>(-9000000000));
    writer.WriteString(2, BinaryString::FromString("hello paimon!", memory_pool.get()));
    writer.Complete();

    // the first 4 bytes on the wire are the big-endian arity (Java-compatible format)
    std::shared_ptr<Bytes> bytes = SerializationUtils::SerializeBinaryRow(row, memory_pool.get());
    ASSERT_TRUE(bytes);
    ASSERT_GE(bytes->size(), 4);
    ASSERT_EQ(static_cast<uint8_t>(bytes->data()[0]), 0x00);
    ASSERT_EQ(static_cast<uint8_t>(bytes->data()[1]), 0x00);
    ASSERT_EQ(static_cast<uint8_t>(bytes->data()[2]), 0x00);
    ASSERT_EQ(static_cast<uint8_t>(bytes->data()[3]), 0x03);

    // round-trip through the stream overloads, which fill a fresh byte buffer on deserialize
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, memory_pool);
    ASSERT_OK(SerializationUtils::SerializeBinaryRow(row, &out));
    auto stream_bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), memory_pool.get());
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(stream_bytes->data(), stream_bytes->size());
    DataInputStream data_input_stream(input_stream);
    ASSERT_OK_AND_ASSIGN(BinaryRow de_row, SerializationUtils::DeserializeBinaryRow(
                                               &data_input_stream, memory_pool.get()));
    ASSERT_EQ(de_row.GetFieldCount(), 3);
    ASSERT_EQ(de_row.GetInt(0), -123456);
    ASSERT_EQ(de_row.GetLong(1), static_cast<int64_t>(-9000000000));
    ASSERT_EQ(de_row.GetString(2).ToString(), "hello paimon!");
}

}  // namespace paimon::test
