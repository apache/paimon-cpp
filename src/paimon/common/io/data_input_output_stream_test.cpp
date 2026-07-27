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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/buffered_input_stream.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/byte_order.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class DataInputOutputStreamTest : public ::testing::Test,
                                  public ::testing::WithParamInterface<ByteOrder> {
 protected:
    void SetUp() override {
        pool_ = GetDefaultPool();
        std::vector<std::uint8_t> bytes_big_endian = {
            127,  125,  67,   127,  0xfe, 0xc7, 127,  127,  0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xfd, 1,    0,    39,   84,   104,  105,  115,  32,   105,  115,  32,   97,   32,
            118,  101,  114,  121,  32,   118,  101,  114,  121,  32,   118,  101,  114,  121,
            32,   108,  111,  110,  103,  32,   115,  101,  110,  116,  101,  110,  99,   101,
            46,   0xe6, 0x88, 0x91, 0xe6, 0x98, 0xaf, 0xe4, 0xb8, 0x80, 0xe4, 0xb8, 0xaa, 0xe7,
            0xb2, 0x89, 0xe5, 0x88, 0xb7, 0xe5, 0x8c, 0xa0, 0xef, 0xbd, 0x9e};
        serialized_bytes_big_endian_ =
            std::make_shared<Bytes>(bytes_big_endian.size(), pool_.get());
        memcpy(serialized_bytes_big_endian_->data(), bytes_big_endian.data(),
               bytes_big_endian.size());

        std::vector<std::uint8_t> bytes_little_endian = {
            127,  67,   125,  127,  0xc7, 0xfe, 127,  0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            127,  1,    39,   0,    84,   104,  105,  115,  32,   105,  115,  32,   97,   32,
            118,  101,  114,  121,  32,   118,  101,  114,  121,  32,   118,  101,  114,  121,
            32,   108,  111,  110,  103,  32,   115,  101,  110,  116,  101,  110,  99,   101,
            46,   0xe6, 0x88, 0x91, 0xe6, 0x98, 0xaf, 0xe4, 0xb8, 0x80, 0xe4, 0xb8, 0xaa, 0xe7,
            0xb2, 0x89, 0xe5, 0x88, 0xb7, 0xe5, 0x8c, 0xa0, 0xef, 0xbd, 0x9e};
        serialized_bytes_little_endian_ =
            std::make_shared<Bytes>(bytes_little_endian.size(), pool_.get());
        memcpy(serialized_bytes_little_endian_->data(), bytes_little_endian.data(),
               bytes_little_endian.size());
    }

    template <typename T>
    void WriteValues(T* data_output_stream) const {
        (void)data_output_stream->WriteValue(static_cast<char>(127));                     // 1 byte
        (void)data_output_stream->WriteValue(static_cast<int16_t>(32067));                // 2 bytes
        (void)data_output_stream->WriteValue(static_cast<int32_t>(2147403647));           // 4 bytes
        (void)data_output_stream->WriteValue(static_cast<int64_t>(9223372036854775805));  // 8 bytes
        (void)data_output_stream->WriteValue(true);                                       // 1 byte
        std::string str1 = "This is a very very very long sentence.";
        if constexpr (std::is_same_v<T, MemorySegmentOutputStream>) {
            (void)data_output_stream->WriteString(str1);  // 39 bytes + 2 bytes len
        } else {
            (void)data_output_stream->WriteString(str1);  // 39 bytes + 2 bytes len
        }
        std::string str2 = "我是一个粉刷匠～";  // 24 bytes
        auto bytes = std::make_shared<Bytes>(str2, pool_.get());
        (void)data_output_stream->WriteBytes(bytes);
    }

    void CheckResult(const InputStream* input_stream,
                     const DataInputStream* data_input_stream) const {
        ASSERT_EQ(127, data_input_stream->ReadValue<char>().value());
        ASSERT_EQ(32067, data_input_stream->ReadValue<int16_t>().value());
        ASSERT_EQ((int32_t)2147403647, data_input_stream->ReadValue<int32_t>().value());
        ASSERT_EQ((int64_t)9223372036854775805, data_input_stream->ReadValue<int64_t>().value());
        ASSERT_EQ(true, data_input_stream->ReadValue<bool>().value());
        std::string str1 = "This is a very very very long sentence.";
        ASSERT_EQ(str1, data_input_stream->ReadString().value());
        std::string str2 = "我是一个粉刷匠～";  // 24 bytes
        auto bytes = std::make_shared<Bytes>(str2, pool_.get());
        auto read_bytes = std::make_shared<Bytes>(str2.length(), pool_.get());
        ASSERT_OK(data_input_stream->ReadBytes(read_bytes.get()));
        ASSERT_EQ(*bytes, *read_bytes);
        // test GetPos
        ASSERT_OK_AND_ASSIGN(int64_t in_pos, input_stream->GetPos());
        ASSERT_EQ(1 + 2 + 4 + 8 + 1 + 41 + 24, in_pos);
        // read eof, return bad status
        ASSERT_NOK(data_input_stream->ReadString());
        // test seek
        ASSERT_OK(data_input_stream->Seek(3));
        ASSERT_EQ((int32_t)2147403647, data_input_stream->ReadValue<int32_t>().value());
    }
    void TearDown() override {}

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<Bytes> serialized_bytes_big_endian_;
    std::shared_ptr<Bytes> serialized_bytes_little_endian_;
};
INSTANTIATE_TEST_SUITE_P(ByteOrder, DataInputOutputStreamTest,
                         ::testing::Values(ByteOrder::PAIMON_BIG_ENDIAN,
                                           ByteOrder::PAIMON_LITTLE_ENDIAN));

TEST_P(DataInputOutputStreamTest, TestFileStream) {
    ByteOrder byte_order = GetParam();
    auto file_system = std::make_unique<LocalFileSystem>();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string test_file = dir->Str() + "/test";
    // prepare output stream
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> output_stream,
                         file_system->Create(test_file, /*overwrite=*/true));
    auto data_output_stream = std::make_unique<DataOutputStream>(output_stream);
    data_output_stream->SetOrder(byte_order);
    WriteValues(data_output_stream.get());
    ASSERT_OK_AND_ASSIGN(int64_t out_pos, output_stream->GetPos());
    ASSERT_EQ(1 + 2 + 4 + 8 + 1 + 41 + 24, out_pos);
    ASSERT_OK(output_stream->Close());
    // check file content
    ASSERT_OK_AND_ASSIGN(bool exist, file_system->Exists(test_file));
    ASSERT_TRUE(exist);
    std::string out_str;
    ASSERT_OK(file_system->ReadFile(test_file, &out_str));
    auto out_bytes = std::make_shared<Bytes>(out_str, pool_.get());
    // print_hex(out_str);
    if (byte_order == ByteOrder::PAIMON_BIG_ENDIAN) {
        ASSERT_EQ(*serialized_bytes_big_endian_, *out_bytes);
    } else {
        ASSERT_EQ(*serialized_bytes_little_endian_, *out_bytes);
    }

    // prepare input stream
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system->Open(test_file));
    auto data_input_stream = std::make_unique<DataInputStream>(input_stream);
    data_input_stream->SetOrder(byte_order);
    CheckResult(input_stream.get(), data_input_stream.get());
}

TEST_P(DataInputOutputStreamTest, TestInMemoryByteArrayStream) {
    ByteOrder byte_order = GetParam();
    // prepare output stream
    auto data_output_stream =
        std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/8, pool_);
    data_output_stream->SetOrder(byte_order);
    WriteValues(data_output_stream.get());
    ASSERT_EQ(1 + 2 + 4 + 8 + 1 + 41 + 24, data_output_stream->CurrentSize());
    auto out_bytes = MemorySegmentUtils::CopyToBytes(
        data_output_stream->Segments(), 0, data_output_stream->CurrentSize(), pool_.get());
    if (byte_order == ByteOrder::PAIMON_BIG_ENDIAN) {
        ASSERT_EQ(*serialized_bytes_big_endian_, *out_bytes);
    } else {
        ASSERT_EQ(*serialized_bytes_little_endian_, *out_bytes);
    }
    // prepare input stream
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(out_bytes->data(), out_bytes->size());
    auto data_input_stream = std::make_unique<DataInputStream>(input_stream);
    data_input_stream->SetOrder(byte_order);
    CheckResult(input_stream.get(), data_input_stream.get());
}

TEST_P(DataInputOutputStreamTest, TestBufferedStream) {
    ByteOrder byte_order = GetParam();
    // prepare output stream
    auto data_output_stream =
        std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/8, pool_);
    data_output_stream->SetOrder(byte_order);
    WriteValues(data_output_stream.get());
    ASSERT_EQ(1 + 2 + 4 + 8 + 1 + 41 + 24, data_output_stream->CurrentSize());
    auto out_bytes = MemorySegmentUtils::CopyToBytes(
        data_output_stream->Segments(), 0, data_output_stream->CurrentSize(), pool_.get());
    if (byte_order == ByteOrder::PAIMON_BIG_ENDIAN) {
        ASSERT_EQ(*serialized_bytes_big_endian_, *out_bytes);
    } else {
        ASSERT_EQ(*serialized_bytes_little_endian_, *out_bytes);
    }
    // prepare input stream
    auto input_stream =
        std::make_unique<ByteArrayInputStream>(out_bytes->data(), out_bytes->size());
    auto buffered_input_stream = std::make_shared<BufferedInputStream>(
        std::move(input_stream), /*buffer_size=*/4, pool_.get());
    auto data_input_stream = std::make_unique<DataInputStream>(buffered_input_stream);
    data_input_stream->SetOrder(byte_order);
    CheckResult(buffered_input_stream.get(), data_input_stream.get());
}
}  // namespace paimon::test
