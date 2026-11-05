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

#include "paimon/io/byte_array_input_stream.h"

#include <chrono>
#include <future>
#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ByteArrayInputStreamTest, TestSimple) {
    auto pool = GetDefaultPool();
    auto output_stream = std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/8, pool);
    std::string str = "abcdef";
    auto bytes = std::make_shared<Bytes>(str, pool.get());
    output_stream->WriteBytes(bytes);
    auto out_bytes = MemorySegmentUtils::CopyToBytes(output_stream->Segments(), 0,
                                                     output_stream->CurrentSize(), pool.get());
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(out_bytes->data(), out_bytes->size());
    ASSERT_EQ(6, input_stream->Length().value());
    ASSERT_TRUE(input_stream->GetUri().value().empty());

    // read from pos 1
    std::string value(4, '\0');
    ASSERT_EQ(4, input_stream->Read(value.data(), value.size(), /*offset=*/1).value());
    ASSERT_EQ("bcde", value);
    ASSERT_EQ(0, input_stream->GetPos().value());

    // seek to pos 2
    ASSERT_OK(input_stream->Seek(2, SeekOrigin::FS_SEEK_SET));
    ASSERT_EQ(4, input_stream->Read(value.data(), value.size()).value());
    ASSERT_EQ("cdef", value);
    ASSERT_EQ(6, input_stream->GetPos().value());

    // although seek to pos 2, read set offset 0
    ASSERT_OK(input_stream->Seek(2, SeekOrigin::FS_SEEK_SET));
    bool read_finished = false;
    auto callback = [&](Status status) {
        ASSERT_OK(status);
        if (status.ok()) {
            read_finished = true;
        }
    };
    input_stream->ReadAsync(value.data(), value.size(), /*offset=*/0, callback);
    ASSERT_TRUE(read_finished);
    ASSERT_EQ("abcd", value);
    ASSERT_EQ(2, input_stream->GetPos().value());

    // test exceed eof, seek to pos 3, want to read 4 bytes
    ASSERT_OK(input_stream->Seek(-3, SeekOrigin::FS_SEEK_END));
    ASSERT_NOK_WITH_MSG(
        input_stream->Read(value.data(), value.size()),
        "assert boundary failed: need length 4, current position 3, exceed length 6");

    // test invalid seek
    ASSERT_NOK_WITH_MSG(input_stream->Seek(100, SeekOrigin::FS_SEEK_CUR),
                        "invalid seek, after seek, current pos 103, length 6");
    ASSERT_OK(input_stream->Close());
}

TEST(ByteArrayInputStreamTest, TestZeroLengthRead) {
    auto pool = GetDefaultPool();
    Bytes empty_bytes(0, pool.get());
    ByteArrayInputStream input_stream("abc", 3);
    ASSERT_OK(input_stream.Seek(1, SeekOrigin::FS_SEEK_SET));

    ASSERT_OK_AND_ASSIGN(int64_t read_size, input_stream.Read(empty_bytes.data(), 0));
    ASSERT_EQ(0, read_size);
    ASSERT_OK_AND_ASSIGN(int64_t position, input_stream.GetPos());
    ASSERT_EQ(1, position);

    ASSERT_OK_AND_ASSIGN(read_size, input_stream.Read(empty_bytes.data(), 0, /*offset=*/0));
    ASSERT_EQ(0, read_size);
    ASSERT_OK_AND_ASSIGN(read_size, input_stream.Read(empty_bytes.data(), 0, /*offset=*/3));
    ASSERT_EQ(0, read_size);
    ASSERT_OK_AND_ASSIGN(position, input_stream.GetPos());
    ASSERT_EQ(1, position);

    auto read_promise = std::make_shared<std::promise<Status>>();
    std::future<Status> read_result = read_promise->get_future();
    input_stream.ReadAsync(empty_bytes.data(), 0, /*offset=*/3, [read_promise](Status status) {
        read_promise->set_value(std::move(status));
    });
    ASSERT_EQ(std::future_status::ready, read_result.wait_for(std::chrono::seconds(5)));
    ASSERT_OK(read_result.get());
    ASSERT_OK_AND_ASSIGN(position, input_stream.GetPos());
    ASSERT_EQ(1, position);

    ASSERT_NOK_WITH_MSG(input_stream.Read(empty_bytes.data(), 0, /*offset=*/-1),
                        "boundary check failed");
    ASSERT_NOK_WITH_MSG(input_stream.Read(empty_bytes.data(), 0, /*offset=*/4),
                        "boundary check failed");

    ASSERT_OK(input_stream.Seek(3, SeekOrigin::FS_SEEK_SET));
    ASSERT_OK_AND_ASSIGN(read_size, input_stream.Read(empty_bytes.data(), 0));
    ASSERT_EQ(0, read_size);
    ASSERT_OK_AND_ASSIGN(position, input_stream.GetPos());
    ASSERT_EQ(3, position);
}

TEST(ByteArrayInputStreamTest, TestZeroLengthReadFromEmptyStream) {
    ByteArrayInputStream input_stream("", 0);
    ASSERT_OK_AND_ASSIGN(int64_t read_size, input_stream.Read(nullptr, 0));
    ASSERT_EQ(0, read_size);
    ASSERT_OK_AND_ASSIGN(read_size, input_stream.Read(nullptr, 0, /*offset=*/0));
    ASSERT_EQ(0, read_size);
    ASSERT_OK_AND_ASSIGN(int64_t position, input_stream.GetPos());
    ASSERT_EQ(0, position);
}

}  // namespace paimon::test
