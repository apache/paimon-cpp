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

#include "paimon/file_index/file_index_writer.h"

#include <string>

#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {
class RecordingIndexOutput : public MockOutputStream {
 public:
    Result<int64_t> Write(const char* buffer, int64_t size) override {
        if (short_write) {
            return size - 1;
        }
        data.append(buffer, size);
        return size;
    }
    Status Flush() override {
        ++flush_count;
        return Status::OK();
    }
    Status Close() override {
        ++close_count;
        return Status::OK();
    }

    std::string data;
    bool short_write = false;
    int32_t flush_count = 0;
    int32_t close_count = 0;
};

class LegacyIndexWriter : public FileIndexWriter {
 public:
    Status AddBatch(::ArrowArray*) override {
        return Status::OK();
    }
    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override {
        if (absent) {
            return PAIMON_UNIQUE_PTR<Bytes>();
        }
        if (zero_length) {
            return Bytes::AllocateBytes("", GetDefaultPool().get());
        }
        return Bytes::AllocateBytes("legacy payload", GetDefaultPool().get());
    }
    bool absent = false;
    bool zero_length = false;
};

}  // namespace

TEST(FileIndexWriterTest, TestLegacyDefaultWriteTo) {
    LegacyIndexWriter writer;
    auto output = std::make_shared<RecordingIndexOutput>();
    ASSERT_OK_AND_ASSIGN(bool written, writer.WriteTo(output));
    ASSERT_TRUE(written);
    ASSERT_EQ("legacy payload", output->data);
    ASSERT_EQ(0, output->flush_count);
    ASSERT_EQ(0, output->close_count);
    ASSERT_NOK_WITH_MSG(writer.WriteTo(nullptr), "cannot be null");
}

TEST(FileIndexWriterTest, TestLegacyWriteToRejectsShortWrite) {
    LegacyIndexWriter writer;
    auto output = std::make_shared<RecordingIndexOutput>();
    output->short_write = true;
    ASSERT_NOK_WITH_MSG(writer.WriteTo(output), "Short write");
}

TEST(FileIndexWriterTest, TestNullAndZeroLengthPayloadsProduceNoIndex) {
    LegacyIndexWriter writer;
    auto output = std::make_shared<RecordingIndexOutput>();
    writer.absent = true;
    ASSERT_OK_AND_ASSIGN(bool written, writer.WriteTo(output));
    ASSERT_FALSE(written);
    ASSERT_TRUE(output->data.empty());
    writer.absent = false;
    writer.zero_length = true;
    ASSERT_OK_AND_ASSIGN(written, writer.WriteTo(output));
    ASSERT_FALSE(written);
    ASSERT_TRUE(output->data.empty());
}

}  // namespace paimon::test
