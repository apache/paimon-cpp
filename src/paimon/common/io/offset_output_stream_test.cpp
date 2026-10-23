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

#include "paimon/common/io/offset_output_stream.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(OffsetOutputStreamTest, TestRelativePositionsAndWrites) {
    auto pool = GetDefaultPool();
    auto output =
        std::make_unique<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
            MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool));
    ASSERT_OK_AND_ASSIGN(int64_t written, output->Write("prefix", 6));
    ASSERT_EQ(6, written);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OffsetOutputStream> first,
                         OffsetOutputStream::Create(output.get()));
    ASSERT_OK_AND_ASSIGN(int64_t position, first->GetPos());
    ASSERT_EQ(0, position);
    ASSERT_OK_AND_ASSIGN(written, first->Write("first", 5));
    ASSERT_EQ(5, written);
    ASSERT_OK_AND_ASSIGN(position, first->GetPos());
    ASSERT_EQ(5, position);
    ASSERT_OK_AND_ASSIGN(position, output->GetPos());
    ASSERT_EQ(11, position);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OffsetOutputStream> second,
                         OffsetOutputStream::Create(output.get()));
    ASSERT_OK_AND_ASSIGN(position, second->GetPos());
    ASSERT_EQ(0, position);
    ASSERT_OK_AND_ASSIGN(written, second->Write("second", 6));
    ASSERT_EQ(6, written);
    ASSERT_OK_AND_ASSIGN(position, second->GetPos());
    ASSERT_EQ(6, position);
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> bytes, output->Finish(pool.get()));
    ASSERT_EQ("prefixfirstsecond", std::string(bytes->data(), bytes->size()));
}

TEST(OffsetOutputStreamTest, TestFlushAndClose) {
    auto dir = UniqueTestDirectory::Create();
    auto fs = dir->GetFileSystem();
    std::string path = dir->Str() + "/offset-output";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> output,
                         fs->Create(path, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OffsetOutputStream> offset,
                         OffsetOutputStream::Create(output.get()));
    ASSERT_OK_AND_ASSIGN(std::string uri, offset->GetUri());
    ASSERT_OK_AND_ASSIGN(std::string wrapped_uri, output->GetUri());
    ASSERT_EQ(wrapped_uri, uri);

    ASSERT_OK_AND_ASSIGN(int64_t written, offset->Write("payload", 7));
    ASSERT_EQ(7, written);
    ASSERT_OK(offset->Flush());
    std::string content;
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("payload", content);

    ASSERT_OK(offset->Close());
    ASSERT_OK_AND_ASSIGN(written, output->Write("suffix", 6));
    ASSERT_EQ(6, written);
    ASSERT_OK(output->Close());
    ASSERT_OK(fs->ReadFile(path, &content));
    ASSERT_EQ("payloadsuffix", content);
}

}  // namespace paimon::test
