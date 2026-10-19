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

#include "paimon/core/io/spillable_index_output_stream.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class SpillableIndexOutputStreamTest : public ::testing::Test {
 protected:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(directory_);
        path_ = directory_->Str() + "/test.index";
    }

    std::unique_ptr<SpillableIndexOutputStream> CreateStream(int64_t threshold) {
        return std::make_unique<SpillableIndexOutputStream>(file_system_, path_, threshold, pool_);
    }

    void Write(SpillableIndexOutputStream* stream, const std::string& data) {
        ASSERT_OK_AND_ASSIGN(int64_t written, stream->Write(data.data(), data.size()));
        ASSERT_EQ(data.size(), written);
    }

    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> file_system_ = std::make_shared<LocalFileSystem>();
    std::unique_ptr<UniqueTestDirectory> directory_;
    std::string path_;
};

TEST_F(SpillableIndexOutputStreamTest, KeepsPayloadAtThresholdInMemory) {
    auto stream = CreateStream(4);
    ASSERT_NO_FATAL_FAILURE(Write(stream.get(), "ab"));
    ASSERT_NO_FATAL_FAILURE(Write(stream.get(), "cd"));
    ASSERT_OK_AND_ASSIGN(int64_t position, stream->GetPos());
    ASSERT_EQ(4, position);
    ASSERT_OK(stream->Flush());
    ASSERT_OK(stream->Close());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, stream->EmbeddedBytes());
    ASSERT_TRUE(bytes);
    ASSERT_EQ("abcd", std::string(bytes->data(), bytes->size()));
    ASSERT_FALSE(stream->Spilled());
    ASSERT_OK_AND_ASSIGN(bool exists, file_system_->Exists(path_));
    ASSERT_FALSE(exists);
}

TEST_F(SpillableIndexOutputStreamTest, SpillsAndPreservesBufferedPrefix) {
    for (const auto& [threshold, prefix] :
         std::vector<std::pair<int64_t, std::string>>{{4, ""}, {4, "abcd"}}) {
        auto stream = CreateStream(threshold);
        if (!prefix.empty()) {
            ASSERT_NO_FATAL_FAILURE(Write(stream.get(), prefix));
        }
        ASSERT_FALSE(stream->Spilled());
        ASSERT_NO_FATAL_FAILURE(Write(stream.get(), "efghi"));
        ASSERT_NO_FATAL_FAILURE(Write(stream.get(), "j"));
        ASSERT_TRUE(stream->Spilled());
        ASSERT_OK_AND_ASSIGN(int64_t position, stream->GetPos());
        ASSERT_EQ(prefix.size() + 6, position);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, stream->EmbeddedBytes());
        ASSERT_FALSE(bytes);
        ASSERT_OK(stream->Flush());
        ASSERT_OK(stream->Close());
        std::string content;
        ASSERT_OK(file_system_->ReadFile(path_, &content));
        ASSERT_EQ(prefix + "efghij", content);
        ASSERT_OK(file_system_->Delete(path_));
    }
}

TEST_F(SpillableIndexOutputStreamTest, AbortRespectsFileOwnership) {
    for (bool spilled : {false, true}) {
        if (!spilled) {
            ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> existing,
                                 file_system_->Create(path_, /*overwrite=*/false));
            ASSERT_OK_AND_ASSIGN(int64_t written, existing->Write("original", 8));
            ASSERT_EQ(8, written);
            ASSERT_OK(existing->Close());
        }
        auto stream = CreateStream(spilled ? 0 : 4);
        ASSERT_NO_FATAL_FAILURE(Write(stream.get(), "ab"));
        ASSERT_OK(stream->Abort());
        ASSERT_OK_AND_ASSIGN(bool exists, file_system_->Exists(path_));
        ASSERT_EQ(!spilled, exists);
        if (!spilled) {
            std::string content;
            ASSERT_OK(file_system_->ReadFile(path_, &content));
            ASSERT_EQ("original", content);
            ASSERT_OK(file_system_->Delete(path_));
        }
    }
}
}  // namespace paimon::test
