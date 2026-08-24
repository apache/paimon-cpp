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

#include "paimon/data/blob.h"

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

/// An input stream that delegates to a real one and counts how often it was closed, so a test
/// can tell whether a handle was handed over or released.
class CloseCountingInputStream : public InputStream {
 public:
    CloseCountingInputStream(std::unique_ptr<InputStream>&& wrapped, int32_t* close_count)
        : wrapped_(std::move(wrapped)), close_count_(close_count) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return wrapped_->Seek(offset, origin);
    }

    Result<int64_t> GetPos() const override {
        return wrapped_->GetPos();
    }

    Result<int64_t> Read(char* buffer, int64_t size) override {
        return wrapped_->Read(buffer, size);
    }

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        return wrapped_->Read(buffer, size, offset);
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        wrapped_->ReadAsync(buffer, size, offset, std::move(callback));
    }

    Status Close() override {
        (*close_count_)++;
        return wrapped_->Close();
    }

    Result<std::string> GetUri() const override {
        return wrapped_->GetUri();
    }

    Result<int64_t> Length() const override {
        return wrapped_->Length();
    }

 private:
    std::unique_ptr<InputStream> wrapped_;
    int32_t* close_count_;
};

/// A local file system handing out those streams.
class CloseCountingFileSystem : public LocalFileSystem {
 public:
    explicit CloseCountingFileSystem(int32_t* close_count) : close_count_(close_count) {}

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> wrapped, LocalFileSystem::Open(path));
        return std::unique_ptr<InputStream>(
            std::make_unique<CloseCountingInputStream>(std::move(wrapped), close_count_));
    }

 private:
    int32_t* close_count_;
};

}  // namespace

class BlobTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        uri_ = dir_->Str() + "/file.blob";
        file_system_ = std::make_shared<LocalFileSystem>();
        ASSERT_OK(file_system_->WriteFile(uri_, "abcdefghijklmn", true));
    }
    void TearDown() override {}

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> file_system_;
    std::string uri_;
};

TEST_F(BlobTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob, Blob::FromPath(uri_));
    auto serialized = blob->ToDescriptor(pool_);

    ASSERT_OK_AND_ASSIGN(auto restored_blob,
                         Blob::FromDescriptor(serialized->data(), serialized->size()));
    ASSERT_EQ(*restored_blob->ToDescriptor(pool_), *serialized);
    ASSERT_OK_AND_ASSIGN(auto input_stream, restored_blob->NewInputStream(file_system_));
    ASSERT_EQ(uri_, restored_blob->Uri());
    std::string str(14, '\0');
    ASSERT_OK(input_stream->Read(str.data(), str.size()));
    ASSERT_EQ("abcdefghijklmn", str);
}

TEST_F(BlobTest, TestInvalidParameters) {
    // Test null file system in NewInputStream
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob, Blob::FromPath(uri_));
    ASSERT_NOK_WITH_MSG(blob->NewInputStream(nullptr), "file system is nullptr");

    // Test invalid descriptor
    std::string invalid_bytes = "invalid_descriptor_bytes";
    ASSERT_NOK(Blob::FromDescriptor(invalid_bytes.data(), invalid_bytes.size()));
}

TEST_F(BlobTest, TestRoundTripConsistency) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> original_blob,
                         Blob::FromPath(uri_, /*offset=*/1, /*length=*/5));

    auto first_descriptor = original_blob->ToDescriptor(pool_);
    ASSERT_OK_AND_ASSIGN(auto restored_blob,
                         Blob::FromDescriptor(first_descriptor->data(), first_descriptor->size()));

    auto second_descriptor = restored_blob->ToDescriptor(pool_);
    ASSERT_EQ(*first_descriptor, *second_descriptor);

    // Verify scheme consistency
    ASSERT_EQ(original_blob->Uri(), restored_blob->Uri());
}

TEST_F(BlobTest, TestInputStreamCreation) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob, Blob::FromPath(uri_));
    ASSERT_OK_AND_ASSIGN(auto input_stream, blob->NewInputStream(file_system_));
    ASSERT_TRUE(input_stream);

    // Test reading from the input stream
    std::string buffer(14, '\0');
    ASSERT_OK_AND_ASSIGN(auto bytes_read, input_stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(14, bytes_read);
    ASSERT_EQ("abcdefghijklmn", buffer);
}

TEST_F(BlobTest, TestBoundaryConditions) {
    // Test with empty path
    ASSERT_NOK_WITH_MSG(Blob::FromPath(""), "path is an empty string");

    // Test with very large offset and length values
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob,
                         Blob::FromPath(uri_, /*offset=*/INT64_MAX, /*length=*/1));
    auto serialized = blob->ToDescriptor(pool_);

    ASSERT_OK_AND_ASSIGN(auto restored_blob,
                         Blob::FromDescriptor(serialized->data(), serialized->size()));
    ASSERT_EQ(*restored_blob->ToDescriptor(pool_), *serialized);
}

TEST_F(BlobTest, TestNewInputStreamWithOffsetAndLength) {
    // Create blob with offset=2, length=6 (should read "cdefgh")
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob,
                         Blob::FromPath(uri_, /*offset=*/2, /*length=*/6));
    ASSERT_OK_AND_ASSIGN(auto input_stream, blob->NewInputStream(file_system_));
    ASSERT_TRUE(input_stream);

    ASSERT_OK_AND_ASSIGN(int64_t length, input_stream->Length());
    ASSERT_EQ(6, length);

    // Test reading with offset and length applied
    std::string buffer(6, '\0');
    ASSERT_OK_AND_ASSIGN(auto bytes_read, input_stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(6, bytes_read);
    ASSERT_EQ("cdefgh", buffer);
}

TEST_F(BlobTest, TestNewInputStreamWithDynamicLength) {
    // Create blob with offset=2, dynamic length (-1 means read to end)
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob,
                         Blob::FromPath(uri_, /*offset=*/2, /*length=*/-1));
    ASSERT_OK_AND_ASSIGN(auto input_stream, blob->NewInputStream(file_system_));
    ASSERT_TRUE(input_stream);

    ASSERT_OK_AND_ASSIGN(int64_t length, input_stream->Length());
    ASSERT_EQ(12, length);

    // Test reading from offset to end (should read "cdefghijklmn")
    std::string buffer(12, '\0');
    ASSERT_OK_AND_ASSIGN(auto bytes_read, input_stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(12, bytes_read);
    ASSERT_EQ("cdefghijklmn", buffer);
}

TEST_F(BlobTest, TestNewInputStreamClosesTheFileOnFailure) {
    // Whether the descriptor fits its file can only be told once the file is open, so a
    // descriptor pointing past the end fails after the open. The handle taken to find that out
    // has to be released there and then: nothing else can reach it any more.
    int32_t close_count = 0;
    auto counting_fs = std::make_shared<CloseCountingFileSystem>(&close_count);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> out_of_range_blob,
                         Blob::FromPath(uri_, /*offset=*/100, /*length=*/1));
    ASSERT_NOK_WITH_MSG(out_of_range_blob->NewInputStream(counting_fs), "exceed total length");
    ASSERT_EQ(close_count, 1);

    // A descriptor that fits hands the open file to the caller instead, who closes it.
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<Blob> blob,
                         Blob::FromPath(uri_, /*offset=*/2, /*length=*/6));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input_stream,
                         blob->NewInputStream(counting_fs));
    ASSERT_EQ(close_count, 1);
    ASSERT_OK(input_stream->Close());
    ASSERT_EQ(close_count, 2);
}

TEST_F(BlobTest, TestArrowField) {
    for (bool nullable : {false, true}) {
        ASSERT_OK_AND_ASSIGN(auto schema, Blob::ArrowField("my_blob", nullable));
        ASSERT_NE(schema, nullptr);

        auto field_result = arrow::ImportField(schema.get());
        ASSERT_TRUE(field_result.ok());
        auto field = field_result.ValueUnsafe();

        ASSERT_EQ(field->name(), "my_blob");
        ASSERT_EQ(field->type()->id(), arrow::Type::LARGE_BINARY);
        ASSERT_EQ(field->nullable(), nullable);
        ASSERT_TRUE(field->HasMetadata());
        auto extension_type = field->metadata()->Get("paimon.extension.type");
        ASSERT_TRUE(extension_type.ok());
        ASSERT_EQ(extension_type.ValueUnsafe(), "paimon.type.blob");
    }
    {
        std::unordered_map<std::string, std::string> custom_metadata = {
            {"custom_key", "custom_value"}};
        ASSERT_OK_AND_ASSIGN(auto schema,
                             Blob::ArrowField("meta_blob", /*nullable=*/false, custom_metadata));
        auto field = arrow::ImportField(schema.get()).ValueUnsafe();
        ASSERT_EQ(field->name(), "meta_blob");
        ASSERT_FALSE(field->nullable());
        ASSERT_TRUE(field->HasMetadata());
        auto extension_type = field->metadata()->Get("paimon.extension.type");
        ASSERT_TRUE(extension_type.ok());
        ASSERT_EQ(extension_type.ValueUnsafe(), "paimon.type.blob");
        auto custom_val = field->metadata()->Get("custom_key");
        ASSERT_TRUE(custom_val.ok());
        ASSERT_EQ(custom_val.ValueUnsafe(), "custom_value");
    }
}

}  // namespace paimon::test
