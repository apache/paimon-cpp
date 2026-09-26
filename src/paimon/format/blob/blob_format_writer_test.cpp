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

#include "paimon/format/blob/blob_format_writer.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/stream_utils.h"
#include "paimon/data/blob.h"
#include "paimon/data/blob_descriptor.h"
#include "paimon/format/blob/blob_file_batch_reader.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {

/// A file system whose Open() always fails with the configured status while Exists() keeps the
/// real local check, standing in for a plugin that reports a missing file as something other than
/// Status::NotExist. Open() calls are counted so a test can assert a missing file is never opened.
class OpenFailFileSystem : public LocalFileSystem {
 public:
    explicit OpenFailFileSystem(Status open_status) : open_status_(std::move(open_status)) {}

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        ++open_call_count_;
        return open_status_;
    }

    int64_t OpenCallCount() const {
        return open_call_count_;
    }

 private:
    Status open_status_;
    mutable int64_t open_call_count_ = 0;
};

/// A file system whose Exists() always fails with the configured status, counting the calls. By
/// default Open() is delegated to a separate LocalFileSystem so that it still succeeds
/// (LocalFileSystem::Open() calls Exists() on itself, so without the delegation a failed check
/// would also fail the open); a non-OK `open_status` makes Open() fail with it instead.
class ExistsFailFileSystem : public LocalFileSystem {
 public:
    explicit ExistsFailFileSystem(Status exists_status, Status open_status = Status::OK())
        : exists_status_(std::move(exists_status)), open_status_(std::move(open_status)) {}

    Result<bool> Exists(const std::string& path) const override {
        ++exists_call_count_;
        return exists_status_;
    }

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        if (!open_status_.ok()) {
            return open_status_;
        }
        return real_fs_.Open(path);
    }

    int64_t ExistsCallCount() const {
        return exists_call_count_;
    }

 private:
    Status exists_status_;
    Status open_status_;
    LocalFileSystem real_fs_;
    mutable int64_t exists_call_count_ = 0;
};

/// A file system that reports a file as present on the first Exists() and absent afterwards,
/// standing in for a file deleted between the check and the open. Open() fails with a plain
/// IOError, so a test can tell a re-checked classification apart from one taken from the open.
class VanishingFileSystem : public LocalFileSystem {
 public:
    Result<bool> Exists(const std::string& path) const override {
        return ++exists_call_count_ == 1;
    }

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        return Status::IOError("mock io error");
    }

    int64_t ExistsCallCount() const {
        return exists_call_count_;
    }

 private:
    mutable int64_t exists_call_count_ = 0;
};

class ShortReadFileSystem : public LocalFileSystem {
 public:
    explicit ShortReadFileSystem(int64_t max_read_size,
                                 int64_t readable_length = std::numeric_limits<int64_t>::max(),
                                 Status end_status = Status::OK())
        : max_read_size_(max_read_size),
          readable_length_(readable_length),
          end_status_(std::move(end_status)) {}

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> in, LocalFileSystem::Open(path));
        return std::unique_ptr<InputStream>(
            std::make_unique<ShortReadInputStream>(std::move(in), this));
    }

    int64_t ReadCallCount() const {
        return read_call_count_;
    }

 private:
    class ShortReadInputStream : public InputStream {
     public:
        ShortReadInputStream(std::unique_ptr<InputStream> wrapped, const ShortReadFileSystem* fs)
            : wrapped_(std::move(wrapped)), fs_(fs) {}

        Status Seek(int64_t offset, SeekOrigin origin) override {
            return wrapped_->Seek(offset, origin);
        }
        Result<int64_t> GetPos() const override {
            return wrapped_->GetPos();
        }
        Result<int64_t> Read(char* buffer, int64_t size) override {
            ++fs_->read_call_count_;
            const int64_t read_size =
                std::min({size, fs_->max_read_size_, fs_->readable_length_ - returned_});
            if (read_size <= 0) {
                PAIMON_RETURN_NOT_OK(fs_->end_status_);
                return 0;
            }
            PAIMON_ASSIGN_OR_RAISE(int64_t read_len, wrapped_->Read(buffer, read_size));
            returned_ += read_len;
            return read_len;
        }
        Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
            return wrapped_->Read(buffer, size, offset);
        }
        void ReadAsync(char* buffer, int64_t size, int64_t offset,
                       std::function<void(Status)>&& callback) override {
            wrapped_->ReadAsync(buffer, size, offset, std::move(callback));
        }
        Status Close() override {
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
        const ShortReadFileSystem* fs_;
        int64_t returned_ = 0;
    };

    int64_t max_read_size_;
    int64_t readable_length_;
    Status end_status_;
    mutable int64_t read_call_count_ = 0;
};

class FailingOutputStream : public OutputStream {
 public:
    FailingOutputStream(int64_t write_limit, bool short_write, bool fail_flush)
        : write_limit_(write_limit), short_write_(short_write), fail_flush_(fail_flush) {}

    Result<int64_t> Write(const char* buffer, int64_t size) override {
        if (pos_ + size > write_limit_) {
            if (!short_write_) {
                return Status::IOError("mock write error");
            }
            size = write_limit_ - pos_;
        }
        pos_ += size;
        return size;
    }
    Status Flush() override {
        return fail_flush_ ? Status::IOError("mock flush error") : Status::OK();
    }
    Result<int64_t> GetPos() const override {
        return pos_;
    }
    Result<std::string> GetUri() const override {
        return std::string("mock.blob");
    }
    Status Close() override {
        return Status::OK();
    }

 private:
    int64_t write_limit_;
    bool short_write_;
    bool fail_flush_;
    int64_t pos_ = 0;
};

class BlobFormatWriterTestBase : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        file_system_ = std::make_shared<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(output_stream_,
                             file_system_->Create(dir_->Str() + "/file.blob", /*overwrite=*/true));

        struct_type_ = arrow::struct_({BlobUtils::ToArrowField("blob_col", true)});
    }
    void TearDown() override {
        ASSERT_OK(output_stream_->Flush());
        ASSERT_OK(output_stream_->Close());
    }

    /// Create a writer on output_stream_ with both write-null options disabled.
    Result<std::unique_ptr<BlobFormatWriter>> CreateDefaultWriter() const {
        return BlobFormatWriter::Create(output_stream_, struct_type_,
                                        /*write_null_on_missing_file=*/false,
                                        /*write_null_on_fetch_failure=*/false,
                                        /*write_placeholder=*/false, file_system_, pool_);
    }

    /// Create a writer in placeholder mode, as used by data-evolution partial updates.
    Result<std::unique_ptr<BlobFormatWriter>> CreatePlaceholderWriter() const {
        return BlobFormatWriter::Create(output_stream_, struct_type_,
                                        /*write_null_on_missing_file=*/false,
                                        /*write_null_on_fetch_failure=*/false,
                                        /*write_placeholder=*/true, file_system_, pool_);
    }

    Status AddBatchOnce(const std::shared_ptr<BlobFormatWriter>& format_writer,
                        const std::shared_ptr<arrow::Array>& blob_array) const {
        auto c_array = std::make_unique<ArrowArray>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*blob_array, c_array.get()));
        return format_writer->AddBatch(c_array.get());
    }

    Result<std::shared_ptr<arrow::Array>> PrepareDescriptorArray(
        const std::shared_ptr<Blob>& blob) const {
        return paimon::test::TestHelper::MakeBlobDescriptorArray(struct_type_, blob, pool_);
    }

    /// Build a single-row blob array holding `bytes` verbatim, bypassing the Blob helpers.
    Result<std::shared_ptr<arrow::Array>> MakeBlobArrayFromBytes(const std::string& bytes) const {
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::LargeBinaryBuilder>()});
        auto blob_builder =
            checked_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->Append(bytes.data(), bytes.size()));
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        return array;
    }

    Result<std::shared_ptr<arrow::StructArray>> ReadBackAsData() const {
        return ReadBack(/*blob_as_descriptor=*/false, /*emit_placeholder_sentinel=*/false,
                        "file.blob");
    }

    Result<std::shared_ptr<arrow::StructArray>> ReadBack(bool blob_as_descriptor,
                                                         bool emit_placeholder_sentinel,
                                                         const std::string& file_name) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               file_system_->Open(dir_->Str() + "/" + file_name));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<BlobFileBatchReader> reader,
            BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024, blob_as_descriptor,
                                        emit_placeholder_sentinel, pool_, GetArrowPool(pool_)));
        auto schema = arrow::schema(struct_type_->fields());
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                                   /*selection_bitmap=*/std::nullopt));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> chunked_array,
                               paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> concat_array,
                                          arrow::Concatenate(chunked_array->chunks()));
        return checked_pointer_cast<arrow::StructArray>(concat_array);
    }

    std::string WriteSourceFile(const std::string& name, const std::string& content) const {
        std::string path = dir_->Str() + "/" + name;
        EXPECT_OK(file_system_->WriteFile(path, content, /*overwrite=*/true));
        return path;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<OutputStream> output_stream_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<arrow::DataType> struct_type_;
};

class BlobFormatWriterTest : public BlobFormatWriterTestBase,
                             public ::testing::WithParamInterface<bool> {
 public:
    void SetUp() override {
        blob_as_descriptor_ = GetParam();
        BlobFormatWriterTestBase::SetUp();
    }

    Result<std::shared_ptr<arrow::Array>> PrepareBlobArray(
        const std::shared_ptr<Blob>& blob) const {
        if (blob_as_descriptor_) {
            return PrepareDescriptorArray(blob);
        }
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {std::make_shared<arrow::LargeBinaryBuilder>()});
        auto blob_builder =
            checked_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> blob_data,
                               blob->ToData(file_system_, pool_));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->Append(blob_data->data(), blob_data->size()));
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        return array;
    }

 private:
    bool blob_as_descriptor_;
};

/// The write-null tests always feed descriptor bytes, so they do not depend on the
/// blob_as_descriptor_ parameter and run once on the non-parameterized fixture.
using BlobFormatWriterWriteNullTest = BlobFormatWriterTestBase;

INSTANTIATE_TEST_SUITE_P(BlobAsDescriptor, BlobFormatWriterTest, ::testing::Values(false, true));

TEST_P(BlobFormatWriterTest, TestSimple) {
    // write
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    std::vector<std::shared_ptr<Blob>> expected_blobs;
    std::string file1 = paimon::test::GetDataDir() + "/avro/data/avro_with_null";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob1, Blob::FromPath(file1));
    expected_blobs.emplace_back(blob1);
    ASSERT_OK_AND_ASSIGN(auto array1, PrepareBlobArray(blob1));
    ASSERT_OK(AddBatchOnce(writer, array1));
    ASSERT_OK(writer->Flush());

    std::string file2 = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob2,
                         Blob::FromPath(file2, /*offset=*/0, /*length=*/91));
    expected_blobs.emplace_back(blob2);
    ASSERT_OK_AND_ASSIGN(auto array2, PrepareBlobArray(blob2));
    ASSERT_OK(AddBatchOnce(writer, array2));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob3,
                         Blob::FromPath(file2, /*offset=*/92, /*length=*/85));
    expected_blobs.emplace_back(blob3);
    ASSERT_OK_AND_ASSIGN(auto array3, PrepareBlobArray(blob3));
    ASSERT_OK(AddBatchOnce(writer, array3));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // read
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(
                             input_stream, /*batch_size=*/1024, blob_as_descriptor_,
                             /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(
        reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));

    // check result
    if (blob_as_descriptor_) {
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = checked_pointer_cast<arrow::StructArray>(concat_array);
        ASSERT_TRUE(struct_array);
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Blob>> result_blobs,
                             paimon::test::TestHelper::ToBlobs(struct_array));
        ASSERT_OK_AND_ASSIGN(bool equal, paimon::test::TestHelper::CheckBlobsEqual(
                                             result_blobs, expected_blobs, file_system_));
        ASSERT_TRUE(equal);
    } else {
        auto expected_chunk_array =
            arrow::ChunkedArray::Make({array1, array2, array3}).ValueOrDie();
        ASSERT_TRUE(expected_chunk_array->Equals(chunked_array))
            << expected_chunk_array->ToString() << chunked_array->ToString();
    }
}

TEST_P(BlobFormatWriterTest, TestCreateWithInvalidParameters) {
    // Test with nullptr output stream
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(nullptr, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_),
        "blob format writer create failed. out is nullptr");

    // Test with nullptr data type
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, nullptr, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_),
        "blob format writer create failed. data_type is nullptr");

    // Test with nullptr memory pool
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, nullptr),
        "blob format writer create failed. pool is nullptr");

    // Test with nullptr file system
    ASSERT_NOK_WITH_MSG(
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, nullptr, pool_),
        "blob format writer create failed. fs is nullptr");

    // Test with invalid field count (more than 1 field)
    auto multi_field_type = arrow::struct_(
        {arrow::field("blob_col1", arrow::binary()), arrow::field("blob_col2", arrow::binary())});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(output_stream_, multi_field_type,
                                                 /*write_null_on_missing_file=*/false,
                                                 /*write_null_on_fetch_failure=*/false,
                                                 /*write_placeholder=*/false, file_system_, pool_),
                        "blob data type field number 2 is not 1");

    // Test with non-blob field (missing blob metadata)
    auto non_blob_field = arrow::field("regular_col", arrow::binary());
    auto non_blob_type = arrow::struct_({non_blob_field});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(output_stream_, non_blob_type,
                                                 /*write_null_on_missing_file=*/false,
                                                 /*write_null_on_fetch_failure=*/false,
                                                 /*write_placeholder=*/false, file_system_, pool_),
                        "field regular_col: binary is not BLOB or ARRAY<BLOB>");

    auto plain_binary_array_type =
        arrow::struct_({arrow::field("array_col", arrow::list(arrow::large_binary()))});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(output_stream_, plain_binary_array_type,
                                                 /*write_null_on_missing_file=*/false,
                                                 /*write_null_on_fetch_failure=*/false,
                                                 /*write_placeholder=*/false, file_system_, pool_),
                        "is not BLOB or ARRAY<BLOB>");
    auto nested_array_type = arrow::struct_({arrow::field(
        "nested_col", arrow::list(arrow::list(BlobUtils::ToArrowField("item", true))))});
    ASSERT_NOK_WITH_MSG(BlobFormatWriter::Create(output_stream_, nested_array_type,
                                                 /*write_null_on_missing_file=*/false,
                                                 /*write_null_on_fetch_failure=*/false,
                                                 /*write_placeholder=*/false, file_system_, pool_),
                        "is not BLOB or ARRAY<BLOB>");
}

TEST_P(BlobFormatWriterTest, TestInvalidCase) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    // Test nullptr batch
    ASSERT_NOK_WITH_MSG(writer->AddBatch(nullptr),
                        "blob format writer add batch failed. batch is nullptr");

    // Test invalid blob
    ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromPath("test_path", 0, 10));
    if (blob_as_descriptor_) {
        ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(std::move(blob)));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, array), "File 'test_path' not exists");
    } else {
        ASSERT_NOK_WITH_MSG(PrepareBlobArray(std::move(blob)), "File 'test_path' not exists");
    }
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithInvalidBatchLength) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    // Test batch with wrong length (not 1)
    arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::BinaryBuilder>()});
    auto blob_builder = checked_cast<arrow::BinaryBuilder*>(struct_builder.field_builder(0));

    // Add two rows instead of one
    ASSERT_OK_AND_ASSIGN(auto blob, Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    ASSERT_TRUE(struct_builder.Append().ok());
    auto blob_descriptor = blob->ToDescriptor(pool_);
    ASSERT_TRUE(blob_builder->Append(blob_descriptor->data(), blob_descriptor->size()).ok());
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(blob_builder->Append(blob_descriptor->data(), blob_descriptor->size()).ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());
    auto c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*array, c_array.get()).ok());

    ASSERT_NOK_WITH_MSG(writer->AddBatch(c_array.get()),
                        "BlobFormatWriter only supports batch with a row count of 1");
    ArrowArrayRelease(c_array.get());
}

TEST_P(BlobFormatWriterTest, TestReachTargetSize) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    // Initially should not reach target size
    ASSERT_OK_AND_ASSIGN(bool reached, writer->ReachTargetSize(true, 1000));
    ASSERT_FALSE(reached);

    // Add some data
    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob, Blob::FromPath(file));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(blob));
    ASSERT_OK(AddBatchOnce(writer, array));
    ASSERT_OK(writer->Flush());

    // Check if we reach a small target size
    ASSERT_OK_AND_ASSIGN(reached, writer->ReachTargetSize(true, 10));
    ASSERT_TRUE(reached);

    // Check if we don't reach a large target size
    ASSERT_OK_AND_ASSIGN(reached, writer->ReachTargetSize(true, 100000));
    ASSERT_FALSE(reached);
}

TEST_P(BlobFormatWriterTest, TestGetWriterMetrics) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    auto metrics = writer->GetWriterMetrics();
    ASSERT_TRUE(metrics);
}

TEST_P(BlobFormatWriterTest, TestEmptyWriter) {
    // Test creating a writer and finishing without adding any data
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify the file is the same with java
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(int64_t file_length, input_stream->Length());
    ASSERT_EQ(file_length, 5);  // Should have footer even if no data
    std::vector<char> buffer(file_length);
    ASSERT_OK_AND_ASSIGN(auto read_length, input_stream->Read(buffer.data(), buffer.size()));
    ASSERT_EQ(read_length, 5);
    std::vector<char> expected = {0x00, 0x00, 0x00, 0x00, 0x01};
    ASSERT_EQ(buffer, expected);
}

TEST_P(BlobFormatWriterTest, TestLargeBlob) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    // Create a temporary large file for testing
    std::string large_file_path = dir_->Str() + "/large_test_file.bin";
    ASSERT_OK_AND_ASSIGN(auto large_file_stream,
                         file_system_->Create(large_file_path, /*overwrite=*/true));

    // Write data larger than TMP_BUFFER_SIZE (1MB)
    const size_t large_size = BlobFormatWriter::kTmpBufferSize * 2 + 1000;  // ~2MB
    std::vector<char> large_data(large_size, 'A');
    ASSERT_OK_AND_ASSIGN(int64_t written, large_file_stream->Write(large_data.data(), large_size));
    ASSERT_EQ(written, large_size);
    ASSERT_OK(large_file_stream->Flush());
    ASSERT_OK(large_file_stream->Close());

    // Create blob from large file and write it
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> large_blob, Blob::FromPath(large_file_path));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(large_blob));
    ASSERT_OK(AddBatchOnce(writer, array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify we can read it back
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(
                             input_stream, /*batch_size=*/1024, blob_as_descriptor_,
                             /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(
        reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));

    // check result
    if (blob_as_descriptor_) {
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = checked_pointer_cast<arrow::StructArray>(concat_array);
        ASSERT_TRUE(struct_array);
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Blob>> result_blobs,
                             paimon::test::TestHelper::ToBlobs(struct_array));
        ASSERT_OK_AND_ASSIGN(bool equal, paimon::test::TestHelper::CheckBlobsEqual(
                                             result_blobs, {large_blob}, file_system_));
        ASSERT_TRUE(equal);
    } else {
        auto expected_chunk_array = arrow::ChunkedArray::Make({array}).ValueOrDie();
        ASSERT_TRUE(expected_chunk_array->Equals(chunked_array));
    }
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithNullValues) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    // Write one row with child-level null blob
    arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::LargeBinaryBuilder>()});
    auto blob_builder = checked_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(blob_builder->AppendNull().ok());
    std::shared_ptr<arrow::Array> null_child_array;
    ASSERT_TRUE(struct_builder.Finish(&null_child_array).ok());
    auto c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*null_child_array, c_array.get()).ok());
    ASSERT_OK(writer->AddBatch(c_array.get()));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Read back and verify
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(
                             input_stream, /*batch_size=*/1024, blob_as_descriptor_,
                             /*emit_placeholder_sentinel=*/false, pool_, GetArrowPool(pool_)));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(
        reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));

    auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
    auto result_struct = checked_pointer_cast<arrow::StructArray>(concat_array);
    ASSERT_TRUE(result_struct);
    ASSERT_EQ(result_struct->length(), 1);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));

    // Struct-level null should still be rejected
    arrow::StructBuilder struct_builder2(struct_type_, arrow::default_memory_pool(),
                                         {std::make_shared<arrow::LargeBinaryBuilder>()});
    ASSERT_TRUE(struct_builder2.AppendNull().ok());
    std::shared_ptr<arrow::Array> null_struct_array;
    ASSERT_TRUE(struct_builder2.Finish(&null_struct_array).ok());
    auto null_c_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*null_struct_array, null_c_array.get()).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer2, CreateDefaultWriter());
    ASSERT_NOK_WITH_MSG(writer2->AddBatch(null_c_array.get()),
                        "BlobFormatWriter does not support struct-level null.");
    ArrowArrayRelease(null_c_array.get());
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnMissingFile) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK(AddBatchOnce(writer, missing_array));

    // A fetch failure is not converted to NULL by write_null_on_missing_file alone; the
    // rejected row leaves the writer usable.
    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(file, /*offset=*/1 << 20, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto bad_offset_array, PrepareDescriptorArray(bad_offset_blob));
    Status status = AddBatchOnce(writer, bad_offset_array);
    ASSERT_NOK_WITH_MSG(status, "for BLOB field blob_col in row 1 of blob file");
    ASSERT_NOK_WITH_MSG(status, "exceed total length");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob, Blob::FromPath(file));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));
    ASSERT_OK(AddBatchOnce(writer, array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 2);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
    ASSERT_FALSE(result_struct->field(0)->IsNull(1));
    auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(result_struct->field(0));
    ASSERT_OK_AND_ASSIGN(auto expected_data, blob->ToData(file_system_, pool_));
    ASSERT_EQ(binary_array->GetView(1),
              std::string_view(expected_data->data(), expected_data->size()));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnFetchFailure) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, file_system_, pool_));

    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(file, /*offset=*/1 << 20, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto bad_offset_array, PrepareDescriptorArray(bad_offset_blob));
    ASSERT_OK(AddBatchOnce(writer, bad_offset_array));

    // Without write_null_on_missing_file no existence check runs, so a missing file is not told
    // apart from any other failed open and is converted by this option.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK(AddBatchOnce(writer, missing_array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Both rows count as fetch failures.
    ASSERT_OK_AND_ASSIGN(
        uint64_t missing_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
    ASSERT_EQ(missing_nulls, 0);
    ASSERT_OK_AND_ASSIGN(
        uint64_t fetch_failure_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
    ASSERT_EQ(fetch_failure_nulls, 2);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 2);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
    ASSERT_TRUE(result_struct->field(0)->IsNull(1));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnBothOptionsEnabled) {
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, file_system_, pool_));

    // Row 0: missing file -> NULL.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK(AddBatchOnce(writer, missing_array));

    // Row 1: fetch failure (offset beyond EOF) -> NULL.
    std::string file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(file, /*offset=*/1 << 20, /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto bad_offset_array, PrepareDescriptorArray(bad_offset_blob));
    ASSERT_OK(AddBatchOnce(writer, bad_offset_array));

    // Row 2: valid blob.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob, Blob::FromPath(file));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));
    ASSERT_OK(AddBatchOnce(writer, array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // The two NULL rows had different causes, counted separately.
    ASSERT_OK_AND_ASSIGN(
        uint64_t missing_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
    ASSERT_EQ(missing_nulls, 1);
    ASSERT_OK_AND_ASSIGN(
        uint64_t fetch_failure_nulls,
        writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
    ASSERT_EQ(fetch_failure_nulls, 1);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 3);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
    ASSERT_TRUE(result_struct->field(0)->IsNull(1));
    ASSERT_FALSE(result_struct->field(0)->IsNull(2));
    auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(result_struct->field(0));
    ASSERT_OK_AND_ASSIGN(auto expected_data, blob->ToData(file_system_, pool_));
    ASSERT_EQ(binary_array->GetView(2),
              std::string_view(expected_data->data(), expected_data->size()));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullClassifiesByExistence) {
    // Each case needs its own option pair and therefore its own writer, so none of them finishes
    // the shared output stream: this test only asserts how a failure is classified. That the
    // resulting NULL element is written correctly is covered by the TestWriteNullOn* tests.
    auto io_error_fs = std::make_shared<OpenFailFileSystem>(Status::IOError("mock io error"));

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir_->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));
    ASSERT_OK_AND_ASSIGN(auto missing_array, PrepareDescriptorArray(missing_blob));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> existing_blob,
                         Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    ASSERT_OK_AND_ASSIGN(auto existing_array, PrepareDescriptorArray(existing_blob));

    // Missing file: classified without opening it, so what Open would return is irrelevant.
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, missing_array));
        ASSERT_EQ(io_error_fs->OpenCallCount(), 0);
    }
    // Existing file that cannot be opened: a fetch failure, which this writer does not convert.
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, existing_array), "mock io error");
    }
    // The same fetch failure, now converted to NULL.
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
    }
    // Missing file with only fetch-failure enabled: no existence check runs, so the file is
    // opened and the failure is converted like any other fetch failure. The mock's count
    // accumulates across cases, so compare against it.
    {
        const int64_t open_calls_before = io_error_fs->OpenCallCount();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, io_error_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, missing_array));
        ASSERT_EQ(io_error_fs->OpenCallCount(), open_calls_before + 1);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 1);
    }

    // A file deleted between the check and the open is still a missing file: the failed open
    // triggers one more check rather than being classified by its status. Without it the deletion
    // would defeat write_null_on_missing_file, which does not convert a fetch failure. Each case
    // needs its own file system, since the mock reports the file as present only on the first call.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
        // One check before the open and one after it.
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 2);
        ASSERT_OK_AND_ASSIGN(
            uint64_t missing_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
        ASSERT_EQ(missing_nulls, 1);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
    }
    // The same deletion with both options enabled: classified as missing rather than swallowed
    // by fetch-failure.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 2);
        ASSERT_OK_AND_ASSIGN(
            uint64_t missing_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
        ASSERT_EQ(missing_nulls, 1);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
    }
    // The same deletion with only fetch-failure enabled: existence is never consulted, and the
    // failed open is converted like any other fetch failure.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, existing_array));
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 0);
    }
    // Neither option: existence is never consulted, and the open failure propagates as it is.
    {
        auto vanishing_fs = std::make_shared<VanishingFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, vanishing_fs, pool_));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, existing_array), "mock io error");
        ASSERT_EQ(vanishing_fs->ExistsCallCount(), 0);
    }
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnInvalidDescriptor) {
    // Descriptor detection only inspects version and magic, so a descriptor truncated after those
    // passes detection and then fails to deserialize: a fetch failure, not a missing file.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob,
                         Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    PAIMON_UNIQUE_PTR<Bytes> descriptor = blob->ToDescriptor(pool_);
    ASSERT_GT(descriptor->size(), 8);
    std::string truncated(descriptor->data(), descriptor->size() - 8);
    ASSERT_OK_AND_ASSIGN(bool is_descriptor,
                         BlobDescriptor::IsBlobDescriptor(truncated.data(), truncated.size()));
    ASSERT_TRUE(is_descriptor);
    ASSERT_OK_AND_ASSIGN(auto array, MakeBlobArrayFromBytes(truncated));

    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, file_system_, pool_));
        ASSERT_NOK_WITH_MSG(AddBatchOnce(writer, array), "invalid blob descriptor");
    }
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, file_system_, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
    }

    // Only the second case reaches the stream; the first fails before writing any byte.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 1);
    ASSERT_TRUE(result_struct->field(0)->IsNull(0));
}

TEST_F(BlobFormatWriterWriteNullTest, TestWriteNullOnExistsCheckFailure) {
    // An existence check that cannot answer leaves it unknown whether the file is there. With no
    // fetch-failure handling to defer to, the write fails; otherwise the failed check is deferred
    // to the open, whose own outcome decides.
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob,
                         Blob::FromPath(paimon::test::GetDataDir() + "/xxhash.data"));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));

    // Deferred check failure whose open succeeds: the blob is written as data, not as NULL.
    // This case finishes the shared output stream, so it runs first and is read back below.
    {
        auto exists_fail_fs =
            std::make_shared<ExistsFailFileSystem>(Status::IOError("mock exists error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        ASSERT_EQ(exists_fail_fs->ExistsCallCount(), 1);
        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        ASSERT_OK_AND_ASSIGN(
            uint64_t missing_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
        ASSERT_EQ(missing_nulls, 0);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
    }
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 1);
    ASSERT_FALSE(result_struct->field(0)->IsNull(0));
    auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(result_struct->field(0));
    ASSERT_OK_AND_ASSIGN(auto expected_data, blob->ToData(file_system_, pool_));
    ASSERT_EQ(binary_array->GetView(0),
              std::string_view(expected_data->data(), expected_data->size()));

    // With no fetch-failure handling to defer to, the check failure fails the write.
    {
        auto exists_fail_fs =
            std::make_shared<ExistsFailFileSystem>(Status::IOError("mock exists error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        // The reported failure names the check and keeps the underlying status message.
        Status check_status = AddBatchOnce(writer, array);
        ASSERT_NOK_WITH_MSG(check_status, "failed to check existence of blob file");
        ASSERT_NOK_WITH_MSG(check_status, "mock exists error");
    }
    // Deferred check failure whose open then fails: a fetch failure. The re-check after the
    // failed open cannot answer either, so it falls through to the open failure.
    {
        auto exists_fail_fs = std::make_shared<ExistsFailFileSystem>(
            Status::IOError("mock exists error"), Status::IOError("mock open error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        // One check before the open and one after it failed.
        ASSERT_EQ(exists_fail_fs->ExistsCallCount(), 2);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 1);
    }
    // With only write_null_on_fetch_failure, no existence check runs at all; the open succeeds
    // and the blob is written as data. A separate output stream keeps the data bytes out of the
    // already finished shared stream.
    {
        auto exists_fail_fs =
            std::make_shared<ExistsFailFileSystem>(Status::IOError("mock exists error"));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> side_stream,
                             file_system_->Create(dir_->Str() + "/side.blob", /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer,
                             BlobFormatWriter::Create(
                                 side_stream, struct_type_, /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/true,
                                 /*write_placeholder=*/false, exists_fail_fs, pool_));
        ASSERT_OK(AddBatchOnce(writer, array));
        ASSERT_EQ(exists_fail_fs->ExistsCallCount(), 0);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, 0);
        ASSERT_OK(side_stream->Flush());
        ASSERT_OK(side_stream->Close());
    }
}

TEST_F(BlobFormatWriterWriteNullTest, TestCopyWithShortReads) {
    const std::string data = "0123456789";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob,
                         Blob::FromPath(WriteSourceFile("source.bin", data)));
    ASSERT_OK_AND_ASSIGN(auto array, PrepareDescriptorArray(blob));

    auto short_read_fs = std::make_shared<ShortReadFileSystem>(/*max_read_size=*/3);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_,
                                 /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, short_read_fs, pool_));
    ASSERT_OK(AddBatchOnce(writer, array));
    ASSERT_OK(writer->Finish());
    ASSERT_EQ(short_read_fs->ReadCallCount(), 4);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::StructArray> result_struct, ReadBackAsData());
    ASSERT_EQ(result_struct->length(), 1);
    auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(result_struct->field(0));
    ASSERT_EQ(binary_array->GetString(0), data);

    const std::vector<std::pair<Status, std::string>> cases = {
        {Status::IOError("mock read error"), "mock read error"},
        {Status::OK(), "unexpected end of blob data after 7 of 10 bytes: read returned 0"}};
    for (const auto& [end_status, expected_error] : cases) {
        SCOPED_TRACE(expected_error);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> failing_writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true, /*write_placeholder=*/false,
                                 std::make_shared<ShortReadFileSystem>(
                                     /*max_read_size=*/3, /*readable_length=*/7, end_status),
                                 pool_));
        Status status = AddBatchOnce(failing_writer, array);
        ASSERT_NOK_WITH_MSG(status, "failed to copy BLOB field blob_col in row 0 of blob file");
        ASSERT_NOK_WITH_MSG(status, expected_error);
    }
}

TEST_P(BlobFormatWriterTest, TestAddBatchWithZeroLengthBlob) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());

    // Create a zero-length file
    std::string zero_file_path = dir_->Str() + "/zero_length_file.bin";
    ASSERT_OK_AND_ASSIGN(auto zero_file_stream,
                         file_system_->Create(zero_file_path, /*overwrite=*/true));
    ASSERT_OK(zero_file_stream->Flush());
    ASSERT_OK(zero_file_stream->Close());

    // Create blob from zero-length file
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> zero_blob, Blob::FromPath(zero_file_path));

    // This should work - zero-length blobs should be supported
    ASSERT_OK_AND_ASSIGN(auto array, PrepareBlobArray(zero_blob));
    ASSERT_OK(AddBatchOnce(writer, array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify the file is the same with java
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(int64_t file_length, input_stream->Length());
    ASSERT_EQ(file_length, 22);
    std::vector<uint8_t> buffer(file_length);
    ASSERT_OK_AND_ASSIGN(auto read_length,
                         input_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_EQ(read_length, 22);
    std::vector<uint8_t> expected = {{0xcf, 0x11, 0x4e, 0x58, 0x10, 0x00, 0x00, 0x00,
                                      0x00, 0x00, 0x00, 0x00, 0x53, 0x7f, 0xdf, 0x03,
                                      0x20, 0x01, 0x00, 0x00, 0x00, 0x01}};
    ASSERT_EQ(buffer, expected);
}

/// Placeholder tests always feed the sentinel bytes of the placeholder write protocol, so
/// they do not depend on the blob_as_descriptor_ parameter and run once on the
/// non-parameterized fixture.
using BlobFormatWriterPlaceholderTest = BlobFormatWriterTestBase;

std::string PlaceholderSentinelBytes() {
    return std::string(BlobDefs::PlaceholderSentinelView());
}

TEST_F(BlobFormatWriterPlaceholderTest, TestWritePlaceholderGoldenBytes) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());

    // row 0: inline bytes "inline"; row 1: null; row 2: placeholder
    ASSERT_OK_AND_ASSIGN(auto inline_array, MakeBlobArrayFromBytes("inline"));
    ASSERT_OK(AddBatchOnce(writer, inline_array));

    arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::LargeBinaryBuilder>()});
    auto blob_builder = checked_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(0));
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(blob_builder->AppendNull().ok());
    std::shared_ptr<arrow::Array> null_array;
    ASSERT_TRUE(struct_builder.Finish(&null_array).ok());
    ASSERT_OK(AddBatchOnce(writer, null_array));

    ASSERT_OK_AND_ASSIGN(auto placeholder_array,
                         MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, placeholder_array));

    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    // Verify byte-level alignment with the Java writer (BlobFormatWriterTest
    // testRawBlobGoldenBytes): null and placeholder rows occupy no data bytes; the index
    // records [22, -1, -2] as zigzag varint deltas [0x2c, 0x2d, 0x01].
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_TRUE(input_stream);
    ASSERT_OK_AND_ASSIGN(int64_t file_length, input_stream->Length());
    ASSERT_EQ(file_length, 30);
    std::vector<uint8_t> buffer(file_length);
    ASSERT_OK_AND_ASSIGN(auto read_length,
                         input_stream->Read(reinterpret_cast<char*>(buffer.data()), buffer.size()));
    ASSERT_EQ(read_length, 30);
    std::vector<uint8_t> expected = {{// record 0: magic + "inline" + bin_length(22) + crc32
                                      0xcf, 0x11, 0x4e, 0x58, 0x69, 0x6e, 0x6c, 0x69, 0x6e, 0x65,
                                      0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x29, 0x60,
                                      0xc8, 0xe9,
                                      // index of [22, -1, -2]
                                      0x2c, 0x2d, 0x01,
                                      // footer: index length + version
                                      0x03, 0x00, 0x00, 0x00, 0x01}};
    ASSERT_EQ(buffer, expected);
}

TEST_F(BlobFormatWriterPlaceholderTest, TestReadPlaceholderStrictAndAwareModes) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());
    ASSERT_OK_AND_ASSIGN(auto inline_array, MakeBlobArrayFromBytes("inline"));
    ASSERT_OK(AddBatchOnce(writer, inline_array));
    ASSERT_OK_AND_ASSIGN(auto placeholder_array,
                         MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, placeholder_array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    auto schema = arrow::schema(struct_type_->fields());

    // default (strict) mode: reading a placeholder entry fails
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                             BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                         /*blob_as_descriptor=*/false,
                                                         /*emit_placeholder_sentinel=*/false, pool_,
                                                         GetArrowPool(pool_)));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_NOK_WITH_MSG(reader->NextBatch(), "placeholder");
    }

    // placeholder-aware mode: the entry is returned as the sentinel bytes
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                             BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                         /*blob_as_descriptor=*/false,
                                                         /*emit_placeholder_sentinel=*/true, pool_,
                                                         GetArrowPool(pool_)));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = checked_pointer_cast<arrow::StructArray>(concat_array);
        ASSERT_EQ(struct_array->length(), 2);
        auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(binary_array->GetString(0), "inline");
        ASSERT_FALSE(binary_array->IsNull(1));
        ASSERT_EQ(binary_array->GetString(1), PlaceholderSentinelBytes());
    }

    // placeholder-aware descriptor mode also returns the sentinel bytes
    {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                             BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                         /*blob_as_descriptor=*/true,
                                                         /*emit_placeholder_sentinel=*/true, pool_,
                                                         GetArrowPool(pool_)));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = checked_pointer_cast<arrow::StructArray>(concat_array);
        auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(binary_array->GetString(1), PlaceholderSentinelBytes());
    }
}

TEST_F(BlobFormatWriterPlaceholderTest, TestReadPlaceholderWithSelectionBitmap) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());
    ASSERT_OK_AND_ASSIGN(auto array0, MakeBlobArrayFromBytes("first"));
    ASSERT_OK(AddBatchOnce(writer, array0));
    ASSERT_OK_AND_ASSIGN(auto array1, MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, array1));
    ASSERT_OK_AND_ASSIGN(auto array2, MakeBlobArrayFromBytes("third"));
    ASSERT_OK(AddBatchOnce(writer, array2));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                     /*blob_as_descriptor=*/false,
                                                     /*emit_placeholder_sentinel=*/true, pool_,
                                                     GetArrowPool(pool_)));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    RoaringBitmap32 selection;
    selection.Add(1);
    selection.Add(2);
    ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr, selection));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
    auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
    auto struct_array = checked_pointer_cast<arrow::StructArray>(concat_array);
    ASSERT_EQ(struct_array->length(), 2);
    auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
    ASSERT_EQ(binary_array->GetString(0), PlaceholderSentinelBytes());
    ASSERT_EQ(binary_array->GetString(1), "third");
}

TEST_F(BlobFormatWriterPlaceholderTest, TestSentinelBytesVerbatimWithoutPlaceholderMode) {
    // outside placeholder mode a user blob whose bytes equal the sentinel is a normal value: it
    // must be stored as a real entry (not persisted as bin_length -2) and read back unchanged
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());
    ASSERT_OK_AND_ASSIGN(auto sentinel_array, MakeBlobArrayFromBytes(PlaceholderSentinelBytes()));
    ASSERT_OK(AddBatchOnce(writer, sentinel_array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                         file_system_->Open(dir_->Str() + "/file.blob"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BlobFileBatchReader> reader,
                         BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                                     /*blob_as_descriptor=*/false,
                                                     /*emit_placeholder_sentinel=*/false, pool_,
                                                     GetArrowPool(pool_)));
    auto schema = arrow::schema(struct_type_->fields());
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
    auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
    auto struct_array = checked_pointer_cast<arrow::StructArray>(concat_array);
    ASSERT_EQ(struct_array->length(), 1);
    auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
    ASSERT_FALSE(binary_array->IsNull(0));
    ASSERT_EQ(binary_array->GetString(0), PlaceholderSentinelBytes());
}

TEST_F(BlobFormatWriterPlaceholderTest, TestSentinelPrefixedValueVerbatimInPlaceholderMode) {
    // placeholders are identified by exact equality only: even in placeholder mode a real
    // value that merely starts with the sentinel bytes is stored verbatim and read back
    // unchanged in both strict and placeholder-aware modes
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());
    std::string sentinel = PlaceholderSentinelBytes();
    ASSERT_OK_AND_ASSIGN(auto doubled_sentinel_array, MakeBlobArrayFromBytes(sentinel + sentinel));
    ASSERT_OK(AddBatchOnce(writer, doubled_sentinel_array));
    ASSERT_OK_AND_ASSIGN(auto prefixed_array, MakeBlobArrayFromBytes(sentinel + "suffix"));
    ASSERT_OK(AddBatchOnce(writer, prefixed_array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());

    auto schema = arrow::schema(struct_type_->fields());
    for (bool emit_placeholder_sentinel : {false, true}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system_->Open(dir_->Str() + "/file.blob"));
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<BlobFileBatchReader> reader,
            BlobFileBatchReader::Create(input_stream, /*batch_size=*/1024,
                                        /*blob_as_descriptor=*/false, emit_placeholder_sentinel,
                                        pool_, GetArrowPool(pool_)));
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&c_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(auto chunked_array,
                             paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
        auto concat_array = arrow::Concatenate(chunked_array->chunks()).ValueOrDie();
        auto struct_array = checked_pointer_cast<arrow::StructArray>(concat_array);
        auto binary_array = checked_pointer_cast<arrow::LargeBinaryArray>(struct_array->field(0));
        ASSERT_EQ(struct_array->length(), 2);
        ASSERT_EQ(binary_array->GetString(0), sentinel + sentinel);
        ASSERT_EQ(binary_array->GetString(1), sentinel + "suffix");
    }
}

class BlobFormatWriterArrayBlobTest : public BlobFormatWriterTestBase {
 public:
    using BlobElements = std::vector<std::optional<std::string>>;

    void SetUp() override {
        BlobFormatWriterTestBase::SetUp();
        struct_type_ = arrow::struct_({arrow::field(
            "array_blob_col", arrow::list(BlobUtils::ToArrowField("item", true)), true)});
    }

    Result<std::shared_ptr<arrow::Array>> MakeArrayBlobRow(
        const std::optional<BlobElements>& elements) const {
        auto list_type = checked_pointer_cast<arrow::ListType>(struct_type_->field(0)->type());
        auto list_builder = std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), std::make_shared<arrow::LargeBinaryBuilder>(), list_type);
        arrow::StructBuilder struct_builder(struct_type_, arrow::default_memory_pool(),
                                            {list_builder});
        auto blob_builder = checked_cast<arrow::LargeBinaryBuilder*>(list_builder->value_builder());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Append());
        if (!elements) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(list_builder->AppendNull());
        } else {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(list_builder->Append());
            for (const std::optional<std::string>& element : *elements) {
                if (element) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->Append(*element));
                } else {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(blob_builder->AppendNull());
                }
            }
        }
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder.Finish(&array));
        return array;
    }

    Status AddArrayBlobRow(const std::shared_ptr<BlobFormatWriter>& writer,
                           const std::optional<BlobElements>& elements) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array, MakeArrayBlobRow(elements));
        return AddBatchOnce(writer, array);
    }

    Result<std::string> DescriptorOf(const std::string& path) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob, Blob::FromPath(path));
        PAIMON_UNIQUE_PTR<Bytes> descriptor = blob->ToDescriptor(pool_);
        return std::string(descriptor->data(), descriptor->size());
    }

    Result<std::shared_ptr<arrow::ListArray>> ReadBackArrays(
        bool blob_as_descriptor, bool emit_placeholder_sentinel,
        const std::string& file_name = "file.blob") const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> struct_array,
                               ReadBack(blob_as_descriptor, emit_placeholder_sentinel, file_name));
        return checked_pointer_cast<arrow::ListArray>(struct_array->field(0));
    }

    Result<std::vector<std::optional<BlobElements>>> ToElements(const arrow::ListArray& list_array,
                                                                bool blob_as_descriptor) const {
        const auto& values = checked_cast<const arrow::LargeBinaryArray&>(*list_array.values());
        std::vector<std::optional<BlobElements>> rows;
        for (int64_t row = 0; row < list_array.length(); ++row) {
            if (list_array.IsNull(row)) {
                rows.emplace_back(std::nullopt);
                continue;
            }
            BlobElements elements;
            for (int64_t i = list_array.value_offset(row); i < list_array.value_offset(row + 1);
                 ++i) {
                if (values.IsNull(i)) {
                    elements.emplace_back(std::nullopt);
                    continue;
                }
                std::string_view stored = values.GetView(i);
                if (!blob_as_descriptor) {
                    elements.emplace_back(std::string(stored));
                    continue;
                }
                PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Blob> blob,
                                       Blob::FromDescriptor(stored.data(), stored.size()));
                PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> data,
                                       blob->ToData(file_system_, pool_));
                elements.emplace_back(std::string(data->data(), data->size()));
            }
            rows.emplace_back(std::move(elements));
        }
        return rows;
    }
};

TEST_F(BlobFormatWriterArrayBlobTest, TestArrayBlobGoldenBytes) {
    ASSERT_OK_AND_ASSIGN(std::string descriptor,
                         DescriptorOf(WriteSourceFile("source.bin", "descriptor")));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());

    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{}));
    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{"inline", std::nullopt, "", descriptor}));
    ASSERT_OK(AddArrayBlobRow(writer, std::nullopt));
    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{PlaceholderSentinelBytes()}));
    ASSERT_OK(writer->Finish());

    std::vector<uint8_t> expected = {
        0xcf, 0x11, 0x4e, 0x58, 0x42, 0x43, 0x42, 0x41, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x1d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x9b, 0xd4, 0x91, 0x57, 0xcf,
        0x11, 0x4e, 0x58, 0x42, 0x43, 0x42, 0x41, 0x01, 0x04, 0x00, 0x00, 0x00, 0x69, 0x6e, 0x6c,
        0x69, 0x6e, 0x65, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x0c, 0x0d,
        0x02, 0x14, 0x04, 0x00, 0x00, 0x00, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0,
        0x83, 0x07, 0x71, 0x3a, 0x28, 0x63, 0x01, 0x04, 0x00, 0x00, 0x00, 0x01};
    std::string content;
    ASSERT_OK(file_system_->ReadFile(dir_->Str() + "/file.blob", &content));
    ASSERT_EQ(std::vector<uint8_t>(content.begin(), content.end()), expected)
        << "expected bytes of Java BlobFormatWriterTest#testArrayBlobGoldenBytes "
           "(apache/paimon#8635)";
}

TEST_F(BlobFormatWriterArrayBlobTest, TestArrayBlobRoundTrip) {
    std::string xxhash_file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> blob_slice,
                         Blob::FromPath(xxhash_file, /*offset=*/92, /*length=*/85));
    PAIMON_UNIQUE_PTR<Bytes> slice_descriptor = blob_slice->ToDescriptor(pool_);
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> slice_data,
                         blob_slice->ToData(file_system_, pool_));
    const std::string slice_bytes(slice_data->data(), slice_data->size());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreateDefaultWriter());
    const std::vector<std::optional<BlobElements>> rows = {
        BlobElements{"first", std::nullopt, "", "last"},
        std::nullopt,
        BlobElements{},
        BlobElements{std::nullopt},
        BlobElements{std::string(slice_descriptor->data(), slice_descriptor->size()), "tail"},
        BlobElements{std::string((1 << 20) + 7, 'x'), "small"},
        BlobElements{PlaceholderSentinelBytes()},
    };
    for (const std::optional<BlobElements>& row : rows) {
        ASSERT_OK(AddArrayBlobRow(writer, row));
    }
    ASSERT_OK(writer->Finish());

    std::vector<std::optional<BlobElements>> expected = rows;
    expected[4] = BlobElements{slice_bytes, "tail"};
    for (bool blob_as_descriptor : {false, true}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ListArray> list_array,
                             ReadBackArrays(blob_as_descriptor,
                                            /*emit_placeholder_sentinel=*/false));
        ASSERT_OK_AND_ASSIGN(std::vector<std::optional<BlobElements>> actual,
                             ToElements(*list_array, blob_as_descriptor));
        ASSERT_EQ(actual, expected) << "blob_as_descriptor: " << blob_as_descriptor;
    }
}

TEST_F(BlobFormatWriterArrayBlobTest, TestArrayBlobPlaceholder) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> writer, CreatePlaceholderWriter());
    const std::string sentinel = PlaceholderSentinelBytes();
    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{"value"}));
    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{sentinel}));
    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{sentinel, sentinel}));
    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{sentinel + "x"}));
    ASSERT_OK(writer->Finish());

    ASSERT_NOK_WITH_MSG(ReadBackArrays(/*blob_as_descriptor=*/false,
                                       /*emit_placeholder_sentinel=*/false),
                        "placeholder");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ListArray> list_array,
                         ReadBackArrays(/*blob_as_descriptor=*/false,
                                        /*emit_placeholder_sentinel=*/true));
    const std::vector<std::optional<BlobElements>> expected = {
        BlobElements{"value"},
        BlobElements{sentinel},
        BlobElements{sentinel, sentinel},
        BlobElements{sentinel + "x"},
    };
    ASSERT_OK_AND_ASSIGN(std::vector<std::optional<BlobElements>> actual,
                         ToElements(*list_array, /*blob_as_descriptor=*/false));
    ASSERT_EQ(actual, expected);
}

TEST_F(BlobFormatWriterArrayBlobTest, TestArrayBlobWriteNullOnUnreachableElements) {
    ASSERT_OK_AND_ASSIGN(const std::string missing_descriptor,
                         DescriptorOf(dir_->Str() + "/not_exist_file"));
    std::string xxhash_file = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> bad_offset_blob,
                         Blob::FromPath(xxhash_file, /*offset=*/1 << 20, /*length=*/10));
    PAIMON_UNIQUE_PTR<Bytes> bad_offset_bytes = bad_offset_blob->ToDescriptor(pool_);
    const std::string bad_offset_descriptor(bad_offset_bytes->data(), bad_offset_bytes->size());
    ASSERT_OK_AND_ASSIGN(const std::string valid_descriptor, DescriptorOf(xxhash_file));
    const std::string truncated_descriptor =
        valid_descriptor.substr(0, valid_descriptor.size() - 8);

    const BlobElements all_unreachable = {
        "first", missing_descriptor,   "second", bad_offset_descriptor,
        "third", truncated_descriptor, "last"};

    struct Case {
        bool write_null_on_missing_file;
        bool write_null_on_fetch_failure;
        BlobElements elements;
        std::string expected_error;
        uint64_t expected_missing_nulls;
        uint64_t expected_fetch_failure_nulls;
    };
    const std::vector<Case> cases = {
        {false, false, {"first", missing_descriptor}, "not exists", 0, 0},
        {false, false, {"first", bad_offset_descriptor}, "exceed total length", 0, 0},
        {false, false, {"first", truncated_descriptor}, "invalid blob descriptor", 0, 0},
        {true, false, {"first", bad_offset_descriptor}, "exceed total length", 0, 0},
        {true, false, {"first", missing_descriptor, "last"}, "", 1, 0},
        {false, true, all_unreachable, "", 0, 3},
        {true, true, all_unreachable, "", 1, 2}};
    for (size_t i = 0; i < cases.size(); ++i) {
        const Case& c = cases[i];
        SCOPED_TRACE(
            fmt::format("case {}: write_null_on_missing_file={}, "
                        "write_null_on_fetch_failure={}",
                        i, c.write_null_on_missing_file, c.write_null_on_fetch_failure));
        const std::string file_name = fmt::format("array_{}.blob", i);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             file_system_->Create(dir_->Str() + "/" + file_name,
                                                  /*overwrite=*/true));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<BlobFormatWriter> writer,
            BlobFormatWriter::Create(out, struct_type_, c.write_null_on_missing_file,
                                     c.write_null_on_fetch_failure,
                                     /*write_placeholder=*/false, file_system_, pool_));
        Status status = AddArrayBlobRow(writer, c.elements);
        if (!c.expected_error.empty()) {
            ASSERT_NOK_WITH_MSG(status,
                                "for element 1 of ARRAY<BLOB> field array_blob_col in row 0 of "
                                "blob file");
            ASSERT_NOK_WITH_MSG(status, c.expected_error);
            ASSERT_OK(out->Close());
            continue;
        }
        ASSERT_OK(status);
        ASSERT_OK_AND_ASSIGN(
            uint64_t missing_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_MISSING_FILE_COUNT));
        ASSERT_EQ(missing_nulls, c.expected_missing_nulls);
        ASSERT_OK_AND_ASSIGN(
            uint64_t fetch_failure_nulls,
            writer->GetWriterMetrics()->GetCounter(BlobMetrics::WRITE_NULL_ON_FETCH_FAILURE_COUNT));
        ASSERT_EQ(fetch_failure_nulls, c.expected_fetch_failure_nulls);
        ASSERT_OK(writer->Finish());
        ASSERT_OK(out->Close());

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ListArray> list_array,
                             ReadBackArrays(/*blob_as_descriptor=*/false,
                                            /*emit_placeholder_sentinel=*/false, file_name));
        BlobElements expected_elements;
        for (const std::optional<std::string>& element : c.elements) {
            const bool unreachable = element == missing_descriptor ||
                                     element == bad_offset_descriptor ||
                                     element == truncated_descriptor;
            expected_elements.push_back(unreachable ? std::optional<std::string>() : element);
        }
        const std::vector<std::optional<BlobElements>> expected = {expected_elements};
        ASSERT_OK_AND_ASSIGN(std::vector<std::optional<BlobElements>> actual,
                             ToElements(*list_array, /*blob_as_descriptor=*/false));
        ASSERT_EQ(actual, expected);
    }
}

TEST_F(BlobFormatWriterArrayBlobTest, TestArrayBlobCopyWithShortReads) {
    const std::string data = "0123456789";
    ASSERT_OK_AND_ASSIGN(std::string descriptor, DescriptorOf(WriteSourceFile("source.bin", data)));

    auto short_read_fs = std::make_shared<ShortReadFileSystem>(/*max_read_size=*/3);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<BlobFormatWriter> writer,
        BlobFormatWriter::Create(output_stream_, struct_type_,
                                 /*write_null_on_missing_file=*/false,
                                 /*write_null_on_fetch_failure=*/false,
                                 /*write_placeholder=*/false, short_read_fs, pool_));
    ASSERT_OK(AddArrayBlobRow(writer, BlobElements{descriptor, "inline", descriptor}));
    ASSERT_OK(writer->Finish());
    ASSERT_EQ(short_read_fs->ReadCallCount(), 8);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ListArray> list_array,
                         ReadBackArrays(/*blob_as_descriptor=*/false,
                                        /*emit_placeholder_sentinel=*/false));
    ASSERT_OK_AND_ASSIGN(std::vector<std::optional<BlobElements>> actual,
                         ToElements(*list_array, /*blob_as_descriptor=*/false));
    const std::vector<std::optional<BlobElements>> expected = {BlobElements{data, "inline", data}};
    ASSERT_EQ(actual, expected);

    const std::vector<std::pair<Status, std::string>> cases = {
        {Status::IOError("mock read error"), "mock read error"},
        {Status::OK(), "unexpected end of blob data after 7 of 10 bytes: read returned 0"}};
    for (const auto& [end_status, expected_error] : cases) {
        SCOPED_TRACE(expected_error);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlobFormatWriter> failing_writer,
                             BlobFormatWriter::Create(
                                 output_stream_, struct_type_, /*write_null_on_missing_file=*/true,
                                 /*write_null_on_fetch_failure=*/true, /*write_placeholder=*/false,
                                 std::make_shared<ShortReadFileSystem>(
                                     /*max_read_size=*/3, /*readable_length=*/7, end_status),
                                 pool_));
        ASSERT_OK(AddArrayBlobRow(failing_writer, std::nullopt));
        Status status = AddArrayBlobRow(failing_writer, BlobElements{"kept", descriptor});
        ASSERT_NOK_WITH_MSG(status,
                            "failed to copy element 1 of ARRAY<BLOB> field array_blob_col "
                            "in row 1 of blob file");
        ASSERT_NOK_WITH_MSG(status, expected_error);
    }
}

TEST_F(BlobFormatWriterArrayBlobTest, TestArrayBlobOutputFailureFailsWrite) {
    struct Case {
        int64_t write_limit;
        bool short_write;
        bool fail_flush;
        bool fails_on_finish;
        std::string expected_error;
    };
    const int64_t array_header_end = 13;
    const int64_t element_data_end = 18;
    const int64_t entry_end = 35;
    const std::string write_error = "failed to write blob file mock.blob: mock write error";
    const std::vector<Case> cases = {
        {0, false, false, false, write_error},
        {2, true, false, false,
         "failed to write blob file mock.blob: unexpected actual length 2 not match with expect 4"},
        {array_header_end, false, false, false,
         "failed to copy element 0 of ARRAY<BLOB> field array_blob_col in row 0 of blob file "
         "mock.blob: " +
             write_error},
        {element_data_end, false, false, false, write_error},
        {entry_end, false, false, true, write_error},
        {std::numeric_limits<int64_t>::max(), false, true, true,
         "failed to flush blob file mock.blob: mock flush error"}};
    for (size_t i = 0; i < cases.size(); ++i) {
        const Case& c = cases[i];
        SCOPED_TRACE(fmt::format("case {}: write_limit={}, short_write={}, fail_flush={}", i,
                                 c.write_limit, c.short_write, c.fail_flush));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<BlobFormatWriter> writer,
            BlobFormatWriter::Create(
                std::make_shared<FailingOutputStream>(c.write_limit, c.short_write, c.fail_flush),
                struct_type_,
                /*write_null_on_missing_file=*/false, /*write_null_on_fetch_failure=*/false,
                /*write_placeholder=*/false, file_system_, pool_));
        Status status = AddArrayBlobRow(writer, BlobElements{"value"});
        if (c.fails_on_finish) {
            ASSERT_OK(status);
            status = writer->Finish();
        }
        ASSERT_NOK_WITH_MSG(status, c.expected_error);
    }
}

}  // namespace paimon::blob::test
