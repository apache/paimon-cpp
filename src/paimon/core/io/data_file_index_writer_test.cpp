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

#include "paimon/core/io/data_file_index_writer.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

struct CloseFailingState {
    int32_t close_count = 0;
    int32_t delete_count = 0;
};

class CloseFailingOutputStream : public MockOutputStream {
 public:
    explicit CloseFailingOutputStream(const std::shared_ptr<CloseFailingState>& state)
        : state_(state) {}

    Result<int64_t> Write(const char*, int64_t size) override {
        return size;
    }

    Status Close() override {
        ++state_->close_count;
        return Status::IOError("close failed");
    }

 private:
    std::shared_ptr<CloseFailingState> state_;
};

class CloseFailingFileSystem : public MockFileSystem {
 public:
    explicit CloseFailingFileSystem(const std::shared_ptr<CloseFailingState>& state)
        : state_(state) {}

    Result<std::unique_ptr<OutputStream>> Create(const std::string&, bool) const override {
        return std::unique_ptr<OutputStream>(new CloseFailingOutputStream(state_));
    }

    Status Delete(const std::string&, bool = true) const override {
        ++state_->delete_count;
        return Status::OK();
    }

 private:
    std::shared_ptr<CloseFailingState> state_;
};

}  // namespace

class DataFileIndexWriterTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        file_system_ = std::make_shared<LocalFileSystem>();
        directory_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(directory_);
        path_factory_ = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory_->Init(directory_->Str(), "orc", "data-", nullptr));
        schema_ =
            arrow::schema({arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32())});
    }

    Result<std::unique_ptr<DataFileIndexWriter>> CreateWriter(
        const std::map<std::string, std::string>& index_options) const {
        PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                               CoreOptions::FromMap(index_options, file_system_));
        PAIMON_ASSIGN_OR_RAISE(FileIndexOptions parsed,
                               FileIndexOptions::FromCoreOptions(core_options));
        return DataFileIndexWriter::Create(schema_, parsed, file_system_, path_factory_, pool_);
    }

    std::shared_ptr<arrow::StructArray> CreateBatch(const std::string& json) const {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema_->fields()), json)
                .ValueOrDie();
        return checked_pointer_cast<arrow::StructArray>(array);
    }

    Result<std::unique_ptr<FileIndexFormat::Reader>> CreateReader(
        const std::shared_ptr<Bytes>& bytes) const {
        auto input = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
        return FileIndexFormat::CreateReader(input, pool_);
    }

    Result<std::vector<std::shared_ptr<FileIndexReader>>> ReadColumn(
        FileIndexFormat::Reader* reader, const std::string& column_name) const {
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema_, &c_schema));
        return reader->ReadColumnIndex(column_name, &c_schema);
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<LocalFileSystem> file_system_;
    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<arrow::Schema> schema_;
};

TEST_F(DataFileIndexWriterTest, TestBitmapAndRangeBitmapEmbeddedRoundTrip) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.bitmap.columns", "f0"},
                                       {"file-index.range-bitmap.columns", "f1"},
                                       {"file-index.range-bitmap.f1.chunk-size", "1KB"},
                                       {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}}));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 10},
                                                {"f0": 2, "f1": 20}])")));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 30},
                                                {"f0": null, "f1": 40}])")));

    ASSERT_OK_AND_ASSIGN(FileIndexWriteResult result, writer->Finish("unused.orc"));
    ASSERT_TRUE(result.embedded_index);
    ASSERT_TRUE(result.extra_files.empty());
    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(result.embedded_index));

    ASSERT_OK_AND_ASSIGN(auto bitmap_readers, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(1, bitmap_readers.size());
    ASSERT_OK_AND_ASSIGN(auto equal_result, bitmap_readers[0]->VisitEqual(Literal(1)));
    ASSERT_EQ("{0,2}", equal_result->ToString());
    ASSERT_OK_AND_ASSIGN(auto null_result, bitmap_readers[0]->VisitIsNull());
    ASSERT_EQ("{3}", null_result->ToString());

    ASSERT_OK_AND_ASSIGN(auto range_readers, ReadColumn(reader.get(), "f1"));
    ASSERT_EQ(1, range_readers.size());
    ASSERT_OK_AND_ASSIGN(auto greater_result, range_readers[0]->VisitGreaterThan(Literal(20)));
    ASSERT_EQ("{2,3}", greater_result->ToString());
}

TEST_F(DataFileIndexWriterTest, TestExternalIndexAndAbortCleanup) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.bitmap.columns", "f0"},
                                       {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1B"}}));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 10}])")));
    std::string data_path = path_factory_->NewPath();

    ASSERT_OK_AND_ASSIGN(FileIndexWriteResult result, writer->Finish(data_path));
    ASSERT_FALSE(result.embedded_index);
    ASSERT_EQ(1, result.extra_files.size());
    ASSERT_TRUE(result.extra_files[0]);
    ASSERT_EQ(PathUtil::GetName(path_factory_->ToFileIndexPath(data_path)),
              result.extra_files[0].value());
    std::string index_path = path_factory_->ToFileIndexPath(data_path);
    ASSERT_OK_AND_ASSIGN(bool exists, file_system_->Exists(index_path));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(index_path));
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input, pool_));
    ASSERT_OK_AND_ASSIGN(auto bitmap_readers, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(1, bitmap_readers.size());
    ASSERT_OK_AND_ASSIGN(auto equal_result, bitmap_readers[0]->VisitEqual(Literal(1)));
    ASSERT_EQ("{0}", equal_result->ToString());

    writer->Abort();
    ASSERT_OK_AND_ASSIGN(exists, file_system_->Exists(index_path));
    ASSERT_FALSE(exists);
}

TEST_F(DataFileIndexWriterTest, TestBsiAndBloomFilterEmbeddedRoundTrip) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.bsi.columns", "f0"},
                                       {"file-index.bloom-filter.columns", "f1"},
                                       {"file-index.bloom-filter.f1.items", "100"},
                                       {"file-index.bloom-filter.f1.fpp", "0.01"},
                                       {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}}));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 10},
                                                {"f0": -2, "f1": 20}])")));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": null, "f1": 30},
                                                {"f0": 5, "f1": 40}])")));

    ASSERT_OK_AND_ASSIGN(FileIndexWriteResult result, writer->Finish("unused.orc"));
    ASSERT_TRUE(result.embedded_index);
    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(result.embedded_index));

    ASSERT_OK_AND_ASSIGN(auto bsi_readers, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(1, bsi_readers.size());
    ASSERT_OK_AND_ASSIGN(auto greater_result, bsi_readers[0]->VisitGreaterThan(Literal(1)));
    ASSERT_EQ("{3}", greater_result->ToString());
    ASSERT_OK_AND_ASSIGN(auto null_result, bsi_readers[0]->VisitIsNull());
    ASSERT_EQ("{2}", null_result->ToString());

    ASSERT_OK_AND_ASSIGN(auto bloom_readers, ReadColumn(reader.get(), "f1"));
    ASSERT_EQ(1, bloom_readers.size());
    ASSERT_OK_AND_ASSIGN(auto present_result, bloom_readers[0]->VisitEqual(Literal(30)));
    ASSERT_TRUE(present_result->IsRemain().value());
}

TEST_F(DataFileIndexWriterTest, TestUnavailableWriterFailsCreation) {
    ASSERT_NOK_WITH_MSG(CreateWriter({{"file-index.unknown.columns", "f0"}}),
                        "File index type 'unknown' is not registered");
}

TEST_F(DataFileIndexWriterTest, TestRejectSystemFieldIndex) {
    std::shared_ptr<arrow::Schema> key_value_schema =
        SpecialFields::CompleteSequenceAndValueKindField(schema_);
    for (const std::string& field_name :
         {SpecialFields::SequenceNumber().Name(), SpecialFields::ValueKind().Name()}) {
        ASSERT_OK_AND_ASSIGN(
            CoreOptions core_options,
            CoreOptions::FromMap({{"file-index.bitmap.columns", field_name}}, file_system_));
        ASSERT_OK_AND_ASSIGN(FileIndexOptions options,
                             FileIndexOptions::FromCoreOptions(core_options));
        ASSERT_NOK_WITH_MSG(DataFileIndexWriter::Create(key_value_schema, options, file_system_,
                                                        path_factory_, pool_),
                            "is a system field");
    }
}

TEST_F(DataFileIndexWriterTest, TestFinishIsOneShot) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.bitmap.columns", "f0"},
                                       {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}}));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 10}])")));
    ASSERT_OK(writer->Finish("unused.orc"));

    ASSERT_NOK_WITH_MSG(writer->Finish("unused.orc"), "already finished");
    ASSERT_NOK_WITH_MSG(writer->AddBatch(CreateBatch(R"([{"f0": 2, "f1": 20}])")),
                        "already finished");
}

TEST_F(DataFileIndexWriterTest, TestCloseFailureClosesExternalStreamOnce) {
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{"file-index.bitmap.columns", "f0"},
                                               {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1B"}},
                                              file_system_));
    ASSERT_OK_AND_ASSIGN(FileIndexOptions options, FileIndexOptions::FromCoreOptions(core_options));
    auto state = std::make_shared<CloseFailingState>();
    auto close_failing_file_system = std::make_shared<CloseFailingFileSystem>(state);
    ASSERT_OK_AND_ASSIGN(auto writer,
                         DataFileIndexWriter::Create(schema_, options, close_failing_file_system,
                                                     path_factory_, pool_));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 10}])")));

    ASSERT_NOK_WITH_MSG(writer->Finish(path_factory_->NewPath()), "close failed");
    ASSERT_EQ(1, state->close_count);
    ASSERT_EQ(1, state->delete_count);
}

}  // namespace paimon::test
