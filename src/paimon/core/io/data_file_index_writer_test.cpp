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
#include <vector>

#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/file_index/empty/empty_file_index_reader.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/defs.h"
#include "paimon/factories/factory.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

// Deliberately implements only WriteTo(), so the V2 integration cannot fall back to a buffer.
class StreamingTestIndexWriter : public FileIndexWriter {
 public:
    StreamingTestIndexWriter(bool fail, bool empty) : fail_(fail), empty_(empty) {}
    Status AddBatch(::ArrowArray* batch) override {
        batch->release(batch);
        return Status::OK();
    }
    Result<bool> WriteTo(OutputStream* output) const override {
        if (empty_) {
            return false;
        }
        for (const std::string chunk : {"stream", "ed"}) {
            PAIMON_ASSIGN_OR_RAISE(int64_t written, output->Write(chunk.data(), chunk.size()));
            if (written != static_cast<int64_t>(chunk.size())) {
                return Status::IOError("short streamed payload");
            }
            if (fail_) {
                return Status::IOError("streamed payload failed");
            }
        }
        return true;
    }

 private:
    bool fail_;
    bool empty_;
};

class StreamingTestIndexer : public FileIndexer {
 public:
    explicit StreamingTestIndexer(const std::map<std::string, std::string>& options)
        : fail_(options.count("fail")), empty_(options.count("empty")) {}
    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* c_schema, const std::shared_ptr<MemoryPool>&) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                          arrow::ImportSchema(c_schema));
        if (schema->num_fields() != 1) {
            return Status::Invalid("expected one field");
        }
        return std::make_shared<StreamingTestIndexWriter>(fail_, empty_);
    }
    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* c_schema, int64_t start, int64_t length,
        const std::shared_ptr<InputStream>& input,
        const std::shared_ptr<MemoryPool>&) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                          arrow::ImportSchema(c_schema));
        if (schema->num_fields() != 1 || length != 8) {
            return Status::Invalid("invalid streamed payload metadata");
        }
        PAIMON_RETURN_NOT_OK(input->Seek(start, SeekOrigin::FS_SEEK_SET));
        char bytes[8];
        PAIMON_ASSIGN_OR_RAISE(int64_t read, input->Read(bytes, length));
        if (read != length || std::string(bytes, sizeof(bytes)) != "streamed") {
            return Status::Invalid("invalid streamed payload bytes");
        }
        return std::make_shared<FileIndexReader>();
    }

 private:
    bool fail_;
    bool empty_;
};

class StreamingTestIndexFactory : public FileIndexerFactory {
 public:
    const char* Identifier() const override {
        return "test-streaming-index";
    }
    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override {
        return std::make_unique<StreamingTestIndexer>(options);
    }
};
REGISTER_PAIMON_FACTORY(StreamingTestIndexFactory);

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
    virtual int32_t FormatVersion() const {
        return FileIndexFormat::V_1;
    }

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
        PAIMON_ASSIGN_OR_RAISE(
            CoreOptions core_options,
            CoreOptions::FromMap(WithFormatVersion(index_options), file_system_));
        PAIMON_ASSIGN_OR_RAISE(FileIndexOptions parsed,
                               FileIndexOptions::FromCoreOptions(core_options));
        return DataFileIndexWriter::Create(schema_, parsed, file_system_, path_factory_, pool_);
    }

    std::map<std::string, std::string> WithFormatVersion(
        std::map<std::string, std::string> options) const {
        options[Options::FILE_INDEX_FORMAT_VERSION] = std::to_string(FormatVersion());
        return options;
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
        return FileIndexFormat::CreateReader(input, pool_, /*options=*/{});
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

class VersionedDataFileIndexWriterTest : public DataFileIndexWriterTest,
                                         public ::testing::WithParamInterface<int32_t> {
 public:
    int32_t FormatVersion() const override {
        return GetParam();
    }
};

class DataFileIndexWriterV2Test : public DataFileIndexWriterTest {
 public:
    int32_t FormatVersion() const override {
        return FileIndexFormat::V_2;
    }
};

TEST_P(VersionedDataFileIndexWriterTest, TestStreamingOnlyPlugin) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.test-streaming-index.columns", "f0"}}));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 2}])")));
    ASSERT_OK_AND_ASSIGN(FileIndexWriteResult result, writer->Finish("stream.orc"));
    ASSERT_TRUE(result.embedded_index);
    ASSERT_TRUE(result.extra_files.empty());
    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(result.embedded_index));
    ASSERT_OK_AND_ASSIGN(auto indexes, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(1, indexes.size());
}

TEST_F(DataFileIndexWriterV2Test, TestStreamingFailureRemovesPartialFile) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.test-streaming-index.columns", "f0"},
                                       {"file-index.test-streaming-index.f0.fail", "true"},
                                       {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1B"}}));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 2}])")));
    const std::string data_path = fmt::format("{}/partial.orc", directory_->Str());
    ASSERT_NOK_WITH_MSG(writer->Finish(data_path), "streamed payload failed");
    ASSERT_OK_AND_ASSIGN(bool exists,
                         file_system_->Exists(path_factory_->ToFileIndexPath(data_path)));
    ASSERT_FALSE(exists);
}

TEST_P(VersionedDataFileIndexWriterTest, TestEmptyStreamingIndex) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.test-streaming-index.columns", "f0"},
                                       {"file-index.test-streaming-index.f0.empty", "true"}}));
    ASSERT_OK(writer->AddBatch(CreateBatch(R"([{"f0": 1, "f1": 2}])")));
    ASSERT_OK_AND_ASSIGN(FileIndexWriteResult result, writer->Finish("empty.orc"));
    ASSERT_TRUE(result.embedded_index);
    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(result.embedded_index));
    ASSERT_OK_AND_ASSIGN(auto indexes, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(1, indexes.size());
    ASSERT_TRUE(dynamic_cast<EmptyFileIndexReader*>(indexes[0].get()));
}

TEST_P(VersionedDataFileIndexWriterTest, TestMultipleIndexesOnSameColumnEmbeddedRoundTrip) {
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.bitmap.columns", "f0"},
                                       {"file-index.bsi.columns", "f0"},
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
    auto input = std::make_shared<ByteArrayInputStream>(result.embedded_index->data(),
                                                        result.embedded_index->size());
    DataInputStream data_input(input);
    ASSERT_OK_AND_ASSIGN(int64_t magic, data_input.ReadValue<int64_t>());
    ASSERT_EQ(FileIndexFormat::MAGIC, magic);
    ASSERT_OK_AND_ASSIGN(int32_t version, data_input.ReadValue<int32_t>());
    ASSERT_EQ(FormatVersion(), version);
    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(result.embedded_index));

    ASSERT_OK_AND_ASSIGN(auto f0_readers, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(2, f0_readers.size());
    for (const std::shared_ptr<FileIndexReader>& f0_reader : f0_readers) {
        ASSERT_OK_AND_ASSIGN(auto equal_result, f0_reader->VisitEqual(Literal(1)));
        ASSERT_EQ("{0,2}", equal_result->ToString());
        ASSERT_OK_AND_ASSIGN(auto null_result, f0_reader->VisitIsNull());
        ASSERT_EQ("{3}", null_result->ToString());
    }

    ASSERT_OK_AND_ASSIGN(auto range_readers, ReadColumn(reader.get(), "f1"));
    ASSERT_EQ(1, range_readers.size());
    ASSERT_OK_AND_ASSIGN(auto greater_result, range_readers[0]->VisitGreaterThan(Literal(20)));
    ASSERT_EQ("{2,3}", greater_result->ToString());
}

TEST_P(VersionedDataFileIndexWriterTest, TestDictionaryEncodedIndexedColumnRoundTrip) {
    // The parquet dictionary passthrough hands compaction batches over still encoded, and the
    // bitmap index only sees the right values if the indexed column is decoded first.
    schema_ =
        arrow::schema({arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto writer,
                         CreateWriter({{"file-index.bitmap.columns", "f0"},
                                       {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}}));

    auto encoded_batch = [](const std::string& indices_json, const std::string& dictionary_json,
                            const std::string& ints_json) {
        std::shared_ptr<arrow::Array> indices =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), indices_json).ValueOrDie();
        std::shared_ptr<arrow::Array> dictionary =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), dictionary_json).ValueOrDie();
        std::shared_ptr<arrow::Array> encoded =
            arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()),
                                               indices, dictionary)
                .ValueOrDie();
        std::shared_ptr<arrow::Array> values =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), ints_json).ValueOrDie();
        return checked_pointer_cast<arrow::StructArray>(
            arrow::StructArray::Make({encoded, values}, std::vector<std::string>{"f0", "f1"})
                .ValueOrDie());
    };

    // Two batches with different dictionaries, which is what a rewrite of several input files
    // hands over: each has to be decoded against its own alphabet, and the pool those decoded
    // columns come from outlives the call that built it.
    ASSERT_OK(
        writer->AddBatch(encoded_batch("[0, 1, 0, 2]", R"(["a", "b", "c"])", "[10, 20, 30, 40]")));
    ASSERT_OK(writer->AddBatch(encoded_batch("[1, 0]", R"(["b", "d"])", "[50, 60]")));

    ASSERT_OK_AND_ASSIGN(FileIndexWriteResult result, writer->Finish("unused.orc"));
    ASSERT_TRUE(result.embedded_index);
    ASSERT_OK_AND_ASSIGN(auto reader, CreateReader(result.embedded_index));

    ASSERT_OK_AND_ASSIGN(auto bitmap_readers, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(1, bitmap_readers.size());
    ASSERT_OK_AND_ASSIGN(auto equal_result,
                         bitmap_readers[0]->VisitEqual(Literal(FieldType::STRING, "a", 1)));
    ASSERT_EQ("{0,2}", equal_result->ToString());
    // Row 1 comes from the first dictionary and row 5 from the second, so a decode that reused the
    // wrong alphabet would land somewhere else.
    ASSERT_OK_AND_ASSIGN(auto shared_value_result,
                         bitmap_readers[0]->VisitEqual(Literal(FieldType::STRING, "b", 1)));
    ASSERT_EQ("{1,5}", shared_value_result->ToString());
    ASSERT_OK_AND_ASSIGN(auto second_batch_result,
                         bitmap_readers[0]->VisitEqual(Literal(FieldType::STRING, "d", 1)));
    ASSERT_EQ("{4}", second_batch_result->ToString());
}

TEST_P(VersionedDataFileIndexWriterTest, TestExternalIndexAndAbortCleanup) {
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
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input, pool_, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(auto bitmap_readers, ReadColumn(reader.get(), "f0"));
    ASSERT_EQ(1, bitmap_readers.size());
    ASSERT_OK_AND_ASSIGN(auto equal_result, bitmap_readers[0]->VisitEqual(Literal(1)));
    ASSERT_EQ("{0}", equal_result->ToString());

    writer->Abort();
    ASSERT_OK_AND_ASSIGN(exists, file_system_->Exists(index_path));
    ASSERT_FALSE(exists);
}

TEST_P(VersionedDataFileIndexWriterTest, TestBsiAndBloomFilterEmbeddedRoundTrip) {
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

TEST_P(VersionedDataFileIndexWriterTest, TestCloseFailureClosesExternalStreamOnce) {
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{"file-index.bitmap.columns", "f0"},
                                               {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1B"},
                                               {Options::FILE_INDEX_FORMAT_VERSION,
                                                std::to_string(FormatVersion())}},
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

TEST_F(DataFileIndexWriterV2Test, TestThresholdIncludesFooterAndTrailer) {
    auto batch = CreateBatch(R"([{"f0": 1, "f1": 10}])");
    ASSERT_OK_AND_ASSIGN(auto sizing_writer,
                         CreateWriter({{"file-index.bitmap.columns", "f0"},
                                       {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD, "1MB"}}));
    ASSERT_OK(sizing_writer->AddBatch(batch));
    ASSERT_OK_AND_ASSIGN(auto sized, sizing_writer->Finish("unused.orc"));
    ASSERT_TRUE(sized.embedded_index);
    int64_t container_size = sized.embedded_index->size();
    {
        // Keep the index embedded when the threshold equals the complete container size.
        ASSERT_OK_AND_ASSIGN(auto writer, CreateWriter({{"file-index.bitmap.columns", "f0"},
                                                        {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD,
                                                         fmt::format("{}B", container_size)}}));
        ASSERT_OK(writer->AddBatch(batch));
        std::string path = path_factory_->NewPath();
        ASSERT_OK_AND_ASSIGN(auto result, writer->Finish(path));
        ASSERT_TRUE(result.embedded_index);
        ASSERT_EQ(container_size, result.embedded_index->size());
        ASSERT_TRUE(result.extra_files.empty());
    }
    {
        // Spill to an external file when the threshold is one byte below the complete container
        // size.
        ASSERT_OK_AND_ASSIGN(auto writer, CreateWriter({{"file-index.bitmap.columns", "f0"},
                                                        {Options::FILE_INDEX_IN_MANIFEST_THRESHOLD,
                                                         fmt::format("{}B", container_size - 1)}}));
        ASSERT_OK(writer->AddBatch(batch));
        std::string path = path_factory_->NewPath();
        ASSERT_OK_AND_ASSIGN(auto result, writer->Finish(path));
        ASSERT_FALSE(result.embedded_index);
        ASSERT_EQ(1, result.extra_files.size());
        ASSERT_OK_AND_ASSIGN(auto file_status,
                             file_system_->GetFileStatus(path_factory_->ToFileIndexPath(path)));
        ASSERT_EQ(container_size, file_status.GetLen());
    }
}

INSTANTIATE_TEST_SUITE_P(ContainerVersions, VersionedDataFileIndexWriterTest,
                         ::testing::Values(1, 2));

}  // namespace paimon::test
