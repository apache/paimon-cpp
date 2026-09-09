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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/lance/lance_utils.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::lance::test {

class LanceFileFormatTest : public ::testing::Test {
 public:
    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(format_, FileFormatFactory::Get("lance", {{"file.format", "lance"}}));
        file_system_ = std::make_shared<LocalFileSystem>();
        directory_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_NE(directory_, nullptr);
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
    }

    Status WriteFile(const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
                     const std::shared_ptr<arrow::Array>& array, int32_t batch_size) const {
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> writer_builder,
                               format_->CreateWriterBuilder(&ffi_schema, batch_size));
        writer_builder->WithMemoryPool(pool_);
        auto* direct_writer_builder = dynamic_cast<DirectWriterBuilder*>(writer_builder.get());
        if (direct_writer_builder == nullptr) {
            return Status::Invalid("Lance writer builder is not path based");
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                               direct_writer_builder->BuildFromPath(path));
        for (int64_t offset = 0; offset < array->length(); offset += batch_size) {
            std::shared_ptr<arrow::Array> slice = array->Slice(offset, batch_size);
            ::ArrowArray ffi_array = {};
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*slice, &ffi_array));
            PAIMON_RETURN_NOT_OK(writer->AddBatch(&ffi_array));
        }
        return writer->Finish();
    }

    Result<std::unique_ptr<FileBatchReader>> OpenReader(const std::string& path,
                                                        int32_t batch_size) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> builder,
                               format_->CreateReaderBuilder(batch_size));
        builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system_->Open(path));
        return builder->Build(input);
    }

 protected:
    std::shared_ptr<FileFormat> format_;
    std::shared_ptr<LocalFileSystem> file_system_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> directory_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

TEST(LanceUtilsTest, ConvertsObjectStoreOptions) {
    auto to_map = [](const LanceStorageOptions& values) {
        return std::map<std::string, std::string>(values.begin(), values.end());
    };
    auto s3_options = to_map(GetLanceStorageOptions({{"s3.region", "us-west-2"},
                                                     {"s3.endpoint", "http://s3.example.com"},
                                                     {"s3.access-key", "key"},
                                                     {"s3.secret-key", "secret"},
                                                     {"s3.session.token", "token"},
                                                     {"s3.path-style-access", "true"},
                                                     {"lance.storage.region", "override"}},
                                                    "s3://bucket/data.lance"));
    ASSERT_EQ(s3_options.at("region"), "override");
    ASSERT_EQ(s3_options.at("endpoint"), "http://s3.example.com");
    ASSERT_EQ(s3_options.at("access_key_id"), "key");
    ASSERT_EQ(s3_options.at("secret_access_key"), "secret");
    ASSERT_EQ(s3_options.at("session_token"), "token");
    ASSERT_EQ(s3_options.at("virtual_hosted_style_request"), "false");
    ASSERT_EQ(s3_options.at("allow_http"), "true");

    auto python_s3_options =
        to_map(GetLanceStorageOptions({{"fs.s3.region", "us-west-1"},
                                       {"fs.s3.endpoint", "s3.python.example.com"},
                                       {"fs.s3.accessKeyId", "python-key"},
                                       {"fs.s3.accessKeySecret", "python-secret"},
                                       {"fs.s3.securityToken", "python-token"},
                                       {"fs.s3.path.style.access", "false"}},
                                      "s3://bucket/data.lance"));
    ASSERT_EQ(python_s3_options.at("region"), "us-west-1");
    ASSERT_EQ(python_s3_options.at("endpoint"), "https://s3.python.example.com");
    ASSERT_EQ(python_s3_options.at("access_key_id"), "python-key");
    ASSERT_EQ(python_s3_options.at("secret_access_key"), "python-secret");
    ASSERT_EQ(python_s3_options.at("session_token"), "python-token");
    ASSERT_EQ(python_s3_options.at("virtual_hosted_style_request"), "true");

    auto java_s3_options = to_map(GetLanceStorageOptions({{"s3a.region", "eu-west-1"},
                                                          {"s3a.access.key", "java-key"},
                                                          {"s3a.secret.key", "java-secret"},
                                                          {"s3a.session.token", "java-token"},
                                                          {"s3.access-key", "canonical-key"}},
                                                         "s3://bucket/data.lance"));
    ASSERT_EQ(java_s3_options.at("region"), "eu-west-1");
    ASSERT_EQ(java_s3_options.at("access_key_id"), "canonical-key");
    ASSERT_EQ(java_s3_options.at("secret_access_key"), "java-secret");
    ASSERT_EQ(java_s3_options.at("session_token"), "java-token");

    auto oss_options =
        to_map(GetLanceStorageOptions({{"fs.oss.endpoint", "global.example.com"},
                                       {"fs.oss.accessKeyId", "global-key"},
                                       {"fs.oss.accessKeySecret", "global-secret"},
                                       {"fs.oss.bucket.bucket.endpoint", "bucket.example.com"},
                                       {"fs.oss.bucket.bucket.accessKeyId", "bucket-key"},
                                       {"fs.oss.bucket.bucket.accessKeySecret", "bucket-secret"},
                                       {"fs.oss.bucket.bucket.securityToken", "bucket-token"}},
                                      "oss://bucket/data.lance"));
    ASSERT_EQ(oss_options.at("oss_endpoint"), "https://bucket.example.com");
    ASSERT_EQ(oss_options.at("oss_access_key_id"), "bucket-key");
    ASSERT_EQ(oss_options.at("oss_secret_access_key"), "bucket-secret");
    ASSERT_EQ(oss_options.at("oss_session_token"), "bucket-token");
    ASSERT_EQ(oss_options.at("access_key_id"), "bucket-key");
    ASSERT_EQ(oss_options.at("secret_access_key"), "bucket-secret");
    ASSERT_EQ(oss_options.at("session_token"), "bucket-token");
}

TEST_F(LanceFileFormatTest, WriteThenReadSupportedTypes) {
    arrow::FieldVector fields = {
        arrow::field("bool_col", arrow::boolean()),
        arrow::field("i8_col", arrow::int8()),
        arrow::field("i16_col", arrow::int16()),
        arrow::field("i32_col", arrow::int32()),
        arrow::field("i64_col", arrow::int64()),
        arrow::field("float_col", arrow::float32()),
        arrow::field("double_col", arrow::float64()),
        arrow::field("string_col", arrow::utf8()),
        arrow::field("binary_col", arrow::binary()),
        arrow::field("date_col", arrow::date32()),
        arrow::field("time_col", arrow::time32(arrow::TimeUnit::MILLI)),
        arrow::field("timestamp_col", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("decimal_col", arrow::decimal128(12, 2)),
        arrow::field("array_col", arrow::list(arrow::float32())),
        arrow::field("row_col", arrow::struct_({arrow::field("value", arrow::int32())})),
        arrow::field(
            "vector_col",
            arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 2)),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [true,-1,-2,-3,-4,1.5,2.5,"one","abc",1,1234,
           "1970-01-01 00:00:00.000001","12.34",[1.0,2.0],[7],[3.0,4.0]],
          [false,1,2,3,4,3.5,4.5,"two","xyz",2,5678,
           "2030-12-31 23:59:59.999999","-12.34",[],[8],[5.0,6.0]],
          [null,null,null,null,null,null,null,null,null,null,null,null,null,null,[null],null]
        ])")
            .ValueOrDie();
    std::string path = PathUtil::JoinPath(directory_->Str(), "supported-types.lance");
    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));

    for (int32_t batch_size : {1, 2, 4}) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, OpenReader(path, batch_size));
        ::ArrowSchema ffi_schema = {};
        ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
        ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                        /*selection_bitmap=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                             paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
        ASSERT_TRUE(actual->Equals(arrow::ChunkedArray(expected))) << actual->ToString() << "\nvs\n"
                                                                   << expected->ToString();
    }
}

TEST_F(LanceFileFormatTest, RejectsNullTopLevelRows) {
    arrow::FieldVector fields = {arrow::field("id", arrow::int32())};
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([[1],null])")
            .ValueOrDie();
    ASSERT_NOK_WITH_MSG(WriteFile(PathUtil::JoinPath(directory_->Str(), "null-row.lance"),
                                  arrow::schema(fields), data, /*batch_size=*/2),
                        "Lance writer does not accept null top-level rows");
}

TEST_F(LanceFileFormatTest, ProjectionSelectionAndFileRowIds) {
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields), R"([[0,"zero"],[1,"one"],[2,"two"],[3,"three"],[4,"four"]])")
            .ValueOrDie();
    std::string path = PathUtil::JoinPath(directory_->Str(), "selection.lance");
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, OpenReader(path, 2));
    std::shared_ptr<arrow::Schema> projected_schema = arrow::schema({fields[1]});
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                    RoaringBitmap32::From({1, 2, 4})));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first_batch, reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> first,
                         paimon::test::ReadResultCollector::GetArray(std::move(first_batch)));
    ASSERT_TRUE(first->Equals(arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[1]}),
                                                                        R"([["one"],["two"]])")
                                  .ValueOrDie()));
    ASSERT_EQ(reader->GetPreviousBatchFileRowId(0).value(), 1);
    ASSERT_EQ(reader->GetPreviousBatchFileRowId(1).value(), 2);

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch second_batch, reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> second,
                         paimon::test::ReadResultCollector::GetArray(std::move(second_batch)));
    ASSERT_TRUE(second->Equals(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[1]}), R"([["four"]])")
            .ValueOrDie()));
    ASSERT_EQ(reader->GetPreviousBatchFileRowId(0).value(), 4);
}

TEST_F(LanceFileFormatTest, EmptyProjectionAndEmptySelection) {
    arrow::FieldVector fields = {arrow::field("id", arrow::int32())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([[1],[2],[3]])")
            .ValueOrDie();
    std::string path = PathUtil::JoinPath(directory_->Str(), "empty-projection.lance");
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, OpenReader(path, 2));
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({}), &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    std::shared_ptr<arrow::RecordBatch> record_batch =
        arrow::ImportRecordBatch(batch.first.get(), batch.second.get()).ValueOrDie();
    ASSERT_EQ(record_batch->num_columns(), 0);
    ASSERT_EQ(record_batch->num_rows(), 2);

    ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr, RoaringBitmap32::From({})));
    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));
}

TEST_F(LanceFileFormatTest, ExtractStatistics) {
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([[1,"a"],[2,null]])")
            .ValueOrDie();
    std::string path = PathUtil::JoinPath(directory_->Str(), "stats.lance");
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatStatsExtractor> extractor,
                         format_->CreateStatsExtractor(&ffi_schema));
    ASSERT_OK_AND_ASSIGN(auto result, extractor->ExtractWithFileInfo(file_system_, path, pool_));
    ASSERT_EQ(result.second.GetRowCount(), 2);
    ASSERT_EQ(result.first.size(), 2);
    ASSERT_EQ(result.first[0]->ToString(), "min null, max null, null count null");
    ASSERT_EQ(result.first[1]->ToString(), "min null, max null, null count null");
}

TEST_F(LanceFileFormatTest, ReadJavaLance039Fixture) {
    std::string path = paimon::test::GetDataDir() + "/lance/java_lance_0_39_0.lance";
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::float64()),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, OpenReader(path, 2));
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields),
            R"([[1,"alpha",1.25],[2,"beta",-2.5],[3,null,null],[4,"delta",4.75]])")
            .ValueOrDie();
    ASSERT_TRUE(actual->Equals(arrow::ChunkedArray(expected))) << actual->ToString();
}

TEST_F(LanceFileFormatTest, RejectsInvalidReaderSettings) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> builder,
                         format_->CreateReaderBuilder(/*batch_size=*/0));
    builder->WithMemoryPool(nullptr);
    ASSERT_NOK_WITH_MSG(builder->Build(nullptr), "memory pool is nullptr");
}

}  // namespace paimon::lance::test
