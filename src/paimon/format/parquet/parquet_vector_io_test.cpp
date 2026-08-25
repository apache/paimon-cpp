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

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_output_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/io/vector_file_batch_reader.h"
#include "paimon/defs.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_format_writer.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/writer.h"
#include "parquet/properties.h"

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::parquet::test {

class ParquetVectorIoTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = dir_->GetFileSystem();
    }

    void WriteAndCheck(const std::string& file_name,
                       const std::shared_ptr<arrow::StructType>& write_type,
                       const std::shared_ptr<arrow::StructType>& read_type,
                       const std::string& json) {
        std::string file_path = dir_->Str() + "/" + file_name;
        WriteWithFormatWriter(file_path, write_type, json, /*max_row_group_length=*/1024);

        std::shared_ptr<arrow::StructType> file_type;
        ReadFileType(file_path, &file_type);
        std::shared_ptr<arrow::DataType> physical_value_type = file_type->field(1)->type();
        if (physical_value_type->id() == arrow::Type::STRUCT) {
            physical_value_type = physical_value_type->field(0)->type();
        }
        ASSERT_EQ(physical_value_type->id(), arrow::Type::LIST);

        std::unique_ptr<FileBatchReader> vector_reader;
        CreateVectorReader(file_path, arrow::schema(read_type->fields()), /*predicate=*/nullptr,
                           /*options=*/{}, /*batch_size=*/10, &vector_reader);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                             paimon::test::ReadResultCollector::CollectResult(vector_reader.get()));

        arrow::Result<std::shared_ptr<arrow::Array>> expected_result =
            arrow::ipc::internal::json::ArrayFromJSON(read_type, json);
        ASSERT_TRUE(expected_result.ok()) << expected_result.status().ToString();
        std::shared_ptr<arrow::Array> expected = std::move(expected_result).ValueOrDie();
        ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(expected)->Equals(actual))
            << actual->ToString();
    }

    /// Writes the JSON rows through the Paimon Parquet writer, which stores VECTOR values as
    /// Parquet LIST.
    void WriteWithFormatWriter(const std::string& file_path,
                               const std::shared_ptr<arrow::StructType>& write_type,
                               const std::string& json, int64_t max_row_group_length) {
        arrow::Result<std::shared_ptr<arrow::Array>> write_array_result =
            arrow::ipc::internal::json::ArrayFromJSON(write_type, json);
        ASSERT_TRUE(write_array_result.ok()) << write_array_result.status().ToString();
        std::shared_ptr<arrow::Array> write_array = std::move(write_array_result).ValueOrDie();
        auto c_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*write_array, c_array.get()).ok());

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder properties_builder;
        properties_builder.max_row_group_length(max_row_group_length);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ParquetFormatWriter> writer,
            ParquetFormatWriter::Create(out, arrow::schema(write_type->fields()),
                                        properties_builder.build(),
                                        DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));
        ASSERT_OK(writer->AddBatch(c_array.get()));
        ASSERT_OK(writer->Finish());
        ASSERT_OK(out->Close());
    }

    /// Writes `array` with the plain Arrow Parquet writer, storing the Arrow schema so that
    /// FixedSizeList columns are read back as FixedSizeList, the way Paimon Rust and Python
    /// writers store them.
    void WriteWithArrowWriter(const std::string& file_path,
                              const std::shared_ptr<arrow::StructType>& type,
                              const std::string& json) {
        arrow::Result<std::shared_ptr<arrow::Array>> array_result =
            arrow::ipc::internal::json::ArrayFromJSON(type, json);
        ASSERT_TRUE(array_result.ok()) << array_result.status().ToString();
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_result =
            arrow::RecordBatch::FromStructArray(std::move(array_result).ValueOrDie());
        ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
        arrow::Result<std::shared_ptr<arrow::Table>> table_result =
            arrow::Table::FromRecordBatches({std::move(batch_result).ValueOrDie()});
        ASSERT_TRUE(table_result.ok()) << table_result.status().ToString();

        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path, /*overwrite=*/false));
        auto arrow_out = std::make_shared<ArrowOutputStreamAdapter>(out);
        ::parquet::WriterProperties::Builder properties_builder;
        std::shared_ptr<::parquet::ArrowWriterProperties> arrow_properties =
            ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
        arrow::Status status = ::parquet::arrow::WriteTable(
            *std::move(table_result).ValueOrDie(), arrow_pool_.get(), arrow_out,
            /*chunk_size=*/1024, properties_builder.build(), arrow_properties);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_OK(out->Close());
    }

    void ReadFileType(const std::string& file_path,
                      std::shared_ptr<arrow::StructType>* file_type_out) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
        ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
        auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, length, arrow_pool_);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ParquetFileBatchReader> reader,
            ParquetFileBatchReader::Create(std::move(in_stream), /*options=*/{},
                                           /*batch_size=*/10, /*file_metadata=*/nullptr,
                                           /*storage_read_bytes=*/nullptr, arrow_pool_,
                                           /*hints=*/std::nullopt));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ArrowSchema> c_file_schema, reader->GetFileSchema());
        arrow::Result<std::shared_ptr<arrow::DataType>> file_type_result =
            arrow::ImportType(c_file_schema.get());
        ASSERT_TRUE(file_type_result.ok()) << file_type_result.status().ToString();
        *file_type_out =
            checked_pointer_cast<arrow::StructType>(std::move(file_type_result).ValueOrDie());
    }

    void CreateVectorReader(const std::string& file_path,
                            const std::shared_ptr<arrow::Schema>& read_schema,
                            const std::shared_ptr<Predicate>& predicate,
                            const std::map<std::string, std::string>& options, int32_t batch_size,
                            std::unique_ptr<FileBatchReader>* vector_reader_out) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in, fs_->Open(file_path));
        ASSERT_OK_AND_ASSIGN(int64_t length, in->Length());
        auto in_stream = std::make_shared<ArrowInputStreamAdapter>(in, length, arrow_pool_);
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<ParquetFileBatchReader> reader,
            ParquetFileBatchReader::Create(std::move(in_stream), options, batch_size,
                                           /*file_metadata=*/nullptr,
                                           /*storage_read_bytes=*/nullptr, arrow_pool_,
                                           /*hints=*/std::nullopt));
        std::unique_ptr<FileBatchReader> vector_reader =
            std::make_unique<VectorFileBatchReader>(std::move(reader), pool_);
        auto c_schema = std::make_unique<ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, c_schema.get()).ok());
        ASSERT_OK(vector_reader->SetReadSchema(c_schema.get(), predicate,
                                               /*selection_bitmap=*/std::nullopt));
        *vector_reader_out = std::move(vector_reader);
    }

    void ReadFixtureAndCheck(
        const std::string& file_name, arrow::Type::type expected_file_vector_type,
        int32_t vector_length, const std::vector<int32_t>& expected_ids,
        const std::vector<std::optional<std::vector<float>>>& expected_vectors) {
        std::string file_path =
            paimon::test::GetDataDir() + "/parquet/vector_compatibility/" + file_name;
        std::shared_ptr<arrow::StructType> file_type;
        ReadFileType(file_path, &file_type);
        std::shared_ptr<arrow::Field> file_vector_field = file_type->GetFieldByName("embedding");
        ASSERT_TRUE(file_vector_field);
        ASSERT_EQ(file_vector_field->type()->id(), expected_file_vector_type);
        std::shared_ptr<arrow::Field> file_id_field = file_type->GetFieldByName("id");
        ASSERT_TRUE(file_id_field);

        auto vector_type = arrow::fixed_size_list(
            arrow::field("element", arrow::float32(), /*nullable=*/false), vector_length);
        auto logical_schema =
            arrow::schema({file_id_field, file_vector_field->WithType(vector_type)});
        std::unique_ptr<FileBatchReader> vector_reader;
        CreateVectorReader(file_path, logical_schema, /*predicate=*/nullptr, /*options=*/{},
                           /*batch_size=*/10, &vector_reader);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                             paimon::test::ReadResultCollector::CollectResult(vector_reader.get()));
        ASSERT_EQ(actual->num_chunks(), 1);
        ASSERT_EQ(actual->type()->id(), arrow::Type::STRUCT);
        auto struct_array = checked_pointer_cast<arrow::StructArray>(actual->chunk(0));
        std::shared_ptr<arrow::Array> id_field = struct_array->GetFieldByName("id");
        std::shared_ptr<arrow::Array> vector_field = struct_array->GetFieldByName("embedding");
        ASSERT_TRUE(id_field);
        ASSERT_TRUE(vector_field);
        ASSERT_EQ(id_field->type_id(), arrow::Type::INT32);
        ASSERT_EQ(vector_field->type_id(), arrow::Type::FIXED_SIZE_LIST);
        auto ids = checked_pointer_cast<arrow::Int32Array>(id_field);
        auto vector_array = checked_pointer_cast<arrow::FixedSizeListArray>(vector_field);
        ASSERT_EQ(ids->length(), static_cast<int64_t>(expected_ids.size()));
        ASSERT_EQ(vector_array->length(), static_cast<int64_t>(expected_vectors.size()));
        for (int64_t i = 0; i < ids->length(); ++i) {
            ASSERT_FALSE(ids->IsNull(i));
            ASSERT_EQ(ids->Value(i), expected_ids[i]);
            if (!expected_vectors[i]) {
                ASSERT_TRUE(vector_array->IsNull(i));
                continue;
            }
            ASSERT_FALSE(vector_array->IsNull(i));
            ASSERT_EQ(vector_array->value_length(i),
                      static_cast<int64_t>(expected_vectors[i]->size()));
            auto values = checked_pointer_cast<arrow::FloatArray>(vector_array->value_slice(i));
            for (int64_t j = 0; j < values->length(); ++j) {
                ASSERT_FALSE(values->IsNull(j));
                ASSERT_FLOAT_EQ(values->Value(j), expected_vectors[i].value()[j]);
            }
        }
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<FileSystem> fs_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
};

TEST_F(ParquetVectorIoTest, WriteAndReadVector) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto struct_type = checked_pointer_cast<arrow::StructType>(arrow::struct_(
        {arrow::field("id", arrow::int32()), arrow::field("embedding", vector_type)}));
    WriteAndCheck("vector.parquet", struct_type, struct_type,
                  R"([[1, [1.0, 2.0, 3.0]], [2, null], [3, [4.0, 5.0, 6.0]]])");
}

TEST_F(ParquetVectorIoTest, WriteAndReadAllNullVector) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto struct_type = checked_pointer_cast<arrow::StructType>(arrow::struct_(
        {arrow::field("id", arrow::int32()), arrow::field("embedding", vector_type)}));
    WriteAndCheck("all-null-vector-list.parquet", struct_type, struct_type,
                  R"([[1, null], [2, null], [3, null]])");
}

TEST_F(ParquetVectorIoTest, WriteAndReadAllNullFixedSizeListWithArrowSchema) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto logical_type = checked_pointer_cast<arrow::StructType>(arrow::struct_({
        arrow::field("id", arrow::int32()),
        arrow::field("embedding", vector_type),
    }));
    const std::string json = R"([[1, null], [2, null], [3, null]])";
    std::string file_path = dir_->Str() + "/all-null-vector.parquet";
    WriteWithArrowWriter(file_path, logical_type, json);

    std::shared_ptr<arrow::StructType> file_type;
    ReadFileType(file_path, &file_type);
    std::shared_ptr<arrow::Field> file_vector_field = file_type->GetFieldByName("embedding");
    ASSERT_TRUE(file_vector_field);
    ASSERT_EQ(file_vector_field->type()->id(), arrow::Type::FIXED_SIZE_LIST);

    std::unique_ptr<FileBatchReader> reader;
    CreateVectorReader(file_path, arrow::schema(logical_type->fields()), /*predicate=*/nullptr,
                       /*options=*/{}, /*batch_size=*/10, &reader);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));
    arrow::Result<std::shared_ptr<arrow::Array>> expected_result =
        arrow::ipc::internal::json::ArrayFromJSON(logical_type, json);
    ASSERT_TRUE(expected_result.ok()) << expected_result.status().ToString();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(std::move(expected_result).ValueOrDie())
                    ->Equals(actual))
        << actual->ToString();
}

TEST_F(ParquetVectorIoTest, ReadOrdinaryParquetListAsVector) {
    auto physical_type = checked_pointer_cast<arrow::StructType>(
        arrow::struct_({arrow::field("id", arrow::int32()),
                        arrow::field("embedding", arrow::list(arrow::float32()))}));
    auto logical_type = checked_pointer_cast<arrow::StructType>(arrow::struct_({
        arrow::field("id", arrow::int32()),
        arrow::field("embedding", arrow::fixed_size_list(arrow::float32(), 3)),
    }));
    WriteAndCheck("list.parquet", physical_type, logical_type,
                  R"([[1, [1.0, 2.0, 3.0]], [2, null], [3, [4.0, 5.0, 6.0]]])");
}

TEST_F(ParquetVectorIoTest, WriteAndReadNestedDoubleVector) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float64(), /*nullable=*/false), 2);
    auto struct_type = checked_pointer_cast<arrow::StructType>(arrow::struct_({
        arrow::field("id", arrow::int32()),
        arrow::field("payload", arrow::struct_({arrow::field("embedding", vector_type),
                                                arrow::field("history", arrow::list(vector_type)),
                                                arrow::field("by_name", arrow::map(arrow::utf8(),
                                                                                   vector_type))})),
    }));
    WriteAndCheck("nested-vector.parquet", struct_type, struct_type,
                  R"([[1, [[1.0, 2.0], [[3.0, 4.0], null], [["a", [5.0, 6.0]]]]],
                       [2, [null, null, [["b", null]]]]])");
}

// Vectors nested in a LIST keep their Arrow type when a third-party writer stores them as
// FixedSizeList, so the Parquet reader must accept a FixedSizeList read type as well.
TEST_F(ParquetVectorIoTest, ReadNestedFixedSizeListFile) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto logical_type = checked_pointer_cast<arrow::StructType>(arrow::struct_({
        arrow::field("id", arrow::int32()),
        arrow::field("history", arrow::list(vector_type)),
    }));
    const std::string json = R"([[1, [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]], [2, []]])";
    std::string file_path = dir_->Str() + "/nested-fixed-size-list.parquet";
    WriteWithArrowWriter(file_path, logical_type, json);

    // Without this the file would expose the column as list<list<float>> and the read would take
    // the LIST to VECTOR conversion instead of the nested FixedSizeList path under test.
    std::shared_ptr<arrow::StructType> file_type;
    ReadFileType(file_path, &file_type);
    std::shared_ptr<arrow::Field> file_history_field = file_type->GetFieldByName("history");
    ASSERT_TRUE(file_history_field);
    ASSERT_EQ(file_history_field->type()->id(), arrow::Type::LIST);
    ASSERT_EQ(file_history_field->type()->field(0)->type()->id(), arrow::Type::FIXED_SIZE_LIST);

    std::unique_ptr<FileBatchReader> reader;
    CreateVectorReader(file_path, arrow::schema(logical_type->fields()), /*predicate=*/nullptr,
                       /*options=*/{}, /*batch_size=*/10, &reader);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));
    arrow::Result<std::shared_ptr<arrow::Array>> expected_result =
        arrow::ipc::internal::json::ArrayFromJSON(logical_type, json);
    ASSERT_TRUE(expected_result.ok()) << expected_result.status().ToString();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(std::move(expected_result).ValueOrDie())
                    ->Equals(actual))
        << actual->ToString();
}

TEST_F(ParquetVectorIoTest, ReadVectorWithPredicatePushdown) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 3);
    auto logical_type = checked_pointer_cast<arrow::StructType>(arrow::struct_(
        {arrow::field("id", arrow::int32()), arrow::field("embedding", vector_type)}));
    // One row per row group, so the predicate on `id` prunes row groups while reading.
    std::string file_path = dir_->Str() + "/vector-predicate.parquet";
    WriteWithFormatWriter(file_path, logical_type,
                          R"([[1, [1.0, 2.0, 3.0]], [2, null], [3, [4.0, 5.0, 6.0]],
                              [4, [7.0, 8.0, 9.0]]])",
                          /*max_row_group_length=*/1);

    std::shared_ptr<Predicate> predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(2));
    std::unique_ptr<FileBatchReader> reader;
    CreateVectorReader(file_path, arrow::schema(logical_type->fields()), predicate,
                       /*options=*/{}, /*batch_size=*/10, &reader);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));
    arrow::Result<std::shared_ptr<arrow::Array>> expected_result =
        arrow::ipc::internal::json::ArrayFromJSON(
            logical_type, R"([[3, [4.0, 5.0, 6.0]], [4, [7.0, 8.0, 9.0]]])");
    ASSERT_TRUE(expected_result.ok()) << expected_result.status().ToString();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(std::move(expected_result).ValueOrDie())
                    ->Equals(actual))
        << actual->ToString();
}

TEST_F(ParquetVectorIoTest, ReadJavaFixture) {
    ReadFixtureAndCheck(
        "java_vector.parquet", arrow::Type::LIST, /*vector_length=*/2,
        /*expected_ids=*/{0, 1, 2, 3, 4},
        /*expected_vectors=*/
        {{{0.0f, 0.0f}}, {{1.0f, 0.0f}}, {{2.0f, 0.0f}}, {{3.0f, 0.0f}}, {{4.0f, 0.0f}}});
}

TEST_F(ParquetVectorIoTest, ReadRustFixture) {
    ReadFixtureAndCheck("rust_vector.parquet", arrow::Type::FIXED_SIZE_LIST,
                        /*vector_length=*/3, /*expected_ids=*/{1, 2, 3},
                        /*expected_vectors=*/
                        {{{1.0f, 2.0f, 3.0f}}, {{7.0f, 8.0f, 9.0f}}, {{4.0f, 5.0f, 6.0f}}});
}

TEST_F(ParquetVectorIoTest, ReadNullableJavaFixture) {
    ReadFixtureAndCheck("java_vector_nullable.parquet", arrow::Type::LIST, /*vector_length=*/3,
                        /*expected_ids=*/{1, 2, 3},
                        /*expected_vectors=*/
                        {{{1.0f, 2.0f, 3.0f}}, std::nullopt, {{4.0f, 5.0f, 6.0f}}});
}

TEST_F(ParquetVectorIoTest, ReadNullableRustFixture) {
    ReadFixtureAndCheck("rust_vector_nullable.parquet", arrow::Type::FIXED_SIZE_LIST,
                        /*vector_length=*/3, /*expected_ids=*/{1, 2, 3},
                        /*expected_vectors=*/
                        {{{1.0f, 2.0f, 3.0f}}, std::nullopt, {{4.0f, 5.0f, 6.0f}}});
}

// A table can hold files from several writers, and Paimon Java stores VECTOR as Parquet LIST
// while Paimon Rust stores it as FixedSizeList. Reading both with the table schema must produce
// batches of one Arrow type, otherwise they cannot be combined into a single result.
TEST_F(ParquetVectorIoTest, ReadMixedListAndFixedSizeListFixtures) {
    // The Arrow type a Paimon schema builds for `id INT, embedding VECTOR<FLOAT, 3>`. The Rust
    // fixture instead names the element field `element` and marks it non-nullable.
    auto logical_schema =
        arrow::schema({arrow::field("id", arrow::int32()),
                       arrow::field("embedding", arrow::fixed_size_list(arrow::float32(), 3))});
    std::shared_ptr<arrow::DataType> logical_type = arrow::struct_(logical_schema->fields());

    // A reader owns the memory pool that its batches are allocated from, so it has to outlive
    // the chunks collected from it. This mirrors a scan, which holds every split reader until
    // the whole result has been consumed.
    std::vector<std::unique_ptr<FileBatchReader>> readers;
    arrow::ArrayVector chunks;
    for (const char* file_name : {"java_vector_nullable.parquet", "rust_vector_nullable.parquet"}) {
        std::string file_path =
            paimon::test::GetDataDir() + "/parquet/vector_compatibility/" + file_name;
        std::unique_ptr<FileBatchReader> reader;
        CreateVectorReader(file_path, logical_schema, /*predicate=*/nullptr, /*options=*/{},
                           /*batch_size=*/10, &reader);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                             paimon::test::ReadResultCollector::CollectResult(reader.get()));
        readers.push_back(std::move(reader));
        ASSERT_TRUE(actual->type()->Equals(logical_type))
            << file_name << ": " << actual->type()->ToString();
        chunks.insert(chunks.end(), actual->chunks().begin(), actual->chunks().end());
    }

    arrow::Result<std::shared_ptr<arrow::ChunkedArray>> merged_result =
        arrow::ChunkedArray::Make(chunks);
    ASSERT_TRUE(merged_result.ok()) << merged_result.status().ToString();
    arrow::Result<std::shared_ptr<arrow::Array>> expected_result =
        arrow::ipc::internal::json::ArrayFromJSON(
            logical_type, R"([[1, [1.0, 2.0, 3.0]], [2, null], [3, [4.0, 5.0, 6.0]],
                              [1, [1.0, 2.0, 3.0]], [2, null], [3, [4.0, 5.0, 6.0]]])");
    ASSERT_TRUE(expected_result.ok()) << expected_result.status().ToString();
    std::shared_ptr<arrow::ChunkedArray> merged = std::move(merged_result).ValueOrDie();
    ASSERT_TRUE(std::make_shared<arrow::ChunkedArray>(std::move(expected_result).ValueOrDie())
                    ->Equals(merged))
        << merged->ToString();
}

}  // namespace paimon::parquet::test
