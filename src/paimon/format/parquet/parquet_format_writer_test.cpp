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

#include "paimon/format/parquet/parquet_format_writer.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/compute/api.h"
#include "arrow/io/file.h"
#include "arrow/ipc/api.h"
#include "arrow/memory_pool.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::parquet::test {

class ParquetFormatWriterTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
    }
    void TearDown() override {}

    std::pair<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::DataType>> PrepareArrowSchema()
        const {
        auto string_field = arrow::field(
            "col1", arrow::utf8(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"0"}));
        auto int_field = arrow::field(
            "col2", arrow::int32(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"1"}));
        auto bool_field = arrow::field(
            "col3", arrow::boolean(),
            arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID}, {"2"}));
        auto struct_type = arrow::struct_({string_field, int_field, bool_field});
        return std::make_pair(
            arrow::schema(arrow::FieldVector({string_field, int_field, bool_field})), struct_type);
    }

    std::shared_ptr<arrow::Array> PrepareArray(const std::shared_ptr<arrow::DataType>& data_type,
                                               int32_t record_batch_size, int32_t offset = 0,
                                               bool all_null_value = false) const {
        arrow::StructBuilder struct_builder(
            data_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
             std::make_shared<arrow::BooleanBuilder>()});
        auto string_builder = checked_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int_builder = checked_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
        auto bool_builder = checked_cast<arrow::BooleanBuilder*>(struct_builder.field_builder(2));
        for (int32_t i = 0 + offset; i < record_batch_size + offset; ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            if (all_null_value) {
                EXPECT_TRUE(string_builder->AppendNull().ok());
                EXPECT_TRUE(int_builder->AppendNull().ok());
                EXPECT_TRUE(bool_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
                if (i % 3 == 0) {
                    // test null
                    EXPECT_TRUE(int_builder->AppendNull().ok());
                } else {
                    EXPECT_TRUE(int_builder->Append(i).ok());
                }
                EXPECT_TRUE(bool_builder->Append(static_cast<bool>(i % 2)).ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    void AddRecordBatchOnce(const std::shared_ptr<FormatWriter>& format_writer,
                            const std::shared_ptr<arrow::DataType>& struct_type,
                            int32_t record_batch_size, int32_t offset,
                            bool all_null_value = false) const {
        auto array = PrepareArray(struct_type, record_batch_size, offset, all_null_value);
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());
        auto batch = std::make_shared<RecordBatch>(
            /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
            /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
        ASSERT_OK(format_writer->AddBatch(batch->GetData()));
    }

    void CheckResult(const std::string& file_path, int32_t row_count,
                     int32_t row_group_count) const {
        auto file = arrow::io::ReadableFile::Open(file_path, arrow_pool_.get());
        ASSERT_TRUE(file.ok());
        std::unique_ptr<::parquet::arrow::FileReader> reader;
        auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
        ASSERT_TRUE(status.ok()) << status.ToString();
        const ::parquet::FileMetaData* metadata = reader->parquet_reader()->metadata().get();
        const ::parquet::SchemaDescriptor* schema = metadata->schema();
        ASSERT_EQ(metadata->num_row_groups(), row_group_count);
        ASSERT_EQ(schema->num_columns(), 3);
        ASSERT_EQ(metadata->num_rows(), row_count);
        ASSERT_EQ("col1", schema->Column(0)->name());
        ASSERT_EQ("col2", schema->Column(1)->name());
        ASSERT_EQ("col3", schema->Column(2)->name());
        ASSERT_EQ(0, schema->Column(0)->schema_node()->field_id());
        ASSERT_EQ(1, schema->Column(1)->schema_node()->field_id());
        ASSERT_EQ(2, schema->Column(2)->schema_node()->field_id());

        std::shared_ptr<::arrow::ChunkedArray> col0_array, col1_array, col2_array;
        ASSERT_TRUE(reader->ReadColumn(0, &col0_array).ok());
        ASSERT_TRUE(reader->ReadColumn(1, &col1_array).ok());
        ASSERT_TRUE(reader->ReadColumn(2, &col2_array).ok());

        const auto& string_array = checked_pointer_cast<arrow::StringArray>(col0_array->chunk(0));
        ASSERT_TRUE(string_array);
        const auto& int_array = checked_pointer_cast<arrow::Int32Array>(col1_array->chunk(0));
        ASSERT_TRUE(int_array);
        const auto& bool_array = checked_pointer_cast<arrow::BooleanArray>(col2_array->chunk(0));
        ASSERT_TRUE(bool_array);
        ASSERT_EQ(string_array->null_count(), 0);
        ASSERT_EQ(int_array->null_count(), (row_count - 1) / 3 + 1);
        ASSERT_EQ(bool_array->null_count(), 0);

        for (int32_t i = 0; i < row_count; i++) {
            ASSERT_EQ("str_" + std::to_string(i), string_array->GetString(i));
            if (i % 3 == 0) {
                ASSERT_TRUE(int_array->IsNull(i));
            } else {
                ASSERT_FALSE(int_array->IsNull(i));
                ASSERT_EQ(i, int_array->Value(i));
            }
            if (i % 2 == 0) {
                ASSERT_EQ(false, bool_array->Value(i));
            } else {
                ASSERT_EQ(true, bool_array->Value(i));
            }
        }
    }

    /// Builds the {col1, col2, col3} batch of this fixture with a low-cardinality col1 that is
    /// either dictionary-encoded or flat, so the same rows reach the writer in both encodings.
    /// Row i holds col1 = "<dictionary_prefix><i % 3>", col2 = i and col3 = i % 2.
    std::shared_ptr<arrow::Array> PrepareEncodedArray(
        int32_t record_batch_size, int32_t offset, bool dictionary_encoded,
        bool null_in_dictionary = false, const std::string& dictionary_prefix = "dict_") const {
        arrow::StringBuilder dictionary_builder;
        for (int32_t i = 0; i < 3; ++i) {
            if (null_in_dictionary && i == 1) {
                EXPECT_TRUE(dictionary_builder.AppendNull().ok());
            } else {
                EXPECT_TRUE(
                    dictionary_builder.Append(fmt::format("{}{}", dictionary_prefix, i)).ok());
            }
        }
        std::shared_ptr<arrow::Array> dictionary;
        EXPECT_TRUE(dictionary_builder.Finish(&dictionary).ok());

        arrow::Int32Builder index_builder;
        arrow::Int32Builder int_builder;
        arrow::BooleanBuilder bool_builder;
        for (int32_t i = offset; i < offset + record_batch_size; ++i) {
            EXPECT_TRUE(index_builder.Append(i % 3).ok());
            EXPECT_TRUE(int_builder.Append(i).ok());
            EXPECT_TRUE(bool_builder.Append(static_cast<bool>(i % 2)).ok());
        }
        std::shared_ptr<arrow::Array> indices, int_array, bool_array;
        EXPECT_TRUE(index_builder.Finish(&indices).ok());
        EXPECT_TRUE(int_builder.Finish(&int_array).ok());
        EXPECT_TRUE(bool_builder.Finish(&bool_array).ok());

        auto dictionary_array =
            arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()),
                                               indices, dictionary)
                .ValueOrDie();
        std::shared_ptr<arrow::Array> string_array = dictionary_array;
        if (!dictionary_encoded) {
            string_array =
                arrow::compute::Cast(dictionary_array, arrow::utf8()).ValueOrDie().make_array();
        }
        return arrow::StructArray::Make({string_array, int_array, bool_array},
                                        std::vector<std::string>{"col1", "col2", "col3"})
            .ValueOrDie();
    }

    void AddStructArrayOnce(const std::shared_ptr<FormatWriter>& format_writer,
                            const std::shared_ptr<arrow::Array>& array) const {
        auto arrow_array = std::make_unique<ArrowArray>();
        ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());
        ASSERT_OK(format_writer->AddBatch(arrow_array.get()));
    }

    /// Fraction of `chunk`'s data pages written with a dictionary index encoding. A dictionary
    /// page on its own says nothing - the writer emits one and then falls back to plain when the
    /// dictionary it kept no longer matches - so the encoding of the data pages is what tells
    /// whether the column really came out dictionary-encoded.
    static std::pair<int32_t, int32_t> CountDictionaryDataPages(
        const ::parquet::ColumnChunkMetaData& chunk) {
        int32_t dictionary_pages = 0;
        int32_t data_pages = 0;
        for (const ::parquet::PageEncodingStats& stats : chunk.encoding_stats()) {
            if (stats.page_type != ::parquet::PageType::DATA_PAGE &&
                stats.page_type != ::parquet::PageType::DATA_PAGE_V2) {
                continue;
            }
            data_pages += stats.count;
            if (stats.encoding == ::parquet::Encoding::RLE_DICTIONARY ||
                stats.encoding == ::parquet::Encoding::PLAIN_DICTIONARY) {
                dictionary_pages += stats.count;
            }
        }
        return {dictionary_pages, data_pages};
    }

    void CheckEncodedResult(const std::string& file_path, int32_t row_count,
                            bool null_in_dictionary) const {
        auto file = arrow::io::ReadableFile::Open(file_path, arrow_pool_.get());
        ASSERT_TRUE(file.ok());
        std::unique_ptr<::parquet::arrow::FileReader> reader;
        auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
        ASSERT_TRUE(status.ok()) << status.ToString();
        const ::parquet::FileMetaData* metadata = reader->parquet_reader()->metadata().get();
        ASSERT_EQ(metadata->num_rows(), row_count);
        // Whether the values arrived encoded or flat, every data page comes out dictionary-encoded.
        auto [dictionary_pages, data_pages] =
            CountDictionaryDataPages(*metadata->RowGroup(0)->ColumnChunk(0));
        ASSERT_GT(data_pages, 0);
        ASSERT_EQ(data_pages, dictionary_pages);

        std::shared_ptr<::arrow::ChunkedArray> col0_array;
        ASSERT_TRUE(reader->ReadColumn(0, &col0_array).ok());
        int32_t row = 0;
        for (const auto& chunk : col0_array->chunks()) {
            const auto& string_array = checked_pointer_cast<arrow::StringArray>(chunk);
            ASSERT_TRUE(string_array);
            for (int64_t i = 0; i < string_array->length(); ++i, ++row) {
                if (null_in_dictionary && row % 3 == 1) {
                    ASSERT_TRUE(string_array->IsNull(i));
                } else {
                    ASSERT_EQ(fmt::format("dict_{}", row % 3), string_array->GetString(i));
                }
            }
        }
        ASSERT_EQ(row_count, row);
    }

    /// @param max_memory_use Lower it to make every AddBatch start a fresh buffered row group,
    ///                       which flushes the previous one to the output stream.
    std::shared_ptr<ParquetFormatWriter> CreateEncodedWriter(
        const std::string& file_path, std::shared_ptr<OutputStream>* out,
        uint64_t max_memory_use = DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE) const {
        EXPECT_OK_AND_ASSIGN(*out, fs_->Create(file_path, /*overwrite=*/false));
        ::parquet::WriterProperties::Builder builder;
        builder.enable_dictionary();
        // What ParquetWriterBuilder does in production. Without it the encoders allocate from
        // Arrow's default pool, so `max_memory_use` would be compared against a pool the writer
        // never touches and the row-group rotation it drives would never fire.
        builder.memory_pool(arrow_pool_.get());
        EXPECT_OK_AND_ASSIGN(
            std::shared_ptr<ParquetFormatWriter> format_writer,
            ParquetFormatWriter::Create(*out, PrepareArrowSchema().first, builder.build(),
                                        max_memory_use, arrow_pool_));
        return format_writer;
    }

 private:
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

TEST_F(ParquetFormatWriterTest, TestWriteWithVariousBatchSize) {
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;
    std::map<std::string, std::string> options;
    for (auto record_batch_size : {1, 2, 3, 5, 20}) {
        for (auto batch_capacity : {1, 2, 3, 5, 20}) {
            std::string file_name =
                std::to_string(record_batch_size) + "_" + std::to_string(batch_capacity);
            std::string file_path = PathUtil::JoinPath(dir_->Str(), file_name);
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                                 fs_->Create(file_path, /*overwrite=*/false));
            ::parquet::WriterProperties::Builder builder;
            builder.write_batch_size(batch_capacity);
            auto writer_properties = builder.build();
            ASSERT_OK_AND_ASSIGN(
                auto format_writer,
                ParquetFormatWriter::Create(out, arrow_schema, writer_properties,
                                            DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));
            auto array = PrepareArray(struct_type, record_batch_size);
            auto arrow_array = std::make_unique<ArrowArray>();
            ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());

            auto batch = std::make_shared<RecordBatch>(
                /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
                /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
            ASSERT_OK(format_writer->AddBatch(batch->GetData()));
            ASSERT_OK(format_writer->Flush());
            ASSERT_OK(format_writer->Finish());
            ASSERT_OK(out->Flush());
            ASSERT_OK(out->Close());
            CheckResult(file_path, record_batch_size, /*row_group_count=*/1);
        }
    }
}

TEST_F(ParquetFormatWriterTest, TestWriteWithV1Version) {
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;
    std::map<std::string, std::string> options;
    auto record_batch_size = 10;
    auto batch_capacity = 5;
    std::string file_name = "test.parquet";
    std::string file_path = PathUtil::JoinPath(dir_->Str(), file_name);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    ::parquet::WriterProperties::Builder builder;
    builder.write_batch_size(batch_capacity);
    builder.version(::parquet::ParquetVersion::type::PARQUET_1_0);
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(
        auto format_writer,
        ParquetFormatWriter::Create(out, arrow_schema, writer_properties,
                                    DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));
    auto array = PrepareArray(struct_type, record_batch_size);
    auto arrow_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());

    auto batch = std::make_shared<RecordBatch>(
        /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
        /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
    ASSERT_OK(format_writer->AddBatch(batch->GetData()));
    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    CheckResult(file_path, record_batch_size, /*row_group_count=*/1);
}

TEST_F(ParquetFormatWriterTest, TestWriteMultipleTimes) {
    // arrow array length = 6 + 10 + 15 + 6 = 37
    // parquet batch capacity = 10
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "write_multiple_times");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    ::parquet::WriterProperties::Builder builder;
    builder.write_batch_size(10);
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ParquetFormatWriter> format_writer,
        ParquetFormatWriter::Create(out, arrow_schema, writer_properties,
                                    DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));

    // add batch first time, 6 rows
    AddRecordBatchOnce(format_writer, struct_type, 6, 0);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len1, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_len1, 0);

    // add batch second times, 10 rows
    AddRecordBatchOnce(format_writer, struct_type, 10, 6);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len2, format_writer->GetEstimateLength());
    ASSERT_EQ(estimate_len2, estimate_len1);

    // add batch third times, 15 rows (expand internal batch)
    AddRecordBatchOnce(format_writer, struct_type, 15, 16);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len3, format_writer->GetEstimateLength());
    ASSERT_EQ(estimate_len3, estimate_len2);

    // add batch fourth times, 6 rows
    AddRecordBatchOnce(format_writer, struct_type, 6, 31);

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    CheckResult(file_path, /*row_count=*/37, /*row_group_count=*/1);
    auto metrics = format_writer->GetWriterMetrics();
    ASSERT_OK_AND_ASSIGN(uint64_t counter, metrics->GetCounter(ParquetMetrics::WRITE_RECORD_COUNT));
    ASSERT_EQ(37, counter);
}

TEST_F(ParquetFormatWriterTest, TestGetEstimateLength) {
    auto schema_pair = PrepareArrowSchema();
    const auto& arrow_schema = schema_pair.first;
    const auto& struct_type = schema_pair.second;

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "get_estimate_length");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/false));
    ::parquet::WriterProperties::Builder builder;
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ParquetFormatWriter> format_writer,
        ParquetFormatWriter::Create(out, arrow_schema, writer_properties,
                                    DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));

    // add batch first time, 1 row
    AddRecordBatchOnce(format_writer, struct_type, 1, 0);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len1, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_len1, 0);

    // add batch second times, 9998 rows
    AddRecordBatchOnce(format_writer, struct_type, 9998, 1);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len2, format_writer->GetEstimateLength());
    ASSERT_EQ(estimate_len2, estimate_len1);

    AddRecordBatchOnce(format_writer, struct_type, 100000, 9999);
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_len3, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_len3, estimate_len2);
    ASSERT_OK(format_writer->Finish());
}

TEST_F(ParquetFormatWriterTest, TestMemoryControl) {
    auto run = [&](bool all_null_value, uint64_t max_memory_use) {
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileFormat> file_format,
                             FileFormatFactory::Get("parquet", {{Options::FILE_FORMAT, "parquet"},
                                                                {"parquet.writer.max.memory.use",
                                                                 std::to_string(max_memory_use)}}));

        std::shared_ptr<MemoryPool> pool = GetMemoryPool();
        auto schema_pair = PrepareArrowSchema();
        const auto& arrow_schema = schema_pair.first;
        const auto& struct_type = schema_pair.second;
        int32_t batch_size = 4096;

        auto c_schema = std::make_unique<::ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*arrow_schema, c_schema.get()).ok());
        ASSERT_OK_AND_ASSIGN(auto writer_builder,
                             file_format->CreateWriterBuilder(c_schema.get(), batch_size));
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<OutputStream> out,
            fs_->Create(
                PathUtil::JoinPath(dir_->Str(), std::to_string(all_null_value) +
                                                    std::to_string(max_memory_use) + ".parquet"),
                /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatWriter> writer,
                             writer_builder->WithMemoryPool(pool)->Build(out, "uncompressed"));

        auto array = PrepareArray(struct_type, batch_size, /*offset=*/0, all_null_value);
        for (int32_t i = 0; i < 2000; ++i) {
            auto arrow_array = std::make_unique<ArrowArray>();
            ASSERT_TRUE(arrow::ExportArray(*array, arrow_array.get()).ok());
            auto batch = std::make_shared<RecordBatch>(
                /*partition=*/std::map<std::string, std::string>(), /*bucket=*/-1,
                /*row_kinds=*/std::vector<RecordBatch::RowKind>(), arrow_array.get());
            ASSERT_OK(writer->AddBatch(batch->GetData()));
            ASSERT_OK(writer->Flush());
        }

        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
        uint64_t actual_max_mem = pool->MaxMemoryUsage();
        ASSERT_GT(actual_max_mem, max_memory_use);
        ASSERT_LT(actual_max_mem, max_memory_use * 1.5);  // allow 50% overhead
    };
    run(/*all_null_value=*/true, /*max_memory_use=*/20 * 1024 * 1024);   // 20MB
    run(/*all_null_value=*/true, /*max_memory_use=*/40 * 1024 * 1024);   // 40MB
    run(/*all_null_value=*/false, /*max_memory_use=*/20 * 1024 * 1024);  // 20MB
    run(/*all_null_value=*/false, /*max_memory_use=*/40 * 1024 * 1024);  // 40MB
}

TEST_F(ParquetFormatWriterTest, TestMemoryControlForCheckRowGroupCount) {
    auto run = [&](int32_t write_times) {
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<FileFormat> file_format,
            FileFormatFactory::Get("parquet", {{Options::FILE_FORMAT, "parquet"},
                                               {"parquet.writer.max.memory.use", "1"}}));

        auto schema_pair = PrepareArrowSchema();
        const auto& arrow_schema = schema_pair.first;
        const auto& struct_type = schema_pair.second;
        int32_t batch_size = 4096;
        std::string file_path =
            PathUtil::JoinPath(dir_->Str(), std::to_string(write_times) + ".parquet");

        auto c_schema = std::make_unique<::ArrowSchema>();
        ASSERT_TRUE(arrow::ExportSchema(*arrow_schema, c_schema.get()).ok());
        ASSERT_OK_AND_ASSIGN(auto writer_builder,
                             file_format->CreateWriterBuilder(c_schema.get(), batch_size));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                             fs_->Create(file_path, /*overwrite=*/false));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FormatWriter> writer,
                             writer_builder->Build(out, "uncompressed"));

        for (int32_t i = 0; i < write_times; ++i) {
            AddRecordBatchOnce(writer, struct_type, 10, i * 10);
        }

        ASSERT_OK(writer->Flush());
        ASSERT_OK(writer->Finish());
        ASSERT_OK(out->Flush());
        ASSERT_OK(out->Close());
        CheckResult(file_path, /*row_count=*/write_times * 10, /*row_group_count=*/write_times);
    };
    run(/*write_times=*/1);
    run(/*write_times=*/2);
    run(/*write_times=*/5);
}

TEST_F(ParquetFormatWriterTest, TestTimestampType) {
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_utc1", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_utc2", arrow::timestamp(arrow::TimeUnit::MICRO, timezone))};

    std::string file_path = PathUtil::JoinPath(dir_->Str(), "timezone.parquet");
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/true));
    ::parquet::WriterProperties::Builder builder;
    auto writer_properties = builder.build();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ParquetFormatWriter> format_writer,
        ParquetFormatWriter::Create(out, std::make_shared<arrow::Schema>(fields), writer_properties,
                                    DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));

    auto array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
["1970-01-01 00:00:01", "1970-01-01 00:00:00.001", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.000000001",
"1970-01-01 00:00:02", "1970-01-01 00:00:00.002"],
["1970-01-01 00:00:01", null, "1970-01-01 00:00:00.000001", null,"1970-01-01 00:00:02", null]
    ])")
            .ValueOrDie());

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &c_array).ok());
    ASSERT_OK(format_writer->AddBatch(&c_array));
    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryEncodedColumn) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_passthrough");
    std::shared_ptr<OutputStream> out;
    std::shared_ptr<ParquetFormatWriter> format_writer = CreateEncodedWriter(file_path, &out);

    // The writer is created from the logical schema, which a dictionary-encoded batch no longer
    // matches, and the encoding may alternate from one batch to the next.
    AddStructArrayOnce(format_writer, PrepareEncodedArray(6, 0, /*dictionary_encoded=*/true));
    AddStructArrayOnce(format_writer, PrepareEncodedArray(4, 6, /*dictionary_encoded=*/false));
    AddStructArrayOnce(format_writer, PrepareEncodedArray(5, 10, /*dictionary_encoded=*/true));

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    CheckEncodedResult(file_path, /*row_count=*/15, /*null_in_dictionary=*/false);
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryChangingAcrossBatches) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_changing");
    std::shared_ptr<OutputStream> out;
    std::shared_ptr<ParquetFormatWriter> format_writer = CreateEncodedWriter(file_path, &out);

    // Compacting several input files puts their different dictionaries in one output row group.
    // Arrow keeps only the first one and falls back to plain encoding for the rest, so the values
    // have to survive that transition - and the output stops being dictionary-encoded there, which
    // is the cost of forwarding an encoding rather than rebuilding one. Pinned here because it is
    // what makes a compacted file bigger than one written from materialized values.
    constexpr int32_t kBatchRows = 4;
    for (int32_t batch = 0; batch < 3; ++batch) {
        AddStructArrayOnce(format_writer, PrepareEncodedArray(kBatchRows, batch * kBatchRows,
                                                              /*dictionary_encoded=*/true,
                                                              /*null_in_dictionary=*/false,
                                                              fmt::format("batch{}_", batch)));
    }

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    auto file = arrow::io::ReadableFile::Open(file_path, arrow_pool_.get());
    ASSERT_TRUE(file.ok());
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
    ASSERT_TRUE(status.ok()) << status.ToString();
    const ::parquet::FileMetaData* metadata = reader->parquet_reader()->metadata().get();
    ASSERT_EQ(3 * kBatchRows, metadata->num_rows());
    auto [dictionary_pages, data_pages] =
        CountDictionaryDataPages(*metadata->RowGroup(0)->ColumnChunk(0));
    ASSERT_GT(data_pages, 0);
    ASSERT_LT(dictionary_pages, data_pages) << "expected a plain fallback after the second batch";

    std::shared_ptr<::arrow::ChunkedArray> col0_array;
    ASSERT_TRUE(reader->ReadColumn(0, &col0_array).ok());
    int32_t row = 0;
    for (const auto& chunk : col0_array->chunks()) {
        const auto& string_array = checked_pointer_cast<arrow::StringArray>(chunk);
        ASSERT_TRUE(string_array);
        for (int64_t i = 0; i < string_array->length(); ++i, ++row) {
            ASSERT_EQ(fmt::format("batch{}_{}", row / kBatchRows, row % 3),
                      string_array->GetString(i));
        }
    }
    ASSERT_EQ(3 * kBatchRows, row);
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryWithNullsInDictionary) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_with_nulls");
    std::shared_ptr<OutputStream> out;
    std::shared_ptr<ParquetFormatWriter> format_writer = CreateEncodedWriter(file_path, &out);

    // parquet::arrow refuses a DictionaryArray whose dictionary holds nulls, so the column is
    // densified rather than failing the batch.
    AddStructArrayOnce(format_writer, PrepareEncodedArray(9, 0, /*dictionary_encoded=*/true,
                                                          /*null_in_dictionary=*/true));

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    CheckEncodedResult(file_path, /*row_count=*/9, /*null_in_dictionary=*/true);
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryWithNullRows) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_null_rows");
    std::shared_ptr<OutputStream> out;
    std::shared_ptr<ParquetFormatWriter> format_writer = CreateEncodedWriter(file_path, &out);

    // Nulls in the indices, not in the dictionary values: the common shape for a nullable column,
    // and the one that makes the writer derive definition levels from the indices' validity
    // bitmap rather than from the values it would otherwise have materialized.
    std::shared_ptr<arrow::Array> indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, null, 1, 2, null, 0]")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c"])").ValueOrDie();
    auto dictionary_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::Array> encoded =
        arrow::DictionaryArray::FromArrays(dictionary_type, indices, dictionary).ValueOrDie();
    std::shared_ptr<arrow::Array> flat = PrepareEncodedArray(6, 0, /*dictionary_encoded=*/false);
    auto struct_array = checked_pointer_cast<arrow::StructArray>(flat);
    arrow::ArrayVector columns = {encoded, struct_array->field(1), struct_array->field(2)};
    auto batch_array =
        arrow::StructArray::Make(columns, std::vector<std::string>{"col1", "col2", "col3"})
            .ValueOrDie();
    AddStructArrayOnce(format_writer, batch_array);

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    auto file = arrow::io::ReadableFile::Open(file_path, arrow_pool_.get());
    ASSERT_TRUE(file.ok());
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
    ASSERT_TRUE(status.ok()) << status.ToString();
    const ::parquet::FileMetaData* metadata = reader->parquet_reader()->metadata().get();
    ASSERT_EQ(6, metadata->num_rows());
    std::unique_ptr<::parquet::ColumnChunkMetaData> column_chunk =
        metadata->RowGroup(0)->ColumnChunk(0);
    ASSERT_TRUE(column_chunk->is_stats_set());
    ASSERT_EQ(2, column_chunk->statistics()->null_count());
    auto [dictionary_pages, data_pages] = CountDictionaryDataPages(*column_chunk);
    ASSERT_GT(data_pages, 0);
    ASSERT_EQ(data_pages, dictionary_pages);

    std::shared_ptr<::arrow::ChunkedArray> col0_array;
    ASSERT_TRUE(reader->ReadColumn(0, &col0_array).ok());
    ASSERT_EQ(6, col0_array->length());
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(),
                                                  R"(["a", null, "b", "c", null, "a"])")
            .ValueOrDie();
    ASSERT_TRUE(col0_array->Equals(arrow::ChunkedArray(expected)))
        << "actual=" << col0_array->ToString();
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryWithDuplicateValues) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_duplicates");
    std::shared_ptr<OutputStream> out;
    std::shared_ptr<ParquetFormatWriter> format_writer = CreateEncodedWriter(file_path, &out);

    // A dictionary whose values repeat. Nothing forbids one - a DictionaryArray only bounds-checks
    // its indices - but the Parquet dict encoder de-duplicates as it inserts, so its memo table
    // ends up shorter than the alphabet the indices were built against. Arrow 17 notices
    // (column_writer.cc, `num_entries() != dictionary->length()`) and falls back to plain rather
    // than emitting a dictionary page sized from the inflated count; older forks did not, which is
    // what made this a corruption rather than a size regression. Pinned because the passthrough
    // relies on that fallback existing.
    std::shared_ptr<arrow::Array> indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 2, 0]").ValueOrDie();
    std::shared_ptr<arrow::Array> dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "a"])").ValueOrDie();
    auto dictionary_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::Array> encoded =
        arrow::DictionaryArray::FromArrays(dictionary_type, indices, dictionary).ValueOrDie();
    std::shared_ptr<arrow::Array> flat = PrepareEncodedArray(4, 0, /*dictionary_encoded=*/false);
    auto struct_array = checked_pointer_cast<arrow::StructArray>(flat);
    arrow::ArrayVector columns = {encoded, struct_array->field(1), struct_array->field(2)};
    auto batch_array =
        arrow::StructArray::Make(columns, std::vector<std::string>{"col1", "col2", "col3"})
            .ValueOrDie();
    AddStructArrayOnce(format_writer, batch_array);

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    auto file = arrow::io::ReadableFile::Open(file_path, arrow_pool_.get());
    ASSERT_TRUE(file.ok());
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
    ASSERT_TRUE(status.ok()) << status.ToString();
    const ::parquet::FileMetaData* metadata = reader->parquet_reader()->metadata().get();
    ASSERT_EQ(4, metadata->num_rows());
    auto [dictionary_pages, data_pages] =
        CountDictionaryDataPages(*metadata->RowGroup(0)->ColumnChunk(0));
    ASSERT_GT(data_pages, 0);
    ASSERT_EQ(0, dictionary_pages) << "expected the duplicate dictionary to force plain encoding";

    // The point of the fallback: the values still come back exactly as the indices addressed them.
    std::shared_ptr<::arrow::ChunkedArray> col0_array;
    ASSERT_TRUE(reader->ReadColumn(0, &col0_array).ok());
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "a", "a"])")
            .ValueOrDie();
    ASSERT_TRUE(col0_array->Equals(arrow::ChunkedArray(expected)))
        << "actual=" << col0_array->ToString();
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryEmptyBatch) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_empty");
    std::shared_ptr<OutputStream> out;
    std::shared_ptr<ParquetFormatWriter> format_writer = CreateEncodedWriter(file_path, &out);

    // A rewrite can hand over an empty batch, and its layout still has to be resolved: the columns
    // carry a dictionary the schema does not declare even with no rows behind it.
    AddStructArrayOnce(format_writer, PrepareEncodedArray(0, 0, /*dictionary_encoded=*/true));
    AddStructArrayOnce(format_writer, PrepareEncodedArray(3, 0, /*dictionary_encoded=*/true));

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    CheckEncodedResult(file_path, /*row_count=*/3, /*null_in_dictionary=*/false);
}

TEST_F(ParquetFormatWriterTest, TestGetEstimateLengthWithDictionaryBatches) {
    // RollingFileWriter decides when to start a new data file from ReachTargetSize(), which is
    // GetEstimateLength() against the target. A dictionary-encoded batch buffers indices rather
    // than values, so the estimate is built from different bytes than it used to be; if it stopped
    // tracking the file, a compaction rewrite would produce one file of unbounded size instead of
    // rolling at `target-file-size`.
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_estimate_length");
    std::shared_ptr<OutputStream> out;
    // Small enough that every batch after the first opens a new buffered row group and flushes the
    // previous one, so the estimate has to move for reasons the test controls.
    std::shared_ptr<ParquetFormatWriter> format_writer =
        CreateEncodedWriter(file_path, &out, /*max_memory_use=*/1);

    AddStructArrayOnce(format_writer, PrepareEncodedArray(64, 0, /*dictionary_encoded=*/true));
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_after_first, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_after_first, 0);

    ASSERT_OK_AND_ASSIGN(bool reached_tiny_target,
                         format_writer->ReachTargetSize(/*suggested_check=*/true,
                                                        /*target_size=*/1));
    ASSERT_TRUE(reached_tiny_target);
    // Not a suggested check: the writer must not go looking at its own size at all.
    ASSERT_OK_AND_ASSIGN(bool reached_unsuggested,
                         format_writer->ReachTargetSize(/*suggested_check=*/false,
                                                        /*target_size=*/1));
    ASSERT_FALSE(reached_unsuggested);
    ASSERT_OK_AND_ASSIGN(bool reached_huge_target,
                         format_writer->ReachTargetSize(/*suggested_check=*/true,
                                                        /*target_size=*/1LL << 40));
    ASSERT_FALSE(reached_huge_target);

    AddStructArrayOnce(format_writer, PrepareEncodedArray(64, 64, /*dictionary_encoded=*/true));
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_after_second, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_after_second, estimate_after_first);

    // A flat batch after an encoded one keeps the estimate moving in the same direction, so the
    // rolling decision does not depend on which encoding the rewrite happens to be forwarding.
    AddStructArrayOnce(format_writer, PrepareEncodedArray(64, 128, /*dictionary_encoded=*/false));
    ASSERT_OK_AND_ASSIGN(uint64_t estimate_after_third, format_writer->GetEstimateLength());
    ASSERT_GT(estimate_after_third, estimate_after_second);

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
    ASSERT_GT(fs_->GetFileStatus(file_path).value().GetLen(), 0);
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryOfBinaryColumn) {
    // BINARY is the other value type an `ArrowArray` layout can describe, so the writer takes a
    // `dictionary(int32, binary)` batch against a plain BINARY schema exactly as it takes a STRING
    // one. ParquetFileBatchReader does not currently hand one over - it forwards STRING alone,
    // because the value accessors cannot read a BINARY dictionary - so this pins the writer half
    // of the contract on its own, and would fail if the layout predicate were narrowed to STRING
    // to enforce the reader's restriction in the wrong place.
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_binary");
    arrow::FieldVector fields = {arrow::field("b", arrow::binary())};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs_->Create(file_path, /*overwrite=*/true));
    ::parquet::WriterProperties::Builder builder;
    builder.enable_dictionary();
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ParquetFormatWriter> format_writer,
        ParquetFormatWriter::Create(out, std::make_shared<arrow::Schema>(fields), builder.build(),
                                    DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE, arrow_pool_));

    std::shared_ptr<arrow::Array> indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 0, 2]").ValueOrDie();
    std::shared_ptr<arrow::Array> dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::binary(), R"(["a", "bb", "ccc"])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> encoded =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::binary()),
                                           indices, dictionary)
            .ValueOrDie();
    auto batch_array =
        arrow::StructArray::Make({encoded}, std::vector<std::string>{"b"}).ValueOrDie();
    AddStructArrayOnce(format_writer, batch_array);

    ASSERT_OK(format_writer->Flush());
    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    auto file = arrow::io::ReadableFile::Open(file_path, arrow_pool_.get());
    ASSERT_TRUE(file.ok());
    std::unique_ptr<::parquet::arrow::FileReader> reader;
    auto status = ::parquet::arrow::OpenFile(file.ValueOrDie(), arrow_pool_.get(), &reader);
    ASSERT_TRUE(status.ok()) << status.ToString();
    const ::parquet::FileMetaData* metadata = reader->parquet_reader()->metadata().get();
    ASSERT_EQ(4, metadata->num_rows());
    // The indices went to Parquet as indices: one dictionary, no plain fallback.
    auto [dictionary_pages, data_pages] =
        CountDictionaryDataPages(*metadata->RowGroup(0)->ColumnChunk(0));
    ASSERT_GT(data_pages, 0);
    ASSERT_EQ(data_pages, dictionary_pages);

    std::shared_ptr<::arrow::ChunkedArray> column;
    ASSERT_TRUE(reader->ReadColumn(0, &column).ok());
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::binary(), R"(["a", "bb", "a", "ccc"])")
            .ValueOrDie();
    ASSERT_TRUE(column->Equals(arrow::ChunkedArray(expected))) << "actual=" << column->ToString();
}

TEST_F(ParquetFormatWriterTest, TestWriteDictionaryOfUnsupportedTypeIsRejected) {
    std::string file_path = PathUtil::JoinPath(dir_->Str(), "dictionary_unsupported");
    std::shared_ptr<OutputStream> out;
    std::shared_ptr<ParquetFormatWriter> format_writer = CreateEncodedWriter(file_path, &out);

    // The backstop, not the behaviour a rewrite relies on: a batch layout cannot describe a
    // dictionary over a non-binary-like column, and this writer only ever sees a layout, so it
    // rejects rather than reinterpreting with a guessed index width. Callers that can produce such
    // a column decode it while its type is still known - see
    // ArrowUtils::FlattenUnresolvableDictionaries, which leaves the other columns encoded.
    std::shared_ptr<arrow::Array> encoded = PrepareEncodedArray(3, 0, /*dictionary_encoded=*/false);
    auto struct_array = checked_pointer_cast<arrow::StructArray>(encoded);
    arrow::Int32Builder index_builder;
    arrow::Int32Builder value_builder;
    for (int32_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(index_builder.Append(i).ok());
        ASSERT_TRUE(value_builder.Append(7 + i).ok());
    }
    std::shared_ptr<arrow::Array> indices, dictionary;
    ASSERT_TRUE(index_builder.Finish(&indices).ok());
    ASSERT_TRUE(value_builder.Finish(&dictionary).ok());
    auto dictionary_int =
        arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::int32()),
                                           indices, dictionary)
            .ValueOrDie();
    auto batch_array =
        arrow::StructArray::Make({struct_array->field(0), dictionary_int, struct_array->field(2)},
                                 std::vector<std::string>{"col1", "col2", "col3"})
            .ValueOrDie();

    auto arrow_array = std::make_unique<ArrowArray>();
    ASSERT_TRUE(arrow::ExportArray(*batch_array, arrow_array.get()).ok());
    Status status = format_writer->AddBatch(arrow_array.get());
    ASSERT_TRUE(status.IsNotImplemented()) << status.ToString();
    ArrowArrayRelease(arrow_array.get());

    ASSERT_OK(format_writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());
}

}  // namespace paimon::parquet::test
