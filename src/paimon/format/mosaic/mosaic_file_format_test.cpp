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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <array>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/mosaic/mosaic_format_defs.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"

namespace paimon::mosaic::test {

class MosaicFileFormatTest : public ::testing::Test {
 public:
    // The footer layout stores the number of buckets followed by the number of row groups.
    using FooterLayout = std::pair<uint32_t, uint32_t>;

    void SetUp() override {
        ASSERT_OK_AND_ASSIGN(format_,
                             FileFormatFactory::Get("mosaic", {{"file.format", "mosaic"}}));
        file_system_ = std::make_shared<LocalFileSystem>();
        directory_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_NE(directory_, nullptr);
        pool_ = GetDefaultPool();
        arrow_pool_ = GetArrowPool(pool_);
    }

    Status WriteFile(const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
                     const std::shared_ptr<arrow::Array>& array, int32_t batch_size,
                     const std::shared_ptr<FileFormat>& format = nullptr) const {
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        const std::shared_ptr<FileFormat>& writer_format = format == nullptr ? format_ : format;
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> writer_builder,
                               writer_format->CreateWriterBuilder(&ffi_schema, batch_size));
        writer_builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> output,
                               file_system_->Create(path, /*overwrite=*/false));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                               writer_builder->Build(output, "zstd"));
        for (int64_t offset = 0; offset < array->length(); offset += batch_size) {
            std::shared_ptr<arrow::Array> slice = array->Slice(offset, batch_size);
            ::ArrowArray ffi_array = {};
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*slice, &ffi_array));
            PAIMON_RETURN_NOT_OK(writer->AddBatch(&ffi_array));
        }
        PAIMON_RETURN_NOT_OK(writer->Finish());
        return output->Close();
    }

    Result<FooterLayout> ReadFooterLayout(const std::string& path) const {
        constexpr int64_t kFooterSize = 32;
        constexpr int64_t kLayoutSize = 8;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system_->Open(path));
        PAIMON_ASSIGN_OR_RAISE(int64_t file_size, input->Length());
        if (file_size < kFooterSize) {
            return Status::Invalid("Mosaic file is shorter than its footer");
        }
        std::array<uint8_t, kLayoutSize> layout = {};
        PAIMON_ASSIGN_OR_RAISE(int64_t bytes_read,
                               input->Read(reinterpret_cast<char*>(layout.data()), kLayoutSize,
                                           file_size - kFooterSize + /*layout offset=*/16));
        if (bytes_read != kLayoutSize) {
            return Status::IOError("short read while reading Mosaic footer");
        }
        auto decode_uint32 = [&layout](size_t offset) -> uint32_t {
            return (uint32_t{layout[offset]} << 24) | (uint32_t{layout[offset + 1]} << 16) |
                   (uint32_t{layout[offset + 2]} << 8) | uint32_t{layout[offset + 3]};
        };
        return std::make_pair(decode_uint32(0), decode_uint32(4));
    }

    Result<std::shared_ptr<arrow::Array>> ReadFile(const std::string& path,
                                                   const std::shared_ptr<arrow::Schema>& schema,
                                                   int32_t batch_size,
                                                   uint64_t expected_row_count) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReaderBuilder> reader_builder,
                               format_->CreateReaderBuilder(batch_size));
        reader_builder->WithMemoryPool(pool_);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system_->Open(path));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> reader,
                               reader_builder->Build(input));
        ::ArrowSchema ffi_schema = {};
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &ffi_schema));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                                   /*selection_bitmap=*/std::nullopt));
        PAIMON_ASSIGN_OR_RAISE(uint64_t total_rows, reader->GetNumberOfRows());
        if (total_rows != expected_row_count) {
            return Status::Invalid("unexpected Mosaic row count");
        }

        std::vector<std::shared_ptr<arrow::Array>> batches;
        uint64_t expected_first_row = 0;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            PAIMON_RETURN_NOT_OK(paimon::test::ReadResultCollector::CheckBatchOffset(batch));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> arrow_batch,
                arrow::ImportArray(batch.first.get(), batch.second.get()));
            if (arrow_batch->length() > batch_size) {
                return Status::Invalid("Mosaic read batch exceeds configured batch size");
            }
            PAIMON_ASSIGN_OR_RAISE(uint64_t first_row,
                                   reader->GetPreviousBatchFileRowId(/*batch_row_id=*/0));
            if (first_row != expected_first_row) {
                return Status::Invalid("unexpected Mosaic batch first row");
            }
            expected_first_row += arrow_batch->length();
            batches.push_back(std::move(arrow_batch));
        }
        if (expected_first_row != expected_row_count) {
            return Status::Invalid("Mosaic batches do not contain all rows");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> result,
                                          arrow::Concatenate(batches, arrow_pool_.get()));
        return result;
    }

    void AssertReadWithBatchSizes(const std::string& path,
                                  const std::shared_ptr<arrow::Schema>& schema,
                                  const std::shared_ptr<arrow::Array>& expected,
                                  std::initializer_list<int32_t> batch_sizes) const {
        for (int32_t batch_size : batch_sizes) {
            SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
            ASSERT_OK_AND_ASSIGN(
                std::shared_ptr<arrow::Array> actual,
                ReadFile(path, schema, batch_size,
                         /*expected_row_count=*/static_cast<uint64_t>(expected->length())));
            ASSERT_TRUE(actual->Equals(expected)) << actual->ToString() << "\nvs\n"
                                                  << expected->ToString();
        }
    }

 protected:
    std::shared_ptr<FileFormat> format_;
    std::shared_ptr<LocalFileSystem> file_system_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> directory_;
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
};

TEST_F(MosaicFileFormatTest, WriteThenRead) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "data.mosaic");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::DataType> data_type = arrow::struct_(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(
            data_type, R"([[1,"one"],[2,null],[3,"three"],[4,"four"],[5,"five"]])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5, 8});
}

TEST_F(MosaicFileFormatTest, EmptyProjectionPreservesRowCount) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "empty-projection.mosaic");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,"one"],[2,"two"],[3,"three"]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> reader_builder,
                         format_->CreateReaderBuilder(/*batch_size=*/2));
    reader_builder->WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, reader_builder->Build(input));
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*arrow::schema({}), &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));

    for (int64_t expected_rows : {2, 1}) {
        ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
        ASSERT_FALSE(BatchReader::IsEofBatch(batch));
        std::shared_ptr<arrow::RecordBatch> record_batch =
            arrow::ImportRecordBatch(batch.first.get(), batch.second.get()).ValueOrDie();
        ASSERT_EQ(record_batch->num_columns(), 0);
        ASSERT_EQ(record_batch->num_rows(), expected_rows);
    }
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch eof_batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(eof_batch));
}

TEST_F(MosaicFileFormatTest, SetReadSchemaResetsReaderToFirstRow) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "reset-reader.mosaic");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("name", arrow::utf8())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,"one"],[2,"two"],[3,"three"]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> reader_builder,
                         format_->CreateReaderBuilder(/*batch_size=*/2));
    reader_builder->WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, reader_builder->Build(input));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch first_batch, reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(uint64_t first_row, reader->GetPreviousBatchFileRowId(/*batch_row_id=*/0));
    ASSERT_EQ(first_row, 0);
    std::shared_ptr<arrow::Array> first_array =
        arrow::ImportArray(first_batch.first.get(), first_batch.second.get()).ValueOrDie();
    ASSERT_TRUE(first_array->Equals(data->Slice(/*offset=*/0, /*length=*/2)));

    std::shared_ptr<arrow::Schema> projected_schema = arrow::schema({fields[1]});
    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*projected_schema, &ffi_schema).ok());
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, /*predicate=*/nullptr,
                                    /*selection_bitmap=*/std::nullopt));
    ASSERT_NOK(reader->GetPreviousBatchFileRowId(/*batch_row_id=*/0));

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch projected_batch, reader->NextBatch());
    ASSERT_OK_AND_ASSIGN(first_row, reader->GetPreviousBatchFileRowId(/*batch_row_id=*/0));
    ASSERT_EQ(first_row, 0);
    std::shared_ptr<arrow::Array> projected_array =
        arrow::ImportArray(projected_batch.first.get(), projected_batch.second.get()).ValueOrDie();
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[1]}),
                                                  R"([["one"],["two"]])")
            .ValueOrDie();
    ASSERT_TRUE(projected_array->Equals(expected)) << projected_array->ToString();
}

TEST_F(MosaicFileFormatTest, WriterOptions) {
    std::map<std::string, std::string> options = {
        {"file.format", "mosaic"},      {Options::FILE_BLOCK_SIZE, "1 B"},
        {MOSAIC_NUM_BUCKETS, "2"},      {MOSAIC_MAX_DICT_TOTAL_BYTES, "1 KB"},
        {MOSAIC_MAX_DICT_ENTRIES, "2"}, {MOSAIC_PAGE_SIZE_THRESHOLD, "1 B"},
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> configured_format,
                         FileFormatFactory::Get("mosaic", options));
    std::string path = PathUtil::JoinPath(directory_->Str(), "writer-options.mosaic");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int32()), arrow::field("f5", arrow::int32()),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,2,3,4,5,6],
                                                     [7,8,9,10,11,12],
                                                     [13,14,15,16,17,18],
                                                     [19,20,21,22,23,24],
                                                     [25,26,27,28,29,30]])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2, configured_format));
    ASSERT_OK_AND_ASSIGN(FooterLayout footer_layout, ReadFooterLayout(path));
    ASSERT_EQ(footer_layout.first, 2);
    ASSERT_EQ(footer_layout.second, 3);
    AssertReadWithBatchSizes(path, schema, expected, {10});
}

TEST_F(MosaicFileFormatTest, ExtractStatistics) {
    std::map<std::string, std::string> options = {
        {"file.format", "mosaic"},
        {Options::FILE_BLOCK_SIZE, "1 B"},
        {MOSAIC_STATS_COLUMNS, " id, name, ts, amount, all_null "},
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> configured_format,
                         FileFormatFactory::Get("mosaic", options));
    std::string path = PathUtil::JoinPath(directory_->Str(), "statistics.mosaic");
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("amount", arrow::decimal128(10, 2)),
        arrow::field("untracked", arrow::int64()),
        arrow::field("all_null", arrow::int32()),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [3,"three","1970-01-01 00:00:00.000000003","3.30",30,null],
          [1,"one","1970-01-01 00:00:00.000000001","1.10",10,null],
          [null,null,null,null,null,null],
          [5,"five","1970-01-01 00:00:00.000000005","5.50",50,null],
          [2,"","1970-01-01 00:00:00.000000002","2.20",20,null]
        ])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2, configured_format));

    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatStatsExtractor> extractor,
                         configured_format->CreateStatsExtractor(&ffi_schema));
    ASSERT_OK_AND_ASSIGN(auto result, extractor->ExtractWithFileInfo(file_system_, path, pool_));
    ASSERT_EQ(result.second.GetRowCount(), 5);
    std::vector<std::string> expected_stats = {
        "min 1, max 5, null count 1",
        "min , max three, null count 1",
        "min 1970-01-01 00:00:00.000000001, max 1970-01-01 00:00:00.000000005, null count 1",
        "min 1.10, max 5.50, null count 1",
        "min null, max null, null count null",
        "min null, max null, null count 5",
    };
    ASSERT_EQ(result.first.size(), expected_stats.size());
    for (size_t i = 0; i < expected_stats.size(); ++i) {
        ASSERT_EQ(result.first[i]->ToString(), expected_stats[i]);
    }
}

TEST_F(MosaicFileFormatTest, RowGroupPredicateFiltering) {
    std::map<std::string, std::string> options = {
        {"file.format", "mosaic"},
        {Options::FILE_BLOCK_SIZE, "1 B"},
        {MOSAIC_STATS_COLUMNS, "id"},
    };
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileFormat> configured_format,
                         FileFormatFactory::Get("mosaic", options));
    std::string path = PathUtil::JoinPath(directory_->Str(), "predicate.mosaic");
    arrow::FieldVector fields = {arrow::field("id", arrow::int32()),
                                 arrow::field("untracked", arrow::int32())};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> data =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_(fields), R"([[1,null],[2,2],[10,10],[11,11],[20,20],[21,21]])")
            .ValueOrDie();
    ASSERT_OK(WriteFile(path, schema, data, /*batch_size=*/2, configured_format));
    ASSERT_OK_AND_ASSIGN(FooterLayout footer_layout, ReadFooterLayout(path));
    ASSERT_EQ(footer_layout.second, 3);

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> reader_builder,
                         configured_format->CreateReaderBuilder(/*batch_size=*/10));
    reader_builder->WithMemoryPool(pool_);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input, file_system_->Open(path));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> reader, reader_builder->Build(input));

    ::ArrowSchema ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    std::shared_ptr<Predicate> predicate = PredicateBuilder::GreaterThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::INT, Literal(15));
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, predicate, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    std::shared_ptr<arrow::Array> actual =
        arrow::ImportArray(batch.first.get(), batch.second.get()).ValueOrDie();
    ASSERT_TRUE(actual->Equals(data->Slice(/*offset=*/4, /*length=*/2))) << actual->ToString();
    ASSERT_OK_AND_ASSIGN(uint64_t first_row, reader->GetPreviousBatchFileRowId(/*batch_row_id=*/0));
    ASSERT_EQ(first_row, 4);
    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));

    ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    predicate = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"id",
                                              FieldType::INT, Literal(100));
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, predicate, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(batch, reader->NextBatch());
    ASSERT_TRUE(BatchReader::IsEofBatch(batch));

    ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    predicate = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"untracked",
                                              FieldType::INT, Literal(100));
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, predicate, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual_without_stats,
                         paimon::test::ReadResultCollector::CollectResult(reader.get()));
    ASSERT_TRUE(actual_without_stats->Equals(arrow::ChunkedArray(data)))
        << actual_without_stats->ToString();

    ffi_schema = {};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &ffi_schema).ok());
    predicate =
        PredicateBuilder::IsNull(/*field_index=*/1, /*field_name=*/"untracked", FieldType::INT);
    ASSERT_OK(reader->SetReadSchema(&ffi_schema, predicate, /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual_is_null_without_stats,
                         paimon::test::ReadResultCollector::CollectResult(std::move(reader)));
    ASSERT_TRUE(actual_is_null_without_stats->Equals(arrow::ChunkedArray(data)))
        << actual_is_null_without_stats->ToString();
}

TEST_F(MosaicFileFormatTest, WriteThenReadSupportedTypes) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "supported-types.mosaic");
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f10", arrow::list(arrow::float32())),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f13", arrow::date32()),
        arrow::field("f14", arrow::decimal128(2, 2)),
        arrow::field("f15", arrow::decimal128(30, 2)),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          [true,-128,-32768,-2147483648,-4294967298,0.5,1.141592659,"mosaic","binary",
           [[-1,-2],[1,2]],[1.5,null],"1970-01-01 00:00:00.000000001",-1,"-0.99","-123456789987654321.45"],
          [false,127,32767,2147483647,4294967296,2.0,3.141592657,"", "",
           [],[],"2030-12-31 23:59:59.999999999",12345,"0.78","123456789987654321.45"],
          [null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5});
}

TEST_F(MosaicFileFormatTest, WriteThenReadNestedTypes) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "nested.mosaic");
    std::shared_ptr<arrow::DataType> list_type = arrow::list(arrow::int32());
    std::shared_ptr<arrow::DataType> map_type = arrow::map(arrow::utf8(), arrow::int64());
    std::shared_ptr<arrow::DataType> nested_list_type = arrow::list(list_type);
    std::shared_ptr<arrow::DataType> list_of_maps_type =
        arrow::list(arrow::map(arrow::utf8(), arrow::int32()));
    std::shared_ptr<arrow::DataType> map_of_lists_type =
        arrow::map(arrow::utf8(), arrow::list(arrow::int32()));
    arrow::FieldVector fields = {
        arrow::field("id", arrow::int32(), false),
        arrow::field("list_col", list_type),
        arrow::field("map_col", map_type),
        arrow::field("nested_list_col", nested_list_type),
        arrow::field("list_of_maps_col", list_of_maps_type),
        arrow::field("map_of_lists_col", map_of_lists_type),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([
              [1,[1,null,3],[["a",10],["b",null]],[[1,2],[3]],
                 [[["a",1]],[["b",2],["c",3]]],[["x",[1,2]],["y",[]]]],
              [2,[],[],[[4]],[],[]],
              [3,null,null,null,null,null],
              [4,[4,5],[["c",30]],[[],[5,null]],[[]],[["z",[null,5]]]],
              [5,[6],[["d",40],["e",50]],[],[[["d",4]]],[["w",[]]]]
            ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5, 8});
}

TEST_F(MosaicFileFormatTest, WriteThenReadTimestampTypes) {
    const std::string timezone = "Asia/Tokyo";
    paimon::test::TimezoneGuard timezone_guard(timezone);
    std::string path = PathUtil::JoinPath(directory_->Str(), "timestamp-types.mosaic");
    arrow::FieldVector fields = {
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> expected =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
          ["1970-01-01 00:00:00.001","1970-01-01 00:00:00.000001","1970-01-01 00:00:00.000000001",
           "1970-01-01 00:00:00.002","1970-01-01 00:00:00.000002","1970-01-01 00:00:00.000000002"],
          ["2030-12-31 23:59:59.999","2030-12-31 23:59:59.999999","2030-12-31 23:59:59.999999999",
           "2031-01-01 00:00:00.001","2031-01-01 00:00:00.000001","2031-01-01 00:00:00.000000001"],
          [null,null,null,null,null,null]
        ])")
            .ValueOrDie();

    ASSERT_OK(WriteFile(path, schema, expected, /*batch_size=*/2));
    AssertReadWithBatchSizes(path, schema, expected, {1, 2, 3, 5});
}

TEST_F(MosaicFileFormatTest, RejectUnsupportedTimestampSecond) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "timestamp-second.mosaic");
    arrow::FieldVector fields = {
        arrow::field("ts_second", arrow::timestamp(arrow::TimeUnit::SECOND)),
    };
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([["1970-01-01 00:00:01"]])")
            .ValueOrDie();

    ASSERT_NOK_WITH_MSG(WriteFile(path, schema, array, /*batch_size=*/1),
                        "unsupported Timestamp unit: Second");
}

TEST_F(MosaicFileFormatTest, RejectUnsupportedStructType) {
    std::string path = PathUtil::JoinPath(directory_->Str(), "struct.mosaic");
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_(
        {arrow::field("value", arrow::int32()), arrow::field("label", arrow::utf8())});
    arrow::FieldVector fields = {arrow::field("id", arrow::int32(), false),
                                 arrow::field("struct_col", struct_type)};
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields),
                                                  R"([[1,[10,"ten"]],[2,null]])")
            .ValueOrDie();

    ASSERT_NOK_WITH_MSG(WriteFile(path, schema, array, /*batch_size=*/2),
                        "unsupported DataType: Struct");
}

}  // namespace paimon::mosaic::test
