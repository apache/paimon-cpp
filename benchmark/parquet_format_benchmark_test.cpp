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

// Smoke test for the assumptions parquet_format_benchmark.cpp is built on.
//
// The benchmark is only compiled under PAIMON_BUILD_BENCHMARKS, which CI does not set, so nothing
// there runs in CI. Every assumption it makes about the format layer - that a codec name is
// accepted, that a dictionary-encoded input array can be written, that a nested or high-precision
// column survives a round trip, that a predicate and a selection bitmap return the rows the
// benchmark asserts on, that the reader metrics it reports exist - is checked here instead, at a
// row count small enough to stay a test. A regression in any of them would otherwise surface as a
// benchmark that quietly measures the wrong thing.

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/defs.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/parquet/parquet_field_id_converter.h"
#include "paimon/format/parquet/parquet_file_batch_reader.h"
#include "paimon/format/parquet/parquet_format_defs.h"
#include "paimon/format/parquet/parquet_writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::parquet {
namespace {

// Small enough to stay a test, large enough to span several batches and row groups.
constexpr int64_t kRows = 2'000;
constexpr int32_t kBatchSize = 256;
constexpr int64_t kRowGroupLength = 500;

std::shared_ptr<arrow::Field> MakeField(const std::string& name,
                                        const std::shared_ptr<arrow::DataType>& type,
                                        int32_t field_id) {
    return arrow::field(name, type,
                        arrow::KeyValueMetadata::Make({ParquetFieldIdConverter::PARQUET_FIELD_ID},
                                                      {std::to_string(field_id)}));
}

class ParquetFormatBenchmarkTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dir_ = test::UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        fs_ = dir_->GetFileSystem();
        pool_ = GetArrowPool(GetDefaultPool());
    }

    std::string PathOf(const std::string& name) const {
        return PathUtil::JoinPath(dir_->Str(), name);
    }

    // Writes the same struct array `batch_count` times through the builder the benchmark uses.
    // More than one call matters for dictionary input: the benchmark always writes several
    // batches, and every batch after the first hands the writer the same dictionary again.
    Status Write(const std::string& path, const std::shared_ptr<arrow::Schema>& schema,
                 const std::shared_ptr<arrow::Array>& batch, const std::string& compression,
                 const std::map<std::string, std::string>& extra_options = {},
                 int32_t batch_count = 1) {
        std::map<std::string, std::string> options = extra_options;
        // emplace, not assignment: a caller that set its own row-group limit is testing that.
        options.emplace(PARQUET_WRITE_MAX_ROW_GROUP_LENGTH, std::to_string(kRowGroupLength));
        ParquetWriterBuilder writer_builder(schema, kBatchSize, options);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                               fs_->Create(path, /*overwrite=*/true));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                               writer_builder.Build(out, compression));
        for (int32_t i = 0; i < batch_count; ++i) {
            ArrowArray c_array;
            PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*batch, &c_array));
            PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array));
        }
        PAIMON_RETURN_NOT_OK(writer->Finish());
        return out->Close();
    }

    // Concatenating `array` with itself `times` times, so a multi-batch write has an expected
    // value to be compared against.
    static Result<std::shared_ptr<arrow::Array>> Repeat(const std::shared_ptr<arrow::Array>& array,
                                                        int32_t times) {
        std::vector<std::shared_ptr<arrow::Array>> chunks(times, array);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> repeated,
                                          arrow::Concatenate(chunks));
        return repeated;
    }

    static Result<std::shared_ptr<arrow::Array>> MakeDictionary(
        const std::shared_ptr<arrow::Array>& indices, const std::shared_ptr<arrow::Array>& values) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::DictionaryArray::FromArrays(indices, values));
        return array;
    }

    struct ReadResult {
        int64_t rows = 0;
        uint64_t row_groups_total = 0;
        uint64_t row_groups_after_filter = 0;
        uint64_t batches = 0;
        // Every batch, imported and concatenated. Row counts alone would let a decoding bug
        // through, so the tests compare this against what was written.
        std::shared_ptr<arrow::Array> data;
    };

    // Reads the file back the way the benchmark does, reporting what the benchmark reports on.
    Result<ReadResult> Read(const std::string& path,
                            const std::shared_ptr<arrow::Schema>& read_schema,
                            const std::shared_ptr<Predicate>& predicate = nullptr,
                            const std::optional<RoaringBitmap32>& selection = std::nullopt) {
        PAIMON_ASSIGN_OR_RAISE(FileStatus file_status, fs_->GetFileStatus(path));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, fs_->Open(path));
        auto in_stream =
            std::make_shared<ArrowInputStreamAdapter>(input, file_status.GetLen(), pool_);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ParquetFileBatchReader> reader,
                               ParquetFileBatchReader::Create(
                                   std::move(in_stream), /*options=*/{}, kBatchSize,
                                   /*file_metadata=*/nullptr, /*storage_read_bytes=*/nullptr, pool_,
                                   /*hints=*/std::nullopt));
        ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*read_schema, &c_schema));
        PAIMON_RETURN_NOT_OK(reader->SetReadSchema(&c_schema, predicate, selection));

        ReadResult result;
        std::vector<std::shared_ptr<arrow::Array>> chunks;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch batch, reader->NextBatch());
            if (BatchReader::IsEofBatch(batch)) {
                break;
            }
            // ImportArray takes ownership of both C structs, so nothing is released by hand here.
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> chunk,
                arrow::ImportArray(batch.first.get(), batch.second.get()));
            result.rows += chunk->length();
            chunks.push_back(std::move(chunk));
        }
        if (!chunks.empty()) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(result.data, arrow::Concatenate(chunks));
        }

        std::shared_ptr<Metrics> metrics = reader->GetReaderMetrics();
        PAIMON_ASSIGN_OR_RAISE(result.row_groups_total,
                               metrics->GetCounter(ParquetMetrics::READ_ROW_GROUPS_TOTAL));
        PAIMON_ASSIGN_OR_RAISE(result.row_groups_after_filter,
                               metrics->GetCounter(ParquetMetrics::READ_ROW_GROUPS_AFTER_FILTER));
        PAIMON_ASSIGN_OR_RAISE(result.batches,
                               metrics->GetCounter(ParquetMetrics::READ_BATCH_COUNT));
        reader->Close();
        return result;
    }

    static Result<std::shared_ptr<arrow::Array>> Wrap(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::shared_ptr<arrow::Array>>& columns) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> array,
                                          arrow::StructArray::Make(columns, schema->fields()));
        return checked_pointer_cast<arrow::Array>(array);
    }

    std::unique_ptr<test::UniqueTestDirectory> dir_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::MemoryPool> pool_;
};

// Every codec name the benchmark registers has to be one Parquet accepts. "lz4" is the trap this
// guards: it resolves to arrow's LZ4_FRAME, which parquet::IsCodecSupported rejects, and the
// failure only shows up once a column chunk is actually written.
TEST_F(ParquetFormatBenchmarkTest, RegisteredCodecsWrite) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema({MakeField("id", arrow::int64(), 0)});
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Reserve(kRows).ok());
    for (int64_t i = 0; i < kRows; ++i) {
        builder.UnsafeAppend(i);
    }
    std::shared_ptr<arrow::Array> ids;
    ASSERT_TRUE(builder.Finish(&ids).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, Wrap(schema, {ids}));

    for (const std::string& codec :
         {"none", "snappy", "gzip", "brotli", "zstd", "lz4_raw", "lz4_hadoop"}) {
        const std::string path = PathOf("codec_" + codec + ".parquet");
        ASSERT_OK(Write(path, schema, batch, codec)) << "codec " << codec;
        ASSERT_OK_AND_ASSIGN(ReadResult result, Read(path, schema));
        EXPECT_EQ(kRows, result.rows) << "codec " << codec;
        ASSERT_TRUE(result.data);
        EXPECT_TRUE(result.data->Equals(*batch)) << "codec " << codec;
    }

    // The name the benchmark deliberately does not register still has to be rejected; if arrow
    // ever starts accepting it, the comment explaining its absence is stale.
    EXPECT_FALSE(Write(PathOf("codec_lz4.parquet"), schema, batch, "lz4").ok());
}

// A dictionary-encoded input array must reach the writer intact. VARCHAR takes arrow's direct
// write path and INT32 gets densified first; both have to produce a readable file whose logical
// values match the flat equivalent.
TEST_F(ParquetFormatBenchmarkTest, DictionaryInputRoundTrip) {
    constexpr int64_t kCardinality = 8;
    arrow::Int32Builder index_builder;
    ASSERT_TRUE(index_builder.Reserve(kRows).ok());
    for (int64_t i = 0; i < kRows; ++i) {
        index_builder.UnsafeAppend(static_cast<int32_t>(i % kCardinality));
    }
    std::shared_ptr<arrow::Array> indices;
    ASSERT_TRUE(index_builder.Finish(&indices).ok());

    arrow::StringBuilder string_values;
    arrow::Int32Builder int_values;
    for (int64_t i = 0; i < kCardinality; ++i) {
        ASSERT_TRUE(string_values.Append("value_" + std::to_string(i)).ok());
        ASSERT_TRUE(int_values.Append(static_cast<int32_t>(i * 7)).ok());
    }
    std::shared_ptr<arrow::Array> string_dict;
    std::shared_ptr<arrow::Array> int_dict;
    ASSERT_TRUE(string_values.Finish(&string_dict).ok());
    ASSERT_TRUE(int_values.Finish(&int_dict).ok());

    // The flat arrays the dictionary-encoded input has to decode back to.
    arrow::StringBuilder flat_strings;
    arrow::Int32Builder flat_ints;
    for (int64_t i = 0; i < kRows; ++i) {
        ASSERT_TRUE(flat_strings.Append("value_" + std::to_string(i % kCardinality)).ok());
        ASSERT_TRUE(flat_ints.Append(static_cast<int32_t>((i % kCardinality) * 7)).ok());
    }
    std::shared_ptr<arrow::Array> flat_string_column;
    std::shared_ptr<arrow::Array> flat_int_column;
    ASSERT_TRUE(flat_strings.Finish(&flat_string_column).ok());
    ASSERT_TRUE(flat_ints.Finish(&flat_int_column).ok());

    struct Case {
        const char* name;
        std::shared_ptr<arrow::Array> dictionary;
        std::shared_ptr<arrow::DataType> read_type;
        std::shared_ptr<arrow::Array> flat;
    };
    // The benchmark always writes several batches, so the writer sees the same dictionary more
    // than once.
    constexpr int32_t kBatches = 3;
    for (const Case& c : {Case{"string", string_dict, arrow::utf8(), flat_string_column},
                          Case{"int32", int_dict, arrow::int32(), flat_int_column}}) {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> dictionary_array,
                             MakeDictionary(indices, c.dictionary));
        std::shared_ptr<arrow::Schema> write_schema =
            arrow::schema({MakeField("v", dictionary_array->type(), 0)});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch,
                             Wrap(write_schema, {dictionary_array}));
        const std::string path = PathOf(std::string("dict_") + c.name + ".parquet");
        ASSERT_OK(Write(path, write_schema, batch, "zstd", /*extra_options=*/{}, kBatches))
            << c.name;

        // Parquet has no dictionary type: the column comes back as its value type either way, and
        // has to carry the values the flat equivalent would have.
        std::shared_ptr<arrow::Schema> read_schema =
            arrow::schema({MakeField("v", c.read_type, 0)});
        ASSERT_OK_AND_ASSIGN(ReadResult result, Read(path, read_schema));
        EXPECT_EQ(kRows * kBatches, result.rows) << c.name;
        ASSERT_TRUE(result.data);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> expected, Repeat(c.flat, kBatches));
        std::shared_ptr<arrow::Array> actual =
            checked_pointer_cast<arrow::StructArray>(result.data)->field(0);
        EXPECT_TRUE(actual->Equals(*expected)) << c.name;
    }
}

// DECIMAL precision selects the Parquet physical type, and precision 38 is the only one that
// reaches FIXED_LEN_BYTE_ARRAY. The benchmark sweeps all three on both sides, so all three have
// to survive a round trip with their type intact.
TEST_F(ParquetFormatBenchmarkTest, DecimalPrecisionRoundTrip) {
    for (int32_t precision : {9, 18, 38}) {
        std::shared_ptr<arrow::DataType> type = arrow::decimal128(precision, 4);
        std::shared_ptr<arrow::Schema> schema = arrow::schema({MakeField("amount", type, 0)});
        arrow::Decimal128Builder builder(type);
        ASSERT_TRUE(builder.Reserve(kRows).ok());
        for (int64_t i = 0; i < kRows; ++i) {
            ASSERT_TRUE(builder.Append(arrow::Decimal128(i)).ok());
        }
        std::shared_ptr<arrow::Array> amounts;
        ASSERT_TRUE(builder.Finish(&amounts).ok());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, Wrap(schema, {amounts}));

        const std::string path = PathOf("decimal_" + std::to_string(precision) + ".parquet");
        ASSERT_OK(Write(path, schema, batch, "zstd")) << "precision " << precision;
        ASSERT_OK_AND_ASSIGN(ReadResult result, Read(path, schema));
        EXPECT_EQ(kRows, result.rows) << "precision " << precision;
        ASSERT_TRUE(result.data);
        EXPECT_TRUE(result.data->type()->field(0)->type()->Equals(*type))
            << "precision " << precision;
        EXPECT_TRUE(result.data->Equals(*batch)) << "precision " << precision;
    }
}

// The nested fixture the benchmark reads is only meaningful if LIST and MAP survive the round
// trip with the shape the read schema asks for.
TEST_F(ParquetFormatBenchmarkTest, NestedRoundTrip) {
    constexpr int32_t kEntries = 4;
    auto list_values = std::make_shared<arrow::Int64Builder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), list_values,
                                    arrow::list(arrow::int64()));
    auto key_builder = std::make_shared<arrow::StringBuilder>();
    auto item_builder = std::make_shared<arrow::Int64Builder>();
    arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder, item_builder,
                                  arrow::map(arrow::utf8(), arrow::int64()));
    for (int64_t i = 0; i < kRows; ++i) {
        ASSERT_TRUE(list_builder.Append().ok());
        ASSERT_TRUE(map_builder.Append().ok());
        for (int32_t j = 0; j < kEntries; ++j) {
            ASSERT_TRUE(list_values->Append(i + j).ok());
            ASSERT_TRUE(key_builder->Append("key_" + std::to_string(j)).ok());
            ASSERT_TRUE(item_builder->Append(i + j).ok());
        }
    }
    std::shared_ptr<arrow::Array> tags;
    std::shared_ptr<arrow::Array> attrs;
    ASSERT_TRUE(list_builder.Finish(&tags).ok());
    ASSERT_TRUE(map_builder.Finish(&attrs).ok());

    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({MakeField("tags", arrow::list(arrow::int64()), 0),
                       MakeField("attrs", arrow::map(arrow::utf8(), arrow::int64()), 1)});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, Wrap(schema, {tags, attrs}));
    const std::string path = PathOf("nested.parquet");
    ASSERT_OK(Write(path, schema, batch, "zstd"));

    ASSERT_OK_AND_ASSIGN(ReadResult result, Read(path, schema));
    EXPECT_EQ(kRows, result.rows);
    ASSERT_TRUE(result.data);
    EXPECT_TRUE(result.data->Equals(*batch));

    // Each nested column also has to be readable on its own, which is what the projected nested
    // cases do.
    for (const std::string& column : {"tags", "attrs"}) {
        std::shared_ptr<arrow::Schema> projected = arrow::schema({schema->GetFieldByName(column)});
        ASSERT_OK_AND_ASSIGN(ReadResult projected_result, Read(path, projected));
        EXPECT_EQ(kRows, projected_result.rows) << "column " << column;
    }
}

// The filtered and skip-heavy cases assert on row counts, so those counts have to mean what the
// benchmark assumes: a predicate keeps at least the matching rows and no more than the row groups
// that could hold them, and a selection bitmap keeps at least the rows it selected.
TEST_F(ParquetFormatBenchmarkTest, FilteredAndBitmapRowCounts) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema({MakeField("id", arrow::int64(), 0)});
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Reserve(kRows).ok());
    for (int64_t i = 0; i < kRows; ++i) {
        builder.UnsafeAppend(i);
    }
    std::shared_ptr<arrow::Array> ids;
    ASSERT_TRUE(builder.Finish(&ids).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, Wrap(schema, {ids}));
    const std::string path = PathOf("filtered.parquet");
    ASSERT_OK(Write(path, schema, batch, "zstd"));

    ASSERT_OK_AND_ASSIGN(ReadResult full, Read(path, schema));
    EXPECT_EQ(kRows, full.rows);
    EXPECT_EQ(static_cast<uint64_t>(kRows / kRowGroupLength), full.row_groups_total);
    EXPECT_EQ(full.row_groups_total, full.row_groups_after_filter);
    EXPECT_GT(full.batches, 0u);

    // `id` is ordered, so row-group statistics alone must discard everything past the threshold.
    // This is the bound BM_ParquetRead_Filtered asserts on.
    constexpr int64_t kThreshold = 300;
    std::shared_ptr<Predicate> predicate = PredicateBuilder::LessThan(
        /*field_index=*/0, /*field_name=*/"id", FieldType::BIGINT, Literal(kThreshold));
    ASSERT_OK_AND_ASSIGN(ReadResult filtered, Read(path, schema, predicate));
    EXPECT_GE(filtered.rows, kThreshold);
    EXPECT_LE(filtered.rows, kRowGroupLength);
    EXPECT_LT(filtered.row_groups_after_filter, filtered.row_groups_total);

    // Selection is not precise, so the bitmap is a lower bound on what comes back.
    RoaringBitmap32 bitmap;
    int64_t selected = 0;
    for (int64_t row = 0; row < kRows; row += 64) {
        bitmap.Add(static_cast<int32_t>(row));
        ++selected;
    }
    ASSERT_OK_AND_ASSIGN(ReadResult skipped, Read(path, schema, /*predicate=*/nullptr, bitmap));
    EXPECT_GE(skipped.rows, selected);
    EXPECT_LE(skipped.rows, kRows);
}

// The encoding case compares a dictionary-encoded file against a plain one, which is only a
// comparison if both files read back identically.
TEST_F(ParquetFormatBenchmarkTest, PlainAndDictionaryFilesAgree) {
    std::shared_ptr<arrow::Schema> schema = arrow::schema({MakeField("name", arrow::utf8(), 0)});
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Reserve(kRows).ok());
    for (int64_t i = 0; i < kRows; ++i) {
        ASSERT_TRUE(builder.Append("value_" + std::to_string(i % 16)).ok());
    }
    std::shared_ptr<arrow::Array> names;
    ASSERT_TRUE(builder.Finish(&names).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, Wrap(schema, {names}));

    const std::string dict_path = PathOf("encoding_dict.parquet");
    const std::string plain_path = PathOf("encoding_plain.parquet");
    ASSERT_OK(Write(dict_path, schema, batch, "zstd"));
    std::map<std::string, std::string> plain_options;
    plain_options[PARQUET_ENABLE_DICTIONARY] = "false";
    ASSERT_OK(Write(plain_path, schema, batch, "zstd", plain_options));

    ASSERT_OK_AND_ASSIGN(ReadResult dict_result, Read(dict_path, schema));
    ASSERT_OK_AND_ASSIGN(ReadResult plain_result, Read(plain_path, schema));
    EXPECT_EQ(kRows, dict_result.rows);
    EXPECT_EQ(dict_result.rows, plain_result.rows);
    EXPECT_EQ(dict_result.row_groups_total, plain_result.row_groups_total);
    ASSERT_TRUE(dict_result.data);
    ASSERT_TRUE(plain_result.data);
    EXPECT_TRUE(dict_result.data->Equals(*batch));
    EXPECT_TRUE(plain_result.data->Equals(*batch));
}

// BM_ParquetWrite_MemoryThreshold rests on one assumption: that a small
// parquet.writer.max.memory.use actually makes ParquetFormatWriter cut extra row groups. This
// covers the mechanism on both input shapes that case can present it with, plain and
// dictionary-encoded, with the row-count limit raised out of the way so the byte threshold is the
// only thing that can flush.
//
// It does not cover the benchmark's own 512 KiB setting: reaching that with cardinality-10
// dictionary data takes the 500K to 2M rows the benchmark writes, which is not a unit test. What
// confirms that setting is the benchmark's own row_groups counter reading more than 1.
TEST_F(ParquetFormatBenchmarkTest, MemoryThresholdFlushesRowGroups) {
    constexpr int32_t kBatches = 8;
    constexpr int64_t kDictCardinality = 10;

    arrow::StringBuilder plain_builder;
    ASSERT_TRUE(plain_builder.Reserve(kRows).ok());
    for (int64_t i = 0; i < kRows; ++i) {
        ASSERT_TRUE(plain_builder.Append("value_" + std::to_string(i)).ok());
    }
    std::shared_ptr<arrow::Array> plain_column;
    ASSERT_TRUE(plain_builder.Finish(&plain_column).ok());

    // The shape BM_ParquetWrite_MemoryThreshold writes: low-cardinality dictionary input.
    arrow::StringBuilder dict_values;
    for (int64_t i = 0; i < kDictCardinality; ++i) {
        ASSERT_TRUE(dict_values.Append("value_" + std::to_string(i)).ok());
    }
    std::shared_ptr<arrow::Array> dict_value_column;
    ASSERT_TRUE(dict_values.Finish(&dict_value_column).ok());
    arrow::Int32Builder dict_indices;
    ASSERT_TRUE(dict_indices.Reserve(kRows).ok());
    for (int64_t i = 0; i < kRows; ++i) {
        dict_indices.UnsafeAppend(static_cast<int32_t>(i % kDictCardinality));
    }
    std::shared_ptr<arrow::Array> index_column;
    ASSERT_TRUE(dict_indices.Finish(&index_column).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> dict_column,
                         MakeDictionary(index_column, dict_value_column));

    struct Case {
        const char* name;
        std::shared_ptr<arrow::Array> column;
    };
    for (const Case& c : {Case{"plain", plain_column}, Case{"dictionary", dict_column}}) {
        std::shared_ptr<arrow::Schema> write_schema =
            arrow::schema({MakeField("name", c.column->type(), 0)});
        // Parquet stores a dictionary column as its value type, so both read back as UTF8.
        std::shared_ptr<arrow::Schema> read_schema =
            arrow::schema({MakeField("name", arrow::utf8(), 0)});
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> batch, Wrap(write_schema, {c.column}));

        std::map<std::string, std::string> options;
        options[PARQUET_WRITE_MAX_ROW_GROUP_LENGTH] = std::to_string(kRows * kBatches * 10);

        options[PARQUET_WRITER_MAX_MEMORY_USE] = std::to_string(uint64_t{8} * 1024);
        const std::string small_path =
            PathOf(std::string("threshold_small_") + c.name + ".parquet");
        ASSERT_OK(Write(small_path, write_schema, batch, "zstd", options, kBatches)) << c.name;
        ASSERT_OK_AND_ASSIGN(ReadResult small, Read(small_path, read_schema));
        EXPECT_EQ(kRows * kBatches, small.rows) << c.name;
        EXPECT_GT(small.row_groups_total, 1u)
            << c.name << ": byte threshold never triggered a flush";

        options[PARQUET_WRITER_MAX_MEMORY_USE] =
            std::to_string(DEFAULT_PARQUET_WRITER_MAX_MEMORY_USE);
        const std::string large_path =
            PathOf(std::string("threshold_large_") + c.name + ".parquet");
        ASSERT_OK(Write(large_path, write_schema, batch, "zstd", options, kBatches)) << c.name;
        ASSERT_OK_AND_ASSIGN(ReadResult large, Read(large_path, read_schema));
        EXPECT_EQ(kRows * kBatches, large.rows) << c.name;
        EXPECT_EQ(1u, large.row_groups_total) << c.name;

        // Row-group boundaries must not change the data.
        ASSERT_TRUE(small.data) << c.name;
        ASSERT_TRUE(large.data) << c.name;
        EXPECT_TRUE(small.data->Equals(*large.data)) << c.name;
    }
}

}  // namespace
}  // namespace paimon::parquet
