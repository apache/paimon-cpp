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
#include "paimon/file_index/file_index_format.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <map>
#include <utility>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index.h"
#include "paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index.h"
#include "paimon/common/file_index/empty/empty_file_index_reader.h"
#include "paimon/common/file_index/file_index_format_utils.h"
#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FileIndexFormatTest : public ::testing::Test {
 public:
    Status WriteIndexes(const FileIndexFormat::ColumnIndexes& indexes,
                        FileIndexFormat::Writer* writer) const {
        for (const auto& [column, entries] : indexes) {
            for (const auto& [type, bytes] : entries) {
                FileIndexFormat::Writer::PayloadWriter payload =
                    [bytes = bytes](OutputStream* output) -> Result<bool> {
                    if (!bytes || bytes->size() == 0) {
                        return false;
                    }
                    PAIMON_RETURN_NOT_OK(DataOutputStream(output).WriteBytes(bytes));
                    return true;
                };
                PAIMON_RETURN_NOT_OK(writer->WriteIndex(column, type, payload));
            }
        }
        return Status::OK();
    }

    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::Schema>& schema) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(FileIndexFormatTest, TestWriteAndReadEmptyIndexGoldenBytes) {
    // the expected bytes are generated from Java Paimon
    std::vector<char> expected = {0,   5,   78,  78, -48, 26, 53, -82, 0, 0, 0, 1, 0, 0, 0,   47,
                                  0,   0,   0,   1,  0,   2,  99, 49,  0, 0, 0, 1, 0, 5, 101, 109,
                                  112, 116, 121, -1, -1,  -1, -1, 0,   0, 0, 0, 0, 0, 0, 0};
    FileIndexFormat::ColumnIndexes indexes;
    indexes["c1"]["empty"] = nullptr;
    auto segment_output = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    auto output = std::make_shared<ByteArrayOutputStream>(std::move(segment_output));

    ASSERT_OK_AND_ASSIGN(auto writer,
                         FileIndexFormat::CreateWriter(output, FileIndexFormat::V_1, pool_));
    ASSERT_OK(WriteIndexes(indexes, writer.get()));
    ASSERT_OK(writer->Finish());
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> actual, output->Finish(pool_.get()));

    ASSERT_EQ(expected, std::vector<char>(actual->data(), actual->data() + actual->size()));
    auto schema = arrow::schema({arrow::field("c1", arrow::utf8())});
    auto input_stream = std::make_shared<ByteArrayInputStream>(actual->data(), actual->size());
    ASSERT_OK_AND_ASSIGN(auto reader,
                         FileIndexFormat::CreateReader(input_stream, pool_, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                         reader->ReadColumnIndex("c1", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, index_file_readers.size());
    auto* empty_reader = dynamic_cast<EmptyFileIndexReader*>(index_file_readers[0].get());
    ASSERT_TRUE(empty_reader);
}

TEST_F(FileIndexFormatTest, TestEmptyVectorIndexCanBeSearched) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["c1"]["lumina"] = nullptr;
    auto segment_output = std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    auto output = std::make_shared<ByteArrayOutputStream>(std::move(segment_output));
    ASSERT_OK_AND_ASSIGN(auto writer,
                         FileIndexFormat::CreateWriter(output, FileIndexFormat::V_1, pool_));
    ASSERT_OK(WriteIndexes(indexes, writer.get()));
    ASSERT_OK(writer->Finish());
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> bytes, output->Finish(pool_.get()));

    auto input_stream = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader,
                         FileIndexFormat::CreateReader(input_stream, pool_, /*options=*/{}));
    auto schema = arrow::schema({arrow::field("c1", arrow::list(arrow::float32()))});
    ASSERT_OK_AND_ASSIGN(auto index_readers,
                         reader->ReadColumnIndex("c1", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, index_readers.size());
    auto search = std::make_shared<VectorSearch>(
        "c1", /*limit=*/1, std::vector<float>{1.0f}, nullptr, nullptr,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         index_readers[0]->VisitVectorSearch(search));
    EXPECT_TRUE(result->IsEmpty());
    EXPECT_TRUE(result->GetScores().empty());
}

TEST_F(FileIndexFormatTest, TestSimple) {
    auto schema =
        arrow::schema({arrow::field("f1", arrow::int32()), arrow::field("f2", arrow::int32()),
                       arrow::field("non-exist", arrow::int32())});
    std::vector<uint8_t> index_file_bytes = {
        0,   5,   78,  78,  208, 26,  53,  174, 0,   0,   0,   1,   0,  0,   0,   96,  0,   0,
        0,   3,   0,   2,   102, 48,  0,   0,   0,   1,   0,   6,   98, 105, 116, 109, 97,  112,
        0,   0,   0,   96,  0,   0,   0,   131, 0,   2,   102, 49,  0,  0,   0,   1,   0,   6,
        98,  105, 116, 109, 97,  112, 0,   0,   0,   227, 0,   0,   0,  74,  0,   2,   102, 50,
        0,   0,   0,   1,   0,   6,   98,  105, 116, 109, 97,  112, 0,  0,   1,   45,  0,   0,
        0,   76,  0,   0,   0,   0,   1,   0,   0,   0,   8,   0,   0,  0,   5,   0,   0,   0,
        0,   5,   65,  108, 105, 99,  101, 0,   0,   0,   0,   0,   0,  0,   4,   76,  117, 99,
        121, 255, 255, 255, 251, 0,   0,   0,   3,   66,  111, 98,  0,  0,   0,   20,  0,   0,
        0,   5,   69,  109, 105, 108, 121, 255, 255, 255, 253, 0,   0,  0,   4,   84,  111, 110,
        121, 0,   0,   0,   40,  58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   1,   0,   16,
        0,   0,   0,   0,   0,   7,   0,   58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   1,
        0,   16,  0,   0,   0,   1,   0,   5,   0,   58,  48,  0,   0,  1,   0,   0,   0,   0,
        0,   1,   0,   16,  0,   0,   0,   3,   0,   6,   0,   1,   0,  0,   0,   8,   0,   0,
        0,   2,   0,   0,   0,   0,   20,  0,   0,   0,   0,   0,   0,  0,   10,  0,   0,   0,
        22,  58,  48,  0,   0,   1,   0,   0,   0,   0,   0,   2,   0,  16,  0,   0,   0,   4,
        0,   6,   0,   7,   0,   58,  48,  0,   0,   1,   0,   0,   0,  0,   0,   4,   0,   16,
        0,   0,   0,   0,   0,   1,   0,   2,   0,   3,   0,   5,   0,  1,   0,   0,   0,   8,
        0,   0,   0,   2,   1,   255, 255, 255, 248, 0,   0,   0,   0,  0,   0,   0,   0,   0,
        0,   0,   1,   0,   0,   0,   22,  58,  48,  0,   0,   1,   0,  0,   0,   0,   0,   2,
        0,   16,  0,   0,   0,   2,   0,   3,   0,   6,   0,   58,  48, 0,   0,   1,   0,   0,
        0,   0,   0,   3,   0,   16,  0,   0,   0,   0,   0,   1,   0,  4,   0,   5,   0};
    auto input_stream = std::make_shared<ByteArrayInputStream>(
        reinterpret_cast<char*>(index_file_bytes.data()), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader,
                         FileIndexFormat::CreateReader(input_stream, pool_, /*options=*/{}));
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("f1", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, index_file_readers.size());
        auto* bitmap_reader = dynamic_cast<BitmapFileIndexReader*>(index_file_readers[0].get());
        ASSERT_TRUE(bitmap_reader);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(10)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{0,1,2,3,5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(Literal(10)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(50)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{}", result->ToString());
        }
    }
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("f2", CreateArrowSchema(schema).get()));
        ASSERT_EQ(1, index_file_readers.size());
        auto* bitmap_reader = dynamic_cast<BitmapFileIndexReader*>(index_file_readers[0].get());
        ASSERT_TRUE(bitmap_reader);
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(0)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitEqual(Literal(1)));
            ASSERT_TRUE(result);
            ASSERT_EQ("{0,1,4,5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_TRUE(result);
            ASSERT_EQ("{7}", result->ToString());
        }
    }
    {
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex("non-exist", CreateArrowSchema(schema).get()));
        ASSERT_TRUE(index_file_readers.empty());
    }
}

// NOLINTNEXTLINE(google-readability-function-size)
TEST_F(FileIndexFormatTest, TestBitmapIndexWithTimestamp) {
    auto schema = arrow::schema({
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, "Asia/Tokyo")),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, "Asia/Tokyo")),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, "Asia/Tokyo")),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Tokyo")),
    });

    auto fs = std::make_shared<LocalFileSystem>();
    std::string file_name = GetDataDir() +
                            "orc/timestamp_index.db/timestamp_index/"
                            "bucket-0/data-18569866-0c37-45e9-9d88-2eaf6dd084b0-0.orc.index";
    std::string index_file_bytes;
    ASSERT_OK(fs->ReadFile(file_name, &index_file_bytes));
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_file_bytes.data(), index_file_bytes.size());
    ASSERT_OK_AND_ASSIGN(auto reader,
                         FileIndexFormat::CreateReader(input_stream, pool_, /*options=*/{}));
    auto check_second = [&](const std::string& field_name) {
        // data: second
        // 1745542802000lms, 0ns
        // 1745542902000lms, 0ns
        // 1745542602000lms, 0ns
        // -1745000lms, 0ns
        // -1765000lms, 0ns
        // null
        // 1745542802000lms, 0ns
        // -1725000lms, 0ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542502000l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitIn({Literal(Timestamp(1745542802000l, 0)),
                                                         Literal(Timestamp(-1745000l, 0)),
                                                         Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802000l, 0)),
                                                        Literal(Timestamp(-1745000l, 0)),
                                                        Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502000l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802000l, 0)),
                                                      Literal(Timestamp(-1745000l, 0)),
                                                      Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotIn({Literal(Timestamp(1745542802000l, 0)),
                                                         Literal(Timestamp(-1745000l, 0)),
                                                         Literal(Timestamp(1745542602000l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitGreaterThan(Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802000l, 0))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745000l, 0))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 0))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725000l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
    };

    auto check_milli = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 0ns
        // 1745542902001lms, 0ns
        // 1745542602001lms, 0ns
        // -1745001lms, 0ns
        // -1765001lms, 0ns
        // null
        // 1745542802001lms, 0ns
        // -1725001lms, 0ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 0)),
                                                         Literal(Timestamp(-1745001l, 0)),
                                                         Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 0)),
                                                        Literal(Timestamp(-1745001l, 0)),
                                                        Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 0))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 0)),
                                                      Literal(Timestamp(-1745001l, 0)),
                                                      Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 0)),
                                                         Literal(Timestamp(-1745001l, 0)),
                                                         Literal(Timestamp(1745542602001l, 0))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitGreaterThan(Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 0))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 0))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 0))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 0)))
                        .value()
                        ->IsRemain()
                        .value());
    };
    auto check_micro = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 1000ns
        // 1745542902001lms, 1000ns
        // 1745542602001lms, 1000ns
        // -1745001lms, 1000ns
        // -1765001lms, 1000ns
        // null
        // 1745542802001lms, 1000ns
        // -1725001lms, 1000ns
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 1000)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1000)),
                                                        Literal(Timestamp(-1745001l, 1000)),
                                                        Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 1000)),
                                                      Literal(Timestamp(-1745001l, 1000)),
                                                      Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1000)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterThan(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 1000))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 1000))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
    };

    auto check_nano = [&](const std::string& field_name) {
        // data: milli second
        // 1745542802001lms, 1001ns
        // 1745542902001lms, 1001ns
        // 1745542602001lms, 1001ns
        // -1745001lms, 1001ns
        // -1765001lms, 1001ns
        // null
        // 1745542802001lms, 1001ns
        // -1725001lms, 1001ns

        // as timestamp is normalized by micro seconds, there is a loss of precision in the
        // nanosecond part
        ASSERT_OK_AND_ASSIGN(auto index_file_readers,
                             reader->ReadColumnIndex(field_name, CreateArrowSchema(schema).get()));
        ASSERT_EQ(3, index_file_readers.size());
        BitSliceIndexBitmapFileIndexReader* bsi_reader = nullptr;
        BitmapFileIndexReader* bitmap_reader = nullptr;
        BloomFilterFileIndexReader* bloom_filter_reader = nullptr;

        for (const auto& reader : index_file_readers) {
            if (auto* r = dynamic_cast<BitSliceIndexBitmapFileIndexReader*>(reader.get())) {
                bsi_reader = r;
            } else if (auto* r = dynamic_cast<BitmapFileIndexReader*>(reader.get())) {
                bitmap_reader = r;
            } else if (auto* r = dynamic_cast<BloomFilterFileIndexReader*>(reader.get())) {
                bloom_filter_reader = r;
            }
        }
        ASSERT_TRUE(bsi_reader);
        ASSERT_TRUE(bitmap_reader);
        ASSERT_TRUE(bloom_filter_reader);
        // test bitmap
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bitmap_reader->VisitNotEqual(
                                                  Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitIn({Literal(Timestamp(1745542802001l, 1001)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bitmap_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1001)),
                                                        Literal(Timestamp(-1745001l, 1000)),
                                                        Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }

        // test bsi
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNull());
            ASSERT_EQ("{5}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitIsNotNull());
            ASSERT_EQ("{0,1,2,3,4,6,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542502001l, 1000))));
            ASSERT_EQ("{}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{0,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotEqual(Literal(Timestamp(1745542802001l, 1001))));
            ASSERT_EQ("{1,2,3,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitIn({Literal(Timestamp(1745542802001l, 1001)),
                                                      Literal(Timestamp(-1745001l, 1000)),
                                                      Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{0,2,3,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(
                auto result, bsi_reader->VisitNotIn({Literal(Timestamp(1745542802001l, 1001)),
                                                     Literal(Timestamp(-1745001l, 1000)),
                                                     Literal(Timestamp(1745542602001l, 1000))}));
            ASSERT_EQ("{1,4,7}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterThan(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{1}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result, bsi_reader->VisitGreaterOrEqual(
                                                  Literal(Timestamp(1745542802001l, 1000))));
            ASSERT_EQ("{0,1,6}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessThan(Literal(Timestamp(-1745001l, 1000))));
            ASSERT_EQ("{4}", result->ToString());
        }
        {
            ASSERT_OK_AND_ASSIGN(auto result,
                                 bsi_reader->VisitLessOrEqual(Literal(Timestamp(0l, 1000))));
            ASSERT_EQ("{3,4,7}", result->ToString());
        }

        // test bloom filter
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542902001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542602001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1745001l, 1001)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1765001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(1745542802001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
        ASSERT_TRUE(bloom_filter_reader->VisitEqual(Literal(Timestamp(-1725001l, 1000)))
                        .value()
                        ->IsRemain()
                        .value());
    };
    check_second("ts_sec");
    check_second("ts_tz_sec");

    check_milli("ts_milli");
    check_milli("ts_tz_milli");

    check_micro("ts_micro");
    check_micro("ts_tz_micro");

    check_nano("ts_nano");
    check_nano("ts_tz_nano");
}

namespace {

std::shared_ptr<ByteArrayOutputStream> NewIndexOutput(const std::shared_ptr<MemoryPool>& pool) {
    return std::make_shared<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool));
}

// Generated with FileIndexFormatV2.Writer from Java Paimon.
// Contains an empty index, a two-byte unknown index, and a present zero-byte index.
const std::vector<uint8_t> kJavaV2Golden = {
    0,   5,   78,  78,  208, 26, 53, 174, 0,  0,  0,   2,   42,  43,  0,   0,   0,   1,
    0,   2,   99,  49,  0,   0,  0,  2,   0,  5,  101, 109, 112, 116, 121, 255, 255, 255,
    255, 255, 255, 255, 255, 0,  0,  0,   0,  0,  0,   0,   0,   0,   7,   117, 110, 107,
    110, 111, 119, 110, 0,   0,  0,  0,   0,  0,  0,   12,  0,   0,   0,   0,   0,   0,
    0,   2,   0,   0,   0,   60, 80, 65,  70, 73, 68,  88,  48,  50,
};

struct SparseIndexState {
    std::map<int64_t, std::string> segments;
    int64_t length = 0;
    bool discard = false;
};

class SparseIndexOutput : public MockOutputStream {
 public:
    explicit SparseIndexOutput(const std::shared_ptr<SparseIndexState>& state) : state_(state) {}
    Result<int64_t> Write(const char* buffer, int64_t size) override {
        if (!state_->discard) {
            state_->segments[state_->length] = std::string(buffer, size);
        }
        state_->length += size;
        return size;
    }
    Result<int64_t> GetPos() const override {
        return state_->length;
    }

 private:
    std::shared_ptr<SparseIndexState> state_;
};

class SparseIndexInput : public MockInputStream {
 public:
    explicit SparseIndexInput(const std::shared_ptr<SparseIndexState>& state) : state_(state) {}
    Status Seek(int64_t offset, SeekOrigin origin) override {
        if (origin == FS_SEEK_CUR) {
            offset += position_;
        }
        if (origin == FS_SEEK_END) {
            offset += state_->length;
        }
        if (offset < 0 || offset > state_->length) {
            return Status::Invalid("Invalid seek");
        }
        position_ = offset;
        return Status::OK();
    }
    Result<int64_t> GetPos() const override {
        return position_;
    }
    Result<int64_t> Length() const override {
        return state_->length;
    }
    Result<int64_t> Read(char* buffer, int64_t size) override {
        PAIMON_ASSIGN_OR_RAISE(int64_t read, Read(buffer, size, position_));
        position_ += read;
        return read;
    }
    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        if (offset < 0 || size < 0 || offset > state_->length) {
            return Status::Invalid("Invalid read");
        }
        size = std::min(size, state_->length - offset);
        std::memset(buffer, 0, size);
        for (const auto& [start, bytes] : state_->segments) {
            int64_t end = start + bytes.size();
            int64_t from = std::max(start, offset);
            int64_t to = std::min(end, offset + size);
            if (from < to) {
                std::memcpy(buffer + from - offset, bytes.data() + from - start, to - from);
            }
        }
        return size;
    }

 private:
    std::shared_ptr<SparseIndexState> state_;
    int64_t position_ = 0;
};
}  // namespace

TEST_F(FileIndexFormatTest, TestV2JavaGoldenBytes) {
    FileIndexFormat::ColumnIndexes indexes;
    indexes["c1"]["empty"] = nullptr;
    indexes["c1"]["unknown"] = std::make_shared<Bytes>(std::string("*+"), pool_.get());
    auto output = NewIndexOutput(pool_);
    ASSERT_OK_AND_ASSIGN(auto writer,
                         FileIndexFormat::CreateWriter(output, FileIndexFormat::V_2, pool_));
    ASSERT_OK(WriteIndexes(indexes, writer.get()));
    ASSERT_OK(writer->Finish());
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(auto bytes, output->Finish(pool_.get()));
    ASSERT_EQ(kJavaV2Golden, std::vector<uint8_t>(bytes->data(), bytes->data() + bytes->size()));

    // Read the Java golden bytes, including empty-index handling and
    // skipping unknown backend types without opening their payloads.
    auto input = std::make_shared<ByteArrayInputStream>(
        reinterpret_cast<const char*>(kJavaV2Golden.data()), kJavaV2Golden.size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input, pool_, /*options=*/{}));
    auto schema = arrow::schema({arrow::field("c1", arrow::int32())});
    ASSERT_OK_AND_ASSIGN(auto readers,
                         reader->ReadColumnIndex("c1", CreateArrowSchema(schema).get()));
    ASSERT_EQ(1, readers.size());
    ASSERT_TRUE(dynamic_cast<EmptyFileIndexReader*>(readers[0].get()));
}

TEST_F(FileIndexFormatTest, TestV2InvalidContainers) {
    auto check = [this](const std::vector<uint8_t>& bytes) {
        auto input = std::make_shared<ByteArrayInputStream>(
            bytes.empty() ? "" : reinterpret_cast<const char*>(bytes.data()), bytes.size());
        ASSERT_NOK(FileIndexFormat::CreateReader(input, pool_, /*options=*/{}));
    };
    for (size_t size : {static_cast<size_t>(0), static_cast<size_t>(11), static_cast<size_t>(24),
                        kJavaV2Golden.size() - 1}) {
        check(std::vector<uint8_t>(kJavaV2Golden.begin(), kJavaV2Golden.begin() + size));
    }
    for (const auto& [offset, value] : std::vector<std::pair<size_t, uint8_t>>{
             {11, 3},
             {kJavaV2Golden.size() - 1, 0},
             {kJavaV2Golden.size() - 12, 0x7f},     // Impossible footer length.
             {kJavaV2Golden.size() - 12, 0xff}}) {  // Negative footer length.
        auto bytes = kJavaV2Golden;
        bytes[offset] = value;
        check(bytes);
    }
}

TEST_F(FileIndexFormatTest, TestV2StreamingLargeContainer) {
    auto state = std::make_shared<SparseIndexState>();
    auto output = std::make_shared<SparseIndexOutput>(state);
    ASSERT_OK_AND_ASSIGN(auto writer,
                         FileIndexFormat::CreateWriter(output, FileIndexFormat::V_2, pool_));
    // Stream a virtual >2 GiB payload in bounded chunks; the mock discards only this gap.
    const int64_t gap_length = int64_t{1} << 31;
    ASSERT_OK(writer->WriteIndex("gap", "unknown", [state](OutputStream* stream) -> Result<bool> {
        state->discard = true;
        std::array<char, 65536> zeros{};
        for (int64_t i = 0; i < gap_length; i += zeros.size()) {
            PAIMON_ASSIGN_OR_RAISE(int64_t written, stream->Write(zeros.data(), zeros.size()));
            if (written != static_cast<int64_t>(zeros.size())) {
                return Status::IOError("short write");
            }
        }
        state->discard = false;
        return true;
    }));
    auto schema = arrow::schema({arrow::field("f0", arrow::int32())});
    std::vector<std::pair<std::string, std::shared_ptr<FileIndexer>>> indexers = {
        {"bitmap", std::make_shared<BitmapFileIndex>(std::map<std::string, std::string>{})},
        {"bsi",
         std::make_shared<BitSliceIndexBitmapFileIndex>(std::map<std::string, std::string>{})},
        {"bloom-filter",
         std::make_shared<BloomFilterFileIndex>(std::map<std::string, std::string>{})},
        {"range-bitmap",
         std::make_shared<RangeBitmapFileIndex>(std::map<std::string, std::string>{})}};
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(schema->fields()),
                                                  R"([[1], [2], [1]])")
            .ValueOrDie();
    auto batch = checked_pointer_cast<arrow::StructArray>(array);
    for (const auto& [type, indexer] : indexers) {
        ASSERT_OK_AND_ASSIGN(auto index_writer,
                             indexer->CreateWriter(CreateArrowSchema(schema).get(), pool_));
        ArrowArray c_batch;
        ASSERT_TRUE(arrow::ExportArray(*batch, &c_batch).ok());
        ASSERT_OK(index_writer->AddBatch(&c_batch));
        ASSERT_OK(writer->WriteIndex(type, type, [index_writer](OutputStream* stream) {
            return index_writer->WriteTo(stream);
        }));
    }
    ASSERT_OK(writer->Finish());
    ASSERT_OK(writer->Close());
    ASSERT_GT(state->length, gap_length);
    auto input = std::make_shared<SparseIndexInput>(state);
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input, pool_, /*options=*/{}));
    for (const auto& [type, indexer] : indexers) {
        ASSERT_OK_AND_ASSIGN(
            auto readers,
            reader->ReadColumnIndex(
                type,
                CreateArrowSchema(arrow::schema({arrow::field(type, arrow::int32())})).get()));
        ASSERT_EQ(1, readers.size()) << type;
        ASSERT_OK_AND_ASSIGN(auto matches, readers[0]->VisitEqual(Literal(1)));
        if (type == "bloom-filter") {
            ASSERT_OK_AND_ASSIGN(bool remains, matches->IsRemain());
            ASSERT_TRUE(remains);
        } else {
            ASSERT_EQ("{0,2}", matches->ToString()) << type;
        }
    }
}

TEST_F(FileIndexFormatTest, TestPluginRangeLimits) {
    auto schema = arrow::schema({arrow::field("f0", arrow::int32())});
    auto state = std::make_shared<SparseIndexState>();
    state->length = int64_t{1} << 32;
    auto input = std::make_shared<SparseIndexInput>(state);
    BitmapFileIndex bitmap({});
    RangeBitmapFileIndex range({});
    const int64_t oversized = int64_t{1} << 31;
    for (FileIndexer* indexer : std::vector<FileIndexer*>{&bitmap, &range}) {
        auto c_schema = CreateArrowSchema(schema);
        ASSERT_NOK_WITH_MSG(indexer->CreateReader(c_schema.get(), 0, oversized, input, pool_),
                            "INT32_MAX");
        if (c_schema->release) {
            c_schema->release(c_schema.get());
        }
    }
}

TEST_F(FileIndexFormatTest, TestV2EmptyContainer) {
    auto output = NewIndexOutput(pool_);
    ASSERT_OK_AND_ASSIGN(auto writer,
                         FileIndexFormat::CreateWriter(output, FileIndexFormat::V_2, pool_));
    ASSERT_OK_AND_ASSIGN(int64_t prefix_length, output->GetPos());
    ASSERT_EQ(12, prefix_length);
    ASSERT_OK(writer->Finish());
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(auto bytes, output->Finish(pool_.get()));
    ASSERT_EQ(28, bytes->size());
    auto input = std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    ASSERT_OK_AND_ASSIGN(auto reader, FileIndexFormat::CreateReader(input, pool_, /*options=*/{}));
}

TEST_F(FileIndexFormatTest, TestV1BuffersPayloadsUntilFinish) {
    FileIndexFormat::ColumnIndexes indexes = {
        {"f0", {{"bitmap", std::make_shared<Bytes>("payload", pool_.get())}, {"empty", nullptr}}}};
    auto output = NewIndexOutput(pool_);
    ASSERT_OK_AND_ASSIGN(auto writer,
                         FileIndexFormat::CreateWriter(output, FileIndexFormat::V_1, pool_));
    ASSERT_OK(WriteIndexes(indexes, writer.get()));
    ASSERT_OK_AND_ASSIGN(int64_t before_finish, output->GetPos());
    ASSERT_EQ(0, before_finish);
    ASSERT_OK(writer->Finish());
    ASSERT_OK(writer->Close());
    ASSERT_OK_AND_ASSIGN(auto actual, output->Finish(pool_.get()));
    auto input = std::make_shared<ByteArrayInputStream>(actual->data(), actual->size());
    DataInputStream in(input);
    ASSERT_OK(in.Seek(12));
    ASSERT_OK_AND_ASSIGN(int32_t head_length, in.ReadValue<int32_t>());
    ASSERT_OK_AND_ASSIGN(FileIndexFormatUtils::FileIndexEntries entries,
                         FileIndexFormatUtils::ReadFileIndexEntries(in, FileIndexFormat::V_1));
    ASSERT_EQ(std::make_pair(int64_t{head_length}, int64_t{7}), entries.at("f0").at("bitmap"));
    ASSERT_EQ(std::make_pair(int64_t{-1}, int64_t{0}), entries.at("f0").at("empty"));
    ASSERT_EQ("payload", std::string(actual->data() + head_length, 7));
}

TEST_F(FileIndexFormatTest, TestAbsentPayloadMustNotWriteBytes) {
    for (int32_t version : {FileIndexFormat::V_1, FileIndexFormat::V_2}) {
        SCOPED_TRACE(version);
        ASSERT_OK_AND_ASSIGN(auto writer,
                             FileIndexFormat::CreateWriter(NewIndexOutput(pool_), version, pool_));
        ASSERT_NOK_WITH_MSG(writer->WriteIndex("f0", "bad",
                                               [](OutputStream* output) -> Result<bool> {
                                                   PAIMON_ASSIGN_OR_RAISE(int64_t written,
                                                                          output->Write("x", 1));
                                                   if (written != 1) {
                                                       return Status::IOError("short write");
                                                   }
                                                   return false;
                                               }),
                            "Absent file index payload must not write bytes");
        ASSERT_NOK(writer->Finish());
        ASSERT_OK(writer->Close());
    }
}

}  // namespace paimon::test
