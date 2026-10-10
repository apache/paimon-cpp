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

#include "paimon/indexer/lumina/lumina_file_index.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::lumina::test {
namespace {

std::shared_ptr<arrow::StructArray> CreateVectors() {
    auto type = arrow::struct_({arrow::field("embedding", arrow::list(arrow::float32()))});
    return checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(type, R"([
            [[0.0, 0.0, 0.0, 0.0]],
            [null],
            [[1.0, 1.0, 1.0, 1.0]]
        ])")
            .ValueOrDie());
}

std::shared_ptr<arrow::StructArray> CreateAllNullVectors() {
    auto type = arrow::struct_({arrow::field("embedding", arrow::list(arrow::float32()))});
    return checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(type, R"([[null], [null], [null]])")
            .ValueOrDie());
}

std::shared_ptr<arrow::StructArray> CreateTaggedVectors() {
    auto type = arrow::struct_({arrow::field("embedding", arrow::list(arrow::float32())),
                                arrow::field("color", arrow::utf8())});
    return checked_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(type, R"([
            [[0.0, 0.0, 0.0, 0.0], "red"],
            [null, "red"],
            [[1.0, 1.0, 1.0, 1.0], "blue"],
            [[1.0, 1.0, 1.0, 1.1], "red"]
        ])")
            .ValueOrDie());
}

}  // namespace

TEST(LuminaFileIndexTest, RoundTripUsesFileLocalRowPositions) {
    const std::map<std::string, std::string> options = {
        {"index.dimension", "4"},
        {"index.type", "bruteforce"},
        {"distance.metric", "l2"},
        {"encoding.type", "rawf32"},
    };
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::StructArray> batch = CreateVectors();
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("embedding", arrow::list(arrow::float32()))});
    LuminaFileIndexer indexer(options);

    ::ArrowSchema writer_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &writer_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexWriter> writer,
                         indexer.CreateWriter(&writer_schema, pool));
    ::ArrowArray c_batch;
    ASSERT_TRUE(arrow::ExportArray(*batch, &c_batch).ok());
    ASSERT_OK(writer->AddBatch(&c_batch));
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> artifact, writer->SerializedBytes());
    ASSERT_TRUE(artifact);

    std::string container = "prefix";
    container.append(artifact->data(), artifact->size());
    std::shared_ptr<InputStream> input =
        std::make_shared<ByteArrayInputStream>(container.data(), container.size());
    ::ArrowSchema reader_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &reader_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexReader> reader,
                         indexer.CreateReader(&reader_schema, /*start=*/6,
                                              static_cast<int32_t>(artifact->size()), input, pool));

    std::shared_ptr<VectorSearch> search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/1, std::vector<float>{1.0f, 1.0f, 1.0f, 1.0f},
        /*pre_filter=*/nullptr, /*predicate=*/nullptr, VectorSearch::DistanceType::EUCLIDEAN,
        std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         reader->VisitVectorSearch(search));
    EXPECT_EQ(RoaringBitmap32::From({2}), result->GetRowPositions());
    ASSERT_EQ(1, result->GetScores().size());
    EXPECT_FLOAT_EQ(0.0f, result->GetScores()[0]);

    std::shared_ptr<VectorSearch> filtered_search =
        search->ReplacePreFilter([](int64_t file_row) { return file_row == 0; });
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(filtered_search));
    EXPECT_EQ(RoaringBitmap32::From({0}), result->GetRowPositions());
    ASSERT_EQ(1, result->GetScores().size());
    EXPECT_FLOAT_EQ(4.0f, result->GetScores()[0]);

    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
        Literal(FieldType::STRING, "red", 3));
    std::shared_ptr<VectorSearch> tag_search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/1, std::vector<float>{1.0f, 1.0f, 1.0f, 1.0f},
        /*pre_filter=*/nullptr, predicate, VectorSearch::DistanceType::EUCLIDEAN,
        std::map<std::string, std::string>{});
    ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(tag_search),
                        "lumina index was not built with tag");
}

TEST(LuminaFileIndexTest, AllNullVectorsProduceEmptyArtifact) {
    const std::map<std::string, std::string> options = {
        {"index.dimension", "4"},
        {"index.type", "bruteforce"},
        {"distance.metric", "l2"},
        {"encoding.type", "rawf32"},
    };
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::StructArray> batch = CreateAllNullVectors();
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("embedding", arrow::list(arrow::float32()))});
    LuminaFileIndexer indexer(options);

    ::ArrowSchema writer_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &writer_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexWriter> writer,
                         indexer.CreateWriter(&writer_schema, pool));
    ::ArrowArray c_batch;
    ASSERT_TRUE(arrow::ExportArray(*batch, &c_batch).ok());
    ASSERT_OK(writer->AddBatch(&c_batch));
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> artifact, writer->SerializedBytes());
    ASSERT_FALSE(artifact);
}

TEST(LuminaFileIndexTest, RoundTripWithTagPredicate) {
    const std::map<std::string, std::string> options = {
        {"index.dimension", "4"},
        {"index.type", "bruteforce"},
        {"distance.metric", "l2"},
        {"encoding.type", "rawf32"},
        {"extension.build.tag.tag_schema",
         R"([{"key_name":"color","type":"enum","value_type":"string"}])"},
    };
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<arrow::StructArray> batch = CreateTaggedVectors();
    std::shared_ptr<arrow::Schema> writer_schema = arrow::schema(batch->type()->fields());
    std::shared_ptr<arrow::Schema> reader_schema =
        arrow::schema({arrow::field("embedding", arrow::list(arrow::float32()))});
    LuminaFileIndexer indexer(options);

    ASSERT_OK_AND_ASSIGN(std::optional<std::vector<std::string>> extra_field_names,
                         indexer.GetExtraFieldNames());
    ASSERT_TRUE(extra_field_names);
    EXPECT_EQ(std::vector<std::string>({"color"}), extra_field_names.value());

    ::ArrowSchema c_writer_schema;
    ASSERT_TRUE(arrow::ExportSchema(*writer_schema, &c_writer_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexWriter> writer,
                         indexer.CreateWriter(&c_writer_schema, pool));
    ::ArrowArray c_batch;
    ASSERT_TRUE(arrow::ExportArray(*batch, &c_batch).ok());
    ASSERT_OK(writer->AddBatch(&c_batch));
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> artifact, writer->SerializedBytes());
    ASSERT_TRUE(artifact);

    std::shared_ptr<InputStream> input = std::make_shared<ByteArrayInputStream>(
        artifact->data(), static_cast<int64_t>(artifact->size()));
    ::ArrowSchema c_reader_schema;
    ASSERT_TRUE(arrow::ExportSchema(*reader_schema, &c_reader_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexReader> reader,
                         indexer.CreateReader(&c_reader_schema, /*start=*/0,
                                              static_cast<int32_t>(artifact->size()), input, pool));

    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
        Literal(FieldType::STRING, "red", 3));
    std::shared_ptr<VectorSearch> search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/1, std::vector<float>{1.0f, 1.0f, 1.0f, 1.0f},
        /*pre_filter=*/nullptr, predicate, VectorSearch::DistanceType::EUCLIDEAN,
        std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         reader->VisitVectorSearch(search));
    EXPECT_EQ(RoaringBitmap32::From({3}), result->GetRowPositions());
    ASSERT_EQ(1, result->GetScores().size());
    EXPECT_NEAR(0.01f, result->GetScores()[0], 1e-5f);

    std::shared_ptr<VectorSearch> filtered_search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/1, std::vector<float>{1.0f, 1.0f, 1.0f, 1.0f},
        [](int64_t file_row) { return file_row == 0; }, predicate,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(filtered_search));
    EXPECT_EQ(RoaringBitmap32::From({0}), result->GetRowPositions());
    ASSERT_EQ(1, result->GetScores().size());
    EXPECT_FLOAT_EQ(4.0f, result->GetScores()[0]);
}

}  // namespace paimon::lumina::test
