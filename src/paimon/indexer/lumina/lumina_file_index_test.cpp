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

#include <atomic>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/file_index_evaluator.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/offset_output_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/testing/mock/mock_file_format_factory.h"
#include "paimon/testing/utils/counting_cache_test_utils.h"
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
                                              static_cast<int64_t>(artifact->size()), input, pool));

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

TEST(LuminaFileIndexTest, WriteToUsesRelativeOffsetsAndKeepsStreamOpen) {
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

    // Write after a prefix so the payload starts at a nonzero stream position.
    // The caller supplies a view so Lumina sees positions relative to the payload start.
    auto output =
        std::make_shared<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
            MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool));
    ASSERT_OK_AND_ASSIGN(int64_t prefix_length, output->Write("prefix", 6));
    ASSERT_EQ(6, prefix_length);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OffsetOutputStream> payload_output,
                         OffsetOutputStream::Create(output.get()));
    ASSERT_OK_AND_ASSIGN(bool written, writer->WriteTo(payload_output.get()));
    ASSERT_TRUE(written);
    ASSERT_OK_AND_ASSIGN(int64_t end, output->GetPos());
    int64_t length = end - prefix_length;
    ASSERT_GT(length, 0);

    // Appending a suffix verifies that WriteTo() leaves the caller's stream open.
    // Check that the surrounding bytes remain intact when retrieving the container.
    ASSERT_OK_AND_ASSIGN(int64_t suffix_length, output->Write("suffix", 6));
    ASSERT_EQ(6, suffix_length);
    ASSERT_OK_AND_ASSIGN(PAIMON_UNIQUE_PTR<Bytes> container, output->Finish(pool.get()));
    ASSERT_EQ("prefix", std::string(container->data(), prefix_length));
    ASSERT_EQ("suffix", std::string(container->data() + end, suffix_length));

    // Reopen only the payload range, excluding the prefix and suffix.
    // This checks that the reader can locate the index inside a larger stream.
    auto input = std::make_shared<ByteArrayInputStream>(container->data(), container->size());
    ::ArrowSchema reader_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &reader_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexReader> reader,
                         indexer.CreateReader(&reader_schema, prefix_length, length, input, pool));

    // Query the reopened index to exercise the offsets stored inside the payload.
    // The exact matching vector must still resolve to file row 2.
    auto search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/1, std::vector<float>{1.0f, 1.0f, 1.0f, 1.0f},
        /*pre_filter=*/nullptr, /*predicate=*/nullptr, VectorSearch::DistanceType::EUCLIDEAN,
        std::map<std::string, std::string>{});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         reader->VisitVectorSearch(search));
    ASSERT_EQ(RoaringBitmap32::From({2}), result->GetRowPositions());
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
    {
        // WriteTo() reports no index and leaves the output empty.
        ::ArrowSchema writer_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &writer_schema).ok());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexWriter> writer,
                             indexer.CreateWriter(&writer_schema, pool));
        ::ArrowArray c_batch;
        ASSERT_TRUE(arrow::ExportArray(*batch, &c_batch).ok());
        ASSERT_OK(writer->AddBatch(&c_batch));
        auto output =
            std::make_shared<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
                MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool));
        ASSERT_OK_AND_ASSIGN(bool written, writer->WriteTo(output.get()));
        ASSERT_FALSE(written);
        ASSERT_OK_AND_ASSIGN(int64_t position, output->GetPos());
        ASSERT_EQ(0, position);
    }
    {
        // SerializedBytes() reports no index by returning a null artifact.
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
    search->pool = GetMemoryPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result,
                         reader->VisitVectorSearch(search));
    EXPECT_EQ(RoaringBitmap32::From({3}), result->GetRowPositions());
    ASSERT_EQ(1, result->GetScores().size());
    EXPECT_NEAR(0.01f, result->GetScores()[0], 1e-5f);

    std::shared_ptr<VectorSearch> filtered_search = std::make_shared<VectorSearch>(
        "embedding", /*limit=*/1, std::vector<float>{1.0f, 1.0f, 1.0f, 1.0f},
        [](int64_t file_row) { return file_row == 0; }, predicate,
        VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
    filtered_search->pool = search->pool;
    ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(filtered_search));
    ASSERT_EQ(0, search->pool->CurrentUsage());
    ASSERT_GT(search->pool->MaxMemoryUsage(), 0);
    EXPECT_EQ(RoaringBitmap32::From({0}), result->GetRowPositions());
    ASSERT_EQ(1, result->GetScores().size());
    EXPECT_FLOAT_EQ(4.0f, result->GetScores()[0]);
}

namespace {

class LuminaFileIndexCacheTest : public ::testing::Test {
 protected:
    void SetUp() override {
        dir_ = paimon::test::UniqueTestDirectory::Create("local");
        cache_ = std::make_shared<paimon::test::CountingRoutingCache>(CacheKind::INDEX_READER,
                                                                      16 * 1024 * 1024);
        path_factory_ = std::make_shared<DataFilePathFactory>();
        ASSERT_OK(path_factory_->Init(dir_->Str(), /*format_identifier=*/"orc",
                                      /*data_file_prefix=*/"data-", nullptr));
        const std::map<std::string, std::string> index_options = {
            {"index.dimension", "4"},
            {"index.type", "bruteforce"},
            {"distance.metric", "l2"},
            {"encoding.type", "rawf32"},
        };
        for (const auto& [key, value] : index_options) {
            options_["file-index.lumina.embedding." + key] = value;
        }
        options_["file-index.lumina.columns"] = "embedding";
        LuminaFileIndexer indexer(index_options);
        ::ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema_, &c_schema).ok());
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexWriter> writer,
                             indexer.CreateWriter(&c_schema, pool_));
        ::ArrowArray c_batch;
        ASSERT_TRUE(arrow::ExportArray(*CreateVectors(), &c_batch).ok());
        ASSERT_OK(writer->AddBatch(&c_batch));
        auto output =
            std::make_shared<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
                MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileIndexFormat::Writer> format_writer,
                             FileIndexFormat::CreateWriter(output, FileIndexFormat::V_2, pool_));
        ASSERT_OK(format_writer->WriteIndex("embedding", "lumina", [&](OutputStream* payload) {
            return writer->WriteTo(payload);
        }));
        ASSERT_OK(format_writer->Finish());
        ASSERT_OK_AND_ASSIGN(container_, output->Finish(pool_.get()));
        ASSERT_OK_AND_ASSIGN(meta_,
                             DataFileMeta::ForAppend(
                                 "data-vectors.orc", /*file_size=*/0, /*row_count=*/3,
                                 SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
                                 /*max_sequence_number=*/2, /*schema_id=*/0, FileSource::Append(),
                                 /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
                                 /*first_row_id=*/std::nullopt, /*write_cols=*/std::nullopt));
        ASSERT_OK(WriteIndex("vectors.index"));
    }

    Status WriteIndex(const std::string& name) {
        meta_->extra_files = {name};
        return dir_->GetFileSystem()->WriteFile(path_factory_->ToAlignedPath(name, meta_),
                                                std::string(container_->data(), container_->size()),
                                                /*overwrite=*/false);
    }

    Result<std::shared_ptr<ScoredFileIndexResult>> Search(
        const std::shared_ptr<VectorSearch>& query) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexEvaluator> evaluator,
                               FileIndexEvaluator::Create(schema_, options_, path_factory_, meta_,
                                                          dir_->GetFileSystem(), cache_, pool_));
        return evaluator->SearchVector(query);
    }

    Result<std::shared_ptr<FileIndexReader>> OpenReader(
        const std::shared_ptr<MemoryPool>& reader_pool) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<InputStream> input,
            dir_->GetFileSystem()->Open(path_factory_->ToAlignedPath("vectors.index", meta_)));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexFormat::Reader> format_reader,
                               FileIndexFormat::CreateReader(input, reader_pool, options_));
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema_, &c_schema));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<FileIndexReader>> readers,
                               format_reader->ReadColumnIndex("embedding", &c_schema));
        if (readers.size() != 1) {
            return Status::Invalid("Expected one Lumina reader");
        }
        return readers.front();
    }

    Result<std::shared_ptr<CacheKey>> ReaderKey(const std::string& index_type) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<InputStream> input,
            dir_->GetFileSystem()->Open(path_factory_->ToAlignedPath("vectors.index", meta_)));
        PAIMON_ASSIGN_OR_RAISE(std::string uri, input->GetUri());
        return CacheKey::ForIndexReader(uri, "embedding", index_type);
    }

    Status PopulateCache() {
        std::shared_ptr<MemoryPool> reader_pool = GetMemoryPool();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexReader> reader, OpenReader(reader_pool));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<CacheKey> key, ReaderKey("lumina"));
        return cache_->Put(key, std::make_shared<ObjectCacheValue<FileIndexReader>>(
                                    std::move(reader), nullptr, reader_pool->CurrentUsage()));
    }

    static std::shared_ptr<VectorSearch> Query(bool filter) {
        auto query = std::make_shared<VectorSearch>(
            "embedding", 1, std::vector<float>{1.0f, 1.0f, 1.0f, 1.0f}, nullptr, nullptr,
            VectorSearch::DistanceType::EUCLIDEAN, std::map<std::string, std::string>{});
        if (filter) {
            query->pre_filter = [](int64_t row) { return row == 0; };
        }
        return query;
    }

    static void CheckResult(const std::shared_ptr<ScoredFileIndexResult>& result, bool filter) {
        ASSERT_TRUE(result);
        ASSERT_EQ(RoaringBitmap32::From({filter ? 0 : 2}), result->GetRowPositions());
        ASSERT_EQ(std::vector<float>({filter ? 4.0f : 0.0f}), result->GetScores());
    }

    std::unique_ptr<paimon::test::UniqueTestDirectory> dir_;
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<paimon::test::CountingRoutingCache> cache_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::shared_ptr<DataFileMeta> meta_;
    std::shared_ptr<Bytes> container_;
    std::shared_ptr<arrow::Schema> schema_ =
        arrow::schema({arrow::field("embedding", arrow::list(arrow::float32()))});
    std::map<std::string, std::string> options_ = {
        {"file.format", paimon::test::MockFileFormatFactory::IDENTIFIER},
        {"manifest.format", paimon::test::MockFileFormatFactory::IDENTIFIER}};
};

TEST_F(LuminaFileIndexCacheTest, ReusesExternalReaderWithConcurrentQueryPools) {
    ASSERT_OK(PopulateCache());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredFileIndexResult> result, Search(Query(false)));
    CheckResult(result, false);
    ASSERT_EQ(0, cache_->SupplierCallCount());

    std::promise<void> start;
    std::shared_future<void> ready = start.get_future().share();
    std::vector<std::future<Status>> searches;
    constexpr int32_t kThreads = 8;
    constexpr int32_t kQueriesPerThread = 32;
    for (int32_t i = 0; i < kThreads; ++i) {
        searches.push_back(std::async(std::launch::async, [&, i, ready] {
            std::shared_ptr<MemoryPool> query_pool = GetMemoryPool();
            ready.wait();
            for (int32_t j = 0; j < kQueriesPerThread; ++j) {
                bool filter = j % 2 == 0;
                auto query = Query(filter);
                if (i % 2 == 0) {
                    query->pool = query_pool;
                }
                query->options["lumina.search.parallel_number"] = "2";
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredFileIndexResult> found, Search(query));
                if (!(found->GetRowPositions() == RoaringBitmap32::From({filter ? 0 : 2})) ||
                    found->GetScores() != std::vector<float>({filter ? 4.0f : 0.0f}) ||
                    query_pool->CurrentUsage() != 0) {
                    return Status::Invalid(
                        "Concurrent query returned wrong results or retained memory");
                }
            }
            if ((query_pool->MaxMemoryUsage() > 0) != (i % 2 == 0)) {
                return Status::Invalid("Search did not use the requested pool");
            }
            return Status::OK();
        }));
    }
    start.set_value();
    for (auto& search : searches) {
        ASSERT_OK(search.get());
    }
    ASSERT_EQ(0, cache_->SupplierCallCount());
    ASSERT_EQ(1 + kThreads * kQueriesPerThread, cache_->GetCount(CacheKind::INDEX_READER));
    ASSERT_EQ(1, cache_->Size());
}

TEST_F(LuminaFileIndexCacheTest, SearchUsesRequestedPoolAndFallsBackToReaderPool) {
    std::shared_ptr<MemoryPool> reader_pool = GetMemoryPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileIndexReader> reader, OpenReader(reader_pool));
    uint64_t retained = reader_pool->CurrentUsage();
    ASSERT_GT(retained, 0);
    uint64_t reader_peak = reader_pool->MaxMemoryUsage();
    for (bool filter : {false, true}) {
        auto query = Query(filter);
        query->pool = GetMemoryPool();
        query->options["lumina.search.parallel_number"] = "2";
        auto copied = query->ReplacePreFilter(query->pre_filter);
        ASSERT_EQ(query->pool, copied->pool);
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitVectorSearch(copied));
        CheckResult(result, filter);
        ASSERT_GT(query->pool->MaxMemoryUsage(), 0);
        ASSERT_EQ(query->pool->CurrentUsage(), 0);
        ASSERT_EQ(reader_pool->CurrentUsage(), retained);
        ASSERT_EQ(reader_pool->MaxMemoryUsage(), reader_peak);

        copied->query.clear();
        ASSERT_NOK(reader->VisitVectorSearch(copied));
        ASSERT_EQ(query->pool->CurrentUsage(), 0);
        copied->query = query->query;
        copied->predicate = PredicateBuilder::Equal(0, "tag", FieldType::INT, Literal(1));
        ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(copied),
                            "lumina index was not built with tag");
        ASSERT_EQ(query->pool->CurrentUsage(), 0);
        ASSERT_OK_AND_ASSIGN(result, reader->VisitVectorSearch(query));
        CheckResult(result, filter);
        ASSERT_EQ(query->pool->CurrentUsage(), 0);
    }

    // No query pool: temporary search allocations belong to the reader's pool.
    std::atomic<bool> allocated_from_reader{false};
    auto fallback = Query(true);
    fallback->pre_filter = [&](int64_t row) {
        if (reader_pool->CurrentUsage() > retained) {
            allocated_from_reader.store(true);
        }
        return row == 0;
    };
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitVectorSearch(fallback));
    CheckResult(result, true);
    ASSERT_TRUE(allocated_from_reader.load());
    ASSERT_EQ(reader_pool->CurrentUsage(), retained);
    auto invalid = Query(false);
    invalid->query.clear();
    ASSERT_NOK(reader->VisitVectorSearch(invalid));
    ASSERT_EQ(reader_pool->CurrentUsage(), retained);
    reader.reset();
    ASSERT_EQ(reader_pool->CurrentUsage(), 0);
}

TEST_F(LuminaFileIndexCacheTest, CacheMissDoesNotPopulateCache) {
    pool_ = GetMemoryPool();
    for (int32_t i = 0; i < 2; ++i) {
        auto query = Query(i != 0);
        query->pool = GetMemoryPool();
        ASSERT_OK_AND_ASSIGN(auto result, Search(query));
        CheckResult(result, i != 0);
        ASSERT_EQ(0, pool_->CurrentUsage());
        ASSERT_EQ(0, query->pool->CurrentUsage());
        ASSERT_GT(query->pool->MaxMemoryUsage(), 0);
        ASSERT_EQ(0, cache_->Size());
    }
    ASSERT_EQ(2, cache_->SupplierCallCount());
}

TEST_F(LuminaFileIndexCacheTest, QueryErrorsAndInFlightEvictionPreserveReaderLifetime) {
    std::shared_ptr<MemoryPool> reader_pool = GetMemoryPool();
    ASSERT_OK_AND_ASSIGN(auto reader, OpenReader(reader_pool));
    std::weak_ptr<FileIndexReader> weak_reader = reader;
    std::weak_ptr<MemoryPool> weak_pool = reader_pool;
    ASSERT_OK_AND_ASSIGN(auto key, ReaderKey("lumina"));
    ASSERT_OK(cache_->Put(key, std::make_shared<ObjectCacheValue<FileIndexReader>>(
                                   reader, nullptr, reader_pool->CurrentUsage())));
    reader.reset();
    reader_pool.reset();
    auto invalid = Query(false);
    invalid->pool = GetMemoryPool();
    invalid->query.clear();
    ASSERT_NOK(Search(invalid));
    ASSERT_EQ(0, invalid->pool->CurrentUsage());
    ASSERT_EQ(1, cache_->Size());

    auto query = Query(true);
    query->pool = GetMemoryPool();
    std::atomic<bool> alive{true};
    query->pre_filter = [&](int64_t row) {
        cache_->InvalidateAll();
        if (weak_reader.expired() || weak_pool.expired()) {
            alive.store(false);
        }
        return row == 0;
    };
    ASSERT_OK_AND_ASSIGN(auto result, Search(query));
    CheckResult(result, true);
    ASSERT_TRUE(alive.load());
    ASSERT_TRUE(weak_reader.expired());
    ASSERT_TRUE(weak_pool.expired());
    ASSERT_EQ(0, query->pool->CurrentUsage());
    ASSERT_EQ(0, cache_->Size());
    ASSERT_EQ(0, cache_->SupplierCallCount());
    ASSERT_OK_AND_ASSIGN(result, Search(Query(false)));
    CheckResult(result, false);
    ASSERT_EQ(1, cache_->SupplierCallCount());
    ASSERT_EQ(0, cache_->Size());
}

TEST_F(LuminaFileIndexCacheTest, IgnoresOtherIndexTypeCacheEntries) {
    ASSERT_OK_AND_ASSIGN(auto key, ReaderKey("bitmap"));
    ASSERT_OK(cache_->Put(key, std::make_shared<ObjectCacheValue<int32_t>>(
                                   std::make_shared<int32_t>(7), nullptr, sizeof(int32_t))));
    for (int32_t i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(auto result, Search(Query(false)));
        CheckResult(result, false);
    }
    ASSERT_EQ(2, cache_->SupplierCallCount());
    ASSERT_EQ(1, cache_->Size());
}

TEST_F(LuminaFileIndexCacheTest, SeparatesFilesAndDoesNotRetainMisses) {
    ASSERT_OK(PopulateCache());
    ASSERT_OK_AND_ASSIGN(auto result, Search(Query(false)));
    CheckResult(result, false);
    ASSERT_EQ(0, cache_->SupplierCallCount());
    ASSERT_OK(WriteIndex("other.index"));
    for (int32_t i = 0; i < 2; ++i) {
        ASSERT_OK_AND_ASSIGN(result, Search(Query(false)));
        CheckResult(result, false);
    }
    ASSERT_EQ(2, cache_->SupplierCallCount());
    ASSERT_EQ(1, cache_->Size());
}

TEST_F(LuminaFileIndexCacheTest, BypassesEmbeddedIndexesAndDisabledCache) {
    meta_->embedded_index = container_;
    ASSERT_OK_AND_ASSIGN(auto result, Search(Query(false)));
    CheckResult(result, false);
    ASSERT_OK_AND_ASSIGN(result, Search(Query(true)));
    CheckResult(result, true);
    ASSERT_EQ(0, cache_->GetCount());
    ASSERT_EQ(0, cache_->Size());

    meta_->embedded_index.reset();
    cache_.reset();
    ASSERT_OK_AND_ASSIGN(result, Search(Query(false)));
    CheckResult(result, false);
}

}  // namespace

}  // namespace paimon::lumina::test
