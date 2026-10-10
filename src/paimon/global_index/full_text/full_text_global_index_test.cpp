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

#include "paimon/global_index/full_text/full_text_global_index.h"

#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_indexer_factory.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/literal.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "rapidjson/document.h"

namespace paimon::full_text::test {

namespace {

struct GoldenSearch {
    std::string query;
    int32_t limit;
    std::optional<std::vector<int64_t>> include_row_ids;
    std::vector<std::pair<int64_t, float>> hits;
};

struct GoldenFixture {
    std::string name;
    std::map<std::string, std::string> options;
    std::string rows;
    std::vector<GoldenSearch> searches;
};

const std::vector<GoldenFixture>& GoldenFixtures() {
    static const std::vector<GoldenFixture> kFixtures = {
        {"default",
         {},
         R"([["Apache Paimon is a streaming data lake platform"], [null],
             ["Paimon supports real-time data ingestion and running queries"],
             ["The runner runs quickly"], ["Lake formats store huge tables"]])",
         {
             {R"({"match":{"query":"paimon"}})",
              10,
              std::nullopt,
              {{0, 0.668293297f}, {2, 0.584465563f}}},
             {R"({"match":{"query":"run"}})",
              10,
              std::nullopt,
              {{2, 0.584465563f}, {3, 0.851480305f}}},
             {R"({"match":{"query":"paimon lake","operator":"And"}})",
              10,
              std::nullopt,
              {{0, 1.33658659f}}},
             {R"({"match":{"query":"paimon lake"}})", 1, std::nullopt, {{0, 1.33658659f}}},
             {R"({"match_phrase":{"query":"data lake"}})", 10, std::nullopt, {{0, 1.33658659f}}},
             {R"({"boolean":{"must":[{"match":{"query":"paimon"}}],"must_not":[{"match":{"query":"streaming"}}]}})",
              10,
              std::nullopt,
              {{2, 0.584465563f}}},
             {R"({"match":{"query":"lake"}})",
              10,
              std::vector<int64_t>({0, 3}),
              {{0, 0.668293297f}}},
         }},
        {"ngram",
         {{"full-text.tokenizer", "ngram"},
          {"full-text.ngram.min-gram", "2"},
          {"full-text.ngram.max-gram", "3"}},
         R"([["paimon"], ["lakehouse"], [null], ["streaming"]])",
         {
             {R"({"match":{"query":"aim"}})", 10, std::nullopt, {{0, 3.43641996f}}},
             {R"({"match":{"query":"house"}})", 10, std::nullopt, {{1, 6.30786085f}}},
         }},
        {"jieba",
         {{"full-text.tokenizer", "jieba"}},
         R"([["张华在百货公司当售货员"], ["Apache Paimon supports full text search"], [null],
             ["我们在数据湖中存储数据"]])",
         {
             {R"({"match":{"query":"售货员"}})", 10, std::nullopt, {{0, 2.89690685f}}},
             {R"({"match":{"query":"数据湖"}})", 10, std::nullopt, {{3, 1.4764061f}}},
             {R"({"match":{"query":"paimon"}})", 10, std::nullopt, {{1, 0.883518577f}}},
         }},
    };
    return kFixtures;
}

class FlushFailingFileWriter : public GlobalIndexFileWriter {
 public:
    FlushFailingFileWriter(const std::shared_ptr<GlobalIndexFileWriter>& delegate,
                           int32_t fail_from)
        : delegate_(delegate), fail_from_(fail_from) {}

    Result<std::string> NewFileName(const std::string& prefix) const override {
        return delegate_->NewFileName(prefix);
    }

    Result<std::unique_ptr<OutputStream>> NewOutputStream(
        const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> stream,
                               delegate_->NewOutputStream(file_name));
        return std::make_unique<FlushFailingOutputStream>(std::move(stream), fail_from_,
                                                          close_count_);
    }

    Result<int64_t> GetFileSize(const std::string& file_name) const override {
        return delegate_->GetFileSize(file_name);
    }

    std::string ToPath(const std::string& file_name) const override {
        return delegate_->ToPath(file_name);
    }

    int32_t CloseCount() const {
        return *close_count_;
    }

 private:
    class FlushFailingOutputStream : public OutputStream {
     public:
        FlushFailingOutputStream(std::unique_ptr<OutputStream> delegate, int32_t fail_from,
                                 const std::shared_ptr<int32_t>& close_count)
            : delegate_(std::move(delegate)), fail_from_(fail_from), close_count_(close_count) {}

        Result<int64_t> Write(const char* buffer, int64_t size) override {
            return delegate_->Write(buffer, size);
        }
        Status Flush() override {
            if (flush_count_++ >= fail_from_) {
                return Status::IOError("injected flush failure");
            }
            return delegate_->Flush();
        }
        Result<int64_t> GetPos() const override {
            return delegate_->GetPos();
        }
        Result<std::string> GetUri() const override {
            return delegate_->GetUri();
        }
        Status Close() override {
            ++*close_count_;
            return delegate_->Close();
        }

     private:
        std::unique_ptr<OutputStream> delegate_;
        int32_t fail_from_;
        int32_t flush_count_ = 0;
        std::shared_ptr<int32_t> close_count_;
    };

    std::shared_ptr<GlobalIndexFileWriter> delegate_;
    int32_t fail_from_;
    std::shared_ptr<int32_t> close_count_ = std::make_shared<int32_t>(0);
};

}  // namespace

class FullTextGlobalIndexTest : public ::testing::Test {
 public:
    void SetUp() override {
        index_dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(index_dir_);
    }

    static std::string FixturePath(const std::string& name) {
        return paimon::test::GetDataDir() + "/full_text_fixtures/" + name + ".archive";
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    std::shared_ptr<GlobalIndexFileManager> CreateFileManager() const {
        return std::make_shared<GlobalIndexFileManager>(
            fs_, std::make_shared<paimon::test::MockIndexPathFactory>(index_dir_->Str()),
            /*checkpoint_path_factory=*/nullptr);
    }

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::map<std::string, std::string>& options) const {
        FullTextGlobalIndex global_index(options);
        return global_index.CreateWriter("f0", CreateArrowSchema(data_type_).get(),
                                         CreateFileManager(), pool_);
    }

    std::shared_ptr<arrow::Array> MakeArray(const std::string& json) const {
        return arrow::ipc::internal::json::ArrayFromJSON(data_type_, json).ValueOrDie();
    }

    Status AddBatch(GlobalIndexWriter* writer, const std::shared_ptr<arrow::Array>& array,
                    int64_t first_row_id) const {
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        std::vector<int64_t> row_ids(array->length());
        std::iota(row_ids.begin(), row_ids.end(), first_row_id);
        return writer->AddBatch(&c_array, std::move(row_ids));
    }

    Result<std::vector<GlobalIndexIOMeta>> WriteIndex(
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<arrow::Array>& array) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> writer, CreateWriter(options));
        PAIMON_RETURN_NOT_OK(AddBatch(writer.get(), array, /*first_row_id=*/0));
        return writer->Finish();
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        const std::vector<GlobalIndexIOMeta>& metas,
        const std::shared_ptr<arrow::DataType>& field_type = arrow::utf8()) const {
        FullTextGlobalIndex global_index(/*options=*/{});
        auto schema = arrow::schema({arrow::field("f0", field_type)});
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return global_index.CreateReader(c_schema.get(), CreateFileManager(), metas, pool_);
    }

    static std::shared_ptr<FullTextSearch> MakeSearch(
        const std::string& query, int32_t limit = 10,
        const std::optional<RoaringBitmap64>& include_row_ids = std::nullopt) {
        return std::make_shared<FullTextSearch>("f0", query, limit, include_row_ids);
    }

    static std::string MatchQuery(const std::string& terms) {
        return R"({"match":{"query":")" + terms + R"("}})";
    }

    static std::vector<std::pair<int64_t, float>> ToHits(
        const std::shared_ptr<GlobalIndexResult>& result) {
        auto scored_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result);
        EXPECT_TRUE(scored_result);
        std::vector<std::pair<int64_t, float>> hits;
        if (!scored_result) {
            return hits;
        }
        auto iter_result = scored_result->CreateScoredIterator();
        EXPECT_TRUE(iter_result.ok()) << iter_result.status().ToString();
        if (!iter_result.ok()) {
            return hits;
        }
        auto iter = std::move(iter_result).value();
        while (iter->HasNext()) {
            hits.push_back(iter->NextWithScore());
        }
        return hits;
    }

    static void CheckRowIds(const std::shared_ptr<GlobalIndexResult>& result,
                            const std::vector<int64_t>& expected_ids) {
        ASSERT_TRUE(result);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexResult::Iterator> iter,
                             result->CreateIterator());
        std::vector<int64_t> row_ids;
        while (iter->HasNext()) {
            row_ids.push_back(iter->Next());
        }
        ASSERT_EQ(row_ids, expected_ids) << result->ToString();
    }

    void CheckGoldenSearches(const GoldenFixture& fixture, const GlobalIndexIOMeta& meta) const {
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader({meta}));
        for (const auto& search : fixture.searches) {
            std::optional<RoaringBitmap64> include_row_ids;
            if (search.include_row_ids) {
                include_row_ids = RoaringBitmap64::From(search.include_row_ids.value());
            }
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> result,
                                 reader->VisitFullTextSearch(
                                     MakeSearch(search.query, search.limit, include_row_ids)));
            auto hits = ToHits(result);
            ASSERT_EQ(hits.size(), search.hits.size())
                << fixture.name << ": " << search.query << ": " << result->ToString();
            for (size_t i = 0; i < hits.size(); ++i) {
                ASSERT_EQ(hits[i].first, search.hits[i].first)
                    << fixture.name << ": " << search.query << ": " << result->ToString();
                ASSERT_NEAR(hits[i].second, search.hits[i].second, 1e-5)
                    << fixture.name << ": " << search.query << ": row " << hits[i].first;
            }
        }
    }

    Result<std::unique_ptr<rapidjson::Document>> ReadHeader(const std::string& path) const {
        std::string content;
        PAIMON_RETURN_NOT_OK(fs_->ReadFile(path, &content));
        if (content.size() < 16 || content.compare(0, 8, "PFTIDX01") != 0) {
            return Status::Invalid("missing PFTIDX01 magic in ", path);
        }
        auto read_be_u32 = [&content](size_t pos) {
            uint32_t value = 0;
            for (size_t i = 0; i < 4; ++i) {
                value = (value << 8) | static_cast<uint8_t>(content[pos + i]);
            }
            return value;
        };
        if (read_be_u32(8) != 1) {
            return Status::Invalid("unexpected format version in ", path);
        }
        uint32_t header_len = read_be_u32(12);
        if (content.size() < 16 + static_cast<size_t>(header_len)) {
            return Status::Invalid("truncated header in ", path);
        }
        auto header = std::make_unique<rapidjson::Document>();
        header->Parse(content.data() + 16, header_len);
        if (header->HasParseError() || !header->IsObject() || !header->HasMember("metadata") ||
            !header->HasMember("files")) {
            return Status::Invalid("invalid header in ", path);
        }
        return header;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<arrow::DataType> data_type_ =
        arrow::struct_({arrow::field("f0", arrow::utf8())});
    std::unique_ptr<paimon::test::UniqueTestDirectory> index_dir_;
};

TEST_F(FullTextGlobalIndexTest, TestWriteAndSearch) {
    std::map<std::string, std::string> options = {{"full-text.tokenizer", "default"},
                                                  {"full-text.with-position", "true"},
                                                  {"tokenizer", "ignored"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<GlobalIndexer> indexer,
                         GlobalIndexerFactory::Get("full-text", options));
    ASSERT_TRUE(dynamic_cast<FullTextGlobalIndex*>(indexer.get()));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexWriter> writer,
                         indexer->CreateWriter("f0", CreateArrowSchema(data_type_).get(),
                                               CreateFileManager(), pool_));
    auto array = MakeArray(R"([
        ["Apache Paimon is a streaming data lake platform"],
        [null],
        ["Lake formats store huge tables"]
    ])");
    ASSERT_OK(AddBatch(writer.get(), array, /*first_row_id=*/0));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1u);
    const GlobalIndexIOMeta& meta = metas[0];

    std::string file_name = PathUtil::GetName(meta.file_path);
    ASSERT_TRUE(StringUtils::StartsWith(file_name, "full-text-global-index-")) << file_name;
    ASSERT_TRUE(StringUtils::EndsWith(file_name, ".index")) << file_name;
    ASSERT_OK_AND_ASSIGN(FileStatus file_status, fs_->GetFileStatus(meta.file_path));
    ASSERT_EQ(file_status.GetLen(), meta.file_size);
    ASSERT_TRUE(meta.metadata);
    ASSERT_EQ(std::string(meta.metadata->data(), meta.metadata->size()),
              R"({"tokenizer":"default","with-position":"true"})");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader(metas));
    ASSERT_EQ(reader->GetIndexType(), "full-text");
    ASSERT_TRUE(reader->IsThreadSafe());
    ASSERT_OK_AND_ASSIGN(auto result,
                         reader->VisitFullTextSearch(MakeSearch(MatchQuery("THE LAKE"))));
    CheckRowIds(result, {0, 2});
    ASSERT_OK_AND_ASSIGN(
        result, reader->VisitFullTextSearch(
                    MakeSearch(R"({"multi_match":{"query":"paimon lake","columns":["text"]}})")));
    CheckRowIds(result, {0, 2});
}

TEST_F(FullTextGlobalIndexTest, TestWriterWithoutRows) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexWriter> writer, CreateWriter(/*options=*/{}));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, writer->Finish());
    ASSERT_TRUE(metas.empty());

    std::vector<BasicFileStatus> file_status_list;
    ASSERT_OK(fs_->ListDir(index_dir_->Str(), &file_status_list));
    ASSERT_TRUE(file_status_list.empty());

    ASSERT_NOK_WITH_MSG(writer->Finish(), "already finished");
    ASSERT_NOK_WITH_MSG(AddBatch(writer.get(), MakeArray(R"([["paimon"]])"), 0),
                        "already finished");
}

TEST_F(FullTextGlobalIndexTest, TestWriterWithOnlyNullRows) {
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas,
                         WriteIndex(/*options=*/{}, MakeArray(R"([[null], [null]])")));
    ASSERT_EQ(metas.size(), 1u);
    ASSERT_EQ(std::string(metas[0].metadata->data(), metas[0].metadata->size()), "{}");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader(metas));
    ASSERT_OK_AND_ASSIGN(auto result,
                         reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"))));
    CheckRowIds(result, {});
}

TEST_F(FullTextGlobalIndexTest, TestRelativeRowIds) {
    const int64_t large_row_id = (int64_t{1} << 32) + 1;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexWriter> writer, CreateWriter(/*options=*/{}));
    ASSERT_OK(AddBatch(writer.get(), MakeArray(R"([["paimon lake"], ["vector search"]])"),
                       /*first_row_id=*/0));
    ASSERT_OK(AddBatch(writer.get(), MakeArray(R"([[null], ["paimon lake"]])"),
                       /*first_row_id=*/2));
    ASSERT_OK(AddBatch(writer.get(), MakeArray(R"([["paimon paimon paimon"]])"),
                       /*first_row_id=*/0));
    ASSERT_OK(AddBatch(writer.get(), MakeArray(R"([["paimon lake"]])"), large_row_id));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, writer->Finish());
    ASSERT_EQ(metas.size(), 1u);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader(metas));
    ASSERT_OK_AND_ASSIGN(auto result,
                         reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"))));
    auto hits = ToHits(result);
    ASSERT_EQ(hits.size(), 3u) << result->ToString();
    ASSERT_EQ(hits[0].first, 0);
    ASSERT_EQ(hits[1].first, 3);
    ASSERT_EQ(hits[2].first, large_row_id);
    ASSERT_FLOAT_EQ(hits[0].second, hits[1].second);
    ASSERT_FLOAT_EQ(hits[1].second, hits[2].second);

    ASSERT_OK_AND_ASSIGN(
        result, reader->VisitFullTextSearch(
                    MakeSearch(MatchQuery("paimon"), 10,
                               RoaringBitmap64::From(std::vector<int64_t>{1, large_row_id}))));
    CheckRowIds(result, {large_row_id});
}

TEST_F(FullTextGlobalIndexTest, TestInvalidWrite) {
    FullTextGlobalIndex global_index(/*options=*/{});
    auto int_type = arrow::struct_({arrow::field("f0", arrow::int32())});
    ASSERT_NOK_WITH_MSG(global_index.CreateWriter("f0", CreateArrowSchema(int_type).get(),
                                                  CreateFileManager(), pool_),
                        "only supports string fields");
    ASSERT_NOK_WITH_MSG(global_index.CreateWriter("f1", CreateArrowSchema(data_type_).get(),
                                                  CreateFileManager(), pool_),
                        "field f1 not exist");
    ASSERT_NOK_WITH_MSG(CreateWriter({{"full-text.tokenizer", "unknown"}}),
                        "open full-text index writer for field f0");
    ASSERT_NOK_WITH_MSG(CreateWriter({{"full-text.lower-case", "maybe"}}), "lower-case");
    ASSERT_NOK_WITH_MSG(CreateWriter({{"full-text.tokenizer", std::string("raw\0jieba", 9)}}),
                        "full-text index option full-text.tokenizer must not contain NUL");
    ASSERT_NOK_WITH_MSG(CreateWriter({{std::string("full-text.tokenizer\0x", 21), "raw"}}),
                        "option keys must not contain NUL");

    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append("paimon").ok());
    ASSERT_TRUE(builder.Append(std::string("pai\0mon", 7)).ok());
    std::shared_ptr<arrow::Array> strings;
    ASSERT_TRUE(builder.Finish(&strings).ok());
    std::shared_ptr<arrow::Array> array =
        arrow::StructArray::Make({strings}, {arrow::field("f0", arrow::utf8())}).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexWriter> writer, CreateWriter(/*options=*/{}));
    ASSERT_NOK_WITH_MSG(AddBatch(writer.get(), array, /*first_row_id=*/0),
                        "full-text index value of row 1 must not contain NUL characters");
}

TEST_F(FullTextGlobalIndexTest, TestIncludeRowIdsAndLimit) {
    auto array = MakeArray(R"([
        ["paimon paimon paimon paimon"],
        ["paimon paimon"],
        ["paimon"],
        ["lake"]
    ])");
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, WriteIndex(/*options=*/{}, array));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader(metas));

    ASSERT_OK_AND_ASSIGN(auto all_result,
                         reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"))));
    auto all_hits = ToHits(all_result);
    ASSERT_EQ(all_hits.size(), 3u);
    ASSERT_GT(all_hits[0].second, all_hits[1].second);
    ASSERT_GT(all_hits[1].second, all_hits[2].second);
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"), 1)));
        CheckRowIds(result, {0});
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto result,
            reader->VisitFullTextSearch(MakeSearch(
                MatchQuery("paimon"), 10, RoaringBitmap64::From(std::vector<int64_t>{1, 2, 3}))));
        CheckRowIds(result, {1, 2});
        auto hits = ToHits(result);
        ASSERT_EQ(hits.size(), 2u);
        ASSERT_FLOAT_EQ(hits[0].second, all_hits[1].second);
        ASSERT_FLOAT_EQ(hits[1].second, all_hits[2].second);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitFullTextSearch(MakeSearch(
                                              MatchQuery("paimon"), 1,
                                              RoaringBitmap64::From(std::vector<int64_t>{1, 2}))));
        CheckRowIds(result, {1});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, reader->VisitFullTextSearch(MakeSearch(
                                              MatchQuery("paimon"), 10, RoaringBitmap64())));
        CheckRowIds(result, {});
    }
}

TEST_F(FullTextGlobalIndexTest, TestInvalidRead) {
    GlobalIndexIOMeta meta("full-text-global-index-1.index", 1, nullptr);
    ASSERT_NOK_WITH_MSG(CreateReader({}), "exactly one index file");
    ASSERT_NOK_WITH_MSG(CreateReader({meta, meta}), "exactly one index file");
    ASSERT_NOK_WITH_MSG(CreateReader({meta}, arrow::int32()), "only supports string fields");

    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas,
                         WriteIndex(/*options=*/{}, MakeArray(R"([["paimon"]])")));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader(metas));

    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(nullptr), "null FullTextSearch");
    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"), 0)),
                        "positive limit");
    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"), -1)),
                        "positive limit");
    ASSERT_NOK_WITH_MSG(
        reader->VisitFullTextSearch(MakeSearch(MatchQuery(std::string("pai\0mon", 7)))),
        "must not contain NUL characters");
    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(MakeSearch("paimon")),
                        "search full-text index " + metas[0].file_path);
    ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(nullptr), "vector search");

    Literal literal(FieldType::STRING, "paimon", 6);
    ASSERT_OK_AND_ASSIGN(auto result, reader->VisitEqual(literal));
    ASSERT_FALSE(result);
    ASSERT_OK_AND_ASSIGN(result, reader->VisitContains(literal));
    ASSERT_FALSE(result);
    ASSERT_OK_AND_ASSIGN(result, reader->VisitLike(literal));
    ASSERT_FALSE(result);
    ASSERT_OK_AND_ASSIGN(result, reader->VisitIsNull());
    ASSERT_FALSE(result);
}

TEST_F(FullTextGlobalIndexTest, TestInvalidIndexFile) {
    std::string file_path = PathUtil::JoinPath(index_dir_->Str(), "invalid.index");
    ASSERT_OK(fs_->WriteFile(file_path, "this is not a full-text index", /*overwrite=*/false));
    std::vector<GlobalIndexIOMeta> metas = {GlobalIndexIOMeta(file_path, 29, nullptr)};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader(metas));
    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"))), "bad magic");

    std::string archive;
    ASSERT_OK(fs_->ReadFile(FixturePath("default"), &archive));
    std::string truncated_path = PathUtil::JoinPath(index_dir_->Str(), "truncated.index");
    ASSERT_OK(fs_->WriteFile(truncated_path, archive.substr(0, 32), /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(reader, CreateReader({GlobalIndexIOMeta(truncated_path, 32, nullptr)}));
    Status status = reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"))).status();
    ASSERT_TRUE(status.IsIOError()) << status.ToString();
    ASSERT_NOK_WITH_MSG(status, "failed to open full-text index " + truncated_path);

    std::string::size_type version_pos = archive.find("tantivy v0.26.1");
    ASSERT_NE(version_pos, std::string::npos);
    std::string other_version_archive = archive;
    other_version_archive.replace(version_pos, 15, "tantivy v0.25.0");
    std::string other_version_path = PathUtil::JoinPath(index_dir_->Str(), "other_version.index");
    ASSERT_OK(fs_->WriteFile(other_version_path, other_version_archive, /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(
        reader,
        CreateReader({GlobalIndexIOMeta(
            other_version_path, static_cast<int64_t>(other_version_archive.size()), nullptr)}));
    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon"))),
                        "unsupported Tantivy index version tantivy v0.25.0");
}

TEST_F(FullTextGlobalIndexTest, TestConcurrentSearch) {
    auto array = MakeArray(R"([
        ["Apache Paimon is a streaming data lake platform"],
        ["Paimon supports real-time data ingestion"],
        ["Native full-text search runs in Rust"]
    ])");
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas, WriteIndex(/*options=*/{}, array));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader, CreateReader(metas));
    std::shared_ptr<FullTextSearch> paimon_search =
        MakeSearch(MatchQuery("paimon"), 10, RoaringBitmap64::From(std::vector<int64_t>{1, 2}));
    std::shared_ptr<FullTextSearch> rust_search = MakeSearch(MatchQuery("rust"));

    constexpr int32_t kThreadNum = 8;
    constexpr int32_t kRoundNum = 10;
    std::vector<std::thread> threads;
    std::vector<Status> statuses(kThreadNum);
    std::vector<int32_t> finished_rounds(kThreadNum, 0);
    for (int32_t i = 0; i < kThreadNum; ++i) {
        threads.emplace_back([&, i]() {
            const std::shared_ptr<FullTextSearch>& search =
                i % 2 == 0 ? paimon_search : rust_search;
            std::vector<int64_t> expected_ids =
                i % 2 == 0 ? std::vector<int64_t>({1}) : std::vector<int64_t>({2});
            for (int32_t round = 0; round < kRoundNum; ++round) {
                Result<std::shared_ptr<ScoredGlobalIndexResult>> result =
                    reader->VisitFullTextSearch(search);
                if (!result.ok()) {
                    statuses[i] = result.status();
                    return;
                }
                Result<std::unique_ptr<GlobalIndexResult::Iterator>> iter =
                    result.value()->CreateIterator();
                if (!iter.ok()) {
                    statuses[i] = iter.status();
                    return;
                }
                std::vector<int64_t> row_ids;
                while (iter.value()->HasNext()) {
                    row_ids.push_back(iter.value()->Next());
                }
                if (row_ids != expected_ids) {
                    statuses[i] = Status::Invalid("unexpected result in round ", round, ": ",
                                                  result.value()->ToString());
                    return;
                }
                ++finished_rounds[i];
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (int32_t i = 0; i < kThreadNum; ++i) {
        ASSERT_OK(statuses[i]) << "thread " << i;
        ASSERT_EQ(finished_rounds[i], kRoundNum) << "thread " << i;
    }
}

TEST_F(FullTextGlobalIndexTest, TestIOException) {
    auto array = MakeArray(R"([["Apache Paimon"], [null], ["full-text search"]])");
    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 500; ++i) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        Result<std::vector<GlobalIndexIOMeta>> metas = WriteIndex(/*options=*/{}, array);
        CHECK_HOOK_STATUS(metas.status(), i);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader,
                             CreateReader(metas.value()));
        Result<std::shared_ptr<ScoredGlobalIndexResult>> result =
            reader->VisitFullTextSearch(MakeSearch(MatchQuery("paimon")));
        CHECK_HOOK_STATUS(result.status(), i);
        CheckRowIds(result.value(), {0});
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(FullTextGlobalIndexTest, TestFlushFailureClosesOutputStream) {
    FullTextGlobalIndex global_index(/*options=*/{});
    bool run_complete = false;
    for (int32_t fail_from = 0; fail_from < 10; ++fail_from) {
        auto file_writer = std::make_shared<FlushFailingFileWriter>(CreateFileManager(), fail_from);
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexWriter> writer,
                             global_index.CreateWriter("f0", CreateArrowSchema(data_type_).get(),
                                                       file_writer, pool_));
        ASSERT_OK(AddBatch(writer.get(), MakeArray(R"([["paimon"]])"), /*first_row_id=*/0));
        Result<std::vector<GlobalIndexIOMeta>> metas = writer->Finish();
        ASSERT_EQ(file_writer->CloseCount(), 1) << "fail_from " << fail_from;
        if (metas.ok()) {
            ASSERT_GT(fail_from, 0);
            run_complete = true;
            break;
        }
        ASSERT_NOK_WITH_MSG(metas.status(), "injected flush failure");
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(FullTextGlobalIndexTest, TestCrossReadFixtures) {
    const char* output_dir = std::getenv("PAIMON_FULL_TEXT_ARCHIVE_OUTPUT_DIR");
    for (const auto& fixture : GoldenFixtures()) {
        std::string fixture_path = FixturePath(fixture.name);
        ASSERT_OK_AND_ASSIGN(FileStatus file_status, fs_->GetFileStatus(fixture_path));
        CheckGoldenSearches(fixture, GlobalIndexIOMeta(fixture_path, file_status.GetLen(),
                                                       /*metadata=*/nullptr));

        ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> metas,
                             WriteIndex(fixture.options, MakeArray(fixture.rows)));
        ASSERT_EQ(metas.size(), 1u) << fixture.name;
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<rapidjson::Document> written_header,
                             ReadHeader(metas[0].file_path));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<rapidjson::Document> fixture_header,
                             ReadHeader(fixture_path));
        ASSERT_TRUE((*written_header)["metadata"] == (*fixture_header)["metadata"]) << fixture.name;
        ASSERT_TRUE((*written_header)["files"].IsArray()) << fixture.name;
        ASSERT_FALSE((*written_header)["files"].Empty()) << fixture.name;
        CheckGoldenSearches(fixture, metas[0]);

        if (output_dir != nullptr) {
            std::string archive;
            ASSERT_OK(fs_->ReadFile(metas[0].file_path, &archive));
            ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(output_dir, fixture.name + ".archive"),
                                     archive, /*overwrite=*/true));
        }
    }
}

}  // namespace paimon::full_text::test
