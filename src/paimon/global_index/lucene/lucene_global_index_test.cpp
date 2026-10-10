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
#include "paimon/global_index/lucene/lucene_global_index.h"

#include <cmath>

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/lucene/lucene_global_index_reader.h"
#include "paimon/global_index/lucene/lucene_global_index_writer.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::lucene::test {
class LuceneGlobalIndexTest : public ::testing::Test,
                              public ::testing::WithParamInterface<int32_t> {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeIndexPathFactory : public IndexPathFactory {
     public:
        explicit FakeIndexPathFactory(const std::string& index_path) : index_path_(index_path) {}
        std::string NewPath() const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::string& file_name) const override {
            return PathUtil::JoinPath(index_path_, file_name);
        }
        bool IsExternalPath() const override {
            return false;
        }

     private:
        std::string index_path_;
    };

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    Result<GlobalIndexIOMeta> WriteGlobalIndex(const std::string& index_root,
                                               const std::shared_ptr<arrow::DataType>& data_type,
                                               const std::map<std::string, std::string>& options,
                                               const std::shared_ptr<arrow::Array>& array,
                                               const Range& expected_range,
                                               const std::string& tmp_dir) const {
        auto global_index = std::make_shared<LuceneGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
        auto file_writer = std::make_shared<GlobalIndexFileManager>(
            fs_, path_factory, /*checkpoint_path_factory=*/nullptr);

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> global_writer,
                               global_index->CreateWriter("f0", CreateArrowSchema(data_type).get(),
                                                          file_writer, pool_));

        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        std::vector<int64_t> row_ids(array->length(), 0);
        std::iota(row_ids.begin(), row_ids.end(), 0);
        PAIMON_RETURN_NOT_OK(global_writer->AddBatch(&c_array, std::move(row_ids)));
        PAIMON_ASSIGN_OR_RAISE(auto result_metas, global_writer->Finish());

        // check tmp dir
        std::vector<BasicFileStatus> file_status_list;
        EXPECT_OK(fs_->ListDir(tmp_dir, &file_status_list));
        EXPECT_EQ(file_status_list.size(), 1);

        // check meta
        EXPECT_EQ(result_metas.size(), 1);
        auto file_name = PathUtil::GetName(result_metas[0].file_path);
        EXPECT_TRUE(StringUtils::StartsWith(file_name, "lucene-fts-global-index-"));
        EXPECT_TRUE(StringUtils::EndsWith(file_name, ".index"));
        EXPECT_TRUE(result_metas[0].metadata);

        // after reset writer, rm tmp files
        global_writer.reset();
        file_status_list.clear();
        EXPECT_OK(fs_->ListDir(tmp_dir, &file_status_list));
        EXPECT_TRUE(file_status_list.empty());

        return result_metas[0];
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateGlobalIndexReader(
        const std::string& index_root, const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options, const GlobalIndexIOMeta& meta) const {
        auto global_index = std::make_shared<LuceneGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(
            fs_, path_factory, /*checkpoint_path_factory=*/nullptr);
        return global_index->CreateReader(CreateArrowSchema(data_type).get(), file_reader, {meta},
                                          pool_);
    }

    static std::shared_ptr<FullTextSearch> MakeSearch(
        const std::string& query, int32_t limit = 10,
        const std::optional<RoaringBitmap64>& include_row_ids = std::nullopt) {
        return std::make_shared<FullTextSearch>("f0", query, limit, include_row_ids);
    }

    void CheckResult(const std::shared_ptr<ScoredGlobalIndexResult>& result,
                     const std::vector<int64_t>& expected_ids) const {
        auto scored_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result);
        ASSERT_TRUE(scored_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, scored_result->GetBitmap());
        ASSERT_EQ(scored_result->GetScores().size(), expected_ids.size());
        for (float score : scored_result->GetScores()) {
            ASSERT_TRUE(std::isfinite(score)) << score;
        }
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*bitmap, RoaringBitmap64::From(expected_ids))
            << "result=" << bitmap->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_ids).ToString();
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<arrow::DataType> data_type_ =
        arrow::struct_({arrow::field("f0", arrow::utf8())});
};

TEST_P(LuceneGlobalIndexTest, TestSimple) {
    int32_t read_buffer_size = GetParam();
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    auto tmp_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(tmp_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> options = {
        {"lucene-fts.write.omit-term-freq-and-position", "false"},
        {"lucene-fts.read.buffer-size", std::to_string(read_buffer_size)},
        {"lucene-fts.write.tmp.directory", tmp_dir->Str()}};
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                    R"([
        ["This is an test document."],
        ["This is an new document document document."],
        ["Document document document document test."],
        ["unordered user-defined doc id"]
    ])")
                                              .ValueOrDie();

    // write index
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root, data_type_, options, array,
                                                     Range(0, 3), tmp_dir->Str()));
    if (read_buffer_size == 10) {
        ASSERT_EQ(
            std::string(meta.metadata->data(), meta.metadata->size()),
            R"({"read.buffer-size":"10","write.omit-term-freq-and-position":"false","write.tmp.directory":")" +
                tmp_dir->Str() + R"("})");
    }

    // create reader
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options, meta));
    auto lucene_reader = std::dynamic_pointer_cast<LuceneGlobalIndexReader>(reader);
    ASSERT_TRUE(lucene_reader);

    // test visit
    auto search = [&](const std::string& query, int32_t limit = 10,
                      const std::optional<RoaringBitmap64>& include_row_ids = std::nullopt) {
        return lucene_reader->VisitFullTextSearch(MakeSearch(query, limit, include_row_ids));
    };
    {
        const std::string query = R"({"match":{"query":"document","operator":"And"}})";
        ASSERT_OK_AND_ASSIGN(auto result, search(query));
        CheckResult(result, {2l, 1l, 0l});
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> iter,
                             result->CreateScoredIterator());
        while (iter->HasNext()) {
            const auto [row_id, score] = iter->NextWithScore();
            ASSERT_OK_AND_ASSIGN(auto single_row_result,
                                 search(query, 1, RoaringBitmap64::From({row_id})));
            ASSERT_OK_AND_ASSIGN(
                std::unique_ptr<ScoredGlobalIndexResult::ScoredIterator> single_row_iter,
                single_row_result->CreateScoredIterator());
            ASSERT_TRUE(single_row_iter->HasNext());
            const auto [expected_row_id, expected_score] = single_row_iter->NextWithScore();
            ASSERT_EQ(row_id, expected_row_id);
            ASSERT_FLOAT_EQ(score, expected_score);
            ASSERT_FALSE(single_row_iter->HasNext());
        }
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(R"({"match":{"query":"document",)"
                                                 R"("ignored":1,"ignored":"\u0000"}})"));
        CheckResult(result, {0l, 1l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(R"({"match":{"query":"document"}})", 1));
        CheckResult(result, {2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(R"({"match":{"terms":"test document","operator":"AND"}})"));
        CheckResult(result, {2l, 0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(R"({"match":{"query":"test new","operator":"Or"}})"));
        CheckResult(result, {1l, 0l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(R"({"match":{"query":"unordered","column":"f0","boost":2.0,)"
                                    R"("fuzziness":0,"max_expansions":10,"prefix_length":1}})"));
        CheckResult(result, {3l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(R"({"match":{"query":"   "}})"));
        CheckResult(result, {});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(R"({"match_phrase":{"query":"test document"}})"));
        CheckResult(result, {0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(R"({"phrase":{"query":"test document","slop":2}})"));
        CheckResult(result, {0l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(
            auto result,
            search(R"({"multi_match":{"query":"test new","columns":["f0"],"boosts":[2.0]}})"));
        CheckResult(result, {1l, 0l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(R"({"multi_match":{"query":"test document",)"
                                                 R"("columns":["f0"],"operator":"And"}})"));
        CheckResult(result, {0l, 2l});
    }
    for (const std::string& boost : std::vector<std::string>{"1e-45", "3.4028235e38"}) {
        SCOPED_TRACE(boost);
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(R"({"match":{"query":"document","boost":)" + boost + "}}"));
        CheckResult(result, {0l, 1l, 2l});
        ASSERT_OK_AND_ASSIGN(result,
                             search(R"({"multi_match":{"query":"document","columns":["f0"],)"
                                    R"("boosts":[)" +
                                    boost + "]}}"));
        CheckResult(result, {0l, 1l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(R"({"boolean":{"should":[
            {"match":{"query":"document","boost":100.0}},
            {"match":{"query":"unordered"}}]}})",
                                                 3));
        CheckResult(result, {0l, 1l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(R"({"boolean":{
            "must":[{"match":{"query":"document"}}],
            "must_not":[{"match":{"query":"new"}}]}})"));
        CheckResult(result, {0l, 2l});
    }
    const std::vector<std::pair<std::string, std::vector<int64_t>>> whitespace_queries = {
        {R"({"match":{"query":"test document","operator":"\u3000And\u00a0",)"
         R"("column":"\u0085f0\u202f"}})",
         {0l, 2l}},
        {R"({"multi_match":{"query":"test new","columns":["\u2007f0\u205f"],)"
         R"("operator":"\u1680Or\u2009"}})",
         {0l, 1l, 2l}},
        {R"({"match_phrase":{"query":"test document","column":"\t\u3000\u00a0\n"}})", {0l}},
        {R"({"multi_match":{"query":"document","columns":["\u2028\u2029"]}})", {0l, 1l, 2l}},
        {R"({"match":{"query":"document","column":""}})", {0l, 1l, 2l}},
        {R"({"boolean":{"queries":[
            ["\u00a0Must\u3000",{"match":{"query":"document"}}],
            ["\u202fShould\u0085",{"match_phrase":{"query":"test document"}}],
            ["\u205fMustNot\u2007",{"match":{"query":"new"}}]]}})",
         {0l, 2l}}};
    for (const auto& [query, expected_ids] : whitespace_queries) {
        SCOPED_TRACE(query);
        ASSERT_OK_AND_ASSIGN(auto result, search(query));
        CheckResult(result, expected_ids);
    }
    // test filter
    std::string match_all_document = R"({"match":{"query":"document","operator":"And"}})";
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(match_all_document, 10, RoaringBitmap64::From({0l, 1l})));
        CheckResult(result, {0l, 1l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(match_all_document, 10, RoaringBitmap64::From({2l, 100l})));
        CheckResult(result, {2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             search(match_all_document, 10, RoaringBitmap64::From({20l, 100l})));
        CheckResult(result, {});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, search(match_all_document, 10, RoaringBitmap64()));
        CheckResult(result, {});
    }
}

TEST_P(LuceneGlobalIndexTest, TestSimpleChinese) {
    int32_t read_buffer_size = GetParam();

    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    auto tmp_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(tmp_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> options = {
        {"lucene-fts.write.omit-term-freq-and-position", "false"},
        {"lucene-fts.read.buffer-size", std::to_string(read_buffer_size)},
        {"lucene-fts.jieba.tokenize-mode", "query"},
        {"lucene-fts.write.tmp.directory", tmp_dir->Str()}};

    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                    R"([
["QianWen 是一个基于 AI 的智能助手，类似于 Siri 和 Alexa。我们正在用 Python 开发 QianWen 的 Natural Language Understanding 模块，该模块支持多轮对话和意图识别功能，是新一代智能助手的核心技术之一。"],
["最近开源了一个新项目叫ｑｉａｎｗｅｎ（全角字符），功能类似之前的 Qianwen，是一个面向 AI 应用的智能助手。它不仅支持 Machine Learning 和 NLP 技术，还提供了可扩展的开发框架，便于开发者构建自己的智能助手系统。"],
["我们在测试 qianwen-core v1.2 和 ai-engine-alpha 中的 bug，重点优化了 qianwen 的响应速度和稳定性。本次更新增强了核心模块的功能，提升了智能助手的开发效率，并修复了与 NLP 模块相关的多个问题。"],
["AI 助手开发中常用的技术包括 Speech Recognition、Natural Language Processing 和 Recommendation System。我们使用 TensorFlow 和 PyTorch 构建模型，开发了多个智能助手原型，支持语音交互和上下文理解功能，是当前热门的人工智能发展应用方向。"],
["新一代的 AI 助手代号为「千问」，内部命名为 QianwenX-2024，计划在 next quarter 发布。QianwenX 将集成更强的 multimodel 能力，支持图像和文本联合处理，进一步提升智能助手的理解能力和交互体验，是未来智能助手的重要发展方向。"]
    ])")
                                              .ValueOrDie();

    // write index
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root, data_type_, options, array,
                                                     Range(0, 4), tmp_dir->Str()));
    if (read_buffer_size == 10) {
        ASSERT_EQ(
            std::string(meta.metadata->data(), meta.metadata->size()),
            R"({"jieba.tokenize-mode":"query","read.buffer-size":"10","write.omit-term-freq-and-position":"false","write.tmp.directory":")" +
                tmp_dir->Str() + R"("})");
    }

    // create reader
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options, meta));
    auto lucene_reader = std::dynamic_pointer_cast<LuceneGlobalIndexReader>(reader);
    ASSERT_TRUE(lucene_reader);

    // test visit
    {
        ASSERT_OK_AND_ASSIGN(auto result, lucene_reader->VisitFullTextSearch(MakeSearch(
                                              R"({"match":{"query":"模块","operator":"And"}})")));
        CheckResult(result, {0l, 2l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, lucene_reader->VisitFullTextSearch(
                                              MakeSearch(R"({"match":{"query":"模块"}})", 1)));
        CheckResult(result, {0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result,
                             lucene_reader->VisitFullTextSearch(
                                 MakeSearch(R"({"match":{"query":"模块技术","operator":"And"}})")));
        CheckResult(result, {0l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, lucene_reader->VisitFullTextSearch(
                                              MakeSearch(R"({"match":{"query":"模块技术"}})")));
        CheckResult(result, {0l, 1l, 2l, 3l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto result, lucene_reader->VisitFullTextSearch(MakeSearch(
                                              R"({"match_phrase":{"query":"发展方向"}})")));
        CheckResult(result, {4l});
    }
    // test filter
    {
        ASSERT_OK_AND_ASSIGN(auto result, lucene_reader->VisitFullTextSearch(
                                              MakeSearch(R"({"match":{"query":"模块技术"}})", 10,
                                                         RoaringBitmap64::From({1l, 3l, 4l}))));

        CheckResult(result, {1l, 3l});
    }
}

TEST_F(LuceneGlobalIndexTest, TestInvalidQuery) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    auto tmp_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(tmp_dir);
    std::map<std::string, std::string> options = {
        {"lucene-fts.write.tmp.directory", tmp_dir->Str()}};
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_, R"([["This is an new document."]])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root_dir->Str(), data_type_, options,
                                                     array, Range(0, 0), tmp_dir->Str()));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root_dir->Str(), data_type_, options, meta));
    auto search = [&](const std::string& query, int32_t limit = 10) {
        return reader->VisitFullTextSearch(MakeSearch(query, limit));
    };

    {
        auto result = search(R"({"boost":{"positive":{"match":{"query":"document"}},)"
                             R"("negative":{"match":{"query":"new"}}}})");
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().IsNotImplemented()) << result.status().ToString();
    }
    for (const std::string& fuzziness : std::vector<std::string>{"1", "2", R"("auto")", "null"}) {
        auto result = search(R"({"match":{"query":"document","fuzziness":)" + fuzziness + "}}");
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().IsNotImplemented()) << result.status().ToString();
    }

    ASSERT_NOK_WITH_MSG(reader->VisitFullTextSearch(nullptr), "null FullTextSearch");
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document"}})", 0),
                        "requires a positive limit");
    ASSERT_NOK_WITH_MSG(search("document"), "invalid full-text query");
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document"}} trailing)"),
                        "invalid full-text query");
    std::string query_with_nul = R"({"match":{"query":"document"}})";
    query_with_nul.push_back('\0');
    query_with_nul.append("trailing");
    ASSERT_NOK_WITH_MSG(search(query_with_nul), "must not contain NUL characters");
    ASSERT_NOK_WITH_MSG(search(R"(["match"])"), "exactly one query type");
    ASSERT_NOK_WITH_MSG(
        search(R"({"match":{"query":"document"},"match_phrase":{"query":"document"}})"),
        "exactly one query type");
    ASSERT_NOK_WITH_MSG(search(R"({"term":{"query":"document"}})"),
                        "unknown full-text query type `term`");
    ASSERT_NOK_WITH_MSG(search(R"({"match":"document"})"), "must be a JSON object");
    ASSERT_NOK_WITH_MSG(search(R"({"match":{}})"), "missing field `query`");
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":1}})"), "field `query` must be a string");
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","terms":"document"}})"),
                        "sets both field `terms` and its alias `query`");
    const std::vector<std::pair<std::string, std::string>> duplicate_field_cases = {
        {R"({"match":{"query":"document","query":"new"}})", "query"},
        {R"({"match":{"query":"document","column":null,"column":"f0"}})", "column"},
        {R"({"match":{"query":"document","operator":"Or","operator":"And"}})", "operator"},
        {R"({"match":{"query":"document","boost":1,"boost":2}})", "boost"},
        {R"({"multi_match":{"query":"document","columns":["f0"],"columns":["f0"]}})", "columns"},
        {R"({"multi_match":{"query":"document","columns":["f0"],"boosts":[1],"boosts":[2]}})",
         "boosts"},
        {R"({"phrase":{"query":"new document","slop":0,"slop":1}})", "slop"},
        {R"({"boolean":{"must":[{"match":{"query":"document"}}],)"
         R"("must":[{"match":{"query":"new"}}]}})",
         "must"},
        {R"({"boolean":{"should":[{"match":{"query":"document","query":"new"}}]}})", "query"}};
    for (const auto& [query, field] : duplicate_field_cases) {
        SCOPED_TRACE(query);
        ASSERT_NOK_WITH_MSG(search(query), "duplicate full-text query field `" + field + "`");
    }
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","column":"f1"}})"),
                        "column 'f1' is not configured for this index");
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","operator":"Xor"}})"),
                        "invalid full-text query operator: Xor");
    for (const std::string& non_whitespace :
         std::vector<std::string>{R"(\u0000)", R"(\u0001)", R"(\u001c)", R"(\u001f)", R"(\u180e)",
                                  R"(\u200b)", R"(\ufeff)", R"(\ufffe)", R"(\uffff)", "\xff"}) {
        SCOPED_TRACE(non_whitespace);
        ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","operator":")" + non_whitespace +
                                   "And" + non_whitespace + R"("}})"),
                            "invalid full-text query operator");
        ASSERT_NOK_WITH_MSG(search(R"({"boolean":{"queries":[[")" + non_whitespace + "Must" +
                                   non_whitespace + R"(",{"match":{"query":"document"}}]]}})"),
                            "invalid boolean query occur");
        ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","column":")" + non_whitespace +
                                   "f0" + non_whitespace + R"("}})"),
                            "full-text query column");
        ASSERT_NOK_WITH_MSG(search(R"({"multi_match":{"query":"document","columns":[")" +
                                   non_whitespace + R"("]}})"),
                            "full-text query column");
    }
    for (const std::string& boost :
         std::vector<std::string>{"0", "1e40", "1e-100", "3.4028236e38", "7e-46"}) {
        SCOPED_TRACE(boost);
        ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","boost":)" + boost + "}}"),
                            "boost must be a finite positive value after float conversion");
        ASSERT_NOK_WITH_MSG(
            search(R"({"multi_match":{"query":"document","columns":["f0"],"boosts":[)" + boost +
                   "]}}"),
            "boost must be a finite positive value after float conversion");
    }
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","fuzziness":3}})"),
                        "fuzziness must be auto/null or a value in [0, 2]");
    ASSERT_NOK_WITH_MSG(search(R"({"match":{"query":"document","max_expansions":0}})"),
                        "max_expansions must be positive");
    ASSERT_NOK_WITH_MSG(search(R"({"match_phrase":{"query":"document","slop":-1}})"),
                        "field `slop` must be an integer");
    ASSERT_NOK_WITH_MSG(search(R"({"multi_match":{"query":"document","columns":[]}})"),
                        "must contain at least one column");
    ASSERT_NOK_WITH_MSG(
        search(R"({"multi_match":{"query":"document","columns":["f0"],"boosts":[1.0,2.0]}})"),
        "boosts length 2 does not match columns length 1");
    ASSERT_NOK_WITH_MSG(search(R"({"multi_match":{"query":"document","columns":["f0","f1"]}})"),
                        "column 'f1' is not configured for this index");
    ASSERT_NOK_WITH_MSG(search(R"({"boolean":{}})"), "must contain at least one clause");
    ASSERT_NOK_WITH_MSG(search(R"({"boolean":{"must_not":[{"match":{"query":"new"}}]}})"),
                        "must contain at least one should or must clause");
    ASSERT_NOK_WITH_MSG(search(R"({"boolean":{"queries":[["should"]]}})"),
                        "must be an array of [occur, query] pairs");
    ASSERT_NOK_WITH_MSG(search(R"({"boolean":{"queries":[["filter",{"match":{"query":"new"}}]]}})"),
                        "invalid boolean query occur: filter");
    ASSERT_NOK_WITH_MSG(search(R"({"boolean":{"should":[{"term":{"query":"new"}}]}})"),
                        "unknown full-text query type `term`");
}

TEST_F(LuceneGlobalIndexTest, TestInvalidWithoutTmpDir) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> options = {
        {"lucene-fts.write.omit-term-freq-and-position", "false"}};
    std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                    R"([
        ["This is an test document."]
    ])")
                                              .ValueOrDie();

    // write index
    ASSERT_NOK_WITH_MSG(WriteGlobalIndex(test_root, data_type_, options, array, Range(0, 0), ""),
                        "key write.tmp.directory does not exist in map");
}
INSTANTIATE_TEST_SUITE_P(ReadBufferSize, LuceneGlobalIndexTest,
                         ::testing::ValuesIn(std::vector<int32_t>({10, 100, 1024})));

}  // namespace paimon::lucene::test
