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
#include "paimon/global_index/lucene/lucene_global_index_reader.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <initializer_list>
#include <limits>
#include <utility>

#include "arrow/c/bridge.h"
#include "lucene++/FileUtils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/lucene/jieba_analyzer.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/lucene/lucene_directory.h"
#include "paimon/global_index/lucene/lucene_filter.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/io/data_input_stream.h"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

namespace paimon::lucene {
namespace {

std::string JsonString(const rapidjson::Value& value) {
    return std::string(value.GetString(), value.GetStringLength());
}

/// Trims Unicode White_Space as Rust `str::trim` does in the native DSL.
std::wstring TrimDslWhitespace(const std::wstring& value) {
    constexpr wchar_t kWhitespace[] =
        L"\t\n\v\f\r \u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005"
        L"\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000";
    size_t first = value.find_first_not_of(kWhitespace);
    if (first == std::wstring::npos) {
        return L"";
    }
    size_t last = value.find_last_not_of(kWhitespace);
    return value.substr(first, last - first + 1);
}

bool TrimmedIsOneOf(const std::string& value, std::initializer_list<const wchar_t*> candidates) {
    std::wstring trimmed = TrimDslWhitespace(LuceneUtils::StringToWstring(value));
    for (const wchar_t* candidate : candidates) {
        if (trimmed == candidate) {
            return true;
        }
    }
    return false;
}

/// Translates the JSON DSL query of `FullTextSearch` into a Lucene query on the indexed field.
///
/// The DSL is defined by apache/paimon-full-text `core/src/query.rs`. The supported queries use
/// its field names, aliases and defaults. Duplicate known fields are rejected, and unknown fields
/// are ignored as in the native engine.
/// `match`, `multi_match`, `match_phrase` and `boolean` queries are translated into term, boolean
/// and phrase queries; `boost` queries and fuzzy matching are rejected.
class DslQueryTranslator {
 public:
    /// Splits text into index terms with the analyzer used at indexing time.
    using Analyzer = std::function<std::vector<std::wstring>(const std::string&)>;

    DslQueryTranslator(const std::wstring& wfield_name, Analyzer analyzer)
        : wfield_name_(wfield_name), analyzer_(std::move(analyzer)) {}

    Result<Lucene::QueryPtr> Translate(const std::string& query) const noexcept(false) {
        // RapidJSON treats a NUL byte as the end of input even when a length is provided.
        if (query.find('\0') != std::string::npos) {
            return Status::Invalid("lucene full-text query must not contain NUL characters");
        }
        rapidjson::Document document;
        document.Parse(query.data(), query.size());
        if (document.HasParseError()) {
            return Status::Invalid(fmt::format(
                "invalid full-text query: {} (at offset {})",
                rapidjson::GetParseError_En(document.GetParseError()), document.GetErrorOffset()));
        }
        return TranslateQuery(document);
    }

 private:
    Result<Lucene::QueryPtr> TranslateQuery(const rapidjson::Value& spec) const noexcept(false) {
        if (!spec.IsObject() || spec.MemberCount() != 1) {
            return Status::Invalid(
                "full-text query must be a JSON object with exactly one query type");
        }
        const auto& member = *spec.MemberBegin();
        std::string type = JsonString(member.name);
        const rapidjson::Value& body = member.value;
        if (!body.IsObject()) {
            return Status::Invalid(fmt::format("full-text query `{}` must be a JSON object", type));
        }
        if (type == "match") {
            return TranslateMatch(body);
        }
        if (type == "multi_match") {
            return TranslateMultiMatch(body);
        }
        if (type == "match_phrase" || type == "phrase") {
            return TranslateMatchPhrase(body);
        }
        if (type == "boolean") {
            return TranslateBoolean(body);
        }
        if (type == "boost") {
            return Status::NotImplemented("lucene full-text search does not support boost queries");
        }
        return Status::Invalid(fmt::format("unknown full-text query type `{}`", type));
    }

    Result<Lucene::QueryPtr> TranslateMatch(const rapidjson::Value& body) const noexcept(false) {
        PAIMON_RETURN_NOT_OK(CheckColumn(body));
        PAIMON_ASSIGN_OR_RAISE(std::string terms, GetTerms(body));
        PAIMON_ASSIGN_OR_RAISE(bool conjunction, IsConjunction(body));
        PAIMON_ASSIGN_OR_RAISE(float boost, GetBoost(body));
        PAIMON_RETURN_NOT_OK(CheckFuzzyOptions(body));
        return BuildMatchQuery(terms, conjunction, boost);
    }

    /// The index has a single field, so every column must name it. As in the engine, the query
    /// is a SHOULD clause of one `match` query per column.
    Result<Lucene::QueryPtr> TranslateMultiMatch(const rapidjson::Value& body) const
        noexcept(false) {
        PAIMON_ASSIGN_OR_RAISE(std::string terms, GetTerms(body));
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* columns, FindField(body, "columns"));
        if (columns == nullptr || !columns->IsArray()) {
            return Status::Invalid("multi_match query field `columns` must be an array of strings");
        }
        if (columns->Empty()) {
            return Status::Invalid("multi_match query must contain at least one column");
        }
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* boosts, FindField(body, "boosts", "boost"));
        if (boosts != nullptr && !boosts->IsArray()) {
            return Status::Invalid("multi_match query field `boosts` must be an array of numbers");
        }
        bool has_boosts = boosts != nullptr && !boosts->Empty();
        if (has_boosts && boosts->Size() != columns->Size()) {
            return Status::Invalid(
                fmt::format("multi_match boosts length {} does not match columns length {}",
                            boosts->Size(), columns->Size()));
        }
        PAIMON_ASSIGN_OR_RAISE(bool conjunction, IsConjunction(body));
        PAIMON_RETURN_NOT_OK(CheckFuzzyOptions(body));

        auto multi_match_query = Lucene::newLucene<Lucene::BooleanQuery>();
        for (rapidjson::SizeType i = 0; i < columns->Size(); ++i) {
            const rapidjson::Value& column = (*columns)[i];
            if (!column.IsString()) {
                return Status::Invalid(
                    "multi_match query field `columns` must be an array of strings");
            }
            PAIMON_RETURN_NOT_OK(CheckColumnName(JsonString(column)));
            float boost = 1.0f;
            if (has_boosts) {
                const rapidjson::Value& boost_value = (*boosts)[i];
                if (!boost_value.IsNumber()) {
                    return Status::Invalid(
                        "multi_match query field `boosts` must be an array of numbers");
                }
                PAIMON_ASSIGN_OR_RAISE(boost, ToFloatBoost(boost_value.GetDouble()));
            }
            multi_match_query->add(BuildMatchQuery(terms, conjunction, boost),
                                   Lucene::BooleanClause::Occur::SHOULD);
        }
        Lucene::QueryPtr query = multi_match_query;
        return query;
    }

    Result<Lucene::QueryPtr> TranslateMatchPhrase(const rapidjson::Value& body) const
        noexcept(false) {
        PAIMON_RETURN_NOT_OK(CheckColumn(body));
        PAIMON_ASSIGN_OR_RAISE(std::string terms, GetTerms(body));
        int32_t slop = 0;
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* slop_value, FindField(body, "slop"));
        if (slop_value != nullptr) {
            if (!slop_value->IsUint() ||
                slop_value->GetUint() >
                    static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
                return Status::Invalid(
                    "match_phrase query field `slop` must be an integer in [0, 2147483647]");
            }
            slop = static_cast<int32_t>(slop_value->GetUint());
        }
        auto phrase_query = Lucene::newLucene<Lucene::PhraseQuery>();
        for (const auto& token : analyzer_(terms)) {
            phrase_query->add(Lucene::newLucene<Lucene::Term>(wfield_name_, token));
        }
        phrase_query->setSlop(slop);
        Lucene::QueryPtr query = phrase_query;
        return query;
    }

    Result<Lucene::QueryPtr> TranslateBoolean(const rapidjson::Value& body) const noexcept(false) {
        std::vector<std::pair<Lucene::BooleanClause::Occur, const rapidjson::Value*>> clauses;
        auto add_clauses = [&](const char* name, Lucene::BooleanClause::Occur occur) -> Status {
            PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* children, FindField(body, name));
            if (children == nullptr) {
                return Status::OK();
            }
            if (!children->IsArray()) {
                return Status::Invalid(
                    fmt::format("boolean query field `{}` must be an array of queries", name));
            }
            for (auto iter = children->Begin(); iter != children->End(); ++iter) {
                clauses.emplace_back(occur, &(*iter));
            }
            return Status::OK();
        };
        PAIMON_RETURN_NOT_OK(add_clauses("should", Lucene::BooleanClause::Occur::SHOULD));
        PAIMON_RETURN_NOT_OK(add_clauses("must", Lucene::BooleanClause::Occur::MUST));
        PAIMON_RETURN_NOT_OK(add_clauses("must_not", Lucene::BooleanClause::Occur::MUST_NOT));
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* queries, FindField(body, "queries"));
        if (queries != nullptr) {
            if (!queries->IsArray()) {
                return Status::Invalid(
                    "boolean query field `queries` must be an array of [occur, query] pairs");
            }
            for (auto iter = queries->Begin(); iter != queries->End(); ++iter) {
                if (!iter->IsArray() || iter->Size() != 2 || !iter->Begin()->IsString()) {
                    return Status::Invalid(
                        "boolean query field `queries` must be an array of [occur, query] pairs");
                }
                PAIMON_ASSIGN_OR_RAISE(Lucene::BooleanClause::Occur occur,
                                       ParseOccur(JsonString(*iter->Begin())));
                clauses.emplace_back(occur, iter->Begin() + 1);
            }
        }

        if (clauses.empty()) {
            return Status::Invalid("boolean query must contain at least one clause");
        }
        bool has_positive_clause =
            std::any_of(clauses.begin(), clauses.end(), [](const auto& clause) {
                return clause.first != Lucene::BooleanClause::Occur::MUST_NOT;
            });
        if (!has_positive_clause) {
            return Status::Invalid("boolean query must contain at least one should or must clause");
        }
        auto boolean_query = Lucene::newLucene<Lucene::BooleanQuery>();
        for (const auto& [occur, child] : clauses) {
            PAIMON_ASSIGN_OR_RAISE(Lucene::QueryPtr child_query, TranslateQuery(*child));
            boolean_query->add(child_query, occur);
        }
        Lucene::QueryPtr query = boolean_query;
        return query;
    }

    /// Builds a `match` query on the indexed field: one term query per analyzed token, combined
    /// with MUST for the `And` operator and SHOULD for `Or`. Text without tokens matches no rows.
    Lucene::QueryPtr BuildMatchQuery(const std::string& terms, bool conjunction, float boost) const
        noexcept(false) {
        std::vector<std::wstring> tokens = analyzer_(terms);
        Lucene::QueryPtr query;
        if (tokens.size() == 1) {
            query = Lucene::newLucene<Lucene::TermQuery>(
                Lucene::newLucene<Lucene::Term>(wfield_name_, tokens[0]));
        } else {
            Lucene::BooleanClause::Occur occur = conjunction ? Lucene::BooleanClause::Occur::MUST
                                                             : Lucene::BooleanClause::Occur::SHOULD;
            auto boolean_query = Lucene::newLucene<Lucene::BooleanQuery>();
            for (const auto& token : tokens) {
                boolean_query->add(Lucene::newLucene<Lucene::TermQuery>(
                                       Lucene::newLucene<Lucene::Term>(wfield_name_, token)),
                                   occur);
            }
            query = boolean_query;
        }
        if (boost != 1.0f) {
            query->setBoost(boost);
        }
        return query;
    }

    Status CheckColumn(const rapidjson::Value& body) const {
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* column, FindField(body, "column"));
        if (column == nullptr || column->IsNull()) {
            return Status::OK();
        }
        if (!column->IsString()) {
            return Status::Invalid("full-text query field `column` must be a string");
        }
        return CheckColumnName(JsonString(*column));
    }

    /// As in the engine, a blank column selects the indexed field.
    Status CheckColumnName(const std::string& column) const {
        std::wstring wcolumn = LuceneUtils::StringToWstring(column);
        // Lucene returns an empty string on decoding failure; it must not select the default field.
        bool column_decoded = column.empty() || !wcolumn.empty();
        wcolumn = TrimDslWhitespace(wcolumn);
        if (!column_decoded || (!wcolumn.empty() && wcolumn != wfield_name_)) {
            return Status::Invalid(fmt::format(
                "full-text query column '{}' is not configured for this index", column));
        }
        return Status::OK();
    }

    /// Returns the text of `terms` or its alias `query`.
    static Result<std::string> GetTerms(const rapidjson::Value& body) {
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* terms, FindField(body, "terms", "query"));
        if (terms == nullptr) {
            return Status::Invalid("full-text query is missing field `query`");
        }
        if (!terms->IsString()) {
            return Status::Invalid("full-text query field `query` must be a string");
        }
        return JsonString(*terms);
    }

    /// Returns true for the `And` operator and false for `Or`, the default.
    static Result<bool> IsConjunction(const rapidjson::Value& body) {
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* match_operator, FindField(body, "operator"));
        if (match_operator == nullptr) {
            return false;
        }
        if (!match_operator->IsString()) {
            return Status::Invalid("full-text query field `operator` must be a string");
        }
        std::string value = JsonString(*match_operator);
        if (TrimmedIsOneOf(value, {L"Or", L"or", L"OR"})) {
            return false;
        }
        if (TrimmedIsOneOf(value, {L"And", L"and", L"AND"})) {
            return true;
        }
        return Status::Invalid(fmt::format("invalid full-text query operator: {}", value));
    }

    static Result<Lucene::BooleanClause::Occur> ParseOccur(const std::string& value) {
        if (TrimmedIsOneOf(value, {L"Should", L"should", L"SHOULD"})) {
            return Lucene::BooleanClause::Occur::SHOULD;
        }
        if (TrimmedIsOneOf(value, {L"Must", L"must", L"MUST"})) {
            return Lucene::BooleanClause::Occur::MUST;
        }
        if (TrimmedIsOneOf(value, {L"MustNot", L"must_not", L"MUST_NOT", L"mustnot", L"MUSTNOT"})) {
            return Lucene::BooleanClause::Occur::MUST_NOT;
        }
        return Status::Invalid(fmt::format("invalid boolean query occur: {}", value));
    }

    static Result<float> GetBoost(const rapidjson::Value& body) {
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* boost, FindField(body, "boost"));
        if (boost == nullptr) {
            return 1.0f;
        }
        if (!boost->IsNumber()) {
            return Status::Invalid("full-text query field `boost` must be a number");
        }
        return ToFloatBoost(boost->GetDouble());
    }

    /// The native DSL stores boosts as f32 and validates the rounded value.
    static Result<float> ToFloatBoost(double boost) {
        const double max_boost = std::numeric_limits<float>::max();
        const double overflow_threshold =
            max_boost + (max_boost - std::nextafter(std::numeric_limits<float>::max(), 0.0f)) / 2.0;
        float rounded_boost = 0.0f;
        if (std::isfinite(boost) && boost > 0.0 && boost < overflow_threshold) {
            // Values just above float's maximum round to that maximum. Clamp them before the cast
            // so the C++ conversion stays within the representable range.
            rounded_boost = static_cast<float>(std::min(boost, max_boost));
        }
        if (rounded_boost <= 0.0f) {
            return Status::Invalid(fmt::format(
                "boost must be a finite positive value after float conversion, got {}", boost));
        }
        return rounded_boost;
    }

    /// Validates `fuzziness`, `max_expansions` and `prefix_length` as the engine does. Only exact
    /// term matching, `fuzziness` 0, is supported.
    static Status CheckFuzzyOptions(const rapidjson::Value& body) {
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* fuzziness, FindField(body, "fuzziness"));
        if (fuzziness != nullptr) {
            bool is_auto = fuzziness->IsNull() ||
                           (fuzziness->IsString() &&
                            StringUtils::EqualsIgnoreCase(JsonString(*fuzziness), "auto"));
            if (!is_auto && (!fuzziness->IsUint64() || fuzziness->GetUint64() > 2)) {
                return Status::Invalid(
                    "match query fuzziness must be auto/null or a value in [0, 2]");
            }
            if (is_auto || fuzziness->GetUint64() != 0) {
                return Status::NotImplemented(
                    "lucene full-text search does not support fuzzy matching, fuzziness must be 0");
            }
        }
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* max_expansions,
                               FindField(body, "max_expansions", "maxExpansions"));
        if (max_expansions != nullptr &&
            (!max_expansions->IsUint64() || max_expansions->GetUint64() == 0)) {
            return Status::Invalid("match query max_expansions must be positive");
        }
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* prefix_length,
                               FindField(body, "prefix_length", "prefixLength"));
        if (prefix_length != nullptr && !prefix_length->IsUint()) {
            return Status::Invalid("match query prefix_length must be a non-negative integer");
        }
        return Status::OK();
    }

    /// Rejects repeated known fields while leaving unknown fields ignored, as Serde does.
    static Result<const rapidjson::Value*> FindField(const rapidjson::Value& body,
                                                     const char* name) {
        const rapidjson::Value* value = nullptr;
        for (auto iter = body.MemberBegin(); iter != body.MemberEnd(); ++iter) {
            if (iter->name != name) {
                continue;
            }
            if (value != nullptr) {
                return Status::Invalid(fmt::format("duplicate full-text query field `{}`", name));
            }
            value = &iter->value;
        }
        return value;
    }

    /// Returns the field `name` or its alias, or nullptr if neither is present.
    static Result<const rapidjson::Value*> FindField(const rapidjson::Value& body, const char* name,
                                                     const char* alias) {
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* value, FindField(body, name));
        PAIMON_ASSIGN_OR_RAISE(const rapidjson::Value* alias_value, FindField(body, alias));
        if (value != nullptr && alias_value != nullptr) {
            return Status::Invalid(fmt::format(
                "full-text query sets both field `{}` and its alias `{}`", name, alias));
        }
        return value != nullptr ? value : alias_value;
    }

    const std::wstring& wfield_name_;
    Analyzer analyzer_;
};

}  // namespace

Result<std::shared_ptr<LuceneGlobalIndexReader>> LuceneGlobalIndexReader::Create(
    const std::string& field_name, const GlobalIndexIOMeta& io_meta,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    try {
        auto meta_bytes = io_meta.metadata;
        if (!meta_bytes) {
            return Status::Invalid("Lucene global index must have meta data");
        }
        std::map<std::string, std::string> write_options;
        PAIMON_RETURN_NOT_OK(RapidJsonUtil::FromJsonString(
            std::string(meta_bytes->data(), meta_bytes->size()), &write_options));

        std::map<std::string, std::pair<int64_t, int64_t>> file_name_to_offset_and_length;
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> paimon_input,
                               file_reader->GetInputStream(io_meta.file_path));
        DataInputStream data_input_stream(paimon_input);
        PAIMON_ASSIGN_OR_RAISE(int32_t version, data_input_stream.ReadValue<int32_t>());
        if (version != kVersion) {
            return Status::Invalid(
                fmt::format("LuceneGlobalIndex not support version {}", kVersion));
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t num_files, data_input_stream.ReadValue<int32_t>());
        for (int32_t i = 0; i < num_files; i++) {
            PAIMON_ASSIGN_OR_RAISE(int32_t file_name_len, data_input_stream.ReadValue<int32_t>());
            auto file_name_bytes = std::make_shared<Bytes>(file_name_len, pool.get());
            PAIMON_RETURN_NOT_OK(data_input_stream.ReadBytes(file_name_bytes.get()));
            std::string file_name(file_name_bytes->data(), file_name_bytes->size());
            PAIMON_ASSIGN_OR_RAISE(int64_t file_len, data_input_stream.ReadValue<int64_t>());
            PAIMON_ASSIGN_OR_RAISE(int64_t pos, data_input_stream.GetPos());
            file_name_to_offset_and_length[file_name] = {pos, file_len};
            pos += file_len;
            if (i != num_files - 1) {
                PAIMON_RETURN_NOT_OK(data_input_stream.Seek(pos));
            }
        }
        PAIMON_ASSIGN_OR_RAISE(
            int32_t read_buffer_size,
            OptionsUtils::GetValueFromMap(options, kLuceneReadBufferSize, kDefaultReadBufferSize));
        Lucene::DirectoryPtr lucene_dir = Lucene::newLucene<LuceneDirectory>(
            PathUtil::GetParentDirPath(io_meta.file_path), file_name_to_offset_and_length,
            paimon_input, read_buffer_size);

        Lucene::IndexReaderPtr reader = Lucene::IndexReader::open(lucene_dir, /*read_only=*/true);
        Lucene::IndexSearcherPtr searcher = Lucene::newLucene<Lucene::IndexSearcher>(reader);

        PAIMON_ASSIGN_OR_RAISE(std::string dictionary_dir, LuceneUtils::GetJiebaDictionaryDir());
        auto jieba = std::make_shared<cppjieba::Jieba>(
            dictionary_dir + "/jieba.dict.utf8", dictionary_dir + "/hmm_model.utf8",
            dictionary_dir + "/user.dict.utf8", dictionary_dir + "/idf.utf8",
            dictionary_dir + "/stop_words.utf8");

        // priority: read options > write options > kDefaultJiebaTokenizeMode
        PAIMON_ASSIGN_OR_RAISE(
            std::string tokenize_mode,
            OptionsUtils::GetValueFromMap(options, kJiebaTokenizeMode, std::string("")));
        if (tokenize_mode.empty()) {
            PAIMON_ASSIGN_OR_RAISE(tokenize_mode, OptionsUtils::GetValueFromMap(
                                                      write_options, kJiebaTokenizeMode,
                                                      std::string(kDefaultJiebaTokenizeMode)));
        }
        return std::shared_ptr<LuceneGlobalIndexReader>(new LuceneGlobalIndexReader(
            LuceneUtils::StringToWstring(field_name), searcher, tokenize_mode, jieba));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("create lucene global index reader failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "create lucene global index reader failed, with unknown error.");
    }
}

std::vector<std::wstring> LuceneGlobalIndexReader::TokenizeQuery(const std::string& query) const {
    std::vector<std::string> terms;
    JiebaTokenizer::CutWithMode(tokenize_mode_, jieba_.get(), query, &terms);
    std::vector<std::string_view> normalized_terms;
    JiebaTokenizer::Normalize(jieba_->extractor.GetStopWords(), &terms, &normalized_terms);
    std::vector<std::wstring> wterms;
    wterms.reserve(normalized_terms.size());
    for (const auto& term : normalized_terms) {
        wterms.push_back(LuceneUtils::StringToWstring(term));
    }
    return wterms;
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LuceneGlobalIndexReader::Search(
    const Lucene::QueryPtr& query, const std::shared_ptr<FullTextSearch>& full_text_search) const
    noexcept(false) {
    Lucene::FilterPtr filter =
        full_text_search->include_row_ids
            ? Lucene::newLucene<LuceneFilter>(&(full_text_search->include_row_ids.value()))
            : Lucene::FilterPtr();

    Lucene::TopDocsPtr results = searcher_->search(query, filter, full_text_search->limit);

    // prepare BitmapScoredGlobalIndexResult
    std::map<int64_t, float> id_to_score;
    for (auto score_doc : results->scoreDocs) {
        id_to_score[static_cast<int64_t>(score_doc->doc)] = static_cast<float>(score_doc->score);
    }
    RoaringBitmap64 bitmap;
    std::vector<float> scores;
    scores.reserve(id_to_score.size());
    for (const auto& [id, score] : id_to_score) {
        bitmap.Add(id);
        scores.push_back(score);
    }
    return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap), std::move(scores));
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> LuceneGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    if (!full_text_search) {
        return Status::Invalid("VisitFullTextSearch: null FullTextSearch pointer");
    }
    if (full_text_search->limit <= 0) {
        return Status::Invalid("lucene full-text search requires a positive limit");
    }
    try {
        DslQueryTranslator translator(
            wfield_name_, [this](const std::string& text) { return TokenizeQuery(text); });
        PAIMON_ASSIGN_OR_RAISE(Lucene::QueryPtr query,
                               translator.Translate(full_text_search->query));
        return Search(query, full_text_search);
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("visit full text search failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError("visit full text search failed, with unknown error.");
    }
}

}  // namespace paimon::lucene
