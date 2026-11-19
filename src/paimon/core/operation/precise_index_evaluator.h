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

#pragma once

#include <algorithm>
#include <cmath>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/index/index_reader_utils.h"
#include "paimon/common/predicate/full_text_match_predicate.h"
#include "paimon/macros.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/vector_search.h"

namespace paimon {

/// Shared precise evaluation over file row positions or real-time offsets.
/// Storage adapters supply readers and physical scans; this class owns tree and leaf semantics.
template <typename Bitmap, typename ScoredResult>
class PreciseIndexEvaluator {
 public:
    struct Evaluation {
        Bitmap rows;
        // Sources retain physical row positions or real-time offsets until all filtering and
        // root TopK are complete.
        std::map<std::string, std::shared_ptr<ScoredResult>> scores;
    };
    using LeafEvaluator =
        std::function<Result<Evaluation>(const std::shared_ptr<LeafPredicate>&, const Bitmap&)>;
    using LeafEstimator =
        std::function<Result<std::optional<double>>(const std::shared_ptr<LeafPredicate>&)>;

    PreciseIndexEvaluator(LeafEvaluator evaluate_leaf, LeafEstimator estimate_leaf)
        : evaluate_leaf_(std::move(evaluate_leaf)), estimate_leaf_(std::move(estimate_leaf)) {}

    template <typename PrepareLeaf>
    static Status Prepare(const std::shared_ptr<Predicate>& predicate, PrepareLeaf prepare_leaf) {
        if (!predicate) {
            return Status::OK();
        }
        if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
            const Function::Type type = compound->GetFunction().GetType();
            if (type != Function::Type::AND && type != Function::Type::OR) {
                return Status::Invalid("Unsupported compound predicate: ", predicate->ToString());
            }
            for (const auto& child : compound->Children()) {
                PAIMON_RETURN_NOT_OK(Prepare(child, prepare_leaf));
            }
            return Status::OK();
        }
        auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
        if (!leaf) {
            return Status::Invalid("Predicate must contain leaf or compound predicates");
        }
        if (leaf->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH &&
            !std::dynamic_pointer_cast<FullTextMatchPredicate>(leaf)) {
            return Status::Invalid("FullTextMatch requires a full-text match predicate");
        }
        return prepare_leaf(leaf);
    }

    /// Field mapping and physical scans remain with the storage adapter. Scalar indexes only
    /// prune candidates; every surviving scalar candidate is checked against the stored value.
    /// @param predicate Original predicate in the query's logical schema, used for exact
    /// scalar filtering and binding scores to the original field name.
    /// @param index_predicate Predicate mapped to the source's physical schema for index
    /// lookup. May be null when no indexed field maps to the logical field; scalar filtering
    /// still uses predicate. Real-time sources currently use the original predicate directly.
    /// @param candidates Rows still eligible after earlier predicates, ranges and deletions.
    /// @param needs_scores Whether full-text matching must also return scores.
    /// @param scan_scalar Callback that precisely filters candidate rows using stored values.
    /// @param indexes Provider of scalar and search index readers for this source.
    /// @return Selected rows and optional scores, or an error.
    template <typename Reader, typename Indexes, typename ScalarScanner>
    static Result<Evaluation> EvaluateLeaf(const std::shared_ptr<LeafPredicate>& predicate,
                                           const std::shared_ptr<LeafPredicate>& index_predicate,
                                           const Bitmap& candidates, bool needs_scores,
                                           ScalarScanner scan_scalar, Indexes* indexes) {
        Evaluation result{candidates, {}};
        if (predicate->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH) {
            auto full_text = std::dynamic_pointer_cast<FullTextMatchPredicate>(index_predicate);
            if (!full_text) {
                return Status::Invalid("Full-text predicate has no matching indexed field");
            }
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<Reader> reader,
                indexes->GetSearchReader(index_predicate->FieldName(),
                                         IndexReaderCapabilities::SearchType::FULL_TEXT_MATCH));
            using FullTextResult = IndexReaderUtils::FullTextResult<Bitmap, ScoredResult>;
            PAIMON_ASSIGN_OR_RAISE(
                FullTextResult indexed,
                IndexReaderUtils::VisitFullTextMatch(reader, full_text->GetFullTextMatch(),
                                                     candidates, needs_scores));
            result.rows &= indexed.rows;
            if (needs_scores) {
                result.scores.emplace(predicate->FieldName(), std::move(indexed.scores));
            }
            return result;
        }
        if (index_predicate) {
            PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Reader>> readers,
                                   indexes->GetReaders(index_predicate->FieldName()));
            for (const auto& reader : readers) {
                // TODO(lisizhuo.lsz): Pass remaining candidates through
                // IndexReaderUtils::VisitScalar to index readers so they can skip rows excluded
                // by earlier predicates, row ranges, or deletion vectors.
                PAIMON_ASSIGN_OR_RAISE(std::optional<Bitmap> indexed,
                                       IndexReaderUtils::VisitScalar(reader, index_predicate));
                if (indexed) {
                    result.rows &= *indexed;
                    if (result.rows.IsEmpty()) {
                        break;
                    }
                }
            }
        }
        // TODO(lisizhuo.lsz): Whether to ScanScalar depends on whether the index returns exact
        // results.
        if (!result.rows.IsEmpty()) {
            // Scalar indexes may contain false positives, even if they return a bitmap.
            PAIMON_ASSIGN_OR_RAISE(Bitmap exact_rows, scan_scalar(predicate, result.rows));
            result.rows &= exact_rows;
        }
        return result;
    }

    Result<Evaluation> Evaluate(const std::shared_ptr<Predicate>& predicate,
                                const Bitmap& candidates) const {
        if (!predicate || candidates.IsEmpty()) {
            return Evaluation{candidates, {}};
        }
        if (auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
            return evaluate_leaf_(leaf, candidates);
        }
        if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
            return EvaluateCompound(compound, candidates);
        }
        return Status::Invalid(fmt::format("unsupported file predicate {}", predicate->ToString()));
    }

    Result<Evaluation> EvaluateCompound(const std::shared_ptr<CompoundPredicate>& predicate,
                                        const Bitmap& candidates) const {
        const Function::Type type = predicate->GetFunction().GetType();
        if (type == Function::Type::OR) {
            Evaluation result;
            for (const auto& child : predicate->Children()) {
                // Every branch must see the same input, including rows matched by earlier branches.
                PAIMON_ASSIGN_OR_RAISE(Evaluation child_result, Evaluate(child, candidates));
                result.rows |= child_result.rows;
                result.scores.insert(child_result.scores.begin(), child_result.scores.end());
            }
            return result;
        }
        if (type != Function::Type::AND) {
            return Status::Invalid(
                fmt::format("unsupported compound file predicate {}", predicate->ToString()));
        }

        struct Child {
            std::shared_ptr<Predicate> predicate;
            std::optional<double> selectivity;
        };
        std::vector<Child> children;
        children.reserve(predicate->Children().size());
        for (const auto& child : predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<double> selectivity, EstimateSelectivity(child));
            children.push_back({child, selectivity});
        }
        std::stable_sort(children.begin(), children.end(), [](const Child& lhs, const Child& rhs) {
            if (!lhs.selectivity) {
                return false;
            }
            return !rhs.selectivity || *lhs.selectivity < *rhs.selectivity;
        });

        Evaluation result{candidates, {}};
        for (const auto& child : children) {
            PAIMON_ASSIGN_OR_RAISE(Evaluation child_result, Evaluate(child.predicate, result.rows));
            result.rows = std::move(child_result.rows);
            result.scores.insert(child_result.scores.begin(), child_result.scores.end());
            if (result.rows.IsEmpty()) {
                break;
            }
        }
        return result;
    }

    Result<std::optional<double>> EstimateSelectivity(
        const std::shared_ptr<Predicate>& predicate) const {
        if (!predicate) {
            return std::optional<double>();
        }
        if (auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<double> estimate, estimate_leaf_(leaf));
            if (estimate && (!std::isfinite(*estimate) || *estimate < 0.0 || *estimate > 1.0)) {
                return std::optional<double>();
            }
            return estimate;
        }
        auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
        if (!compound) {
            return Status::Invalid(
                fmt::format("unsupported file predicate {}", predicate->ToString()));
        }
        const Function::Type type = compound->GetFunction().GetType();
        if (type != Function::Type::AND && type != Function::Type::OR) {
            return Status::Invalid(
                fmt::format("unsupported compound file predicate {}", predicate->ToString()));
        }
        std::optional<double> estimate =
            type == Function::Type::AND ? std::nullopt : std::make_optional(0.0);
        // These are scheduling hints. Continue visiting all children to propagate errors.
        for (const auto& child : compound->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<double> child_estimate,
                                   EstimateSelectivity(child));
            if (type == Function::Type::AND) {
                if (child_estimate) {
                    if (estimate.has_value()) {
                        estimate = std::min(*estimate, *child_estimate);
                    } else {
                        estimate = *child_estimate;
                    }
                }
            } else if (estimate && child_estimate) {
                estimate = std::min(1.0, *estimate + *child_estimate);
            } else {
                estimate = std::nullopt;
            }
        }
        return estimate;
    }

    static std::shared_ptr<VectorSearch> FilterVectorSearch(
        const std::shared_ptr<VectorSearch>& search, const Bitmap& rows) {
        auto eligible = std::make_shared<Bitmap>(rows);
        auto user_filter = search->pre_filter;
        return search->ReplacePreFilter([eligible, user_filter](int64_t row) {
            return row >= 0 && row < Bitmap::MAX_VALUE && eligible->Contains(row) &&
                   (!user_filter || user_filter(row));
        });
    }

    static std::shared_ptr<FullTextSearch> FilterFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search, const Bitmap& rows) {
        RoaringBitmap64 eligible(rows);
        if (search->include_row_ids) {
            eligible &= *search->include_row_ids;
        }
        auto filtered = std::make_shared<FullTextSearch>(*search);
        filtered->include_row_ids = std::move(eligible);
        return filtered;
    }

    template <typename Reader, typename Indexes>
    static Status ApplyRootSearch(const std::shared_ptr<VectorSearch>& vector_search,
                                  const std::shared_ptr<FullTextSearch>& full_text_search,
                                  const std::string& score_field, Indexes* indexes,
                                  Evaluation* evaluated) {
        std::shared_ptr<ScoredResult> scored;
        if (vector_search) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<Reader> reader,
                indexes->GetSearchReader(vector_search->field_name,
                                         IndexReaderCapabilities::SearchType::VECTOR_SEARCH));
            PAIMON_ASSIGN_OR_RAISE(scored, reader->VisitVectorSearch(
                                               FilterVectorSearch(vector_search, evaluated->rows)));
        } else if (full_text_search) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<Reader> reader,
                indexes->GetSearchReader(full_text_search->field_name,
                                         IndexReaderCapabilities::SearchType::FULL_TEXT_SEARCH));
            PAIMON_ASSIGN_OR_RAISE(scored, reader->VisitFullTextSearch(FilterFullTextSearch(
                                               full_text_search, evaluated->rows)));
        } else {
            return Status::OK();
        }
        if (!scored) {
            return Status::Invalid("Root search returned a null result");
        }
        evaluated->rows &= scored->GetRowPositions();
        evaluated->scores.emplace(score_field, std::move(scored));
        return Status::OK();
    }

    static Result<std::vector<float>> AlignScores(const std::shared_ptr<ScoredResult>& source,
                                                  const Bitmap& rows) {
        if (!source) {
            return Status::Invalid("Projected search source did not return scores");
        }
        std::vector<float> aligned;
        aligned.reserve(rows.Cardinality());
        size_t score = 0;
        for (auto iter = source->GetRowPositions().Begin(); iter != source->GetRowPositions().End();
             ++iter, ++score) {
            if (rows.Contains(*iter)) {
                aligned.push_back(source->GetScores()[score]);
            }
        }
        if (aligned.size() != static_cast<size_t>(rows.Cardinality())) {
            return Status::Invalid("Projected scores do not cover all selected file rows");
        }
        return aligned;
    }

    template <typename ScoreBindings>
    static Result<std::map<std::string, std::vector<float>>> CreateOutputScores(
        const Evaluation& evaluated, const ScoreBindings& bindings,
        const std::shared_ptr<arrow::Schema>& output_schema) {
        std::map<std::string, std::vector<float>> scores;
        for (const auto& [position, source] : bindings) {
            auto found = evaluated.scores.find(source.field_name);
            if (found == evaluated.scores.end()) {
                return Status::Invalid("Projected search field did not return scores: ",
                                       source.field_name);
            }
            PAIMON_ASSIGN_OR_RAISE(std::vector<float> aligned,
                                   AlignScores(found->second, evaluated.rows));
            scores.emplace(output_schema->field(position)->name(), std::move(aligned));
        }
        return scores;
    }

 private:
    LeafEvaluator evaluate_leaf_;
    LeafEstimator estimate_leaf_;
};

}  // namespace paimon
