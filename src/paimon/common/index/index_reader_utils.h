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

#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/predicate/full_text_match.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/realtime/index/realtime_index_reader.h"

namespace paimon {

/// Reader selection and conversion of backend results to row candidates and optional scores.
class IndexReaderUtils {
 public:
    IndexReaderUtils() = delete;
    ~IndexReaderUtils() = delete;

    template <typename Bitmap>
    struct FullTextResult {
        Bitmap rows;
        std::shared_ptr<ScoredIndexResult<Bitmap>> scores;
    };

    template <typename Reader>
    static Result<std::shared_ptr<Reader>> GetSearchReader(
        const std::vector<std::shared_ptr<Reader>>& readers,
        IndexReaderCapabilities::SearchType type, const std::string& multiple_error,
        const std::string& missing_error) {
        std::shared_ptr<Reader> selected;
        for (const auto& reader : readers) {
            if (!reader->SupportsSearch(type)) {
                continue;
            }
            if (selected) {
                return Status::Invalid(multiple_error);
            }
            selected = reader;
        }
        if (!selected) {
            return Status::NotImplemented(missing_error);
        }
        return selected;
    }

    template <typename Reader, typename Indexes>
    static Result<std::optional<double>> EstimateSelectivity(
        const std::shared_ptr<LeafPredicate>& predicate, Indexes* indexes) {
        if (predicate->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<Reader> reader,
                indexes->GetSearchReader(predicate->FieldName(),
                                         IndexReaderCapabilities::SearchType::FULL_TEXT_MATCH));
            return reader->EstimateSelectivity(predicate);
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Reader>> readers,
                               indexes->GetReaders(predicate->FieldName()));
        std::optional<double> estimate;
        for (const auto& reader : readers) {
            auto value = reader->EstimateSelectivity(predicate);
            if (value && std::isfinite(*value) && *value >= 0.0 && *value <= 1.0 &&
                (!estimate || *value < *estimate)) {
                estimate = value;
            }
        }
        return estimate;
    }

    /// Extract scalar row selection at the reader boundary. Remain is unrestricted;
    /// Skip is empty. Neither sentinel needs to own a bitmap.
    template <typename Bitmap>
    static Result<std::optional<Bitmap>> GetScalarRows(
        const std::shared_ptr<IndexResult<Bitmap>>& result) {
        if (!result) {
            return Status::Invalid("Scalar index returned a null result");
        }
        if (auto bitmap = std::dynamic_pointer_cast<BitmapResult<Bitmap>>(result)) {
            PAIMON_ASSIGN_OR_RAISE(const Bitmap* rows, bitmap->GetBitmap());
            return std::optional<Bitmap>(*rows);
        }
        if (std::dynamic_pointer_cast<SkipIndexResult<Bitmap>>(result)) {
            return std::optional<Bitmap>(Bitmap());
        }
        if (std::dynamic_pointer_cast<RemainIndexResult<Bitmap>>(result)) {
            return std::optional<Bitmap>();
        }
        return Status::Invalid("Scalar evaluation requires a bitmap, Skip or Remain");
    }

    /// Restrict a scalar result without changing the predicate's exactness.
    template <typename Bitmap>
    static Result<std::shared_ptr<IndexResult<Bitmap>>> RestrictScalarResult(
        const std::shared_ptr<IndexResult<Bitmap>>& result, const Bitmap* candidates) {
        if (!result) {
            return Status::Invalid("Scalar index returned a null result");
        }
        if (auto bitmap = std::dynamic_pointer_cast<BitmapResult<Bitmap>>(result)) {
            if (!candidates) {
                return result;
            }
            return std::make_shared<BitmapResult<Bitmap>>(
                [bitmap, rows = *candidates]() -> Result<Bitmap> {
                    PAIMON_ASSIGN_OR_RAISE(const Bitmap* matches, bitmap->GetBitmap());
                    return Bitmap::And(rows, *matches);
                },
                bitmap->IsExact());
        }
        if (std::dynamic_pointer_cast<RemainIndexResult<Bitmap>>(result)) {
            if (candidates) {
                return std::make_shared<BitmapResult<Bitmap>>(
                    [rows = *candidates]() -> Result<Bitmap> { return rows; },
                    /*is_exact=*/false);
            }
            return result;
        }
        if (std::dynamic_pointer_cast<SkipIndexResult<Bitmap>>(result)) {
            return result;
        }
        return Status::Invalid("Scalar evaluation requires a bitmap, Skip or Remain");
    }

    /// Each index evaluates the same input scope: inexact results cannot discard matches.
    template <typename Bitmap, typename Reader>
    static Result<std::shared_ptr<IndexResult<Bitmap>>> EvaluateScalar(
        const std::vector<std::shared_ptr<Reader>>& readers,
        const std::shared_ptr<LeafPredicate>& predicate, const Bitmap* candidates) {
        if (candidates && candidates->IsEmpty()) {
            return IndexResult<Bitmap>::Skip();
        }
        std::shared_ptr<IndexResult<Bitmap>> combined = IndexResult<Bitmap>::Remain();
        for (const auto& reader : readers) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexResult<Bitmap>> result,
                                   reader->Evaluate(predicate, candidates));
            if (!result) {
                return Status::Invalid("Scalar index returned a null result");
            }
            if (result->IsExact()) {
                return result;
            }
            PAIMON_ASSIGN_OR_RAISE(combined, combined->And(result));
            PAIMON_ASSIGN_OR_RAISE(bool remain, combined->IsRemain());
            if (!remain) {
                return IndexResult<Bitmap>::Skip();
            }
        }
        return combined;
    }

    template <typename Reader, typename Bitmap>
    static Result<FullTextResult<Bitmap>> VisitFullTextMatch(
        const std::shared_ptr<Reader>& reader, const std::shared_ptr<FullTextMatch>& match,
        const Bitmap& candidates, bool needs_scores) {
        using ScoredResult = ScoredIndexResult<Bitmap>;
        auto filtered = std::make_shared<FullTextMatch>(*match);
        RoaringBitmap64 eligible(candidates);
        if (filtered->include_row_ids) {
            *filtered->include_row_ids &= eligible;
        } else {
            filtered->include_row_ids = std::move(eligible);
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexResult<Bitmap>> result,
                               reader->VisitFullTextMatch(filtered, needs_scores));
        if (!result) {
            return Status::Invalid("Full-text match returned a null result");
        }
        std::shared_ptr<ScoredResult> scored;
        if (needs_scores) {
            scored = std::dynamic_pointer_cast<ScoredResult>(result);
            if (!scored) {
                return Status::Invalid("Full-text index did not return requested scores");
            }
        }
        if (auto scored_result = std::dynamic_pointer_cast<ScoredResult>(result)) {
            return FullTextResult<Bitmap>{scored_result->GetRowPositions(), std::move(scored)};
        }
        PAIMON_ASSIGN_OR_RAISE(std::optional<Bitmap> rows, GetScalarRows(result));
        if (!rows) {
            return Status::Invalid("Full-text index did not return row positions");
        }
        return FullTextResult<Bitmap>{std::move(*rows), nullptr};
    }
};

}  // namespace paimon
