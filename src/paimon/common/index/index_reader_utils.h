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
#include <type_traits>
#include <utility>
#include <vector>

#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/scored_file_index_result.h"
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

    template <typename Bitmap, typename ScoredResult>
    struct FullTextResult {
        Bitmap rows;
        std::shared_ptr<ScoredResult> scores;
    };

    template <typename Bitmap>
    using FullTextScoredResult =
        std::conditional_t<std::is_same_v<Bitmap, RoaringBitmap32>, ScoredFileIndexResult,
                           ScoredRealtimeIndexResult>;

    template <typename Reader>
    static Result<std::shared_ptr<Reader>> GetSearchReader(
        const std::vector<std::shared_ptr<Reader>>& readers,
        IndexReaderCapabilities::SearchType type, const std::string& multiple_error,
        const std::string& missing_error) {
        std::shared_ptr<Reader> selected;
        for (const auto& reader : readers) {
            if (!reader) {
                return Status::Invalid("Index source returned a null reader");
            }
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

    static Result<std::optional<RoaringBitmap32>> VisitScalar(
        const std::shared_ptr<FileIndexReader>& reader,
        const std::shared_ptr<LeafPredicate>& predicate) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileIndexResult> result,
            PredicateUtils::VisitPredicate<std::shared_ptr<FileIndexResult>>(predicate, reader));
        if (!result) {
            return Status::Invalid("Scalar index returned a null result");
        }
        if (auto bitmap = std::dynamic_pointer_cast<BitmapIndexResult>(result)) {
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* rows, bitmap->GetBitmap());
            return std::optional<RoaringBitmap32>(*rows);
        }
        if (std::dynamic_pointer_cast<Skip>(result)) {
            return std::optional<RoaringBitmap32>(RoaringBitmap32());
        }
        if (std::dynamic_pointer_cast<Remain>(result)) {
            return std::optional<RoaringBitmap32>();
        }
        return Status::Invalid("Invalid scalar index result for predicate ", predicate->ToString());
    }

    static Result<std::optional<RoaringBitmap64>> VisitScalar(
        const std::shared_ptr<RealtimeIndexReader>& reader,
        const std::shared_ptr<LeafPredicate>& predicate) {
        return PredicateUtils::VisitPredicate<std::optional<RoaringBitmap64>>(predicate, reader);
    }

    template <typename Reader, typename Bitmap>
    static Result<FullTextResult<Bitmap, FullTextScoredResult<Bitmap>>> VisitFullTextMatch(
        const std::shared_ptr<Reader>& reader, const std::shared_ptr<FullTextMatch>& match,
        const Bitmap& candidates, bool needs_scores) {
        using ScoredResult = FullTextScoredResult<Bitmap>;
        auto filtered = std::make_shared<FullTextMatch>(*match);
        RoaringBitmap64 eligible(candidates);
        if (filtered->include_row_ids) {
            *filtered->include_row_ids &= eligible;
        } else {
            filtered->include_row_ids = std::move(eligible);
        }
        PAIMON_ASSIGN_OR_RAISE(auto result, reader->VisitFullTextMatch(filtered, needs_scores));
        if (!result) {
            return Status::Invalid("Full-text match returned a null result");
        }
        if (needs_scores) {
            auto scored = std::dynamic_pointer_cast<ScoredResult>(result);
            if (!scored) {
                return Status::Invalid("Full-text index did not return requested scores");
            }
            return FullTextResult<Bitmap, ScoredResult>{scored->GetRowPositions(),
                                                        std::move(scored)};
        }
        if constexpr (std::is_same_v<Bitmap, RoaringBitmap32>) {
            auto bitmap = std::dynamic_pointer_cast<BitmapIndexResult>(result);
            if (!bitmap) {
                return Status::Invalid("Full-text index did not return a bitmap");
            }
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* rows, bitmap->GetBitmap());
            return FullTextResult<Bitmap, ScoredResult>{*rows, nullptr};
        } else {
            return FullTextResult<Bitmap, ScoredResult>{result->GetRowPositions(), nullptr};
        }
    }
};

}  // namespace paimon
