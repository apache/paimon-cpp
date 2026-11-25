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

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "paimon/index/index_reader_capabilities.h"
#include "paimon/predicate/function_visitor.h"
#include "paimon/realtime/index/realtime_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {
class LeafPredicate;
struct VectorSearch;
struct FullTextSearch;
struct FullTextMatch;

/// Visits predicates on one indexed field, returning realtime offsets.
/// Remain means no pruning; Skip means no candidates. Scalar visits must not omit matches.
/// IsExact() additionally guarantees that all returned candidates satisfy the predicate.
/// Unsupported predicates default to no pruning. Plugins override only the supported visits.
class PAIMON_EXPORT RealtimeIndexReader
    : public IndexReaderCapabilities,
      public FunctionVisitor<std::shared_ptr<RealtimeIndexResult>> {
 public:
    ~RealtimeIndexReader() override = default;

    /// Evaluate within eligible offsets, preserving result exactness. Null means the source range.
    virtual Result<std::shared_ptr<RealtimeIndexResult>> Evaluate(
        const std::shared_ptr<LeafPredicate>& predicate, const RoaringBitmap64* candidates);

    /// Search pre-filters and result IDs use real-time offsets, not file-local row positions.
    /// Apply filters and pinned visibility before TopK; filtering a truncated result is
    /// insufficient. Search results must be stable for the pinned snapshot even while later writes
    /// proceed.
    virtual Result<std::shared_ptr<ScoredRealtimeIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& search);

    virtual Result<std::shared_ptr<ScoredRealtimeIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& search);

    /// Returns all matching offsets without TopK truncation. When needs_scores is true,
    /// returns ScoredRealtimeIndexResult, including for an empty result; otherwise scores
    /// need not be computed.
    virtual Result<std::shared_ptr<RealtimeIndexResult>> VisitFullTextMatch(
        const std::shared_ptr<FullTextMatch>& match, bool needs_scores);

    Result<std::shared_ptr<RealtimeIndexResult>> VisitIsNotNull() override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitNotEqual(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitStartsWith(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitEndsWith(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitContains(const Literal& literal) override;
    Result<std::shared_ptr<RealtimeIndexResult>> VisitLike(const Literal& literal) override;
};

}  // namespace paimon
