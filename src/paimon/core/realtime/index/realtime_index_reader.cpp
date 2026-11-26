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

#include "paimon/realtime/index/realtime_index_reader.h"

#include <utility>

#include "paimon/common/index/index_reader_utils.h"
#include "paimon/macros.h"

namespace paimon {

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::Evaluate(
    const std::shared_ptr<LeafPredicate>& predicate, const RoaringBitmap64* candidates) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<RealtimeIndexResult> result,
        PredicateUtils::VisitPredicate<std::shared_ptr<RealtimeIndexResult>>(predicate, this));
    return IndexReaderUtils::RestrictScalarResult(result, candidates);
}

Result<std::shared_ptr<ScoredRealtimeIndexResult>> RealtimeIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>&) {
    return Status::NotImplemented("Real-time index does not support vector search");
}

Result<std::shared_ptr<ScoredRealtimeIndexResult>> RealtimeIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>&) {
    return Status::NotImplemented("Real-time index does not support full-text search");
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitFullTextMatch(
    const std::shared_ptr<FullTextMatch>&, bool) {
    return Status::NotImplemented("Real-time index does not support full-text match");
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitIsNotNull() {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitIsNull() {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitEqual(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitNotEqual(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitLessThan(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitLessOrEqual(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitGreaterThan(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitGreaterOrEqual(
    const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    std::shared_ptr<RealtimeIndexResult> result = RealtimeIndexResult::Skip();
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexResult> child, VisitEqual(literal));
        PAIMON_ASSIGN_OR_RAISE(result, result->Or(child));
        if (std::dynamic_pointer_cast<RemainIndexResult<RoaringBitmap64>>(result)) {
            return result;
        }
    }
    return result;
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    std::shared_ptr<RealtimeIndexResult> result;
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexResult> child, VisitNotEqual(literal));
        if (!result) {
            result = std::move(child);
        } else {
            PAIMON_ASSIGN_OR_RAISE(result, result->And(child));
        }
    }
    return result ? result : RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitStartsWith(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitEndsWith(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitContains(const Literal&) {
    return RealtimeIndexResult::Remain();
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexReader::VisitLike(const Literal&) {
    return RealtimeIndexResult::Remain();
}

}  // namespace paimon
