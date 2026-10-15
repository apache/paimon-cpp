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

#include "paimon/common/global_index/offset_global_index_reader.h"

#include <limits>
#include <utility>
#include <vector>

#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
namespace {
std::shared_ptr<ScoredGlobalIndexResult> EmptyScoredResult() {
    return std::make_shared<BitmapScoredGlobalIndexResult>(RoaringBitmap64(), std::vector<float>());
}
}  // namespace

OffsetGlobalIndexReader::OffsetGlobalIndexReader(std::shared_ptr<GlobalIndexReader>&& wrapped,
                                                 int64_t offset, int64_t to)
    : OffsetGlobalIndexReader(std::move(wrapped), offset, to,
                              /*skip_to=*/std::numeric_limits<int64_t>::min()) {}

OffsetGlobalIndexReader::OffsetGlobalIndexReader(std::shared_ptr<GlobalIndexReader>&& wrapped,
                                                 int64_t offset, int64_t to, int64_t skip_to)
    : wrapped_(std::move(wrapped)), offset_(offset), to_(to), skip_to_(skip_to) {}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitIsNotNull() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitIsNotNull());
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitIsNull() {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitIsNull());
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitNotEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitLessThan(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitLessOrEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitGreaterThan(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitGreaterOrEqual(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitIn(literals));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitNotIn(literals));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitStartsWith(prefix));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitEndsWith(suffix));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitContains(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                           wrapped_->VisitContains(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::VisitLike(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result, wrapped_->VisitLike(literal));
    return ApplyOffset(result);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> OffsetGlobalIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    std::shared_ptr<VectorSearch> rewritten_search = vector_search;
    if (vector_search && vector_search->pre_filter) {
        auto original_filter = vector_search->pre_filter;
        auto offset = offset_;
        rewritten_search =
            vector_search->ReplacePreFilter([original_filter, offset](int64_t local_id) -> bool {
                return original_filter(local_id + offset);
            });
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                           wrapped_->VisitVectorSearch(rewritten_search));
    return ApplyScoredOffset(result);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> OffsetGlobalIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    std::shared_ptr<FullTextSearch> local_search = full_text_search;
    if (full_text_search) {
        if (full_text_search->limit <= 0) {
            return Status::Invalid("full-text search requires a positive limit");
        }
        // Rows up to `skip_to_` are ranked by a reader over an overlapping row range.
        if (skip_to_ >= to_) {
            return EmptyScoredResult();
        }
        bool skips_rows = skip_to_ >= offset_;
        if (full_text_search->include_row_ids) {
            local_search = full_text_search->OffsetRange(offset_, to_);
            if (skips_rows) {
                local_search->include_row_ids->RemoveRange(0, skip_to_ - offset_ + 1);
            }
            if (local_search->include_row_ids->IsEmpty()) {
                return EmptyScoredResult();
            }
        } else if (skips_rows) {
            // `local_to` may be the largest int64 value, so add it apart from the half-open range.
            int64_t local_to = to_ - offset_;
            RoaringBitmap64 local_row_ids;
            local_row_ids.AddRange(skip_to_ - offset_ + 1, local_to);
            local_row_ids.Add(local_to);
            local_search = std::make_shared<FullTextSearch>(
                full_text_search->field_name, full_text_search->query, full_text_search->limit,
                std::move(local_row_ids));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ScoredGlobalIndexResult> result,
                           wrapped_->VisitFullTextSearch(local_search));
    return ApplyScoredOffset(result);
}

Result<std::shared_ptr<GlobalIndexResult>> OffsetGlobalIndexReader::ApplyOffset(
    const std::shared_ptr<GlobalIndexResult>& result) {
    if (result == nullptr) {
        return result;
    }
    return result->AddOffset(offset_);
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> OffsetGlobalIndexReader::ApplyScoredOffset(
    const std::shared_ptr<ScoredGlobalIndexResult>& result) {
    if (result == nullptr) {
        return result;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> offset_result,
                           result->AddOffset(offset_));
    auto scored_result = std::dynamic_pointer_cast<ScoredGlobalIndexResult>(offset_result);
    if (!scored_result) {
        return Status::Invalid(
            "AddOffset on ScoredGlobalIndexResult did not return ScoredGlobalIndexResult");
    }
    return scored_result;
}

}  // namespace paimon
