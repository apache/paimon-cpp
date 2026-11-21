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

#include "paimon/file_index/file_index_reader.h"

#include <utility>

#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/status.h"

namespace paimon {
Result<std::shared_ptr<FileIndexResult>> FileIndexReader::Evaluate(
    const std::shared_ptr<LeafPredicate>& predicate, const RoaringBitmap32* candidates) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileIndexResult> result,
        PredicateUtils::VisitPredicate<std::shared_ptr<FileIndexResult>>(predicate, this));
    if (auto bitmap = std::dynamic_pointer_cast<BitmapIndexResult>(result)) {
        if (!candidates) {
            return result;
        }
        return std::make_shared<BitmapIndexResult>(
            [bitmap, rows = *candidates]() -> Result<RoaringBitmap32> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* matches, bitmap->GetBitmap());
                RoaringBitmap32 filtered(rows);
                filtered &= *matches;
                return filtered;
            },
            bitmap->IsExact());
    } else if (std::dynamic_pointer_cast<Remain>(result)) {
        if (candidates) {
            return std::make_shared<BitmapIndexResult>(
                [rows = *candidates]() -> Result<RoaringBitmap32> { return rows; },
                /*is_exact=*/false);
        }
        return result;
    } else if (std::dynamic_pointer_cast<Skip>(result)) {
        return result;
    }
    return Status::Invalid("Scalar evaluation requires BitmapIndexResult, Skip or Remain");
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitIsNotNull() {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitIsNull() {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitStartsWith(const Literal& prefix) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitEndsWith(const Literal& suffix) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitContains(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitLike(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitLessThan(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitNotEqual(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitLessOrEqual(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitEqual(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitGreaterThan(const Literal& literal) {
    return FileIndexResult::Remain();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    if (literals.empty()) {
        return Status::Invalid("literals cannot be empty in In predicate");
    }
    std::shared_ptr<FileIndexResult> file_index_result;
    for (const Literal& key : literals) {
        if (!file_index_result) {
            PAIMON_ASSIGN_OR_RAISE(file_index_result, VisitEqual(key));
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> inner_result, VisitEqual(key));
            PAIMON_ASSIGN_OR_RAISE(file_index_result, file_index_result->Or(inner_result));
        }
    }
    return file_index_result;
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    if (literals.empty()) {
        return Status::Invalid("literals cannot be empty in NotIn predicate");
    }
    std::shared_ptr<FileIndexResult> file_index_result;
    for (const Literal& key : literals) {
        if (!file_index_result) {
            PAIMON_ASSIGN_OR_RAISE(file_index_result, VisitNotEqual(key));
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> inner_result,
                                   VisitNotEqual(key));
            PAIMON_ASSIGN_OR_RAISE(file_index_result, file_index_result->And(inner_result));
        }
    }
    return file_index_result;
}

Result<std::shared_ptr<ScoredFileIndexResult>> FileIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return Status::NotImplemented("This file index does not support vector search");
}

Result<std::shared_ptr<ScoredFileIndexResult>> FileIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Status::NotImplemented("This file index does not support full text search");
}

Result<std::shared_ptr<FileIndexResult>> FileIndexReader::VisitFullTextMatch(
    const std::shared_ptr<FullTextMatch>& full_text_match, bool needs_scores) {
    return Status::NotImplemented("This file index does not support complete full-text matching");
}
}  // namespace paimon
