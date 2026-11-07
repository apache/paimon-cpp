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

#include "paimon/core/realtime/index/realtime_index_evaluator.h"

#include <utility>

#include "paimon/core/realtime/index/realtime_index_set.h"
#include "paimon/macros.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/realtime/index/realtime_index.h"
#include "paimon/realtime/index/realtime_index_reader.h"

namespace paimon {

Result<std::optional<RoaringBitmap64>> RealtimeIndexEvaluator::Evaluate(
    const std::shared_ptr<RealtimeIndexSet>& indexes, const std::shared_ptr<Predicate>& predicate,
    const OffsetRange& offset_range) {
    if (offset_range.begin < 0 || offset_range.begin > offset_range.end) {
        return Status::Invalid("real-time index query offset range is invalid");
    }
    if (offset_range.Empty()) {
        return std::optional<RoaringBitmap64>(RoaringBitmap64());
    }
    if (!indexes || !predicate) {
        return std::optional<RoaringBitmap64>();
    }
    const Function::Type function = predicate->GetFunction().GetType();
    if (function == Function::Type::AND || function == Function::Type::OR) {
        const std::shared_ptr<CompoundPredicate> compound =
            std::dynamic_pointer_cast<CompoundPredicate>(predicate);
        if (!compound || compound->Children().empty()) {
            return Status::Invalid("invalid or empty real-time index compound predicate");
        }
        return EvaluateCompound(indexes, compound, offset_range);
    }
    const std::shared_ptr<LeafPredicate> leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (!leaf) {
        return std::optional<RoaringBitmap64>();
    }
    return EvaluateLeaf(indexes, leaf, offset_range);
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexEvaluator::EvaluateCompound(
    const std::shared_ptr<RealtimeIndexSet>& indexes,
    const std::shared_ptr<CompoundPredicate>& compound, const OffsetRange& offset_range) {
    const Function::Type function = compound->GetFunction().GetType();
    std::optional<RoaringBitmap64> result;
    for (const std::shared_ptr<Predicate>& child : compound->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap64> candidates,
                               Evaluate(indexes, child, offset_range));
        if (!candidates) {
            if (function == Function::Type::OR) {
                return std::optional<RoaringBitmap64>();
            }
            continue;
        }
        if (!result) {
            result = std::move(candidates);
        } else if (function == Function::Type::AND) {
            *result &= *candidates;
        } else {
            *result |= *candidates;
        }
        if (function == Function::Type::AND && result->IsEmpty()) {
            break;
        }
    }
    return result;
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexEvaluator::EvaluateLeaf(
    const std::shared_ptr<RealtimeIndexSet>& indexes, const std::shared_ptr<LeafPredicate>& leaf,
    const OffsetRange& offset_range) {
    std::optional<RoaringBitmap64> result;
    for (const std::shared_ptr<RealtimeIndex>& index :
         indexes->FindIndexes(leaf->FieldName(), leaf->GetFieldType())) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexReader> reader,
                               index->CreateReader(offset_range));
        if (!reader) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::optional<RoaringBitmap64> candidates,
            PredicateUtils::VisitPredicate<std::optional<RoaringBitmap64>>(leaf, reader));
        if (!candidates) {
            continue;
        }
        if (!result) {
            result = std::move(candidates);
        } else {
            *result &= *candidates;
        }
        if (result->IsEmpty()) {
            break;
        }
    }
    return result;
}

}  // namespace paimon
