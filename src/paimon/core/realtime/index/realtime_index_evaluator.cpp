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

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexEvaluator::Evaluate(
    const std::shared_ptr<RealtimeIndexSet>& indexes, const std::shared_ptr<Predicate>& predicate,
    const OffsetRange& offset_range, const std::shared_ptr<MemoryPool>& pool) {
    if (offset_range.begin < 0 || offset_range.begin > offset_range.end) {
        return Status::Invalid("real-time index query offset range is invalid");
    }
    if (offset_range.Empty()) {
        return RealtimeIndexResult::Skip();
    }
    if (!indexes || !predicate) {
        return RealtimeIndexResult::Remain();
    }
    const Function::Type function = predicate->GetFunction().GetType();
    if (function == Function::Type::AND || function == Function::Type::OR) {
        const std::shared_ptr<CompoundPredicate> compound =
            std::dynamic_pointer_cast<CompoundPredicate>(predicate);
        if (!compound || compound->Children().empty()) {
            return Status::Invalid("invalid or empty real-time index compound predicate");
        }
        return EvaluateCompound(indexes, compound, offset_range, pool);
    }
    const std::shared_ptr<LeafPredicate> leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (!leaf) {
        return Status::Invalid("invalid real-time index leaf predicate");
    }
    return EvaluateLeaf(indexes, leaf, offset_range, pool);
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexEvaluator::EvaluateCompound(
    const std::shared_ptr<RealtimeIndexSet>& indexes,
    const std::shared_ptr<CompoundPredicate>& compound, const OffsetRange& offset_range,
    const std::shared_ptr<MemoryPool>& pool) {
    const Function::Type function = compound->GetFunction().GetType();
    std::shared_ptr<RealtimeIndexResult> result;
    for (const std::shared_ptr<Predicate>& child : compound->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexResult> evaluated,
                               Evaluate(indexes, child, offset_range, pool));
        if (!result) {
            result = std::move(evaluated);
        } else if (function == Function::Type::AND) {
            PAIMON_ASSIGN_OR_RAISE(result, result->And(evaluated));
        } else {
            PAIMON_ASSIGN_OR_RAISE(result, result->Or(evaluated));
        }
        PAIMON_ASSIGN_OR_RAISE(bool remain, result->IsRemain());
        if ((function == Function::Type::AND && !remain) ||
            (function == Function::Type::OR &&
             std::dynamic_pointer_cast<RemainIndexResult<RoaringBitmap64>>(result))) {
            break;
        }
    }
    return result;
}

Result<std::shared_ptr<RealtimeIndexResult>> RealtimeIndexEvaluator::EvaluateLeaf(
    const std::shared_ptr<RealtimeIndexSet>& indexes, const std::shared_ptr<LeafPredicate>& leaf,
    const OffsetRange& offset_range, const std::shared_ptr<MemoryPool>& pool) {
    std::shared_ptr<RealtimeIndexResult> result = RealtimeIndexResult::Remain();
    for (const std::shared_ptr<RealtimeIndex>& index :
         indexes->FindIndexes(leaf->FieldName(), leaf->GetFieldType())) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeIndexReader> reader,
                               index->CreateReader(offset_range, pool));
        if (!reader) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<RealtimeIndexResult> indexed,
            PredicateUtils::VisitPredicate<std::shared_ptr<RealtimeIndexResult>>(leaf, reader));
        if (!indexed) {
            return Status::Invalid("Scalar index returned a null result");
        }
        // These readers guarantee no false negatives. One exact result fully resolves
        // this leaf; otherwise their candidate supersets can be intersected for pruning.
        if (indexed->IsExact()) {
            return indexed;
        }
        PAIMON_ASSIGN_OR_RAISE(result, result->And(indexed));
        PAIMON_ASSIGN_OR_RAISE(bool remain, result->IsRemain());
        if (!remain) {
            break;
        }
    }
    return result;
}

}  // namespace paimon
