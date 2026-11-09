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

#include "paimon/realtime/offset_range.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
class CompoundPredicate;
class LeafPredicate;
class Predicate;
class RealtimeIndexSet;

/// Evaluates compound predicates using field index readers.
class RealtimeIndexEvaluator {
 public:
    RealtimeIndexEvaluator() = delete;

    static Result<std::optional<RoaringBitmap64>> Evaluate(
        const std::shared_ptr<RealtimeIndexSet>& indexes,
        const std::shared_ptr<Predicate>& predicate, const OffsetRange& offset_range);

 private:
    static Result<std::optional<RoaringBitmap64>> EvaluateCompound(
        const std::shared_ptr<RealtimeIndexSet>& indexes,
        const std::shared_ptr<CompoundPredicate>& compound, const OffsetRange& offset_range);

    static Result<std::optional<RoaringBitmap64>> EvaluateLeaf(
        const std::shared_ptr<RealtimeIndexSet>& indexes,
        const std::shared_ptr<LeafPredicate>& leaf, const OffsetRange& offset_range);
};

}  // namespace paimon
