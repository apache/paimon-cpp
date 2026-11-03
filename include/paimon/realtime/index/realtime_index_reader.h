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

#include "paimon/predicate/function_visitor.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {

/// Visits predicates on one indexed field, returning realtime offsets.
/// nullopt means no pruning; an empty bitmap means no candidates. False negatives are not allowed.
/// Unsupported predicates default to no pruning. Plugins override only the supported visits.
class PAIMON_EXPORT RealtimeIndexReader : public FunctionVisitor<std::optional<RoaringBitmap64>> {
 public:
    ~RealtimeIndexReader() override = default;

    Result<std::optional<RoaringBitmap64>> VisitIsNotNull() override;
    Result<std::optional<RoaringBitmap64>> VisitIsNull() override;
    Result<std::optional<RoaringBitmap64>> VisitEqual(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitNotEqual(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitLessThan(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitGreaterThan(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitIn(const std::vector<Literal>& literals) override;
    Result<std::optional<RoaringBitmap64>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::optional<RoaringBitmap64>> VisitStartsWith(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitEndsWith(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitContains(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitLike(const Literal& literal) override;
    Result<std::optional<RoaringBitmap64>> VisitFullTextMatch(
        const std::shared_ptr<FullTextMatch>& full_text_match) override;
};

}  // namespace paimon
