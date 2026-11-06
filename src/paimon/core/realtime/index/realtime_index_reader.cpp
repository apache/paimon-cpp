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

#include "paimon/macros.h"

namespace paimon {

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitIsNotNull() {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitIsNull() {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitEqual(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitNotEqual(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitLessThan(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitLessOrEqual(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitGreaterThan(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitGreaterOrEqual(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    RoaringBitmap64 result;
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap64> candidates, VisitEqual(literal));
        if (!candidates) {
            return std::optional<RoaringBitmap64>();
        }
        result |= *candidates;
    }
    return std::optional<RoaringBitmap64>(std::move(result));
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    std::optional<RoaringBitmap64> result;
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(std::optional<RoaringBitmap64> candidates, VisitNotEqual(literal));
        if (!candidates) {
            continue;
        }
        if (!result) {
            result = std::move(candidates);
        } else {
            *result &= *candidates;
        }
    }
    return result;
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitStartsWith(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitEndsWith(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitContains(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

Result<std::optional<RoaringBitmap64>> RealtimeIndexReader::VisitLike(const Literal&) {
    return std::optional<RoaringBitmap64>();
}

}  // namespace paimon
