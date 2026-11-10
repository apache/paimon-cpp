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

#include <memory>
#include <optional>

#include "paimon/visibility.h"

namespace paimon {
class LeafPredicate;

/// Search capabilities and predicate scheduling hints shared by index readers, independently of
/// their result types and row ID domains.
class PAIMON_EXPORT IndexReaderCapabilities {
 public:
    enum class SearchType { FULL_TEXT_MATCH, FULL_TEXT_SEARCH, VECTOR_SEARCH };

    virtual ~IndexReaderCapabilities() = default;

    /// Whether this reader can execute the requested search without probing it with a query.
    /// The default implementation returns false.
    virtual bool SupportsSearch(SearchType search_type) const;

    /// Estimates the fraction of this reader's indexed rows expected to match the predicate.
    /// Returns a value in [0, 1], or std::nullopt if no estimate is available.
    /// Values outside [0, 1] are also treated as unknown by evaluators.
    /// Estimates only guide predicate execution order and must never exclude rows.
    /// The default implementation returns std::nullopt.
    virtual std::optional<double> EstimateSelectivity(
        const std::shared_ptr<LeafPredicate>& predicate) const;
};

}  // namespace paimon
