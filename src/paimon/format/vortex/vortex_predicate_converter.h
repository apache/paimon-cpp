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

#include <cstdint>
#include <memory>

#include "paimon/format/vortex/vortex_ffi_util.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::vortex {

enum class MembershipPushdownMode { AUTO, FORCE, OFF };

struct VortexPredicateOptions {
    MembershipPushdownMode membership_pushdown = MembershipPushdownMode::AUTO;
    // Per-leaf comparison and boolean node budget after deduplication/range compression.
    uint32_t max_membership_nodes = 64;
};

/// Converts Paimon predicates into Vortex scan filters, leaving unsupported conditions to the
/// caller's residual filter.
class VortexPredicateConverter {
 public:
    VortexPredicateConverter() = delete;
    ~VortexPredicateConverter() = delete;

    /// @param predicate The predicate to convert.
    /// @param file_schema Stored column types, including timestamp units and zones.
    /// @param options Membership pushdown policy and expression budget.
    /// @return An owned expression, or nullptr if no condition can be pushed.
    static VxExpressionPtr Convert(const std::shared_ptr<Predicate>& predicate,
                                   const std::shared_ptr<arrow::Schema>& file_schema,
                                   const VortexPredicateOptions& options = {});
};

}  // namespace paimon::vortex
