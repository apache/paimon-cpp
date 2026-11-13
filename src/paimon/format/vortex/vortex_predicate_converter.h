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

#include "paimon/format/vortex/vortex_defs.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Predicate;
}  // namespace paimon

namespace paimon::vortex {

/// Converts Paimon predicates into Vortex scan filters, leaving unsupported conditions to the
/// caller's residual filter.
class VortexPredicateConverter {
 public:
    VortexPredicateConverter() = delete;
    ~VortexPredicateConverter() = delete;

    /// @param predicate The predicate to convert, with literal types already checked by the
    /// caller (for example, through PredicateValidator).
    /// @param file_schema Stored column types, including timestamp units and zones.
    /// @param options Maximum input list length for IN and NOT IN pushdown.
    /// @return An owned expression, nullptr if no condition can be pushed, or an error if
    /// the predicate is malformed, its field types do not match the file, or native expression
    /// construction fails.
    static Result<VxExpressionPtr> Convert(const std::shared_ptr<Predicate>& predicate,
                                           const std::shared_ptr<arrow::Schema>& file_schema,
                                           const VortexPredicateOptions& options);
};

}  // namespace paimon::vortex
