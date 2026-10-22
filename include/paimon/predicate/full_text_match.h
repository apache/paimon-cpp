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

#include <optional>
#include <string>
#include <utility>

#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {

/// A full-text condition returning every matching row without TopK truncation.
/// Project `_<field>_INDEX_SCORE` in the read schema to return its scores.
struct PAIMON_EXPORT FullTextMatch {
    FullTextMatch(const std::string& _field_name, const std::string& _query,
                  std::optional<RoaringBitmap64> _include_row_ids = std::nullopt)
        : field_name(_field_name), query(_query), include_row_ids(std::move(_include_row_ids)) {}

    bool operator==(const FullTextMatch& other) const {
        return field_name == other.field_name && query == other.query &&
               include_row_ids == other.include_row_ids;
    }

    /// Table column containing the indexed text.
    std::string field_name;
    /// JSON DSL query with the same syntax and analyzer semantics as `FullTextSearch::query`.
    std::string query;
    /// Optional eligible row ids. File readers use file-local physical row positions;
    /// An empty set matches no rows. If not set, all rows are searched.
    std::optional<RoaringBitmap64> include_row_ids;
};

}  // namespace paimon
