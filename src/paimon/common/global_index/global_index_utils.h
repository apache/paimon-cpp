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
#include <optional>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/status.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap64.h"
namespace paimon {
class GlobalIndexUtils {
 public:
    GlobalIndexUtils() = delete;
    ~GlobalIndexUtils() = delete;

    static Status CheckRelativeRowIds(::ArrowArray* c_arrow_array,
                                      const std::vector<int64_t>& relative_row_ids,
                                      std::optional<int64_t> expected_next_row_id) {
        if (!c_arrow_array) {
            return Status::Invalid("CheckRelativeRowIds failed: null c_arrow_array");
        }
        int64_t length = c_arrow_array->length;
        ScopeGuard guard([c_arrow_array]() -> void { ArrowArrayRelease(c_arrow_array); });
        if (static_cast<int64_t>(relative_row_ids.size()) != length) {
            return Status::Invalid(fmt::format(
                "relative_row_ids length {} mismatch arrow_array length {} in CheckRelativeRowIds",
                relative_row_ids.size(), length));
        }
        if (!relative_row_ids.empty() && expected_next_row_id &&
            relative_row_ids[0] != expected_next_row_id.value()) {
            return Status::Invalid(
                fmt::format("first relative_row_ids {} mismatch inner expected_next_row_id {} in "
                            "CheckRelativeRowIds",
                            relative_row_ids[0], expected_next_row_id.value()));
        }
        guard.Release();
        return Status::OK();
    }

    /// Returns the row ids of `range`.
    static RoaringBitmap64 RowIdsOf(const Range& range) {
        // `range.to` may be the largest int64 value, so add it apart from the half-open range.
        RoaringBitmap64 row_ids;
        row_ids.AddRange(range.from, range.to);
        row_ids.Add(range.to);
        return row_ids;
    }

    /// Returns the row ids of `row_ids` within `range`. Trimming a copy costs time in proportion to
    /// the containers of `row_ids`, not to the rows or the span of `range`.
    static RoaringBitmap64 RowIdsWithin(const RoaringBitmap64& row_ids, const Range& range) {
        RoaringBitmap64 result = row_ids;
        result.RemoveRange(0, range.from);
        if (range.to < RoaringBitmap64::MAX_VALUE) {
            // RemoveRange leaves out its end, so the largest int64 value is removed on its own.
            result.RemoveRange(range.to + 1, RoaringBitmap64::MAX_VALUE);
            result -= RoaringBitmap64::From({RoaringBitmap64::MAX_VALUE});
        }
        return result;
    }
};
}  // namespace paimon
