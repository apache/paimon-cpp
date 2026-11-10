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
#include <utility>

#include "paimon/macros.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {

/// Matching real-time offsets. ScoredRealtimeIndexResult additionally carries scores.
class PAIMON_EXPORT RealtimeIndexResult {
 public:
    virtual ~RealtimeIndexResult() = default;

    static Result<std::shared_ptr<RealtimeIndexResult>> Create(RoaringBitmap64 rows) {
        PAIMON_RETURN_NOT_OK(ValidateRowPositions(rows));
        return std::shared_ptr<RealtimeIndexResult>(new RealtimeIndexResult(std::move(rows)));
    }

    const RoaringBitmap64& GetRowPositions() const {
        return rows_;
    }

 protected:
    explicit RealtimeIndexResult(RoaringBitmap64 rows) : rows_(std::move(rows)) {}

    static Status ValidateRowPositions(const RoaringBitmap64& rows) {
        for (auto iter = rows.Begin(); iter != rows.End(); ++iter) {
            if (*iter < 0) {
                return Status::Invalid("Real-time search returned a negative offset");
            }
        }
        return Status::OK();
    }

 private:
    RoaringBitmap64 rows_;
};

}  // namespace paimon
