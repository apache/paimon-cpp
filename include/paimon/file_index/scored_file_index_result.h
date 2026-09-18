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
#include <string>
#include <vector>

#include "paimon/file_index/file_index_result.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/visibility.h"

namespace paimon {

/// File-local vector search result. Scores correspond to row positions in ascending order.
class PAIMON_EXPORT ScoredFileIndexResult : public FileIndexResult {
 public:
    static Result<std::shared_ptr<ScoredFileIndexResult>> Create(RoaringBitmap32&& row_positions,
                                                                 std::vector<float>&& scores);

    bool IsEmpty() const {
        return row_positions_.IsEmpty();
    }

    Result<bool> IsRemain() const override;

    const RoaringBitmap32& GetRowPositions() const {
        return row_positions_;
    }

    const std::vector<float>& GetScores() const {
        return scores_;
    }

    std::string ToString() const override;

 private:
    ScoredFileIndexResult(RoaringBitmap32&& row_positions, std::vector<float>&& scores);

    RoaringBitmap32 row_positions_;
    std::vector<float> scores_;
};

}  // namespace paimon
