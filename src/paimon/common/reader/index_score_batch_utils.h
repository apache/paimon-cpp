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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "arrow/c/abi.h"
#include "paimon/reader/batch_reader.h"

namespace arrow {
class MemoryPool;
class Schema;
class StructArray;
}  // namespace arrow

namespace paimon {

/// Completes score columns and restores projection; the caller supplies row-to-score lookup.
class IndexScoreBatchUtils {
 public:
    IndexScoreBatchUtils() = delete;
    ~IndexScoreBatchUtils() = delete;

    template <typename Scores, typename ScoreLookup>
    static Result<BatchReader::ReadBatchWithBitmap> CompleteScores(
        BatchReader::ReadBatchWithBitmap batch, const std::shared_ptr<arrow::StructArray>& data,
        const std::shared_ptr<arrow::Schema>& output_schema, const Scores& scores,
        ScoreLookup lookup, const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
        ScoreLookups lookups;
        for (const auto& entry : scores) {
            lookups.emplace(entry.first,
                            [&lookup, field_name = entry.first, values = &entry.second](
                                int64_t row, size_t selected) -> Result<float> {
                                return lookup(field_name, *values, row, selected);
                            });
        }
        return CompleteScoresImpl(std::move(batch), data, output_schema, lookups, arrow_pool);
    }

 private:
    // Maps score field names to callbacks invoked only for selected rows. The callback receives
    // the zero-based row index in the original batch and the zero-based ordinal among selected
    // rows in that batch, and returns the row's score or an error.
    using ScoreLookups =
        std::map<std::string, std::function<Result<float>(int64_t row, size_t selected)>>;

    static Result<BatchReader::ReadBatchWithBitmap> CompleteScoresImpl(
        BatchReader::ReadBatchWithBitmap batch, const std::shared_ptr<arrow::StructArray>& data,
        const std::shared_ptr<arrow::Schema>& output_schema, const ScoreLookups& lookups,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool);
};

}  // namespace paimon
