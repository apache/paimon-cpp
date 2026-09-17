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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/global_index/indexed_split.h"

namespace paimon {
class IndexedSplitImpl : public IndexedSplit {
 public:
    static constexpr int64_t MAGIC = -938472394838495695L;
    static constexpr int32_t VERSION = 1;

    IndexedSplitImpl(const std::shared_ptr<DataSplitImpl>& data_split,
                     const std::vector<Range>& row_ranges, const std::vector<float>& scores)
        : data_split_(data_split), row_ranges_(row_ranges), scores_(scores) {}
    IndexedSplitImpl(const std::shared_ptr<DataSplitImpl>& data_split,
                     const std::vector<Range>& row_ranges)
        : IndexedSplitImpl(data_split, row_ranges, {}) {}

    std::shared_ptr<DataSplit> GetDataSplit() const override {
        return data_split_;
    }
    const std::vector<Range>& RowRanges() const override {
        return row_ranges_;
    }
    const std::vector<float>& Scores() const override {
        return scores_;
    }

    bool operator==(const IndexedSplitImpl& other) const {
        if (this == &other) {
            return true;
        }
        bool score_equal =
            (scores_.size() == other.scores_.size()) &&
            std::equal(scores_.begin(), scores_.end(), other.scores_.begin(),
                       [](float left, float right) { return std::abs(left - right) <= kEpsilon; });
        return score_equal && *data_split_ == *(other.data_split_) &&
               row_ranges_ == other.row_ranges_;
    }

    bool TEST_Equal(const IndexedSplitImpl& other) const {
        if (this == &other) {
            return true;
        }
        bool score_equal =
            (scores_.size() == other.scores_.size()) &&
            std::equal(scores_.begin(), scores_.end(), other.scores_.begin(),
                       [](float left, float right) { return std::abs(left - right) <= kEpsilon; });

        return score_equal && data_split_->TEST_Equal(*other.data_split_) &&
               row_ranges_ == other.row_ranges_;
    }

    std::string ToString() const {
        std::vector<std::string> row_ranges_str_vec;
        row_ranges_str_vec.reserve(row_ranges_.size());
        for (const auto& range : row_ranges_) {
            row_ranges_str_vec.push_back(range.ToString());
        }
        std::string row_ranges_str = fmt::format("[{}]", fmt::join(row_ranges_str_vec, ","));
        std::string scores_str = fmt::format("[{}]", fmt::join(scores_, ","));
        return fmt::format("IndexedSplit{{split={}, rowRanges={}, scores={}}}",
                           data_split_->ToString(), row_ranges_str, scores_str);
    }

    Status Validate() const {
        if ((row_ranges_.empty() && !data_split_->DataFiles().empty()) ||
            (!row_ranges_.empty() && data_split_->DataFiles().empty())) {
            return Status::Invalid("Invalid IndexedSplit: row ranges mismatch data files.");
        }
        if (!scores_.empty()) {
            size_t row_count = 0;
            auto sorted_ranges = row_ranges_;
            std::sort(sorted_ranges.begin(), sorted_ranges.end());
            std::optional<int64_t> previous_end;
            for (const auto& range : sorted_ranges) {
                if (range.from < 0 || range.from > range.to) {
                    return Status::Invalid("Invalid row id range in scored indexed split.");
                }
                if (range.to == std::numeric_limits<int64_t>::max()) {
                    return Status::Invalid(
                        "Row id range upper bound must be less than INT64_MAX in scored indexed "
                        "split.");
                }
                if (previous_end && range.from <= previous_end.value()) {
                    return Status::Invalid("Duplicate row id in scored indexed split.");
                }
                previous_end = range.to;
                row_count += range.Count();
            }
            if (row_count != scores_.size()) {
                return Status::Invalid("Scores length does not match row ranges in indexed split.");
            }
        }
        return Status::OK();
    }

 private:
    static constexpr float kEpsilon = 1e-5f;

    std::shared_ptr<DataSplitImpl> data_split_;
    std::vector<Range> row_ranges_;
    std::vector<float> scores_;
};
}  // namespace paimon
