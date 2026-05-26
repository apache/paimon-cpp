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
#include <functional>
#include <utility>
#include <vector>

#include "paimon/result.h"

namespace paimon {
/// A helper class to handle ranges.
template <typename T>
class RangeHelper {
 public:
    // start_func and end_func refer to a closed interval (inclusive of both endpoints).
    RangeHelper(std::function<Result<int64_t>(const T&)> start_func,
                std::function<Result<int64_t>(const T&)> end_func)
        : start_func_(std::move(start_func)), end_func_(std::move(end_func)) {}

    Result<bool> AreAllRangesSame(const std::vector<T>& ranges) const {
        if (ranges.empty()) {
            return true;
        }
        // Get the first range as reference
        const auto& first = ranges[0];
        PAIMON_ASSIGN_OR_RAISE(int64_t first_start, start_func_(first));
        PAIMON_ASSIGN_OR_RAISE(int64_t first_end, end_func_(first));

        // Compare all other ranges with the first one
        for (size_t i = 1; i < ranges.size(); i++) {
            const auto& current = ranges[i];
            PAIMON_ASSIGN_OR_RAISE(int64_t current_start, start_func_(current));
            PAIMON_ASSIGN_OR_RAISE(int64_t current_end, end_func_(current));
            if (current_start != first_start || current_end != first_end) {
                // Found a different range
                return false;
            }
        }
        return true;
    }

    /// A helper class to track original indices of range.
    struct IndexedValue {
        IndexedValue(T&& _value, int32_t _original_idx, int64_t _start, int64_t _end)
            : value(std::move(_value)), original_idx(_original_idx), start(_start), end(_end) {}
        IndexedValue(IndexedValue&& other)
            : value(std::move(other.value)),
              original_idx(other.original_idx),
              start(other.start),
              end(other.end) {}
        IndexedValue(const IndexedValue& other)
            : value(other.value),
              original_idx(other.original_idx),
              start(other.start),
              end(other.end) {}
        IndexedValue& operator=(IndexedValue&& other) noexcept {
            if (&other == this) {
                return *this;
            }
            value = std::move(other.value);
            original_idx = other.original_idx;
            start = other.start;
            end = other.end;
            return *this;
        }

        IndexedValue& operator=(const IndexedValue& other) noexcept {
            if (&other == this) {
                return *this;
            }
            value = other.value;
            original_idx = other.original_idx;
            start = other.start;
            end = other.end;
            return *this;
        }

        T value;
        int32_t original_idx;
        int64_t start;
        int64_t end;
    };

    Result<std::vector<std::vector<T>>> MergeOverlappingRanges(std::vector<T>&& ranges) const {
        std::vector<std::vector<T>> result;
        if (ranges.empty()) {
            return result;
        }
        // Create a list of IndexedValue to keep track of original indices
        std::vector<IndexedValue> index_ranges;
        index_ranges.reserve(ranges.size());
        for (int32_t i = 0; i < static_cast<int32_t>(ranges.size()); i++) {
            // just check start_func_ and end_func_ is valid for ranges[i]
            // range in IndexedValue must be valid
            PAIMON_ASSIGN_OR_RAISE(int64_t start, start_func_(ranges[i]));
            PAIMON_ASSIGN_OR_RAISE(int64_t end, end_func_(ranges[i]));
            index_ranges.emplace_back(std::move(ranges[i]), /*original_idx=*/i, start, end);
        }

        // Sort the ranges by their start value
        std::stable_sort(index_ranges.begin(), index_ranges.end(),
                         [&](const IndexedValue& r1, const IndexedValue& r2) {
                             if (r1.start != r2.start) {
                                 return r1.start < r2.start;
                             }
                             return r1.end < r2.end;
                         });
        // Initialize with the first range
        std::vector<std::vector<IndexedValue>> groups;
        std::vector<IndexedValue> current_group;
        current_group.emplace_back(std::move(index_ranges[0]));
        int64_t current_end = current_group.back().end;

        // Iterate through the sorted ranges and merge overlapping ones
        for (size_t i = 1; i < index_ranges.size(); i++) {
            auto& current = index_ranges[i];
            int64_t start = current.start;
            int64_t end = current.end;

            // If the current range overlaps with the current group, merge it
            if (start <= current_end) {
                current_group.emplace_back(std::move(current));
                // Update the current end to the maximum end of the merged ranges
                if (end > current_end) {
                    current_end = end;
                }
            } else {
                // Otherwise, start a new group
                groups.emplace_back(std::move(current_group));
                current_group.clear();
                current_group.emplace_back(std::move(current));
                current_end = end;
            }
        }
        // Add the last group
        groups.emplace_back(std::move(current_group));

        // Convert the groups to the required format and sort each group by original index
        for (auto& group : groups) {
            // Sort the group by original index to maintain the input order
            std::stable_sort(group.begin(), group.end(),
                             [](const IndexedValue& r1, const IndexedValue& r2) {
                                 return r1.original_idx < r2.original_idx;
                             });
            std::vector<T> sorted_group;
            sorted_group.reserve(group.size());
            for (auto& index_value : group) {
                sorted_group.emplace_back(std::move(index_value.value));
            }
            result.emplace_back(std::move(sorted_group));
        }
        return result;
    }

 private:
    std::function<Result<int64_t>(const T&)> start_func_;
    std::function<Result<int64_t>(const T&)> end_func_;
};

}  // namespace paimon
