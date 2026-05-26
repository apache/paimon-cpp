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

#include "paimon/common/utils/range_helper.h"

#include <optional>

#include "gtest/gtest.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class RangeHelperTest : public ::testing::Test {
 public:
    void SetUp() override {
        start_func_ = [](const Range& range) -> Result<int64_t> {
            if (range.start) {
                return range.start.value();
            }
            return Status::Invalid("start is null");
        };
        end_func_ = [](const Range& range) -> Result<int64_t> {
            if (range.end) {
                return range.end.value();
            }
            return Status::Invalid("end is null");
        };
    }
    void TearDown() override {}
    struct Range {
        Range(const std::optional<int64_t>& _start, const std::optional<int64_t>& _end)
            : start(_start), end(_end) {}
        Range(Range&& other) : start(other.start), end(other.end) {}
        Range(const Range& other) = default;
        Range& operator=(Range&& other) noexcept {
            if (&other == this) {
                return *this;
            }
            start = other.start;
            end = other.end;
            return *this;
        }
        Range& operator=(const Range& other) noexcept {
            if (&other == this) {
                return *this;
            }
            start = other.start;
            end = other.end;
            return *this;
        }
        bool operator==(const Range& other) const {
            return start == other.start && end == other.end;
        }
        std::optional<int64_t> start;
        std::optional<int64_t> end;
    };

 private:
    std::function<Result<int64_t>(const Range&)> start_func_;
    std::function<Result<int64_t>(const Range&)> end_func_;
};

TEST_F(RangeHelperTest, TestAreAllRangesSame) {
    {
        std::vector<Range> ranges = {Range(0l, 100l), Range(0l, 100l), Range(1l, 100l)};
        ASSERT_OK_AND_ASSIGN(bool same,
                             RangeHelper(start_func_, end_func_).AreAllRangesSame(ranges));
        ASSERT_FALSE(same);
    }
    {
        std::vector<Range> ranges = {Range(0l, 100l), Range(0l, 100l), Range(0l, 300l)};
        ASSERT_OK_AND_ASSIGN(bool same,
                             RangeHelper(start_func_, end_func_).AreAllRangesSame(ranges));
        ASSERT_FALSE(same);
    }
    {
        std::vector<Range> ranges = {Range(0l, 100l), Range(0l, 100l), Range(0l, 100l)};
        ASSERT_OK_AND_ASSIGN(bool same,
                             RangeHelper(start_func_, end_func_).AreAllRangesSame(ranges));
        ASSERT_TRUE(same);
    }
    {
        ASSERT_OK_AND_ASSIGN(bool same, RangeHelper(start_func_, end_func_).AreAllRangesSame({}));
        ASSERT_TRUE(same);
    }
    {
        std::vector<Range> ranges = {Range(0l, 100l)};
        ASSERT_OK_AND_ASSIGN(bool same,
                             RangeHelper(start_func_, end_func_).AreAllRangesSame(ranges));
        ASSERT_TRUE(same);
    }
    {
        std::vector<Range> ranges = {Range(0l, std::nullopt)};
        ASSERT_NOK_WITH_MSG(RangeHelper(start_func_, end_func_).AreAllRangesSame(ranges),
                            "end is null");
    }
}
TEST_F(RangeHelperTest, TestMergeOverlappingRanges) {
    {
        std::vector<Range> ranges = {Range(200, 299), Range(0, 99),    Range(110, 199),
                                     Range(100, 109), Range(100, 199), Range(0, 49),
                                     Range(50, 99)};
        ASSERT_OK_AND_ASSIGN(
            auto merged_ranges,
            RangeHelper(start_func_, end_func_).MergeOverlappingRanges(std::move(ranges)));
        std::vector<std::vector<Range>> expected = {
            {Range(0, 99), Range(0, 49), Range(50, 99)},
            {Range(110, 199), Range(100, 109), Range(100, 199)},
            {Range(200, 299)},
        };
        ASSERT_EQ(expected, merged_ranges);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto merged_ranges,
                             RangeHelper(start_func_, end_func_).MergeOverlappingRanges({}));
        ASSERT_TRUE(merged_ranges.empty());
    }
    {
        std::vector<Range> ranges = {Range(0, 99)};
        ASSERT_OK_AND_ASSIGN(
            auto merged_ranges,
            RangeHelper(start_func_, end_func_).MergeOverlappingRanges(std::move(ranges)));
        std::vector<std::vector<Range>> expected = {{Range(0, 99)}};
        ASSERT_EQ(expected, merged_ranges);
    }
    {
        std::vector<Range> ranges = {Range(200, 299), Range(std::nullopt, 299)};
        ASSERT_NOK_WITH_MSG(
            RangeHelper(start_func_, end_func_).MergeOverlappingRanges(std::move(ranges)),
            "start is null");
    }
}

}  // namespace paimon::test
