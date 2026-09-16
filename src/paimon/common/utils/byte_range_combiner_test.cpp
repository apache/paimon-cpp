/**
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

// Adapted from Apache ORC
// https://github.com/apache/orc/blob/main/c%2B%2B/test/TestCache.cc

#include "paimon/common/utils/byte_range_combiner.h"

#include <limits>

#include "gtest/gtest.h"
#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(ByteRangeCombinerTest, TestBasics) {
    auto check = [](std::vector<ByteRange> ranges, std::vector<ByteRange> expected) -> void {
        const uint64_t hole_size_limit = 9;
        const uint64_t range_size_limit = 99;
        auto actual = ranges;
        ASSERT_OK_AND_ASSIGN(auto coalesced,
                             ByteRangeCombiner::CoalesceByteRanges(
                                 std::move(actual), hole_size_limit, range_size_limit));
        ASSERT_EQ(coalesced, expected);
    };

    check({}, {});
    // Zero sized range that ends up in empty list
    check({{110, 0}}, {});
    // Combination on 1 zero sized range and 1 non-zero sized range
    check({{110, 10}, {120, 0}}, {{110, 10}});
    // 1 non-zero sized range
    check({{110, 10}}, {{110, 10}});
    // No holes + unordered ranges
    check({{130, 10}, {110, 10}, {120, 10}}, {{110, 30}});
    // No holes
    check({{110, 10}, {120, 10}, {130, 10}}, {{110, 30}});
    // Small holes only
    check({{110, 11}, {130, 11}, {150, 11}}, {{110, 51}});
    // Large holes
    check({{110, 10}, {130, 10}}, {{110, 10}, {130, 10}});
    check({{110, 11}, {130, 11}, {150, 10}, {170, 11}, {190, 11}}, {{110, 50}, {170, 31}});

    // With zero-sized ranges
    check({{110, 11}, {130, 0}, {130, 11}, {145, 0}, {150, 11}, {200, 0}}, {{110, 51}});

    // No holes but large ranges
    check({{110, 100}, {210, 100}}, {{110, 99}, {209, 1}, {210, 99}, {309, 1}});
    // Small holes and large range in the middle (*)
    check({{110, 10}, {120, 11}, {140, 100}, {240, 11}, {260, 11}},
          {{110, 21}, {140, 99}, {239, 32}});
    // Mid-size ranges that would turn large after coalescing
    check({{100, 50}, {150, 50}}, {{100, 50}, {150, 50}});
    check({{100, 30}, {130, 30}, {160, 30}, {190, 30}, {220, 30}}, {{100, 90}, {190, 60}});

    // Same as (*) but unsorted
    check({{140, 100}, {120, 11}, {240, 11}, {110, 10}, {260, 11}},
          {{110, 21}, {140, 99}, {239, 32}});

    // Completely overlapping ranges should be eliminated
    check({{20, 5}, {20, 5}, {21, 2}}, {{20, 5}});
}

// Ranges beyond the int64 bound (e.g. negative signed metadata values cast to uint64_t)
// must be rejected before the unchecked offset + length arithmetic, which would otherwise
// wrap around or spin the splitting loop until memory is exhausted.
TEST(ByteRangeCombinerTest, TestRejectsRangesBeyondInt64Bound) {
    constexpr auto kInt64Max = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    auto check_invalid = [](std::vector<ByteRange> ranges) -> void {
        ASSERT_NOK_WITH_MSG(
            ByteRangeCombiner::CoalesceByteRanges(std::move(ranges), /*hole_size_limit=*/9,
                                                  /*range_size_limit=*/99),
            "exceeds the int64 bound");
    };

    // Offset beyond int64 (negative int64 cast to uint64_t lands here).
    check_invalid({{kInt64Max + 1, 1}});
    // Length beyond int64 (e.g. -1 cast to uint64_t), which would explode the split loop.
    check_invalid({{0, std::numeric_limits<uint64_t>::max()}});
    // Both in range individually, but the end position overflows the int64 bound.
    check_invalid({{kInt64Max - 10, 20}});
    // One bad range among valid ones still fails the whole batch.
    check_invalid({{100, 10}, {0, std::numeric_limits<uint64_t>::max()}});
}

// The adaptive variant derives the size the ranges are cut at from the bytes they cover, so a
// batch too small to fill the concurrency on its own is cut finer than the fixed limit cuts it.
TEST(ByteRangeCombinerTest, TestAdaptiveRangeSizeLimit) {
    auto check = [](std::vector<ByteRange> ranges, uint64_t alignment, uint64_t target_concurrency,
                    std::vector<ByteRange> expected) -> void {
        ASSERT_OK_AND_ASSIGN(auto coalesced,
                             ByteRangeCombiner::CoalesceByteRangesAdaptive(
                                 std::move(ranges), /*hole_size_limit=*/1,
                                 /*range_size_limit=*/8, alignment, target_concurrency));
        ASSERT_EQ(coalesced, expected);
    };

    // 20 bytes over 10 requests is 2 per request, which the alignment lifts to one 4 byte
    // range each: cut at 4 instead of at the limit of 8.
    check({{0, 20}}, /*alignment=*/4, /*target_concurrency=*/10,
          {{0, 4}, {4, 4}, {8, 4}, {12, 4}, {16, 4}});
    // The same bytes cut at the fixed limit, for comparison: 3 ranges rather than 5.
    check({{0, 20}}, /*alignment=*/0, /*target_concurrency=*/10, {{0, 8}, {8, 8}, {16, 4}});
    check({{0, 20}}, /*alignment=*/4, /*target_concurrency=*/0, {{0, 8}, {8, 8}, {16, 4}});
    // 30 bytes over 3 requests is 10 each, which rounds up past the limit, so the limit wins
    // and the batch is cut exactly as the non-adaptive variant cuts it.
    check({{0, 30}}, /*alignment=*/4, /*target_concurrency=*/3, {{0, 8}, {8, 8}, {16, 8}, {24, 6}});
    // The alignment is the floor: 10 bytes over 10 requests is 1 each, not cut into 1 byte
    // requests that could never amortize their own round trip.
    check({{0, 10}}, /*alignment=*/4, /*target_concurrency=*/10, {{0, 4}, {4, 4}, {8, 2}});
    // A derived limit still coalesces across the holes that fit within it, and still bounds the
    // coalesced result: the third range would take it past 4 bytes, so it starts a new range.
    check({{0, 1}, {2, 1}}, /*alignment=*/4, /*target_concurrency=*/10, {{0, 3}});
    check({{0, 1}, {2, 1}, {4, 1}}, /*alignment=*/4, /*target_concurrency=*/10, {{0, 3}, {4, 1}});
    check({}, /*alignment=*/4, /*target_concurrency=*/10, {});
}

// CoalesceByteRanges() rejects a size limit that does not exceed the hole size, so the derived
// limit must stay above it however small the batch is.
TEST(ByteRangeCombinerTest, TestAdaptiveRangeSizeLimitStaysAboveHoleSize) {
    // 10 bytes over 10 requests would derive 4, below the hole size of 5.
    ASSERT_OK_AND_ASSIGN(auto coalesced, ByteRangeCombiner::CoalesceByteRangesAdaptive(
                                             {{0, 10}}, /*hole_size_limit=*/5,
                                             /*range_size_limit=*/8, /*alignment=*/4,
                                             /*target_concurrency=*/10));
    ASSERT_EQ(coalesced, (std::vector<ByteRange>{{0, 8}, {8, 2}}));

    // A limit that cannot exceed the hole size is still reported, not papered over.
    ASSERT_NOK_WITH_MSG(ByteRangeCombiner::CoalesceByteRangesAdaptive(
                            {{0, 10}}, /*hole_size_limit=*/8, /*range_size_limit=*/8,
                            /*alignment=*/4, /*target_concurrency=*/10),
                        "should be larger than hole size limit");
}

}  // namespace paimon::test
