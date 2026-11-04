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

#include "paimon/common/metrics/histogram_windowing.h"

#include "gtest/gtest.h"

namespace paimon::test {

class HistogramWindowingImplTest : public ::testing::Test {
 protected:
    HistogramWindowingImpl CreateHistogram(uint64_t num_windows, uint64_t micros_per_window,
                                           uint64_t min_num_per_window) {
        return HistogramWindowingImpl(num_windows, micros_per_window, min_num_per_window,
                                      [this]() { return now_micros_; });
    }

    uint64_t now_micros_ = 1000000;
};

TEST_F(HistogramWindowingImplTest, TestAdvanceAndAggregateAcrossWindows) {
    auto h = CreateHistogram(/*num_windows=*/2, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/1);
    h.Add(1);
    now_micros_ += 1000;
    h.Add(2);

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, 2);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, 2);
    EXPECT_NEAR(s.average, 1.5, 1e-12);

    now_micros_ += 1000;
    h.Add(3);
    ASSERT_EQ(h.GetStats().count, 2);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 5);
}

TEST_F(HistogramWindowingImplTest, TestResetAtOrBeyondMaxSpan) {
    for (const uint64_t elapsed_micros : {2000, 3000}) {
        SCOPED_TRACE(elapsed_micros);
        auto h = CreateHistogram(/*num_windows=*/2, /*micros_per_window=*/1000,
                                 /*min_num_per_window=*/1);
        h.Add(1);
        now_micros_ += 1000;
        h.Add(2);
        ASSERT_EQ(h.GetStats().count, 2);

        // Reach or exceed max_span (2ms) after advancing to the second window.
        now_micros_ += elapsed_micros;
        h.Add(3);

        HistogramStats s = h.GetStats();
        EXPECT_EQ(s.count, 1);
        EXPECT_DOUBLE_EQ(s.min, 3);
        EXPECT_DOUBLE_EQ(s.max, 3);
        EXPECT_NEAR(s.average, 3.0, 1e-12);

        // The new sample expires relative to the reset time.
        now_micros_ += 1000;
        ASSERT_EQ(h.GetStats().count, 1);
    }
}

TEST_F(HistogramWindowingImplTest, TestExpiresAtMaxSpan) {
    auto h = CreateHistogram(/*num_windows=*/2, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/1);
    h.Add(3);

    now_micros_ += 1999;
    ASSERT_EQ(h.GetStats().count, 1);
    now_micros_ += 1;
    EXPECT_EQ(h.GetStats().count, 0);

    h.Add(4);
    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, 1);
    EXPECT_DOUBLE_EQ(s.sum, 4);
}

TEST_F(HistogramWindowingImplTest, TestMergeSameParams) {
    auto source = CreateHistogram(/*num_windows=*/4, /*micros_per_window=*/1000,
                                  /*min_num_per_window=*/1);
    source.Add(3);
    now_micros_ += 1000;
    auto target = CreateHistogram(/*num_windows=*/4, /*micros_per_window=*/1000,
                                  /*min_num_per_window=*/1);
    target.Add(1);
    target.Add(2);

    target.Merge(source);
    HistogramStats s = target.GetStats();
    EXPECT_EQ(s.count, 3);
    EXPECT_DOUBLE_EQ(s.sum, 6);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, 3);

    now_micros_ += 3000;
    ASSERT_EQ(target.GetStats().count, 2);
    ASSERT_DOUBLE_EQ(target.GetStats().sum, 3);
}

TEST_F(HistogramWindowingImplTest, TestMergeDifferentParamsFallback) {
    auto h1 = CreateHistogram(/*num_windows=*/4, /*micros_per_window=*/1000,
                              /*min_num_per_window=*/1);
    auto h2 = CreateHistogram(/*num_windows=*/8, /*micros_per_window=*/1000,
                              /*min_num_per_window=*/1);
    h2.Add(10);
    h2.Add(20);

    h1.Merge(h2);
    HistogramStats s = h1.GetStats();
    EXPECT_EQ(s.count, 2);
    EXPECT_DOUBLE_EQ(s.min, 10);
    EXPECT_DOUBLE_EQ(s.max, 20);
}

TEST_F(HistogramWindowingImplTest, TestMergePlainHistogram) {
    auto h = CreateHistogram(/*num_windows=*/4, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/1);
    HistogramImpl other;
    other.Add(10);
    other.Add(20);

    now_micros_ += 1000;
    h.Merge(other);
    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, 2);
    EXPECT_DOUBLE_EQ(s.sum, 30);
    EXPECT_DOUBLE_EQ(s.min, 10);
    EXPECT_DOUBLE_EQ(s.max, 20);
}

TEST_F(HistogramWindowingImplTest, TestClockRollback) {
    auto h = CreateHistogram(/*num_windows=*/3, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/1);
    h.Add(1);
    HistogramImpl plain;
    plain.Add(2);
    auto same = CreateHistogram(/*num_windows=*/3, /*micros_per_window=*/1000,
                                /*min_num_per_window=*/1);
    same.Add(3);
    auto different = CreateHistogram(/*num_windows=*/4, /*micros_per_window=*/1000,
                                     /*min_num_per_window=*/1);
    different.Add(4);

    now_micros_ -= 1000;
    h.Add(5);
    h.Merge(plain);
    h.Merge(same);
    h.Merge(different);
    now_micros_ += 1000;
    ASSERT_EQ(h.GetStats().count, 1);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 1);
    h.Add(6);
    ASSERT_EQ(h.GetStats().count, 2);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 7);
}

TEST_F(HistogramWindowingImplTest, TestZeroWindows) {
    auto h = CreateHistogram(/*num_windows=*/0, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/1);
    h.Add(1);
    HistogramImpl source;
    source.Add(2);
    h.Merge(source);
    ASSERT_EQ(h.GetStats().count, 0);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 0);
    auto cloned = h.Clone();
    ASSERT_TRUE(cloned);
    cloned->Add(3);
    ASSERT_EQ(cloned->GetStats().count, 0);
}

TEST_F(HistogramWindowingImplTest, TestZeroDuration) {
    auto h = CreateHistogram(/*num_windows=*/3, /*micros_per_window=*/0,
                             /*min_num_per_window=*/1);
    h.Add(1);
    now_micros_ += 1000000;
    h.Add(2);
    HistogramImpl source;
    source.Add(3);
    h.Merge(source);
    ASSERT_EQ(h.GetStats().count, 3);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 6);
    now_micros_ += 1000000;
    ASSERT_EQ(h.GetStats().count, 3);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 6);
}

TEST_F(HistogramWindowingImplTest, TestMinNumPerWindow100Case) {
    // Validate min_num_per_window behavior:
    // - window advancement is gated by sample count
    // - if the histogram doesn't advance in time, it may get reset once beyond max span
    auto h = CreateHistogram(/*num_windows=*/3, /*micros_per_window=*/50000,
                             /*min_num_per_window=*/100);

    // Fill current window but keep it below min_num.
    for (int32_t i = 0; i < 99; ++i) {
        h.Add(1);
    }

    // Cross at least one window.
    now_micros_ += 75000;
    h.Add(2);  // 100th sample; advancement check happens before this add.

    HistogramStats s1 = h.GetStats();
    EXPECT_EQ(s1.count, 100);
    EXPECT_DOUBLE_EQ(s1.min, 1);
    EXPECT_DOUBLE_EQ(s1.max, 2);

    h.Add(3);
    now_micros_ += 75000;
    ASSERT_EQ(h.GetStats().count, 1);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 3);

    // Cross enough time so that aligned_now - current_window_start >= max_span
    // (max_span = num_windows * micros_per_window = 150ms here).
    now_micros_ += 100000;
    h.Add(1000);

    HistogramStats s2 = h.GetStats();
    EXPECT_EQ(s2.count, 1);
    EXPECT_DOUBLE_EQ(s2.min, 1000);
    EXPECT_DOUBLE_EQ(s2.max, 1000);
}

TEST_F(HistogramWindowingImplTest, TestLargeDatasetInSingleWindow) {
    auto h = CreateHistogram(/*num_windows=*/4, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/1);

    constexpr uint64_t n = 10000;
    for (uint64_t i = 1; i <= n; ++i) {
        h.Add(static_cast<double>(i));
    }

    HistogramStats s = h.GetStats();
    EXPECT_EQ(s.count, n);
    EXPECT_DOUBLE_EQ(s.min, 1);
    EXPECT_DOUBLE_EQ(s.max, static_cast<double>(n));
    EXPECT_DOUBLE_EQ(s.sum, static_cast<double>(n) * static_cast<double>(n + 1) / 2.0);
    EXPECT_NEAR(s.average, (static_cast<double>(n) + 1.0) / 2.0, 1e-12);
}

TEST_F(HistogramWindowingImplTest, TestCloneConsistencyAndIndependence) {
    auto h = CreateHistogram(/*num_windows=*/4, /*micros_per_window=*/1000,
                             /*min_num_per_window=*/2);
    h.Add(1);
    h.Add(2);
    now_micros_ += 1000;
    h.Add(3);
    h.Add(4);

    const HistogramStats before = h.GetStats();

    auto cloned_base = h.Clone();
    auto cloned = std::dynamic_pointer_cast<HistogramWindowingImpl>(cloned_base);
    ASSERT_TRUE(cloned != nullptr);

    const HistogramStats cloned_before = cloned->GetStats();
    EXPECT_EQ(cloned_before.count, before.count);
    EXPECT_DOUBLE_EQ(cloned_before.sum, before.sum);
    EXPECT_DOUBLE_EQ(cloned_before.min, before.min);
    EXPECT_DOUBLE_EQ(cloned_before.max, before.max);
    EXPECT_DOUBLE_EQ(cloned_before.average, before.average);
    EXPECT_DOUBLE_EQ(cloned_before.stddev, before.stddev);
    EXPECT_DOUBLE_EQ(cloned_before.p50, before.p50);
    EXPECT_DOUBLE_EQ(cloned_before.p90, before.p90);
    EXPECT_DOUBLE_EQ(cloned_before.p95, before.p95);
    EXPECT_DOUBLE_EQ(cloned_before.p99, before.p99);
    EXPECT_DOUBLE_EQ(cloned_before.p999, before.p999);

    now_micros_ += 1000;
    // Mutating original should not affect cloned.
    h.Add(100);
    const HistogramStats after_original = h.GetStats();
    const HistogramStats after_clone = cloned->GetStats();
    EXPECT_EQ(after_clone.count, cloned_before.count);
    EXPECT_EQ(after_original.count, cloned_before.count + 1);

    // Mutating cloned should not affect original.
    cloned->Add(200);
    const HistogramStats after_clone2 = cloned->GetStats();
    const HistogramStats after_original2 = h.GetStats();
    EXPECT_EQ(after_original2.count, after_original.count);
    EXPECT_EQ(after_clone2.count, cloned_before.count + 1);

    now_micros_ += 3000;
    ASSERT_EQ(h.GetStats().count, 1);
    ASSERT_DOUBLE_EQ(h.GetStats().sum, 100);
    ASSERT_EQ(cloned->GetStats().count, 1);
    ASSERT_DOUBLE_EQ(cloned->GetStats().sum, 200);

    // Clones share the same time source, while keeping independent samples.
    now_micros_ += 1000;
    EXPECT_EQ(h.GetStats().count, 0);
    EXPECT_EQ(cloned->GetStats().count, 0);
}

}  // namespace paimon::test
