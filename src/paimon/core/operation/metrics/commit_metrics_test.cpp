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

#include "paimon/core/operation/metrics/commit_metrics.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CommitMetricsTest, TestSimple) {
    auto commit_metrics = std::make_shared<MetricsImpl>();
    commit_metrics->SetCounter("some_metric", 100);
    commit_metrics->SetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS, 30);
    ASSERT_OK_AND_ASSIGN(uint64_t counter,
                         commit_metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(30, counter);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter("some_metric"));
    ASSERT_EQ(100, counter);
    auto other = std::make_shared<MetricsImpl>();
    other->SetCounter("some_metric_2", 200);
    other->SetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS, 50);
    commit_metrics->Merge(other);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter(CommitMetrics::LAST_COMMIT_ATTEMPTS));
    ASSERT_EQ(80, counter);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter("some_metric"));
    ASSERT_EQ(100, counter);
    ASSERT_OK_AND_ASSIGN(counter, commit_metrics->GetCounter("some_metric_2"));
    ASSERT_EQ(200, counter);
}

}  // namespace paimon::test
