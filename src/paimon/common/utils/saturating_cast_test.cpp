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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/common/utils/saturating_cast.h"

#include <cstdint>
#include <limits>

#include "gtest/gtest.h"

namespace paimon::test {

TEST(SaturatingCastTest, TestInt64InRangeTruncatesTowardZero) {
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(0.0), 0);
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(1.9), 1);
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(-1.9), -1);
    // 2^63 - 1024 is the largest double below 2^63: it stays on the truncation path.
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(9223372036854774784.0), 9223372036854774784LL);
}

TEST(SaturatingCastTest, TestInt64Saturation) {
    // (double)INT64_MAX rounds up to 2^63, so the boundary double already saturates.
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(
                  static_cast<double>(std::numeric_limits<int64_t>::max())),
              std::numeric_limits<int64_t>::max());
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(1e300), std::numeric_limits<int64_t>::max());
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(-1e300), std::numeric_limits<int64_t>::lowest());
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(std::numeric_limits<double>::infinity()),
              std::numeric_limits<int64_t>::max());
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(-std::numeric_limits<double>::infinity()),
              std::numeric_limits<int64_t>::lowest());
    // The lowest bound is exactly representable and must survive as a value.
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(
                  static_cast<double>(std::numeric_limits<int64_t>::lowest())),
              std::numeric_limits<int64_t>::lowest());
}

TEST(SaturatingCastTest, TestInt64NaNBecomesZero) {
    // Java's (long)Double.NaN == 0.
    ASSERT_EQ(SaturatingDoubleToInteger<int64_t>(std::numeric_limits<double>::quiet_NaN()), 0);
}

TEST(SaturatingCastTest, TestInt32Path) {
    // SstFileWriter converts through the int32_t instantiation.
    ASSERT_EQ(SaturatingDoubleToInteger<int32_t>(42.7), 42);
    ASSERT_EQ(SaturatingDoubleToInteger<int32_t>(-42.7), -42);
    // The int32_t bounds are exactly representable as doubles and saturate inclusively.
    ASSERT_EQ(SaturatingDoubleToInteger<int32_t>(2147483647.0),
              std::numeric_limits<int32_t>::max());
    ASSERT_EQ(SaturatingDoubleToInteger<int32_t>(2147483648.0),
              std::numeric_limits<int32_t>::max());
    ASSERT_EQ(SaturatingDoubleToInteger<int32_t>(-2147483648.0),
              std::numeric_limits<int32_t>::lowest());
    ASSERT_EQ(SaturatingDoubleToInteger<int32_t>(-2147483649.0),
              std::numeric_limits<int32_t>::lowest());
    ASSERT_EQ(SaturatingDoubleToInteger<int32_t>(std::numeric_limits<double>::quiet_NaN()), 0);
}

}  // namespace paimon::test
