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

#include "paimon/common/predicate/like_optimization.h"

#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/predicate/contains.h"
#include "paimon/common/predicate/ends_with.h"
#include "paimon/common/predicate/equal.h"
#include "paimon/common/predicate/starts_with.h"

namespace paimon::test {
namespace {

Literal StringLiteral(const std::string& value) {
    return Literal(FieldType::STRING, value.data(), value.size());
}

void CheckOptimized(const std::string& pattern, const Function* expected_function,
                    const std::string& expected_literal) {
    std::optional<OptimizedLike> optimized = LikeOptimization::TryOptimize(StringLiteral(pattern));
    ASSERT_TRUE(optimized.has_value());
    ASSERT_EQ(expected_function, optimized->function);
    ASSERT_EQ(expected_literal, optimized->literal.GetValue<std::string>());
}

void CheckNotOptimized(const std::string& pattern) {
    ASSERT_FALSE(LikeOptimization::TryOptimize(StringLiteral(pattern)).has_value());
}

}  // namespace

TEST(LikeOptimizationTest, OptimizesSimplePatterns) {
    CheckOptimized("abc", &Equal::Instance(), "abc");
    CheckOptimized("abc%", &StartsWith::Instance(), "abc");
    CheckOptimized("%abc", &EndsWith::Instance(), "abc");
    CheckOptimized("%abc%", &Contains::Instance(), "abc");
}

TEST(LikeOptimizationTest, DoesNotOptimizeComplexPatterns) {
    CheckNotOptimized("");
    CheckNotOptimized("%");
    CheckNotOptimized("%%");
    CheckNotOptimized("a%b");
    CheckNotOptimized("%a%b%");
    CheckNotOptimized("abc%%");
    CheckNotOptimized("%%abc");
    CheckNotOptimized("a_c");
    CheckNotOptimized("a\\c");
}

TEST(LikeOptimizationTest, DoesNotOptimizeNullOrNonStringLiteral) {
    ASSERT_FALSE(LikeOptimization::TryOptimize(Literal(FieldType::STRING)).has_value());
    ASSERT_FALSE(LikeOptimization::TryOptimize(Literal(1)).has_value());
}

}  // namespace paimon::test
