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

#include "paimon/common/utils/arrow/vector_utils.h"

#include <memory>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

std::shared_ptr<arrow::Array> ArrayFromJSON(const std::shared_ptr<arrow::DataType>& type,
                                            const std::string& json) {
    arrow::Result<std::shared_ptr<arrow::Array>> result =
        arrow::ipc::internal::json::ArrayFromJSON(type, json);
    EXPECT_TRUE(result.ok()) << result.status().ToString();
    return std::move(result).ValueOrDie();
}

}  // namespace

TEST(VectorUtilsTest, TestContainsVector) {
    auto vector_type = arrow::fixed_size_list(arrow::float32(), 3);
    ASSERT_TRUE(VectorUtils::ContainsVectorType(vector_type));
    ASSERT_TRUE(VectorUtils::ContainsVectorType(arrow::list(vector_type)));
    ASSERT_TRUE(VectorUtils::ContainsVectorType(arrow::map(arrow::utf8(), vector_type)));
    ASSERT_TRUE(VectorUtils::ContainsVectorType(arrow::struct_({arrow::field("v", vector_type)})));
    ASSERT_FALSE(VectorUtils::ContainsVectorType(arrow::list(arrow::float32())));
    ASSERT_FALSE(VectorUtils::ContainsVectorType(nullptr));

    ASSERT_TRUE(VectorUtils::ContainsVectorField(arrow::field("v", arrow::list(vector_type))));
    ASSERT_FALSE(VectorUtils::ContainsVectorField(arrow::field("v", arrow::int32())));
    ASSERT_FALSE(VectorUtils::ContainsVectorField(nullptr));

    ASSERT_TRUE(VectorUtils::ContainsVector(
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("v", vector_type)})));
    ASSERT_FALSE(VectorUtils::ContainsVector(arrow::schema({arrow::field("id", arrow::int32())})));
    ASSERT_FALSE(VectorUtils::ContainsVector(nullptr));
}

TEST(VectorUtilsTest, TestValidateVectorElements) {
    auto vector_type = arrow::fixed_size_list(arrow::float32(), 3);
    ASSERT_OK(VectorUtils::ValidateVectorElements(
        *ArrayFromJSON(vector_type, R"([[1.0, 2.0, 3.0], null, [4.0, 5.0, 6.0]])")));
    ASSERT_NOK_WITH_MSG(VectorUtils::ValidateVectorElements(
                            *ArrayFromJSON(vector_type, R"([[1.0, 2.0, 3.0], [4.0, null, 6.0]])")),
                        "VECTOR cannot contain null elements, found one at row 1 position 1");

    // A sliced array must be validated against its own rows only.
    std::shared_ptr<arrow::Array> sliced =
        ArrayFromJSON(vector_type, R"([[1.0, null, 3.0], [4.0, 5.0, 6.0]])")->Slice(1, 1);
    ASSERT_OK(VectorUtils::ValidateVectorElements(*sliced));

    auto list_type = arrow::list(arrow::float32());
    ASSERT_OK(VectorUtils::ValidateVectorElements(
        *ArrayFromJSON(list_type, R"([[1.0, 2.0, 3.0], null])")));
    ASSERT_NOK_WITH_MSG(
        VectorUtils::ValidateVectorElements(*ArrayFromJSON(list_type, R"([[1.0, null, 3.0]])")),
        "VECTOR cannot contain null elements, found one at row 0 position 1");

    ASSERT_NOK_WITH_MSG(
        VectorUtils::ValidateVectorElements(*ArrayFromJSON(arrow::int32(), "[1, 2]")),
        "Cannot validate VECTOR values of type int32");
}

// Arrow does not check that a FixedSizeList child holds `length * list_size` values when
// importing an array over the C data interface, so the element scan must reject it instead of
// reading past the end of the child.
TEST(VectorUtilsTest, TestValidateVectorElementsRejectsTruncatedValues) {
    auto vector_type = arrow::fixed_size_list(arrow::float32(), 3);
    std::shared_ptr<arrow::Array> values = ArrayFromJSON(arrow::float32(), "[1.0, null, 3.0]");
    auto truncated = arrow::MakeArray(arrow::ArrayData::Make(vector_type, /*length=*/2, {nullptr},
                                                             {values->data()},
                                                             /*null_count=*/0));

    ASSERT_NOK_WITH_MSG(VectorUtils::ValidateVectorElements(*truncated),
                        "VECTOR holds 3 elements while 2 rows of dimension 3 require 6");
}

}  // namespace paimon::test
