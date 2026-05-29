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
#include "paimon/common/data/columnar/columnar_utils.h"

#include <string>

#include "arrow/api.h"
#include "arrow/array/array_dict.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {
TEST(ColumnarUtilsTest, TestGetViewAndBytes) {
    auto pool = GetDefaultPool();
    auto array = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["abc", "def", "hi"])")
                     .ValueOrDie();
    std::string_view view = ColumnarUtils::GetView(array.get(), 2);
    ASSERT_EQ(std::string(view), "hi");
    auto bytes = ColumnarUtils::GetBytes<arrow::BinaryType>(array.get(), 1, pool.get());
    ASSERT_EQ(*std::make_shared<Bytes>("def", pool.get()), *bytes);
}

TEST(ColumnarUtilsTest, TestGetViewAndBytesOfDict) {
    auto pool = GetDefaultPool();
    auto dict = arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["foo", "bar", "baz"])")
                    .ValueOrDie();
    auto dict_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[1, 2, 0, 2, 0]").ValueOrDie();
    std::shared_ptr<arrow::DictionaryArray> dict_array =
        std::make_shared<arrow::DictionaryArray>(dict_type, indices, dict);

    ASSERT_EQ("bar", std::string(ColumnarUtils::GetView(dict_array.get(), 0)));
    ASSERT_EQ("baz", std::string(ColumnarUtils::GetView(dict_array.get(), 1)));
    ASSERT_EQ("foo", std::string(ColumnarUtils::GetView(dict_array.get(), 2)));
    ASSERT_EQ("baz", std::string(ColumnarUtils::GetView(dict_array.get(), 3)));
    ASSERT_EQ("foo", std::string(ColumnarUtils::GetView(dict_array.get(), 4)));
}

}  // namespace paimon::test
