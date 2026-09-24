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

#include "paimon/testing/utils/dict_array_converter.h"

#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DictArrayConverterTest, TestNestedBinaryDictionary) {
    const std::string bytes("a\0\xff\x80", 4);
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(bytes).ok());
    ASSERT_TRUE(builder.Append("").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> binary_dictionary;
    ASSERT_TRUE(builder.Finish(&binary_dictionary).ok());
    for (const auto& value_type : {arrow::binary(), arrow::large_binary()}) {
        SCOPED_TRACE(value_type->ToString());
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Array> dictionary,
            CastingUtils::Cast(binary_dictionary, value_type, arrow::compute::CastOptions::Safe(),
                               arrow::default_memory_pool()));
        auto indices =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int16(), "[1, 0, null, 1, 2, 0]")
                .ValueOrDie();
        auto encoded =
            arrow::DictionaryArray::FromArrays(indices, dictionary).ValueOrDie()->Slice(1);
        auto offsets =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 2, 2, 5]").ValueOrDie();
        auto list = arrow::ListArray::FromArrays(*offsets, *encoded).ValueOrDie();
        auto keys =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c", "d", "e"])")
                .ValueOrDie();
        auto map = arrow::MapArray::FromArrays(offsets, keys, encoded).ValueOrDie();
        auto array =
            arrow::StructArray::Make({list, map}, std::vector<std::string>({"list", "map"}))
                .ValueOrDie();
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::Array> result,
            DictArrayConverter::ConvertDictArray(array, arrow::default_memory_pool()));
        ASSERT_TRUE(result->ValidateFull().ok());
        auto decoded_struct = checked_pointer_cast<arrow::StructArray>(result);
        auto decoded_list = checked_pointer_cast<arrow::ListArray>(decoded_struct->field(0));
        auto decoded_map = checked_pointer_cast<arrow::MapArray>(decoded_struct->field(1));
        ASSERT_TRUE(decoded_list->value_type()->Equals(value_type));
        ASSERT_TRUE(decoded_map->items()->type()->Equals(value_type));
        ASSERT_TRUE(decoded_list->values()->Equals(decoded_map->items()));
        auto values = decoded_list->values();
        ASSERT_TRUE(values->IsNull(1));
        ASSERT_TRUE(values->IsNull(3));
        auto check_values = [&](const auto& typed) {
            ASSERT_EQ(bytes, typed->GetView(0));
            ASSERT_EQ("", typed->GetView(2));
            ASSERT_EQ(bytes, typed->GetView(4));
        };
        if (value_type->id() == arrow::Type::BINARY) {
            check_values(checked_pointer_cast<arrow::BinaryArray>(values));
        } else {
            check_values(checked_pointer_cast<arrow::LargeBinaryArray>(values));
        }
    }
}
}  // namespace paimon::test
