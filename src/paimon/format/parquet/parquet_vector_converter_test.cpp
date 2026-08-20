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

#include "paimon/format/parquet/parquet_vector_converter.h"

#include <memory>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::parquet::test {

TEST(ParquetVectorConverterTest, ConvertNullableVectorToList) {
    auto vector_type = arrow::fixed_size_list(arrow::float32(), 3);
    auto vector_array = arrow::ipc::internal::json::ArrayFromJSON(
                            vector_type, R"([[1.0, 2.0, 3.0], null, [4.0, 5.0, 6.0]])")
                            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> converted,
        ParquetVectorConverter::ConvertToWriteType(vector_array, arrow::default_memory_pool()));
    ASSERT_EQ(converted->type()->id(), arrow::Type::LIST);
    auto list_array = checked_pointer_cast<arrow::ListArray>(converted);
    ASSERT_EQ(list_array->value_length(0), 3);
    ASSERT_TRUE(list_array->IsNull(1));
    // The Parquet writer rejects a null LIST slot spanning values, so the values Arrow keeps for
    // a null VECTOR row are dropped.
    ASSERT_EQ(list_array->value_length(1), 0);
    ASSERT_EQ(list_array->value_length(2), 3);
    ASSERT_EQ(list_array->values()->length(), 6);
    auto values = checked_pointer_cast<arrow::FloatArray>(list_array->values());
    ASSERT_FLOAT_EQ(values->Value(3), 4.0f);
}

TEST(ParquetVectorConverterTest, ConvertNestedVectorsToList) {
    auto vector_type =
        arrow::fixed_size_list(arrow::field("item", arrow::float32(), /*nullable=*/false), 2);
    auto nested_type = arrow::struct_({
        arrow::field("vectors", arrow::list(vector_type)),
        arrow::field("by_name", arrow::map(arrow::utf8(), vector_type)),
    });
    auto nested_array =
        arrow::ipc::internal::json::ArrayFromJSON(nested_type,
                                                  R"([[[[1.0, 2.0], null], [["a", [3.0, 4.0]]]],
                                 [null, [["b", null]]]])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> physical_array,
        ParquetVectorConverter::ConvertToWriteType(nested_array, arrow::default_memory_pool()));
    auto physical_type = checked_pointer_cast<arrow::StructType>(physical_array->type());
    auto physical_list = checked_pointer_cast<arrow::ListType>(physical_type->field(0)->type());
    auto physical_map = checked_pointer_cast<arrow::MapType>(physical_type->field(1)->type());
    ASSERT_EQ(physical_list->value_type()->id(), arrow::Type::LIST);
    ASSERT_EQ(physical_map->item_type()->id(), arrow::Type::LIST);
}

TEST(ParquetVectorConverterTest, ConvertSlicedVectorToList) {
    auto vector_type = arrow::fixed_size_list(arrow::float64(), 2);
    auto vector_array =
        arrow::ipc::internal::json::ArrayFromJSON(vector_type, R"([[1.0, 2.0], [3.0, 4.0], null])")
            .ValueOrDie()
            ->Slice(1, 2);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::Array> converted,
        ParquetVectorConverter::ConvertToWriteType(vector_array, arrow::default_memory_pool()));
    auto list_array = checked_pointer_cast<arrow::ListArray>(converted);
    ASSERT_EQ(list_array->length(), 2);
    ASSERT_EQ(list_array->value_length(0), 2);
    ASSERT_TRUE(list_array->IsNull(1));
    auto values = checked_pointer_cast<arrow::DoubleArray>(list_array->values());
    ASSERT_DOUBLE_EQ(values->Value(0), 3.0);
    ASSERT_DOUBLE_EQ(values->Value(1), 4.0);
}

}  // namespace paimon::parquet::test
