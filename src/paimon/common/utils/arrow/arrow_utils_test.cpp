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

#include "paimon/common/utils/arrow/arrow_utils.h"

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class CastBase {
 public:
    virtual ~CastBase() = default;
};

class CastDerived : public CastBase {};

}  // namespace

TEST(CheckedCastTest, DelegatesToArrowCheckedCast) {
    std::shared_ptr<CastBase> shared_base = std::make_shared<CastDerived>();
    std::shared_ptr<CastDerived> shared_derived = checked_pointer_cast<CastDerived>(shared_base);
    ASSERT_NE(shared_derived, nullptr);

    std::unique_ptr<CastBase> unique_base = std::make_unique<CastDerived>();
    std::unique_ptr<CastDerived> unique_derived =
        checked_pointer_cast<CastDerived>(std::move(unique_base));
    ASSERT_NE(unique_derived, nullptr);

    CastBase* raw_base = shared_base.get();
    ASSERT_EQ(checked_cast<CastDerived*>(raw_base), shared_derived.get());

    std::shared_ptr<CastBase> null_base;
    ASSERT_EQ(checked_pointer_cast<CastDerived>(null_base), nullptr);

#ifndef NDEBUG
    std::shared_ptr<CastBase> wrong_type = std::make_shared<CastBase>();
    ASSERT_EQ(checked_pointer_cast<CastDerived>(wrong_type), nullptr);
    ASSERT_EQ(checked_cast<CastDerived*>(wrong_type.get()), nullptr);
#endif
}

TEST(ArrowUtilsTest, TestCreateProjection) {
    arrow::FieldVector file_fields = {
        arrow::field("k0", arrow::int32()),   arrow::field("k1", arrow::int32()),
        arrow::field("p1", arrow::int32()),   arrow::field("s1", arrow::utf8()),
        arrow::field("v0", arrow::float64()), arrow::field("v1", arrow::boolean()),
        arrow::field("s0", arrow::utf8())};
    auto file_schema = arrow::schema(file_fields);

    {
        // normal case
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v0", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
    {
        // duplicate read field
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()),   arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()),    arrow::field("v0", arrow::float64()),
            arrow::field("v0", arrow::float64()), arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
    {
        // duplicate read field, and sizeof(read_fields) > sizeof(file_fields)
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()),   arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()),    arrow::field("v0", arrow::float64()),
            arrow::field("v0", arrow::float64()), arrow::field("v0", arrow::float64()),
            arrow::field("v0", arrow::float64()), arrow::field("v0", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 4, 4, 4, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
    {
        // read field not found in src schema
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v2", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_NOK_WITH_MSG(ArrowUtils::CreateProjection(file_schema, read_schema->fields()),
                            "Field 'v2' not found or duplicate in src schema");
    }
    {
        // duplicate field in src schema
        arrow::FieldVector file_fields_dup = {
            arrow::field("k0", arrow::int32()),   arrow::field("k1", arrow::int32()),
            arrow::field("p1", arrow::int32()),   arrow::field("s1", arrow::utf8()),
            arrow::field("v0", arrow::float64()), arrow::field("v1", arrow::boolean()),
            arrow::field("v1", arrow::boolean()), arrow::field("s0", arrow::utf8())};
        auto file_schema_dup = arrow::schema(file_fields_dup);
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v1", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_NOK_WITH_MSG(ArrowUtils::CreateProjection(file_schema_dup, read_schema->fields()),
                            "Field 'v1' not found or duplicate in src schema");
    }
    {
        arrow::FieldVector read_fields = {
            arrow::field("k1", arrow::int32()), arrow::field("p1", arrow::int32()),
            arrow::field("s1", arrow::utf8()), arrow::field("v0", arrow::float64()),
            arrow::field("v1", arrow::boolean())};
        auto read_schema = arrow::schema(read_fields);
        ASSERT_OK_AND_ASSIGN(std::vector<int32_t> projection,
                             ArrowUtils::CreateProjection(file_schema, read_schema->fields()));
        std::vector<int32_t> expected_projection = {1, 2, 3, 4, 5};
        ASSERT_EQ(projection, expected_projection);
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchSimple) {
    auto field = arrow::field("column1", arrow::int32(), /*nullable=*/false);
    auto schema = arrow::schema({field});
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
      [20],
      [null],
      [10]
])")
                .ValueOrDie();

        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field column1 not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({field}), R"([
      [20],
      [10]
])")
                .ValueOrDie();

        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchWithStruct) {
    auto child1 = arrow::field("child1", arrow::int32(), /*nullable=*/false);
    auto child2 = arrow::field("child2", arrow::float64(), /*nullable=*/true);
    auto struct_field =
        arrow::field("parent", arrow::struct_({child1, child2}), /*nullable=*/false);
    auto schema = arrow::schema({struct_field});
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({struct_field}), R"([
      [null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field parent not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({struct_field}), R"([
      [[1, null]],
      [[null, 10.0]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field child1 not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({struct_field}), R"([
      [[1, null]],
      [[2, 10.0]]
])")
                .ValueOrDie();
        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchWithList) {
    auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/false);
    auto list_field = arrow::field("list_column", arrow::list(value_field), /*nullable=*/false);
    auto schema = arrow::schema({list_field});

    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({list_field}), R"([
      [[1, 2, null, 4, 5]],
      [null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(ArrowUtils::CheckNullabilityMatch(schema, array),
                            "CheckNullabilityMatch failed, field list_column not nullable while "
                            "data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({list_field}), R"([
      [[1, 2, null, 4, 5]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({list_field}), R"([
      [[1, 2, 3, 4, 5]]
])")
                .ValueOrDie();
        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchRejectsNullVectorElement) {
    auto vector_type = arrow::fixed_size_list(arrow::float32(), 3);
    auto vector_field = arrow::field("embedding", vector_type);
    arrow::FloatBuilder values_builder;
    ASSERT_TRUE(values_builder.Append(1.0f).ok());
    ASSERT_TRUE(values_builder.AppendNull().ok());
    ASSERT_TRUE(values_builder.Append(3.0f).ok());
    std::shared_ptr<arrow::Array> values = values_builder.Finish().ValueOrDie();
    auto vector_data = arrow::ArrayData::Make(vector_type, 1, {nullptr}, {values->data()}, 0);
    auto vector_array = arrow::MakeArray(vector_data);
    auto struct_array = arrow::StructArray::Make({vector_array}, {vector_field}).ValueOrDie();

    ASSERT_NOK_WITH_MSG(
        ArrowUtils::CheckNullabilityMatch(arrow::schema({vector_field}), struct_array),
        "VECTOR field embedding is invalid: VECTOR cannot contain null elements");
}

// Arrow accepts a FixedSizeList whose child is shorter than `length * list_size` when importing
// it over the C data interface, so the nullability check must reject it rather than scan past the
// end of the child.
TEST(ArrowUtilsTest, TestCheckNullableMatchRejectsTruncatedVector) {
    auto vector_type = arrow::fixed_size_list(arrow::float32(), 3);
    auto vector_field = arrow::field("embedding", vector_type);
    arrow::FloatBuilder values_builder;
    ASSERT_TRUE(values_builder.AppendValues({1.0f, 2.0f, 3.0f}).ok());
    std::shared_ptr<arrow::Array> values = values_builder.Finish().ValueOrDie();
    auto vector_data = arrow::ArrayData::Make(vector_type, /*length=*/2, {nullptr},
                                              {values->data()}, /*null_count=*/0);
    auto vector_array = arrow::MakeArray(vector_data);
    auto struct_array = arrow::StructArray::Make({vector_array}, {vector_field}).ValueOrDie();

    ASSERT_NOK_WITH_MSG(
        ArrowUtils::CheckNullabilityMatch(arrow::schema({vector_field}), struct_array),
        "VECTOR field embedding is invalid: VECTOR holds 3 elements while 2 rows of dimension 3");
}

TEST(ArrowUtilsTest, TestCheckNullableMatchWithMap) {
    auto key_field = arrow::field("key", arrow::int32(), /*nullable=*/false);
    auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/true);
    auto map_type = std::make_shared<arrow::MapType>(key_field, value_field);
    auto map_field = arrow::field("map_column", map_type, /*nullable=*/false);
    auto schema = arrow::schema({map_field});

    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({map_field}), R"([
      [null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(ArrowUtils::CheckNullabilityMatch(schema, array),
                            "CheckNullabilityMatch failed, field map_column not nullable while "
                            "data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({map_field}), R"([
      [[[1, null]]]
])")
                .ValueOrDie();
        ASSERT_OK(ArrowUtils::CheckNullabilityMatch(schema, array));
    }
}

TEST(ArrowUtilsTest, TestCheckNullableMatchComplex) {
    auto key_field = arrow::field("key", arrow::int32(), /*nullable=*/false);
    auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/false);

    auto inner_child1 =
        arrow::field("inner1",
                     arrow::map(arrow::utf8(), arrow::field("inner_list", arrow::list(value_field),
                                                            /*nullable=*/true)),
                     /*nullable=*/false);
    auto inner_child2 = arrow::field(
        "inner2",
        arrow::map(arrow::utf8(), arrow::field("inner_map", arrow::map(arrow::utf8(), value_field),
                                               /*nullable=*/true)),
        /*nullable=*/false);
    auto inner_child3 = arrow::field(
        "inner3",
        arrow::map(arrow::utf8(),
                   arrow::field("inner_struct", arrow::struct_({key_field, value_field}),
                                /*nullable=*/true)),
        /*nullable=*/false);

    auto schema = arrow::schema({inner_child1, inner_child2, inner_child3});
    // test inner1
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3, null]]], [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]],
[[["outer_key", null]], [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]],
[null, [["outer_key", [["key1", 1]]]], [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field inner1 not nullable while data have null value");
    }
    // test inner2
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", [100, 200]]]],
[[["outer_key", null]], [["outer_key", [["key1", null]]]], [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", [100, 200]]]],
[[["outer_key", [1, 2, 3]]], null, [["outer_key", [100, 200]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field inner2 not nullable while data have null value");
    }
    // test inner3
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", null]]],
[[["outer_key", null]], [["outer_key", [["key1", 2]]]], [["outer_key", [100, null]]]]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field value not nullable while data have null value");
    }
    {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(
                arrow::struct_({inner_child1, inner_child2, inner_child3}), R"([
[[["outer_key", [1, 2, 3]]], [["outer_key", null]], [["outer_key", null]]],
[[["outer_key", null]], [["outer_key", [["key1", 2]]]], null]
])")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            ArrowUtils::CheckNullabilityMatch(schema, array),
            "CheckNullabilityMatch failed, field inner3 not nullable while data have null value");
    }
}

TEST(ArrowUtilsTest, TestRemoveFieldFromStructArrayFieldNotFound) {
    auto struct_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8())});
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(
                         struct_type, R"([{"a":1,"b":"x"},{"a":2,"b":"y"},{"a":3,"b":"z"}])")
                         .ValueOrDie();
    auto src_struct_array = checked_pointer_cast<arrow::StructArray>(src_array);

    ASSERT_OK_AND_ASSIGN(auto result,
                         ArrowUtils::RemoveFieldFromStructArray(src_struct_array, "missing"));

    ASSERT_TRUE(result->Equals(src_struct_array));
    ASSERT_EQ(result->type()->num_fields(), 2);
}

TEST(ArrowUtilsTest, TestRemoveFieldFromStructArraySuccess) {
    auto struct_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8()),
                        arrow::field("c", arrow::int64())});
    auto src_array =
        arrow::ipc::internal::json::ArrayFromJSON(
            struct_type,
            R"([{"a":1,"b":"x","c":10},{"a":2,"b":"y","c":20},{"a":3,"b":"z","c":30}])")
            .ValueOrDie();
    auto src_struct_array = checked_pointer_cast<arrow::StructArray>(src_array);

    ASSERT_OK_AND_ASSIGN(auto result,
                         ArrowUtils::RemoveFieldFromStructArray(src_struct_array, "b"));

    auto expected_type =
        arrow::struct_({arrow::field("a", arrow::int32()), arrow::field("c", arrow::int64())});
    auto expected_array = arrow::ipc::internal::json::ArrayFromJSON(
                              expected_type, R"([{"a":1,"c":10},{"a":2,"c":20},{"a":3,"c":30}])")
                              .ValueOrDie();
    auto expected_struct_array = checked_pointer_cast<arrow::StructArray>(expected_array);

    ASSERT_EQ(result->type()->num_fields(), 2);
    ASSERT_EQ(result->type()->field(0)->name(), "a");
    ASSERT_EQ(result->type()->field(1)->name(), "c");
    ASSERT_TRUE(result->Equals(expected_struct_array));
}

TEST(ArrowUtilsTest, TestNormalizeRecordBatchOffsets) {
    auto value_field = arrow::field("value", arrow::int32());
    auto nested_field = arrow::field("nested", arrow::struct_({value_field}));
    auto text_field = arrow::field("text", arrow::utf8());
    auto clean_field = arrow::field("clean", arrow::boolean());
    auto schema = arrow::schema({nested_field, text_field, clean_field});

    std::shared_ptr<arrow::Array> values =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 2, 3, 4]").ValueOrDie();
    std::shared_ptr<arrow::Array> sliced_values = values->Slice(1, 3);
    std::shared_ptr<arrow::StructArray> nested_column =
        arrow::StructArray::Make({sliced_values}, {value_field->name()}).ValueOrDie();
    std::shared_ptr<arrow::Array> text =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c", "d", "e"])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> sliced_text = text->Slice(1, 3);
    std::shared_ptr<arrow::Array> clean_column =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::boolean(), "[true, false, true]")
            .ValueOrDie();
    std::shared_ptr<arrow::RecordBatch> record_batch = arrow::RecordBatch::Make(
        schema, /*num_rows=*/3, {nested_column, sliced_text, clean_column});

    ASSERT_EQ(nested_column->offset(), 0);
    ASSERT_EQ(nested_column->field(0)->offset(), 1);
    ASSERT_EQ(sliced_text->offset(), 1);
    ASSERT_EQ(clean_column->offset(), 0);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::RecordBatch> normalized_batch,
        ArrowUtils::NormalizeRecordBatchOffsets(record_batch, arrow::default_memory_pool()));
    ASSERT_NE(normalized_batch.get(), record_batch.get());
    ASSERT_TRUE(normalized_batch->Equals(*record_batch));
    std::shared_ptr<arrow::StructArray> normalized_nested =
        checked_pointer_cast<arrow::StructArray>(normalized_batch->column(0));
    ASSERT_EQ(normalized_nested->offset(), 0);
    ASSERT_EQ(normalized_nested->field(0)->offset(), 0);
    ASSERT_EQ(normalized_batch->column(1)->offset(), 0);
    ASSERT_EQ(normalized_batch->column(2)->offset(), 0);
    ASSERT_NE(normalized_batch->column_data(0).get(), record_batch->column_data(0).get());
    ASSERT_NE(normalized_batch->column_data(1).get(), record_batch->column_data(1).get());
    ASSERT_EQ(normalized_batch->column_data(2).get(), record_batch->column_data(2).get());

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::RecordBatch> unchanged_batch,
        ArrowUtils::NormalizeRecordBatchOffsets(normalized_batch, arrow::default_memory_pool()));
    ASSERT_EQ(unchanged_batch.get(), normalized_batch.get());
}

TEST(ArrowUtilsTest, TestNormalizeArrayOffsetsSlicesZeroOffsetStructChildren) {
    std::shared_ptr<arrow::Array> ints =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 2, 3]").ValueOrDie();
    std::shared_ptr<arrow::Array> texts =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["a", "b", "c", "d"])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> array =
        arrow::StructArray::Make({ints, texts}, std::vector<std::string>{"i", "s"}).ValueOrDie();
    std::shared_ptr<arrow::Array> sliced = array->Slice(/*offset=*/0, /*length=*/2);
    ASSERT_EQ(0, sliced->offset());
    ASSERT_EQ(4, sliced->data()->child_data[0]->length);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Array> normalized,
                         ArrowUtils::NormalizeArrayOffsets(sliced, arrow::default_memory_pool()));
    ASSERT_TRUE(normalized->Equals(sliced));
    ASSERT_EQ(0, normalized->offset());
    ASSERT_EQ(2, normalized->data()->child_data[0]->length);
    ASSERT_EQ(2, normalized->data()->child_data[1]->length);

    ::ArrowArray c_array = {};
    ::ArrowSchema c_schema = {};
    ASSERT_TRUE(arrow::ExportArray(*normalized, &c_array, &c_schema).ok());
    std::shared_ptr<arrow::RecordBatch> batch =
        arrow::ImportRecordBatch(&c_array, &c_schema).ValueOrDie();
    ASSERT_EQ(2, batch->num_rows());
}

namespace {

/// A buffer that rebasing must expose as a view into the source.
// This struct tells where a ArrayData stores value.
struct SharedBuffer {
    std::vector<int32_t> child_path;
    int buffer_index;
};

struct NormalizeCase {
    std::shared_ptr<arrow::DataType> type;
    std::string json;
    /// The buffers holding the values of this layout, which rebasing must never copy.
    std::vector<SharedBuffer> value_buffers;
};

/// Ten values per case, so that the slices taken below stay in range.
std::vector<NormalizeCase> NormalizeCases() {
    auto int_field = arrow::field("a", arrow::int32());
    auto text_field = arrow::field("b", arrow::utf8());
    return {
        {arrow::boolean(),
         "[true, null, false, true, true, null, false, false, true, null]",
         {{{}, 1}}},
        {arrow::int8(), "[0, 1, null, 3, 4, 5, null, 7, 8, 9]", {{{}, 1}}},
        {arrow::int32(), "[0, 1, null, 3, 4, 5, null, 7, 8, 9]", {{{}, 1}}},
        {arrow::int64(), "[0, 1, null, 3, 4, 5, null, 7, 8, 9]", {{{}, 1}}},
        {arrow::float64(), "[0.5, 1.5, null, 3.5, 4.5, 5.5, null, 7.5, 8.5, 9.5]", {{{}, 1}}},
        {arrow::date32(), "[0, 1, null, 3, 4, 5, null, 7, 8, 9]", {{{}, 1}}},
        {arrow::timestamp(arrow::TimeUnit::MICRO),
         "[0, 1, null, 3, 4, 5, null, 7, 8, 9]",
         {{{}, 1}}},
        {arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"),
         "[0, 1, null, 3, 4, 5, null, 7, 8, 9]",
         {{{}, 1}}},
        {arrow::decimal128(10, 2),
         R"(["1.23", null, "3.45", "6.78", "0.01", null, "9.99", "8.88", "7.77", "6.66"])",
         {{{}, 1}}},
        // no nulls at all, so the validity bitmap is dropped rather than rebased
        {arrow::int32(), "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]", {{{}, 1}}},
        {arrow::utf8(),
         R"(["a", null, "ccc", "dddd", "", "ffffff", null, "h", "ii", "jjj"])",
         {{{}, 2}}},
        {arrow::binary(),
         R"(["a", null, "ccc", "dddd", "", "ffffff", null, "h", "ii", "jjj"])",
         {{{}, 2}}},
        {arrow::large_binary(),
         R"(["a", null, "ccc", "dddd", "", "ffffff", null, "h", "ii", "jjj"])",
         {{{}, 2}}},
        {arrow::list(arrow::int32()),
         "[[1], null, [2, 3], [], [4, 5, 6], null, [7], [8, 9], [], [10]]",
         {{{0}, 1}}},
        {arrow::large_list(arrow::int32()),
         "[[1], null, [2, 3], [], [4, 5, 6], null, [7], [8, 9], [], [10]]",
         {{{0}, 1}}},
        {arrow::list(arrow::utf8()),
         R"([["a"], null, ["bb", "ccc"], [], ["d"], null, ["e", "f"], [], ["g"], ["h"]])",
         {{{0}, 2}}},
        {arrow::fixed_size_list(arrow::int32(), 2),
         "[[0, 1], null, [2, 3], [4, 5], [6, 7], null, [8, 9], [10, 11], [12, 13], [14, 15]]",
         {{{0}, 1}}},
        {arrow::struct_({int_field, text_field}),
         R"([{"a": 0, "b": "x"}, null, {"a": 2, "b": null}, {"a": null, "b": "yyy"},
             {"a": 4, "b": "z"}, {"a": 5, "b": ""}, null, {"a": 7, "b": "w"},
             {"a": 8, "b": "vv"}, {"a": 9, "b": "u"}])",
         {{{0}, 1}, {{1}, 2}}},
        // a list of structs exercises two levels of rebasing at once
        {arrow::list(arrow::struct_({int_field, text_field})),
         R"([[{"a": 0, "b": "x"}], null, [{"a": 2, "b": "y"}, {"a": 3, "b": null}], [],
             [{"a": 4, "b": "z"}], null, [{"a": 6, "b": "w"}], [], [{"a": 8, "b": "v"}],
             [{"a": 9, "b": "u"}]])",
         {{{0, 0}, 1}, {{0, 1}, 2}}},
        {arrow::map(arrow::utf8(), arrow::int32()),
         R"([[["k0", 0]], null, [["k1", 1], ["k2", null]], [], [["k3", 3]], null,
             [["k4", 4], ["k5", 5]], [], [["k6", 6]], [["k7", 7]]])",
         {{{0, 0}, 2}, {{0, 1}, 1}}},
    };
}

const arrow::ArrayData& ResolvePath(const arrow::ArrayData& data,
                                    const std::vector<int32_t>& child_path) {
    const arrow::ArrayData* node = &data;
    for (int32_t child_index : child_path) {
        node = node->child_data[child_index].get();
    }
    return *node;
}

void ExpectAllOffsetsZero(const arrow::ArrayData& data, const std::string& path) {
    ASSERT_EQ(data.offset, 0) << "non-zero offset at " << path;
    for (size_t i = 0; i < data.child_data.size(); i++) {
        ExpectAllOffsetsZero(*data.child_data[i], path + "/child" + std::to_string(i));
    }
}

/// A freshly allocated buffer cannot live inside a buffer that is still alive, so containment
/// proves that `rebased` references the source bytes instead of copying them.
bool IsViewInto(const arrow::Buffer& rebased, const arrow::Buffer& source) {
    return rebased.data() >= source.data() &&
           rebased.data() + rebased.size() <= source.data() + source.size();
}

std::shared_ptr<arrow::RecordBatch> MakeSliceBatch(const std::shared_ptr<arrow::Array>& array,
                                                   int64_t offset, int64_t length) {
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("f", array->type())}), length,
                                    {array->Slice(offset, length)});
}

/// Checks that normalization keeps the same rows with every offset zeroed. `array` is used as a
/// single column batch, so its own offset is whatever the caller built it with.
void CheckNormalizedArray(const std::shared_ptr<arrow::Array>& array) {
    SCOPED_TRACE("type=" + array->type()->ToString());
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("f", array->type())}), array->length(), {array});

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::RecordBatch> normalized,
        ArrowUtils::NormalizeRecordBatchOffsets(batch, arrow::default_memory_pool()));
    // A batch that needs normalization must not be returned unchanged.
    ASSERT_NE(normalized.get(), batch.get());

    arrow::Status validated = normalized->ValidateFull();
    ASSERT_TRUE(validated.ok()) << validated.ToString();
    ASSERT_TRUE(normalized->Equals(*batch))
        << "expected " << batch->ToString() << " but got " << normalized->ToString();
    ExpectAllOffsetsZero(*normalized->column_data(0), "f");
}

/// Slices `array` and checks that normalization keeps the same rows with every offset zeroed.
void CheckNormalizedSlice(const std::shared_ptr<arrow::Array>& array, int64_t offset,
                          int64_t length) {
    SCOPED_TRACE("type=" + array->type()->ToString() + " offset=" + std::to_string(offset) +
                 " length=" + std::to_string(length));
    std::shared_ptr<arrow::RecordBatch> batch = MakeSliceBatch(array, offset, length);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::RecordBatch> normalized,
        ArrowUtils::NormalizeRecordBatchOffsets(batch, arrow::default_memory_pool()));

    arrow::Status validated = normalized->ValidateFull();
    ASSERT_TRUE(validated.ok()) << validated.ToString();
    ASSERT_TRUE(normalized->Equals(*batch))
        << "expected " << batch->ToString() << " but got " << normalized->ToString();
    ExpectAllOffsetsZero(*normalized->column_data(0), "f");
}

}  // namespace

// Check the equality of normalized batches and original batches.
TEST(ArrowUtilsTest, TestNormalizeRecordBatchOffsetsCoversSupportedTypes) {
    for (const NormalizeCase& normalize_case : NormalizeCases()) {
        SCOPED_TRACE("type=" + normalize_case.type->ToString());
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(normalize_case.type, normalize_case.json)
                .ValueOrDie();
        ASSERT_EQ(array->length(), 10);
        // offset 0 takes the no-op path, offsets 1/3/5/9 are not byte aligned, offset 8 is
        for (const auto& [offset, length] : std::vector<std::pair<int64_t, int64_t>>{
                 {0, 10}, {1, 9}, {1, 3}, {3, 4}, {5, 5}, {8, 2}, {9, 1}, {2, 0}}) {
            CheckNormalizedSlice(array, offset, length);
        }
    }
}

// Check the zero-copy property of value buffer rebasing.
TEST(ArrowUtilsTest, TestNormalizeRecordBatchOffsetsSharesValueBuffers) {
    for (const NormalizeCase& normalize_case : NormalizeCases()) {
        SCOPED_TRACE("type=" + normalize_case.type->ToString());
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(normalize_case.type, normalize_case.json)
                .ValueOrDie();
        // A byte aligned offset lets bitmaps be sliced too, so nothing has to be copied here.
        std::shared_ptr<arrow::RecordBatch> batch =
            MakeSliceBatch(array, /*offset=*/8, /*length=*/2);

        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<arrow::RecordBatch> normalized,
            ArrowUtils::NormalizeRecordBatchOffsets(batch, arrow::default_memory_pool()));
        ASSERT_NE(normalized.get(), batch.get());

        for (const SharedBuffer& value_buffer : normalize_case.value_buffers) {
            SCOPED_TRACE("buffer_index=" + std::to_string(value_buffer.buffer_index));
            const arrow::ArrayData& rebased =
                ResolvePath(*normalized->column_data(0), value_buffer.child_path);
            const arrow::ArrayData& source = ResolvePath(*array->data(), value_buffer.child_path);
            ASSERT_TRUE(IsViewInto(*rebased.buffers[value_buffer.buffer_index],
                                   *source.buffers[value_buffer.buffer_index]))
                << "value buffer was copied instead of sliced";
        }
    }
}

// Check the situation that child offsets are non-zero while parent offset is zero.
TEST(ArrowUtilsTest, TestNormalizeRecordBatchOffsetsRebasesNestedOffsetsUnderZeroParent) {
    std::shared_ptr<arrow::Array> ints =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 2, 3, 4, 5, 6, 7]")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> texts =
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::utf8(), R"(["a", "bb", null, "dddd", "e", "ff", "ggg", "h"])")
            .ValueOrDie();

    {
        // struct whose children are sliced: parent offset 0, both children offset 2
        std::shared_ptr<arrow::Array> array =
            arrow::StructArray::Make({ints->Slice(2, 4), texts->Slice(2, 4)},
                                     std::vector<std::string>{"a", "b"})
                .ValueOrDie();
        ASSERT_EQ(array->offset(), 0);
        ASSERT_EQ(array->data()->child_data[0]->offset, 2);
        ASSERT_EQ(array->data()->child_data[1]->offset, 2);
        CheckNormalizedArray(array);
    }
    {
        // only the innermost array is sliced, so detection has to walk two levels down
        std::shared_ptr<arrow::Array> inner =
            arrow::StructArray::Make({ints->Slice(3, 4)}, std::vector<std::string>{"a"})
                .ValueOrDie();
        std::shared_ptr<arrow::Array> array =
            arrow::StructArray::Make({inner}, std::vector<std::string>{"inner"}).ValueOrDie();
        ASSERT_EQ(array->offset(), 0);
        ASSERT_EQ(array->data()->child_data[0]->offset, 0);
        ASSERT_EQ(array->data()->child_data[0]->child_data[0]->offset, 3);
        CheckNormalizedArray(array);
    }
    {
        // list built over sliced values: parent offset 0, values offset 2, and the list offsets
        // address the values relative to that slice
        std::shared_ptr<arrow::Array> offsets =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, 1, 3, 4]")
                .ValueOrDie();
        std::shared_ptr<arrow::Array> array =
            arrow::ListArray::FromArrays(*offsets, *ints->Slice(2, 4)).ValueOrDie();
        ASSERT_EQ(array->offset(), 0);
        ASSERT_EQ(array->data()->child_data[0]->offset, 2);
        CheckNormalizedArray(array);
    }
    {
        // map built over sliced keys and items, which land under the entries struct. Map keys
        // cannot be null, so this slice avoids the null in `texts`.
        std::shared_ptr<arrow::Array> offsets =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 2, 2, 4]").ValueOrDie();
        std::shared_ptr<arrow::Array> array =
            arrow::MapArray::FromArrays(offsets, texts->Slice(3, 4), ints->Slice(4, 4))
                .ValueOrDie();
        ASSERT_EQ(array->offset(), 0);
        const arrow::ArrayData& entries = *array->data()->child_data[0];
        ASSERT_EQ(entries.offset, 0);
        ASSERT_EQ(entries.child_data[0]->offset, 3);
        ASSERT_EQ(entries.child_data[1]->offset, 4);
        CheckNormalizedArray(array);
    }
}

TEST(ArrowUtilsTest, TestNormalizeRecordBatchOffsetsFallsBackForDictionary) {
    // A dictionary is not part of child_data, so this layout takes the copying fallback.
    std::shared_ptr<arrow::Array> indices =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[0, 1, null, 2, 1, 0]")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> dictionary =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::utf8(), R"(["x", "yy", "zzz"])")
            .ValueOrDie();
    auto dictionary_type = arrow::dictionary(arrow::int32(), arrow::utf8());
    std::shared_ptr<arrow::Array> array =
        arrow::DictionaryArray::FromArrays(dictionary_type, indices, dictionary).ValueOrDie();

    CheckNormalizedSlice(array, /*offset=*/1, /*length=*/4);
    CheckNormalizedSlice(array, /*offset=*/3, /*length=*/3);

    // The fallback copies, which is also what makes the sharing checks above meaningful.
    std::shared_ptr<arrow::RecordBatch> batch = MakeSliceBatch(array, /*offset=*/1, /*length=*/4);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<arrow::RecordBatch> normalized,
        ArrowUtils::NormalizeRecordBatchOffsets(batch, arrow::default_memory_pool()));
    ASSERT_FALSE(IsViewInto(*normalized->column_data(0)->buffers[1], *array->data()->buffers[1]));
}

TEST(ArrowUtilsTest, TestEqualsIgnoreNullable) {
    {
        // test simple
        ASSERT_FALSE(ArrowUtils::EqualsIgnoreNullable(arrow::int32(), arrow::int64()));
        ASSERT_TRUE(ArrowUtils::EqualsIgnoreNullable(arrow::int32(), arrow::int32()));
    }
    {
        // test struct
        auto child1 = arrow::field("child1", arrow::int32(), /*nullable=*/false);
        auto child2 = arrow::field("child2", arrow::int32(), /*nullable=*/false);
        auto child3 = arrow::field("child1", arrow::int32(), /*nullable=*/true);
        auto struct_type1 = arrow::struct_({child1});
        auto struct_type2 = arrow::struct_({child2});
        auto struct_type3 = arrow::struct_({child3});
        auto struct_type4 = arrow::struct_({child3, child1});
        ASSERT_FALSE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type2));
        ASSERT_TRUE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type3));
        ASSERT_FALSE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type4));
    }
    {
        auto vector3 = arrow::fixed_size_list(arrow::float32(), 3);
        auto vector3_non_null =
            arrow::fixed_size_list(arrow::field("item", arrow::float32(), false), 3);
        auto vector5 = arrow::fixed_size_list(arrow::float32(), 5);
        ASSERT_TRUE(ArrowUtils::EqualsIgnoreNullable(vector3, vector3_non_null));
        ASSERT_FALSE(ArrowUtils::EqualsIgnoreNullable(vector3, vector5));
    }
    {
        // test complex
        auto key_field = arrow::field("key", arrow::int32(), /*nullable=*/false);
        auto value_field = arrow::field("value", arrow::int32(), /*nullable=*/false);
        auto inner_child1 = arrow::field(
            "inner1",
            arrow::map(arrow::utf8(), arrow::field("inner_list", arrow::list(value_field),
                                                   /*nullable=*/true)),
            /*nullable=*/false);
        auto inner_child2 = arrow::field(
            "inner2",
            arrow::map(arrow::utf8(),
                       arrow::field("inner_map", arrow::map(arrow::utf8(), value_field),
                                    /*nullable=*/true)),
            /*nullable=*/false);
        auto inner_child3 = arrow::field(
            "inner3",
            arrow::map(arrow::utf8(),
                       arrow::field("inner_struct", arrow::struct_({key_field, value_field}),
                                    /*nullable=*/true)),
            /*nullable=*/false);
        auto struct_type1 = arrow::struct_({inner_child1, inner_child2, inner_child3});

        auto key_field_other = arrow::field("key", arrow::int32(), /*nullable=*/true);
        auto value_field_other = arrow::field("value", arrow::int32(), /*nullable=*/true);
        auto inner_child1_other = arrow::field(
            "inner1",
            arrow::map(arrow::utf8(), arrow::field("inner_list", arrow::list(value_field_other),
                                                   /*nullable=*/false)),
            /*nullable=*/true);
        auto inner_child2_other = arrow::field(
            "inner2",
            arrow::map(arrow::utf8(),
                       arrow::field("inner_map", arrow::map(arrow::utf8(), value_field_other),
                                    /*nullable=*/false)),
            /*nullable=*/true);
        auto inner_child3_other = arrow::field(
            "inner3",
            arrow::map(
                arrow::utf8(),
                arrow::field("inner_struct", arrow::struct_({key_field_other, value_field_other}),
                             /*nullable=*/false)),
            /*nullable=*/true);
        auto struct_type2 =
            arrow::struct_({inner_child1_other, inner_child2_other, inner_child3_other});
        ASSERT_TRUE(ArrowUtils::EqualsIgnoreNullable(struct_type1, struct_type2));
    }
}

TEST(ArrowUtilsTest, TestGetCompressionType) {
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType(""));
        ASSERT_EQ(type, arrow::Compression::UNCOMPRESSED);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("none"));
        ASSERT_EQ(type, arrow::Compression::UNCOMPRESSED);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("uncompressed"));
        ASSERT_EQ(type, arrow::Compression::UNCOMPRESSED);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("zstd"));
        ASSERT_EQ(type, arrow::Compression::ZSTD);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("ZSTD"));
        ASSERT_EQ(type, arrow::Compression::ZSTD);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("lz4"));
        ASSERT_EQ(type, arrow::Compression::LZ4_FRAME);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("snappy"));
        ASSERT_EQ(type, arrow::Compression::SNAPPY);
    }
    {
        ASSERT_OK_AND_ASSIGN(auto type, ArrowUtils::GetCompressionType("gzip"));
        ASSERT_EQ(type, arrow::Compression::GZIP);
    }
    {
        // test invalid codec
        ASSERT_NOK(ArrowUtils::GetCompressionType("invalid_codec"));
    }
}

}  // namespace paimon::test
