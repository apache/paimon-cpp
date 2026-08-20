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

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/type.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/arrow/vector_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/status.h"

namespace paimon::parquet {
namespace {

Result<std::shared_ptr<arrow::Array>> CastToListType(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& write_type,
    arrow::MemoryPool* pool) {
    arrow::compute::ExecContext exec_context(pool);
    arrow::TypeHolder type_holder(write_type.get());
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> result,
        arrow::compute::Cast(*array, type_holder, options, &exec_context));
    return result;
}

/// Rebuilds a nullable VECTOR as a LIST whose null slots have a zero length, dropping the
/// values Arrow keeps for them.
///
/// TODO(ChaomingZhangCN): Cast the whole array once Arrow is upgraded. Arrow 17 casts a null
/// FixedSizeList row to a null LIST slot spanning `list_size` values, and the Parquet writer
/// rejects a LIST with non-zero length null slots.
Result<std::shared_ptr<arrow::Array>> CompactNullVectorsToList(
    const arrow::FixedSizeListArray& vector_array,
    const std::shared_ptr<arrow::DataType>& write_type, arrow::MemoryPool* pool) {
    const auto& vector_type = checked_cast<const arrow::FixedSizeListType&>(*vector_array.type());
    const int32_t vector_length = vector_type.list_size();
    if (vector_array.length() > std::numeric_limits<int32_t>::max() / vector_length) {
        return Status::Invalid("VECTOR values exceed the maximum Parquet LIST offset");
    }

    arrow::Int32Builder offsets_builder(pool);
    arrow::Int64Builder indices_builder(pool);
    arrow::BooleanBuilder validity_builder(pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.Reserve(vector_array.length() + 1));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(indices_builder.Reserve(vector_array.length() * vector_length));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(validity_builder.Reserve(vector_array.length()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.Append(0));

    int32_t offset = 0;
    for (int64_t i = 0; i < vector_array.length(); ++i) {
        bool valid = !vector_array.IsNull(i);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(validity_builder.Append(valid));
        if (valid) {
            int64_t value_offset = (vector_array.offset() + i) * vector_length;
            for (int32_t j = 0; j < vector_length; ++j) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(indices_builder.Append(value_offset + j));
            }
            offset += vector_length;
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.Append(offset));
    }

    std::shared_ptr<arrow::Array> offsets;
    std::shared_ptr<arrow::Array> indices;
    std::shared_ptr<arrow::Array> validity;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(offsets_builder.Finish(&offsets));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(indices_builder.Finish(&indices));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(validity_builder.Finish(&validity));

    arrow::compute::ExecContext exec_context(pool);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        arrow::Datum values,
        arrow::compute::Take(arrow::Datum(vector_array.values()), arrow::Datum(indices),
                             arrow::compute::TakeOptions::NoBoundsCheck(), &exec_context));
    return std::make_shared<arrow::ListArray>(
        write_type, vector_array.length(), offsets->data()->buffers[1], values.make_array(),
        validity->data()->buffers[1], vector_array.null_count());
}

}  // namespace

std::shared_ptr<arrow::DataType> ParquetVectorConverter::GetWriteType(
    const std::shared_ptr<arrow::DataType>& logical_type) {
    switch (logical_type->id()) {
        case arrow::Type::FIXED_SIZE_LIST: {
            const auto& vector_type = checked_cast<const arrow::FixedSizeListType&>(*logical_type);
            return arrow::list(
                vector_type.value_field()->WithType(GetWriteType(vector_type.value_type())));
        }
        case arrow::Type::STRUCT: {
            arrow::FieldVector fields;
            fields.reserve(logical_type->num_fields());
            for (const auto& field : logical_type->fields()) {
                fields.push_back(field->WithType(GetWriteType(field->type())));
            }
            return arrow::struct_(fields);
        }
        case arrow::Type::LIST:
            return arrow::list(
                logical_type->field(0)->WithType(GetWriteType(logical_type->field(0)->type())));
        case arrow::Type::MAP: {
            const auto& map_type = checked_cast<const arrow::MapType&>(*logical_type);
            return std::make_shared<arrow::MapType>(
                map_type.value_field()->WithType(arrow::struct_(
                    {map_type.key_field()->WithType(GetWriteType(map_type.key_type())),
                     map_type.item_field()->WithType(GetWriteType(map_type.item_type()))})),
                map_type.keys_sorted());
        }
        default:
            return logical_type;
    }
}

Result<std::shared_ptr<arrow::Array>> ParquetVectorConverter::ConvertToWriteType(
    const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* pool) {
    if (!VectorUtils::ContainsVectorType(array->type())) {
        return array;
    }
    std::shared_ptr<arrow::DataType> write_type = GetWriteType(array->type());
    switch (array->type_id()) {
        case arrow::Type::FIXED_SIZE_LIST: {
            PAIMON_RETURN_NOT_OK(VectorUtils::ValidateVectorElements(*array));
            const auto& vector_array = checked_cast<const arrow::FixedSizeListArray&>(*array);
            if (vector_array.null_count() == 0) {
                return CastToListType(array, write_type, pool);
            }
            return CompactNullVectorsToList(vector_array, write_type, pool);
        }
        case arrow::Type::STRUCT:
        case arrow::Type::LIST:
        case arrow::Type::MAP: {
            std::vector<std::shared_ptr<arrow::ArrayData>> children;
            children.reserve(array->data()->child_data.size());
            for (const auto& child_data : array->data()->child_data) {
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> child,
                                       ConvertToWriteType(arrow::MakeArray(child_data), pool));
                children.push_back(child->data());
            }
            std::shared_ptr<arrow::ArrayData> data = array->data()->Copy();
            data->child_data = std::move(children);
            data->type = write_type;
            return arrow::MakeArray(data);
        }
        default:
            return array;
    }
}

}  // namespace paimon::parquet
