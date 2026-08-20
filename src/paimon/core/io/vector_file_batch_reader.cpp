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

#include "paimon/core/io/vector_file_batch_reader.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/compute/api.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/arrow/vector_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/status.h"

namespace paimon {
namespace {

std::shared_ptr<arrow::Field> FindField(const std::shared_ptr<arrow::DataType>& type,
                                        const std::string& name) {
    for (const auto& field : type->fields()) {
        if (field->name() == name) {
            return field;
        }
    }
    return nullptr;
}

/// Rebuilds `map_type` with new key and item types, keeping the name and metadata of its
/// entries field.
std::shared_ptr<arrow::DataType> MakeMapType(const arrow::MapType& map_type,
                                             const std::shared_ptr<arrow::Field>& key_field,
                                             const std::shared_ptr<arrow::Field>& item_field) {
    return std::make_shared<arrow::MapType>(
        map_type.value_field()->WithType(arrow::struct_({key_field, item_field})),
        map_type.keys_sorted());
}

/// Returns the type to request from the file format plugin. A VECTOR is only read back as a
/// LIST when the file itself stores it as one: writers such as Paimon Java expose VECTOR
/// columns as Arrow LIST, while Paimon Rust and Python expose them as FixedSizeList.
std::shared_ptr<arrow::DataType> GetPhysicalReadType(
    const std::shared_ptr<arrow::DataType>& logical_type,
    const std::shared_ptr<arrow::DataType>& file_type) {
    switch (logical_type->id()) {
        case arrow::Type::FIXED_SIZE_LIST: {
            if (!file_type || file_type->id() != arrow::Type::LIST) {
                return logical_type;
            }
            const auto& vector_type = checked_cast<const arrow::FixedSizeListType&>(*logical_type);
            const auto& list_type = checked_cast<const arrow::ListType&>(*file_type);
            return arrow::list(vector_type.value_field()->WithType(
                GetPhysicalReadType(vector_type.value_type(), list_type.value_type())));
        }
        case arrow::Type::STRUCT: {
            if (!file_type || file_type->id() != arrow::Type::STRUCT) {
                return logical_type;
            }
            arrow::FieldVector fields;
            fields.reserve(logical_type->num_fields());
            for (const auto& field : logical_type->fields()) {
                std::shared_ptr<arrow::Field> file_field = FindField(file_type, field->name());
                fields.push_back(field->WithType(
                    GetPhysicalReadType(field->type(), file_field ? file_field->type() : nullptr)));
            }
            return arrow::struct_(fields);
        }
        case arrow::Type::LIST: {
            if (!file_type || file_type->id() != arrow::Type::LIST) {
                return logical_type;
            }
            return arrow::list(logical_type->field(0)->WithType(
                GetPhysicalReadType(logical_type->field(0)->type(), file_type->field(0)->type())));
        }
        case arrow::Type::MAP: {
            if (!file_type || file_type->id() != arrow::Type::MAP) {
                return logical_type;
            }
            const auto& map_type = checked_cast<const arrow::MapType&>(*logical_type);
            const auto& file_map_type = checked_cast<const arrow::MapType&>(*file_type);
            return MakeMapType(map_type,
                               map_type.key_field()->WithType(GetPhysicalReadType(
                                   map_type.key_type(), file_map_type.key_type())),
                               map_type.item_field()->WithType(GetPhysicalReadType(
                                   map_type.item_type(), file_map_type.item_type())));
        }
        default:
            return logical_type;
    }
}

Result<std::shared_ptr<arrow::Array>> CastListToVector(
    const std::shared_ptr<arrow::Array>& array,
    const std::shared_ptr<arrow::FixedSizeListType>& read_type, arrow::MemoryPool* pool) {
    if (array->type_id() != arrow::Type::LIST) {
        return Status::Invalid(
            fmt::format("Cannot restore VECTOR from type {}", array->type()->ToString()));
    }
    PAIMON_RETURN_NOT_OK(VectorUtils::ValidateVectorElements(*array));
    arrow::compute::ExecContext exec_context(pool);
    arrow::TypeHolder type_holder(read_type.get());
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> result,
        arrow::compute::Cast(*array, type_holder, options, &exec_context));
    return result;
}

std::shared_ptr<arrow::DataType> RebuildNestedType(
    const std::shared_ptr<arrow::DataType>& read_type,
    const std::vector<std::shared_ptr<arrow::ArrayData>>& children) {
    if (read_type->id() == arrow::Type::STRUCT) {
        arrow::FieldVector fields;
        fields.reserve(children.size());
        for (int32_t i = 0; i < static_cast<int32_t>(children.size()); ++i) {
            fields.push_back(read_type->field(i)->WithType(children[i]->type));
        }
        return arrow::struct_(fields);
    }
    if (read_type->id() == arrow::Type::LIST) {
        return arrow::list(read_type->field(0)->WithType(children[0]->type));
    }

    const auto& entries_type = checked_cast<const arrow::StructType&>(*children[0]->type);
    const auto& map_type = checked_cast<const arrow::MapType&>(*read_type);
    return MakeMapType(map_type, map_type.key_field()->WithType(entries_type.field(0)->type()),
                       map_type.item_field()->WithType(entries_type.field(1)->type()));
}

Result<std::shared_ptr<arrow::Array>> ConvertToReadType(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& read_type,
    arrow::MemoryPool* pool) {
    if (!VectorUtils::ContainsVectorType(read_type)) {
        return array;
    }
    switch (read_type->id()) {
        case arrow::Type::FIXED_SIZE_LIST: {
            if (array->type_id() == arrow::Type::FIXED_SIZE_LIST) {
                const auto& source_type =
                    checked_cast<const arrow::FixedSizeListType&>(*array->type());
                const auto& vector_type = checked_cast<const arrow::FixedSizeListType&>(*read_type);
                if (source_type.list_size() != vector_type.list_size() ||
                    !source_type.value_type()->Equals(vector_type.value_type())) {
                    return Status::Invalid(fmt::format("VECTOR type mismatch: data {} vs read {}",
                                                       source_type.ToString(),
                                                       vector_type.ToString()));
                }
                PAIMON_RETURN_NOT_OK(VectorUtils::ValidateVectorElements(*array));
                // Writers disagree on the element field, for example `element: float not null`
                // for Paimon Rust against the `item: float` of a Paimon schema. Restore the
                // requested type so that files storing VECTOR as LIST and files storing it as
                // FixedSizeList produce batches of one type.
                std::shared_ptr<arrow::ArrayData> data = array->data()->Copy();
                data->type = read_type;
                return arrow::MakeArray(data);
            }
            return CastListToVector(
                array, checked_pointer_cast<arrow::FixedSizeListType>(read_type), pool);
        }
        case arrow::Type::STRUCT:
        case arrow::Type::LIST:
        case arrow::Type::MAP: {
            if (array->type_id() != read_type->id()) {
                return Status::Invalid(fmt::format("Cannot reconcile file type {} with {}",
                                                   array->type()->ToString(),
                                                   read_type->ToString()));
            }
            if (array->type()->num_fields() != read_type->num_fields() ||
                array->data()->child_data.size() != static_cast<size_t>(read_type->num_fields())) {
                return Status::Invalid(
                    fmt::format("Cannot reconcile file type {} with {}: nested field count differs",
                                array->type()->ToString(), read_type->ToString()));
            }
            std::vector<std::shared_ptr<arrow::ArrayData>> children;
            children.reserve(read_type->num_fields());
            for (int32_t i = 0; i < read_type->num_fields(); ++i) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<arrow::Array> child,
                    ConvertToReadType(arrow::MakeArray(array->data()->child_data[i]),
                                      read_type->field(i)->type(), pool));
                children.push_back(child->data());
            }
            std::shared_ptr<arrow::ArrayData> data = array->data()->Copy();
            data->child_data = std::move(children);
            data->type = RebuildNestedType(read_type, data->child_data);
            return arrow::MakeArray(data);
        }
        default:
            return array;
    }
}

}  // namespace

VectorFileBatchReader::VectorFileBatchReader(std::unique_ptr<FileBatchReader>&& reader,
                                             const std::shared_ptr<MemoryPool>& pool)
    : arrow_pool_(GetArrowPool(pool)), reader_(std::move(reader)) {}

bool VectorFileBatchReader::ContainsVector(const std::shared_ptr<arrow::Schema>& schema) {
    return VectorUtils::ContainsVector(schema);
}

Status VectorFileBatchReader::SetReadSchema(
    ::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
    const std::optional<RoaringBitmap32>& selection_bitmap) {
    if (!read_schema) {
        return Status::Invalid("SetReadSchema failed: read schema cannot be nullptr");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> logical_schema,
                                      arrow::ImportSchema(read_schema));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<::ArrowSchema> c_file_schema, reader_->GetFileSchema());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> file_schema,
                                      arrow::ImportSchema(c_file_schema.get()));
    arrow::FieldVector physical_fields;
    physical_fields.reserve(logical_schema->num_fields());
    for (const auto& field : logical_schema->fields()) {
        std::shared_ptr<arrow::Field> file_field = file_schema->GetFieldByName(field->name());
        physical_fields.push_back(field->WithType(
            GetPhysicalReadType(field->type(), file_field ? file_field->type() : nullptr)));
    }
    std::shared_ptr<arrow::Schema> physical_schema =
        arrow::schema(physical_fields, logical_schema->metadata());
    ArrowSchema c_physical_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*physical_schema, &c_physical_schema));
    PAIMON_RETURN_NOT_OK(reader_->SetReadSchema(&c_physical_schema, predicate, selection_bitmap));
    read_type_ = arrow::struct_(logical_schema->fields());
    return Status::OK();
}

Result<BatchReader::ReadBatch> VectorFileBatchReader::ConvertBatch(ReadBatch&& batch) const {
    if (BatchReader::IsEofBatch(batch) || !read_type_) {
        return std::move(batch);
    }
    auto& [c_array, c_schema] = batch;
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    PAIMON_ASSIGN_OR_RAISE(array, ConvertToReadType(array, read_type_, arrow_pool_.get()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(array->Validate());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
    return std::move(batch);
}

Result<BatchReader::ReadBatch> VectorFileBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, reader_->NextBatch());
    return ConvertBatch(std::move(batch));
}

Result<BatchReader::ReadBatchWithBitmap> VectorFileBatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap, reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        return std::move(batch_with_bitmap);
    }
    PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, ConvertBatch(std::move(batch_with_bitmap.first)));
    batch_with_bitmap.first = std::move(batch);
    return std::move(batch_with_bitmap);
}

}  // namespace paimon
