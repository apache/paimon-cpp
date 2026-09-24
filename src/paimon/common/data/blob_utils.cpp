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

#include "paimon/common/data/blob_utils.h"

#include <cstddef>
#include <set>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_view_struct.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/data/blob_descriptor.h"
namespace arrow {
class Array;
}

namespace paimon {
BlobUtils::SeparatedSchemas BlobUtils::SeparateBlobSchema(
    const std::shared_ptr<arrow::Schema>& schema, const std::set<std::string>& inline_fields) {
    std::vector<std::shared_ptr<arrow::Field>> main_fields;
    std::vector<std::shared_ptr<arrow::Field>> blob_fields;
    for (int32_t i = 0; i < schema->num_fields(); i++) {
        auto field = schema->field(i);
        if (IsBlobField(field) && inline_fields.count(field->name()) == 0) {
            // Non-inline BLOB -> goes to blob file
            blob_fields.emplace_back(field);
        } else {
            // Non-blob fields OR inline BLOB fields -> stay in main
            main_fields.emplace_back(field);
        }
    }
    SeparatedSchemas result;
    result.main_schema = arrow::schema(main_fields);
    result.blob_schema = arrow::schema(blob_fields);
    return result;
}

Result<BlobUtils::SeparatedStructArrays> BlobUtils::SeparateBlobArray(
    const std::shared_ptr<arrow::StructArray>& struct_array,
    const std::set<std::string>& inline_fields) {
    std::shared_ptr<arrow::StructType> old_type =
        checked_pointer_cast<arrow::StructType>(struct_array->type());
    const auto& old_fields = old_type->fields();
    const auto& old_arrays = struct_array->fields();

    arrow::ArrayVector main_arrays;
    arrow::ArrayVector blob_arrays;
    arrow::FieldVector main_fields;
    arrow::FieldVector blob_fields;

    for (size_t i = 0; i < old_fields.size(); i++) {
        if (IsBlobField(old_fields[i]) && inline_fields.count(old_fields[i]->name()) == 0) {
            blob_fields.push_back(old_fields[i]);
            blob_arrays.push_back(old_arrays[i]);
        } else {
            main_fields.push_back(old_fields[i]);
            main_arrays.push_back(old_arrays[i]);
        }
    }

    if (blob_fields.empty()) {
        return Status::Invalid(
            "SeparateBlobArray expects at least one non-inline blob field, but got none.");
    }

    SeparatedStructArrays result;
    if (!main_fields.empty()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(result.main_array,
                                          arrow::StructArray::Make(main_arrays, main_fields));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(result.blob_array,
                                      arrow::StructArray::Make(blob_arrays, blob_fields));
    return result;
}

bool BlobUtils::IsBlobField(const std::shared_ptr<arrow::Field>& field) {
    if (field == nullptr) {
        return false;
    }
    const auto& type = field->type();
    if (type->id() != arrow::Type::LARGE_BINARY) {
        return false;
    }
    if (!field->HasMetadata()) {
        return false;
    }
    return IsBlobMetadata(field->metadata());
}

bool BlobUtils::IsArrayBlobField(const std::shared_ptr<arrow::Field>& field) {
    if (field == nullptr || field->type()->id() != arrow::Type::LIST) {
        return false;
    }
    const auto& list_type = checked_cast<const arrow::ListType&>(*field->type());
    // Arrow's C schema importer passes MakeChildField(0) directly to ListType, retaining the
    // element field's metadata.
    return IsBlobField(list_type.value_field());
}

bool BlobUtils::IsMapBlobField(const std::shared_ptr<arrow::Field>& field) {
    if (field == nullptr || field->type()->id() != arrow::Type::MAP) {
        return false;
    }
    const auto& map_type = checked_cast<const arrow::MapType&>(*field->type());
    // Arrow's C schema bridge does not retain nested field metadata for MapType. Paimon's
    // ordinary binary type is BINARY, so LARGE_BINARY uniquely identifies a BLOB value here.
    return map_type.item_type()->id() == arrow::Type::LARGE_BINARY;
}

bool BlobUtils::IsBlobFileField(const std::shared_ptr<arrow::Field>& field) {
    return IsBlobField(field) || IsArrayBlobField(field) || IsMapBlobField(field);
}

bool BlobUtils::IsArrayBlobPlaceholder(const arrow::ListArray& array, int64_t row) {
    if (array.IsNull(row) || array.value_length(row) != 1) {
        return false;
    }
    const std::shared_ptr<arrow::Array>& values = array.values();
    if (values->type_id() != arrow::Type::LARGE_BINARY) {
        return false;
    }
    const int64_t value_index = array.value_offset(row);
    if (values->IsNull(value_index)) {
        return false;
    }
    const auto& binary_values = checked_cast<const arrow::LargeBinaryArray&>(*values);
    const std::string_view value = binary_values.GetView(value_index);
    return BlobDefs::IsPlaceholderSentinel(value.data(), value.size());
}

bool BlobUtils::IsMapBlobPlaceholder(const arrow::MapArray& array, int64_t row) {
    if (array.IsNull(row) || array.value_length(row) != 2) {
        return false;
    }
    const int64_t entry_index = array.value_offset(row);
    const std::shared_ptr<arrow::Array>& items = array.items();
    if (!items->IsNull(entry_index) || !items->IsNull(entry_index + 1)) {
        return false;
    }
    const std::shared_ptr<arrow::Array>& keys = array.keys();
    return keys->RangeEquals(entry_index, entry_index + 1, entry_index + 1, *keys);
}

Status BlobUtils::ValidateContainerBlobWriteSchema(const std::shared_ptr<arrow::Schema>& schema) {
    for (const auto& field : schema->fields()) {
        if (IsMapBlobField(field)) {
            return Status::NotImplemented(
                "Writing a table with MAP<..., BLOB> is not supported by the C++ writer.");
        }
        if (IsArrayBlobField(field)) {
            return Status::NotImplemented(
                "Writing a table with ARRAY<BLOB> is not supported by the C++ writer.");
        }
    }
    return Status::OK();
}

bool BlobUtils::IsBlobMetadata(const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
    if (!metadata) {
        return false;
    }
    auto extension_name = metadata->Get(BlobDefs::kExtensionTypeKey);
    if (!extension_name.ok()) {
        return false;
    }
    return extension_name.ValueUnsafe() == BlobDefs::kExtensionTypeValue;
}

bool BlobUtils::IsBlobFile(const std::string& file_name) {
    return StringUtils::EndsWith(file_name, ".blob");
}

std::shared_ptr<arrow::Field> BlobUtils::ToArrowField(
    const std::string& field_name, bool nullable,
    std::unordered_map<std::string, std::string> metadata) {
    metadata[BlobDefs::kExtensionTypeKey] = BlobDefs::kExtensionTypeValue;
    return arrow::field(field_name, arrow::large_binary(), nullable,
                        std::make_shared<arrow::KeyValueMetadata>(metadata));
}

Status BlobUtils::ValidateBlobInlineFields(const std::shared_ptr<arrow::StructArray>& struct_array,
                                           const std::set<std::string>& field_names,
                                           const std::string& config_label) {
    if (field_names.empty()) {
        return Status::OK();
    }
    if (!struct_array) {
        return Status::Invalid("array in ValidateBlobInlineFields must be a struct_array");
    }

    bool is_descriptor = (config_label == "blob-descriptor-field");
    for (const auto& field_name : field_names) {
        auto field_array = struct_array->GetFieldByName(field_name);
        if (!field_array) {
            continue;
        }
        if (field_array->type_id() != arrow::Type::LARGE_BINARY) {
            return Status::Invalid(
                fmt::format("cannot cast array for field {} to LargeBinaryArray", field_name));
        }
        const auto* binary_array = checked_cast<const arrow::LargeBinaryArray*>(field_array.get());
        for (int64_t row = 0; row < binary_array->length(); ++row) {
            if (binary_array->IsNull(row)) {
                continue;
            }
            auto value = binary_array->GetView(row);
            Result<bool> valid = is_descriptor
                                     ? BlobDescriptor::IsBlobDescriptor(value.data(), value.size())
                                     : BlobViewStruct::IsBlobViewStruct(value.data(), value.size());
            PAIMON_ASSIGN_OR_RAISE(bool is_valid, std::move(valid));
            if (!is_valid) {
                return Status::Invalid(fmt::format(
                    "BLOB inline field {} require values to be set as corresponding type.",
                    field_name));
            }
        }
    }
    return Status::OK();
}

std::vector<DataField> BlobUtils::ConvertBlobInlineDataFields(
    const std::vector<DataField>& data_fields, const std::vector<std::string>& blob_inline_fields) {
    if (blob_inline_fields.empty()) {
        return data_fields;
    }

    std::set<std::string> blob_inline_field_set(blob_inline_fields.begin(),
                                                blob_inline_fields.end());
    std::vector<DataField> converted_fields;
    converted_fields.reserve(data_fields.size());
    for (const auto& data_field : data_fields) {
        if (blob_inline_field_set.find(data_field.Name()) == blob_inline_field_set.end()) {
            converted_fields.push_back(data_field);
            continue;
        }

        auto binary_field = arrow::field(data_field.Name(), arrow::binary(), data_field.Nullable(),
                                         data_field.ArrowField()->metadata());
        converted_fields.emplace_back(data_field.Id(), binary_field, data_field.Description());
    }
    return converted_fields;
}

}  // namespace paimon
