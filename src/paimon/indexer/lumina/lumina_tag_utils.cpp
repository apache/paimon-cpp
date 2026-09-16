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

#include "paimon/indexer/lumina/lumina_tag_utils.h"

#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "fmt/format.h"
#include "lumina/core/Constants.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "rapidjson/document.h"

namespace paimon::lumina {
namespace {

using TagDimensionData = ::lumina::extensions::experimental::TagDimensionData;
using TagFilter = ::lumina::extensions::experimental::TagFilter;
using TagValue = ::lumina::extensions::experimental::TagValue;
using TagValues = ::lumina::extensions::experimental::TagValues;

Result<std::string> GetRequiredStringMember(const rapidjson::Value& object,
                                            const std::string& field_name,
                                            const std::string& tag_label) {
    rapidjson::Value::ConstMemberIterator iter = object.FindMember(field_name.c_str());
    if (iter == object.MemberEnd()) {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} missing required field: {}", tag_label, field_name));
    }
    if (!iter->value.IsString()) {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} field {} must be string", tag_label, field_name));
    }
    return std::string(iter->value.GetString(), iter->value.GetStringLength());
}

Result<LuminaTagField> ParseTagField(const rapidjson::Value& object, const std::string& tag_label) {
    if (!object.IsObject()) {
        return Status::Invalid(fmt::format("lumina tag_schema {} must be object", tag_label));
    }
    if (object.MemberCount() != 3) {
        return Status::Invalid(fmt::format(
            "lumina tag_schema {} must have exactly 3 fields: key_name, type, value_type",
            tag_label));
    }

    PAIMON_ASSIGN_OR_RAISE(std::string key_name,
                           GetRequiredStringMember(
                               object, std::string(::lumina::core::kExtensionTagKName), tag_label));
    PAIMON_ASSIGN_OR_RAISE(
        std::string type,
        GetRequiredStringMember(object, std::string(::lumina::core::kExtensionTagType), tag_label));
    PAIMON_ASSIGN_OR_RAISE(std::string value_type,
                           GetRequiredStringMember(
                               object, std::string(::lumina::core::kExtensionTagVType), tag_label));
    if (key_name.empty()) {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} key_name must not be empty", tag_label));
    }

    LuminaTagField::Type parsed_type;
    if (type == std::string(::lumina::core::kExtensionTagTypeEnum)) {
        parsed_type = LuminaTagField::Type::ENUM;
    } else if (type == std::string(::lumina::core::kExtensionTagTypeRange)) {
        parsed_type = LuminaTagField::Type::RANGE;
    } else {
        return Status::Invalid(
            fmt::format("lumina tag_schema {} has unsupported type: {}", tag_label, type));
    }

    LuminaTagField::ValueType parsed_value_type;
    if (value_type == std::string(::lumina::core::kExtensionTagVTypeInt32)) {
        parsed_value_type = LuminaTagField::ValueType::INT32;
    } else if (value_type == std::string(::lumina::core::kExtensionTagVTypeInt64)) {
        parsed_value_type = LuminaTagField::ValueType::INT64;
    } else if (value_type == std::string(::lumina::core::kExtensionTagVTypeFloat)) {
        parsed_value_type = LuminaTagField::ValueType::FLOAT;
    } else if (value_type == std::string(::lumina::core::kExtensionTagVTypeDouble)) {
        parsed_value_type = LuminaTagField::ValueType::DOUBLE;
    } else if (value_type == std::string(::lumina::core::kExtensionTagVTypeString)) {
        parsed_value_type = LuminaTagField::ValueType::STRING;
    } else {
        return Status::Invalid(fmt::format("lumina tag_schema {} has unsupported value_type: {}",
                                           tag_label, value_type));
    }
    return LuminaTagField{key_name, parsed_type, parsed_value_type};
}

Status ValidateTagArrowType(const LuminaTagField& tag_field,
                            const std::shared_ptr<arrow::DataType>& field_type) {
    std::shared_ptr<arrow::DataType> value_type = field_type;
    std::shared_ptr<arrow::ListType> list_type =
        std::dynamic_pointer_cast<arrow::ListType>(field_type);
    if (list_type) {
        value_type = list_type->value_type();
    }

    bool compatible = false;
    switch (tag_field.value_type) {
        case LuminaTagField::ValueType::INT32:
            compatible = value_type->id() == arrow::Type::INT8 ||
                         value_type->id() == arrow::Type::INT16 ||
                         value_type->id() == arrow::Type::INT32;
            break;
        case LuminaTagField::ValueType::INT64:
            compatible = value_type->id() == arrow::Type::INT64;
            break;
        case LuminaTagField::ValueType::FLOAT:
            compatible = value_type->id() == arrow::Type::FLOAT;
            break;
        case LuminaTagField::ValueType::DOUBLE:
            compatible = value_type->id() == arrow::Type::DOUBLE;
            break;
        case LuminaTagField::ValueType::STRING:
            compatible = value_type->id() == arrow::Type::STRING;
            break;
    }
    if (!compatible) {
        return Status::Invalid(
            fmt::format("lumina tag field {} type {} is not compatible with tag_schema value_type",
                        tag_field.name, field_type->ToString()));
    }
    return Status::OK();
}

template <typename ValueType, typename ArrayType>
void AppendPrimitiveTagValue(const std::shared_ptr<arrow::Array>& array, int64_t index,
                             std::vector<ValueType>* values) {
    values->push_back(
        static_cast<ValueType>(checked_cast<const ArrayType*>(array.get())->Value(index)));
}

template <typename ValueType>
Status AppendTagValue(const std::shared_ptr<arrow::Array>& array, int64_t index,
                      std::vector<ValueType>* values) {
    if (array->IsNull(index)) {
        return Status::OK();
    }
    auto validate_array_type = [&](arrow::Type::type expected_type,
                                   const char* value_type_name) -> Status {
        if (array->type_id() != expected_type) {
            return Status::Invalid(fmt::format("lumina {} tag field has unsupported arrow type {}",
                                               value_type_name, array->type()->ToString()));
        }
        return Status::OK();
    };

    if constexpr (std::is_same_v<ValueType, int32_t>) {
        switch (array->type_id()) {
            case arrow::Type::INT8:
                AppendPrimitiveTagValue<ValueType, arrow::Int8Array>(array, index, values);
                break;
            case arrow::Type::INT16:
                AppendPrimitiveTagValue<ValueType, arrow::Int16Array>(array, index, values);
                break;
            case arrow::Type::INT32:
                AppendPrimitiveTagValue<ValueType, arrow::Int32Array>(array, index, values);
                break;
            default:
                return Status::Invalid(
                    fmt::format("lumina integer tag field has unsupported arrow type {}",
                                array->type()->ToString()));
        }
    } else if constexpr (std::is_same_v<ValueType, int64_t>) {
        PAIMON_RETURN_NOT_OK(validate_array_type(arrow::Type::INT64, "int64"));
        AppendPrimitiveTagValue<ValueType, arrow::Int64Array>(array, index, values);
    } else if constexpr (std::is_same_v<ValueType, float>) {
        PAIMON_RETURN_NOT_OK(validate_array_type(arrow::Type::FLOAT, "float"));
        AppendPrimitiveTagValue<ValueType, arrow::FloatArray>(array, index, values);
    } else if constexpr (std::is_same_v<ValueType, double>) {
        PAIMON_RETURN_NOT_OK(validate_array_type(arrow::Type::DOUBLE, "double"));
        AppendPrimitiveTagValue<ValueType, arrow::DoubleArray>(array, index, values);
    } else if constexpr (std::is_same_v<ValueType, std::string>) {
        PAIMON_RETURN_NOT_OK(validate_array_type(arrow::Type::STRING, "string"));
        const arrow::StringArray* string_array =
            checked_cast<const arrow::StringArray*>(array.get());
        std::string_view value = string_array->GetView(index);
        values->emplace_back(value.data(), value.size());
    } else {
        return Status::Invalid("lumina tag field has unsupported value type");
    }
    return Status::OK();
}

template <typename ValueType>
Status ExtractTagValues(const std::shared_ptr<arrow::Array>& field_array, int64_t segment_start,
                        int64_t segment_len, std::vector<std::vector<ValueType>>* values) {
    values->resize(segment_len);
    std::shared_ptr<arrow::ListArray> list_array =
        std::dynamic_pointer_cast<arrow::ListArray>(field_array);
    if (list_array) {
        std::shared_ptr<arrow::Array> child_values = list_array->values();
        for (int64_t i = 0; i < segment_len; ++i) {
            int64_t row = segment_start + i;
            if (list_array->IsNull(row)) {
                continue;
            }
            int64_t value_start = list_array->value_offset(row);
            int64_t value_end = list_array->value_offset(row + 1);
            std::vector<ValueType>& row_values = (*values)[i];
            row_values.reserve(value_end - value_start);
            for (int64_t value_index = value_start; value_index < value_end; ++value_index) {
                PAIMON_RETURN_NOT_OK(AppendTagValue(child_values, value_index, &row_values));
            }
        }
        return Status::OK();
    }

    for (int64_t i = 0; i < segment_len; ++i) {
        PAIMON_RETURN_NOT_OK(AppendTagValue(field_array, segment_start + i, &(*values)[i]));
    }
    return Status::OK();
}

Result<TagValue> LiteralToTagValue(const Literal& literal) {
    if (literal.IsNull()) {
        return Status::Invalid("lumina tag predicate does not support null literal");
    }
    switch (literal.GetType()) {
        case FieldType::TINYINT:
            return TagValue(static_cast<int32_t>(literal.GetValue<int8_t>()));
        case FieldType::SMALLINT:
            return TagValue(static_cast<int32_t>(literal.GetValue<int16_t>()));
        case FieldType::INT:
            return TagValue(literal.GetValue<int32_t>());
        case FieldType::BIGINT:
            return TagValue(literal.GetValue<int64_t>());
        case FieldType::FLOAT:
            return TagValue(literal.GetValue<float>());
        case FieldType::DOUBLE:
            return TagValue(literal.GetValue<double>());
        case FieldType::STRING:
            return TagValue(literal.GetValue<std::string>());
        default:
            return Status::Invalid(
                fmt::format("lumina tag predicate does not support literal type {}",
                            static_cast<int32_t>(literal.GetType())));
    }
}

Result<const Literal*> GetSingleLiteral(const std::vector<Literal>& literals,
                                        const std::string& function_name) {
    if (literals.size() != 1) {
        return Status::Invalid(
            fmt::format("lumina tag {} predicate requires one literal", function_name));
    }
    return &literals[0];
}

template <typename ValueType>
Result<TagValues> LiteralsToTypedTagValues(const std::vector<Literal>& literals) {
    std::vector<ValueType> values;
    values.reserve(literals.size());
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(literal));
        ValueType* typed_value = std::get_if<ValueType>(&value);
        if (!typed_value) {
            return Status::Invalid(
                "lumina tag predicate IN literals must have the same value type");
        }
        values.push_back(std::move(*typed_value));
    }
    return TagValues(std::move(values));
}

Result<TagValues> LiteralsToTagValues(const std::vector<Literal>& literals) {
    if (literals.empty()) {
        return Status::Invalid("lumina tag predicate IN requires at least one literal");
    }
    switch (literals[0].GetType()) {
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::INT:
            return LiteralsToTypedTagValues<int32_t>(literals);
        case FieldType::BIGINT:
            return LiteralsToTypedTagValues<int64_t>(literals);
        case FieldType::FLOAT:
            return LiteralsToTypedTagValues<float>(literals);
        case FieldType::DOUBLE:
            return LiteralsToTypedTagValues<double>(literals);
        case FieldType::STRING:
            return LiteralsToTypedTagValues<std::string>(literals);
        default:
            return Status::Invalid(
                fmt::format("lumina tag predicate IN does not support literal type {}",
                            static_cast<int32_t>(literals[0].GetType())));
    }
}

}  // namespace

Result<std::vector<LuminaTagField>> LuminaTagUtils::ParseTagSchema(
    const std::map<std::string, std::string>& lumina_options) {
    auto iter = lumina_options.find(std::string(::lumina::core::kExtensionTagSchema));
    if (iter == lumina_options.end()) {
        return std::vector<LuminaTagField>();
    }

    rapidjson::Document document;
    document.Parse(iter->second.c_str());
    if (document.HasParseError()) {
        return Status::Invalid("lumina tag_schema must be a valid JSON string");
    }

    std::vector<LuminaTagField> tag_fields;
    if (document.IsArray()) {
        if (document.Empty()) {
            return Status::Invalid("lumina tag_schema must contain at least one tag definition");
        }
        tag_fields.reserve(document.Size());
        for (rapidjson::SizeType i = 0; i < document.Size(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(LuminaTagField field,
                                   ParseTagField(document[i], fmt::format("tag[{}]", i)));
            tag_fields.push_back(std::move(field));
        }
    } else if (document.IsObject()) {
        PAIMON_ASSIGN_OR_RAISE(LuminaTagField field, ParseTagField(document, "tag[0]"));
        tag_fields.push_back(std::move(field));
    } else {
        return Status::Invalid("lumina tag_schema must be an object or array of objects");
    }

    std::unordered_set<std::string> seen_names;
    for (const LuminaTagField& field : tag_fields) {
        if (!seen_names.insert(field.name).second) {
            return Status::Invalid(
                fmt::format("lumina tag_schema has duplicate key_name: {}", field.name));
        }
    }
    return tag_fields;
}

Result<std::optional<std::vector<std::string>>> LuminaTagUtils::GetExtraFieldNames(
    const std::map<std::string, std::string>& lumina_options) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<LuminaTagField> tag_fields, ParseTagSchema(lumina_options));
    if (tag_fields.empty()) {
        return std::optional<std::vector<std::string>>(std::nullopt);
    }
    std::vector<std::string> field_names;
    field_names.reserve(tag_fields.size());
    for (const LuminaTagField& tag_field : tag_fields) {
        field_names.push_back(tag_field.name);
    }
    return std::optional<std::vector<std::string>>(std::move(field_names));
}

Status LuminaTagUtils::ValidateTagFields(const arrow::StructType& struct_type,
                                         const std::vector<LuminaTagField>& tag_fields) {
    for (const LuminaTagField& tag_field : tag_fields) {
        std::shared_ptr<arrow::Field> field = struct_type.GetFieldByName(tag_field.name);
        if (!field) {
            return Status::Invalid(
                fmt::format("lumina tag field {} not exist in arrow schema", tag_field.name));
        }
        PAIMON_RETURN_NOT_OK(ValidateTagArrowType(tag_field, field->type()));
    }
    return Status::OK();
}

Result<std::vector<TagDimensionData>> LuminaTagUtils::ExtractTagDataForSegment(
    const std::shared_ptr<arrow::StructArray>& struct_array,
    const std::vector<LuminaTagField>& tag_fields, int64_t segment_start, int64_t segment_len) {
    std::vector<TagDimensionData> tag_dimensions_data;
    tag_dimensions_data.reserve(tag_fields.size());
    for (const LuminaTagField& tag_field : tag_fields) {
        std::shared_ptr<arrow::Array> field_array = struct_array->GetFieldByName(tag_field.name);
        if (!field_array) {
            return Status::Invalid(
                fmt::format("lumina tag field {} not in input array", tag_field.name));
        }

        TagDimensionData tag_dimension_data;
        tag_dimension_data.tagkName = tag_field.name;
        switch (tag_field.value_type) {
            case LuminaTagField::ValueType::INT32: {
                std::vector<std::vector<int32_t>> values;
                PAIMON_RETURN_NOT_OK(
                    ExtractTagValues<int32_t>(field_array, segment_start, segment_len, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
            case LuminaTagField::ValueType::INT64: {
                std::vector<std::vector<int64_t>> values;
                PAIMON_RETURN_NOT_OK(
                    ExtractTagValues<int64_t>(field_array, segment_start, segment_len, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
            case LuminaTagField::ValueType::FLOAT: {
                std::vector<std::vector<float>> values;
                PAIMON_RETURN_NOT_OK(
                    ExtractTagValues<float>(field_array, segment_start, segment_len, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
            case LuminaTagField::ValueType::DOUBLE: {
                std::vector<std::vector<double>> values;
                PAIMON_RETURN_NOT_OK(
                    ExtractTagValues<double>(field_array, segment_start, segment_len, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
            case LuminaTagField::ValueType::STRING: {
                std::vector<std::vector<std::string>> values;
                PAIMON_RETURN_NOT_OK(ExtractTagValues<std::string>(field_array, segment_start,
                                                                   segment_len, &values));
                tag_dimension_data.values = std::move(values);
                break;
            }
        }
        tag_dimensions_data.push_back(std::move(tag_dimension_data));
    }
    return tag_dimensions_data;
}

Result<TagFilter> LuminaTagUtils::PredicateToTagFilter(
    const std::shared_ptr<Predicate>& predicate) {
    if (!predicate) {
        return Status::Invalid("lumina tag predicate must not be null");
    }

    std::shared_ptr<CompoundPredicate> compound_predicate =
        std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (compound_predicate) {
        std::vector<TagFilter> children;
        children.reserve(compound_predicate->Children().size());
        for (const std::shared_ptr<Predicate>& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(TagFilter tag_filter, PredicateToTagFilter(child));
            children.push_back(std::move(tag_filter));
        }
        if (children.empty()) {
            return Status::Invalid("lumina tag compound predicate must have at least one child");
        }
        if (children.size() == 1) {
            return std::move(children.front());
        }
        switch (compound_predicate->GetFunction().GetType()) {
            case Function::Type::AND:
                return TagFilter::And(std::move(children));
            case Function::Type::OR:
                return TagFilter::Or(std::move(children));
            default:
                return Status::NotImplemented(
                    fmt::format("lumina tag predicate does not support compound function {}",
                                compound_predicate->GetFunction().ToString()));
        }
    }

    std::shared_ptr<LeafPredicate> leaf_predicate =
        std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (!leaf_predicate) {
        return Status::Invalid(
            fmt::format("cannot cast predicate {} to CompoundPredicate or LeafPredicate",
                        predicate->ToString()));
    }
    const std::vector<Literal>& literals = leaf_predicate->Literals();
    const std::string& field_name = leaf_predicate->FieldName();
    switch (leaf_predicate->GetFunction().GetType()) {
        case Function::Type::EQUAL: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal, GetSingleLiteral(literals, "equal"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return TagFilter::Eq(field_name, std::move(value));
        }
        case Function::Type::GREATER_THAN: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal,
                                   GetSingleLiteral(literals, "greater than"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return TagFilter::Gt(field_name, std::move(value));
        }
        case Function::Type::GREATER_OR_EQUAL: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal,
                                   GetSingleLiteral(literals, "greater or equal"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return TagFilter::Gte(field_name, std::move(value));
        }
        case Function::Type::LESS_THAN: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal, GetSingleLiteral(literals, "less than"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return TagFilter::Lt(field_name, std::move(value));
        }
        case Function::Type::LESS_OR_EQUAL: {
            PAIMON_ASSIGN_OR_RAISE(const Literal* literal,
                                   GetSingleLiteral(literals, "less or equal"));
            PAIMON_ASSIGN_OR_RAISE(TagValue value, LiteralToTagValue(*literal));
            return TagFilter::Lte(field_name, std::move(value));
        }
        case Function::Type::IN: {
            PAIMON_ASSIGN_OR_RAISE(TagValues values, LiteralsToTagValues(literals));
            return TagFilter::In(field_name, std::move(values));
        }
        default:
            return Status::NotImplemented(
                fmt::format("lumina tag predicate does not support leaf function {}",
                            leaf_predicate->GetFunction().ToString()));
    }
}

}  // namespace paimon::lumina
