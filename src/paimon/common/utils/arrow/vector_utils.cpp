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

#include <cstdint>

#include "arrow/array.h"
#include "arrow/array/array_nested.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/checked_cast.h"

namespace paimon {
namespace {

Status ValidateListVector(const arrow::ListArray& array) {
    if (array.values()->null_count() == 0) {
        return Status::OK();
    }
    for (int64_t i = 0; i < array.length(); ++i) {
        if (array.IsNull(i)) {
            continue;
        }
        int64_t value_offset = array.value_offset(i);
        int64_t value_length = array.value_length(i);
        for (int64_t j = 0; j < value_length; ++j) {
            if (array.values()->IsNull(value_offset + j)) {
                return Status::Invalid(fmt::format(
                    "VECTOR cannot contain null elements, found one at row {} position {}", i, j));
            }
        }
    }
    return Status::OK();
}

Status ValidateFixedSizeListVector(const arrow::FixedSizeListArray& array) {
    const auto& vector_type = checked_cast<const arrow::FixedSizeListType&>(*array.type());
    int32_t vector_length = vector_type.list_size();
    const std::shared_ptr<arrow::Array>& values = array.values();
    // Arrow does not check this when importing an array over the C data interface, so the
    // element scan below would otherwise read past the end of the values array.
    if (values->length() < (array.offset() + array.length()) * vector_length) {
        return Status::Invalid(fmt::format(
            "VECTOR holds {} elements while {} rows of dimension {} require {}", values->length(),
            array.length(), vector_length, (array.offset() + array.length()) * vector_length));
    }
    if (values->null_count() == 0) {
        return Status::OK();
    }
    for (int64_t i = 0; i < array.length(); ++i) {
        if (array.IsNull(i)) {
            continue;
        }
        int64_t value_offset = (array.offset() + i) * vector_length;
        for (int32_t j = 0; j < vector_length; ++j) {
            if (values->IsNull(value_offset + j)) {
                return Status::Invalid(fmt::format(
                    "VECTOR cannot contain null elements, found one at row {} position {}", i, j));
            }
        }
    }
    return Status::OK();
}

}  // namespace

bool VectorUtils::ContainsVectorType(const std::shared_ptr<arrow::DataType>& type) {
    if (!type) {
        return false;
    }
    if (type->id() == arrow::Type::FIXED_SIZE_LIST) {
        return true;
    }
    for (const auto& field : type->fields()) {
        if (ContainsVectorType(field->type())) {
            return true;
        }
    }
    return false;
}

bool VectorUtils::ContainsVectorField(const std::shared_ptr<arrow::Field>& field) {
    return field != nullptr && ContainsVectorType(field->type());
}

bool VectorUtils::ContainsVector(const std::shared_ptr<arrow::Schema>& schema) {
    if (!schema) {
        return false;
    }
    for (const auto& field : schema->fields()) {
        if (ContainsVectorField(field)) {
            return true;
        }
    }
    return false;
}

Status VectorUtils::ValidateVectorElements(const arrow::Array& array) {
    switch (array.type_id()) {
        case arrow::Type::LIST:
            return ValidateListVector(checked_cast<const arrow::ListArray&>(array));
        case arrow::Type::FIXED_SIZE_LIST:
            return ValidateFixedSizeListVector(
                checked_cast<const arrow::FixedSizeListArray&>(array));
        default:
            return Status::Invalid(
                fmt::format("Cannot validate VECTOR values of type {}", array.type()->ToString()));
    }
}

}  // namespace paimon
