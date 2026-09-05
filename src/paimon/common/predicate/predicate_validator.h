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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/defs.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate.h"
#include "paimon/status.h"

namespace paimon {
class PredicateValidator {
 public:
    PredicateValidator() = delete;
    ~PredicateValidator() = delete;

    static Status ValidatePredicateWithLiterals(const std::shared_ptr<Predicate>& predicate) {
        if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
            const auto& field_name = leaf_predicate->FieldName();
            // check field type (predicate vs. literals)
            auto field_type = leaf_predicate->GetFieldType();
            const auto& literals = leaf_predicate->Literals();
            for (const auto& literal : literals) {
                if (literal.IsNull()) {
                    return Status::Invalid(fmt::format(
                        "literal cannot be null in predicate, field name {}", field_name));
                }
                if (field_type != literal.GetType()) {
                    return Status::Invalid(fmt::format(
                        "field {} has field type {} in literal, mismatch field type {} "
                        "in predicate",
                        field_name, FieldTypeUtils::FieldTypeToString(literal.GetType()),
                        FieldTypeUtils::FieldTypeToString(field_type)));
                }
            }
        } else if (auto compound_predicate =
                       std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
            const auto& children = compound_predicate->Children();
            for (const auto& child : children) {
                PAIMON_RETURN_NOT_OK(ValidatePredicateWithLiterals(child));
            }
        }
        return Status::OK();
    }

    static Status ValidatePredicateWithSchema(const arrow::Schema& schema,
                                              const std::shared_ptr<Predicate>& predicate,
                                              bool validate_field_idx) {
        if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
            const auto& field_name = leaf_predicate->FieldName();
            // check field index
            int32_t schema_field_idx = schema.GetFieldIndex(field_name);
            if (schema_field_idx == -1) {
                return Status::Invalid(
                    fmt::format("field {} does not exist in schema", field_name));
            }
            if (validate_field_idx && schema_field_idx != leaf_predicate->FieldIndex()) {
                return Status::Invalid(
                    fmt::format("field {} has field idx {} in input schema, mismatch field idx "
                                "{} in predicate",
                                field_name, schema_field_idx, leaf_predicate->FieldIndex()));
            }
            // check field type (schema vs. predicate)
            const std::shared_ptr<arrow::DataType>& schema_type =
                schema.field(schema_field_idx)->type();
            PAIMON_RETURN_NOT_OK(ValidateDataTypeWithSchemaAndPredicate(
                *schema_type, leaf_predicate->GetFieldType()));
            if (schema_type->id() == arrow::Type::DECIMAL128) {
                PAIMON_RETURN_NOT_OK(ValidateDecimalLiterals(
                    *checked_pointer_cast<arrow::Decimal128Type>(schema_type), *leaf_predicate));
            }
        } else if (auto compound_predicate =
                       std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
            const auto& children = compound_predicate->Children();
            for (const auto& child : children) {
                PAIMON_RETURN_NOT_OK(
                    ValidatePredicateWithSchema(schema, child, validate_field_idx));
            }
        }
        return Status::OK();
    }

 private:
    static Status ValidateDecimalLiterals(const arrow::Decimal128Type& field_type,
                                          const LeafPredicate& predicate) {
        const std::string& field_name = predicate.FieldName();
        for (const Literal& literal : predicate.Literals()) {
            const auto decimal = literal.GetValue<Decimal>();
            if (decimal.Scale() != field_type.scale()) {
                return Status::Invalid(fmt::format(
                    "decimal literal for field {} has scale {}, expected {}; rescale the literal "
                    "before building the predicate",
                    field_name, decimal.Scale(), field_type.scale()));
            }

            arrow::Decimal128 unscaled_value(decimal.HighBits(), decimal.LowBits());
            if (!unscaled_value.FitsInPrecision(field_type.precision())) {
                return Status::Invalid(fmt::format(
                    "decimal literal {} for field {} does not fit field type DECIMAL({}, {})",
                    decimal.ToString(), field_name, field_type.precision(), field_type.scale()));
            }
        }
        return Status::OK();
    }

    static Status ValidateDataTypeWithSchemaAndPredicate(const arrow::DataType& schema_type,
                                                         const FieldType& field_type) {
        const auto kind = schema_type.id();
        switch (kind) {
            case arrow::Type::type::BOOL: {
                if (field_type == FieldType::BOOLEAN) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::INT8: {
                if (field_type == FieldType::TINYINT) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::INT16: {
                if (field_type == FieldType::SMALLINT) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::INT32: {
                if (field_type == FieldType::INT) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::INT64: {
                if (field_type == FieldType::BIGINT) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::FLOAT: {
                if (field_type == FieldType::FLOAT) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::DOUBLE: {
                if (field_type == FieldType::DOUBLE) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::STRING: {
                if (field_type == FieldType::STRING) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::BINARY: {
                if (field_type == FieldType::BINARY) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::DATE32: {
                if (field_type == FieldType::DATE) {
                    return Status::OK();
                }
                break;
            }
            case arrow::Type::type::DECIMAL128:
                if (field_type == FieldType::DECIMAL) {
                    return Status::OK();
                }
                break;
            case arrow::Type::type::TIMESTAMP: {
                if (field_type == FieldType::TIMESTAMP) {
                    return Status::OK();
                }
                break;
            }
            default: {
                return Status::Invalid(
                    fmt::format("Invalid type {} for predicate", schema_type.ToString()));
            }
        }
        return Status::Invalid(fmt::format("schema type {} mismatches predicate field type {}",
                                           schema_type.ToString(),
                                           FieldTypeUtils::FieldTypeToString(field_type)));
    }
};
}  // namespace paimon
