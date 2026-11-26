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

#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>

#include "arrow/type.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/result.h"

namespace paimon {

/// Separates the requested output from the columns needed to produce it.
class ReadSchemaPlan {
 public:
    static Result<std::shared_ptr<ReadSchemaPlan>> Create(
        const std::shared_ptr<arrow::Schema>& source_schema,
        const std::shared_ptr<arrow::Schema>& output_schema,
        const std::set<std::string>& required_fields) {
        if (!source_schema || !output_schema) {
            return Status::Invalid("ReadSchemaPlan requires non-null schemas");
        }
        for (const std::string& name : required_fields) {
            if (source_schema->GetFieldIndex(name) < 0) {
                return Status::Invalid("Cannot resolve required field: ", name);
            }
        }
        PAIMON_RETURN_NOT_OK(ArrowSchemaValidator::ValidateSchema(*output_schema));
        arrow::FieldVector fields = output_schema->fields();
        std::set<std::string> included;
        for (const auto& field : fields) {
            included.insert(field->name());
        }
        // Retain output order and nested projections; append dependencies in source order.
        for (const auto& field : source_schema->fields()) {
            if (required_fields.count(field->name()) != 0 &&
                included.insert(field->name()).second) {
                fields.push_back(field);
            }
        }
        return std::shared_ptr<ReadSchemaPlan>(
            new ReadSchemaPlan(output_schema, arrow::schema(fields, output_schema->metadata())));
    }

    const std::shared_ptr<arrow::Schema>& GetReadSchema() const {
        return read_schema_;
    }
    const std::shared_ptr<arrow::Schema>& GetOutputSchema() const {
        return output_schema_;
    }
    bool NeedsProjection() const {
        return read_schema_->num_fields() != output_schema_->num_fields();
    }

 private:
    ReadSchemaPlan(std::shared_ptr<arrow::Schema> output_schema,
                   std::shared_ptr<arrow::Schema> read_schema)
        : output_schema_(std::move(output_schema)), read_schema_(std::move(read_schema)) {}

    std::shared_ptr<arrow::Schema> output_schema_;
    std::shared_ptr<arrow::Schema> read_schema_;
};
}  // namespace paimon
