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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/result.h"

namespace paimon {

class RealtimeUtils {
 public:
    RealtimeUtils() = delete;
    ~RealtimeUtils() = delete;

    static Result<int32_t> GetDeduplicateKeyPosition(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& deduplicate_key_fields) {
        if (!schema) {
            return Status::Invalid("real-time deduplicate schema must not be null");
        }
        // TODO(xinyu.lxy): Support multiple user-defined deduplicate key fields. This requires
        // composite-key projection and serialization in the configured field order.
        if (deduplicate_key_fields.size() != 1) {
            return Status::Invalid(
                "real-time deduplicate currently requires exactly one deduplicate key field");
        }
        const int32_t key_position = schema->GetFieldIndex(deduplicate_key_fields[0]);
        if (key_position < 0) {
            return Status::Invalid("real-time deduplicate key field does not exist: ",
                                   deduplicate_key_fields[0]);
        }
        return key_position;
    }

    static Status ValidateOffsetField(const std::shared_ptr<arrow::Schema>& schema) {
        if (!schema) {
            return Status::Invalid("real-time schema must not be null");
        }
        const std::shared_ptr<arrow::Field> offset_field =
            schema->GetFieldByName(SpecialFields::RealtimeOffset().Name());
        if (!offset_field || offset_field->type()->id() != arrow::Type::INT64 ||
            offset_field->nullable()) {
            return Status::Invalid("real-time schema requires non-null int64 _REALTIME_OFFSET");
        }
        return Status::OK();
    }

    static Status ValidateDeduplicateSchema(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<std::string>& deduplicate_key_fields) {
        PAIMON_RETURN_NOT_OK(ValidateOffsetField(schema));
        PAIMON_ASSIGN_OR_RAISE(int32_t key_position,
                               GetDeduplicateKeyPosition(schema, deduplicate_key_fields));
        const std::shared_ptr<arrow::Field>& key_field = schema->field(key_position);
        if (key_field->name() == SpecialFields::RealtimeOffset().Name()) {
            return Status::Invalid("real-time offset field cannot be used as the deduplicate key");
        }
        if (key_field->nullable()) {
            return Status::Invalid("real-time deduplicate key field must be non-null: ",
                                   key_field->name());
        }
        if (ArrowSchemaValidator::IsNestedType(key_field->type())) {
            return Status::Invalid("real-time deduplicate key field must not be nested: ",
                                   key_field->name());
        }
        return Status::OK();
    }

    // TODO(xinyu.lxy): Relax the bitmap-only restriction after other exact file indexes can
    // return row positions for realtime offsets.
    static Status ValidateDeduplicateFileIndex(const CoreOptions& options) {
        PAIMON_ASSIGN_OR_RAISE(FileIndexOptions file_indexes,
                               FileIndexOptions::FromCoreOptions(options));
        const std::string& offset_name = SpecialFields::RealtimeOffset().Name();
        for (const FileIndexDefinition& definition : file_indexes.Definitions()) {
            if (definition.column_name == offset_name && definition.index_type == "bitmap") {
                return Status::OK();
            }
        }
        return Status::Invalid(
            "real-time deduplicate mode requires file-index.bitmap.columns to include ",
            offset_name);
    }
};

}  // namespace paimon
