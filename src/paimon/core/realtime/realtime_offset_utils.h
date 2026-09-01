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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/macros.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"

namespace paimon {

class RealtimeOffsetUtils {
 public:
    struct ValidatedBatch {
        std::shared_ptr<arrow::StructArray> data;
        std::shared_ptr<arrow::Int64Array> offsets;
        OffsetRange offset_range;
    };

    static std::shared_ptr<arrow::Schema> CreateInputSchema(
        const std::shared_ptr<arrow::Schema>& write_schema) {
        arrow::FieldVector fields = {
            DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset())};
        fields.insert(fields.end(), write_schema->fields().begin(), write_schema->fields().end());
        return arrow::schema(std::move(fields), write_schema->metadata());
    }

    static Result<ValidatedBatch> ValidateBatch(
        RecordBatch* batch, const std::shared_ptr<arrow::Schema>& realtime_input_schema,
        int64_t minimum_offset) {
        if (batch == nullptr || batch->GetData() == nullptr) {
            return Status::Invalid("real-time write batch is null");
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> input,
            arrow::ImportArray(batch->GetData(), arrow::struct_(realtime_input_schema->fields())));
        if (!input || input->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("real-time write data is not a StructArray");
        }
        std::shared_ptr<arrow::StructArray> data = checked_pointer_cast<arrow::StructArray>(input);
        if (data->length() <= 0) {
            return Status::Invalid("real-time offset validation requires a non-empty batch");
        }
        std::shared_ptr<arrow::Array> offset_field =
            data->GetFieldByName(SpecialFields::RealtimeOffset().Name());
        if (!offset_field || offset_field->type_id() != arrow::Type::INT64) {
            return Status::Invalid("real-time write batch must contain int64 _REALTIME_OFFSET");
        }
        std::shared_ptr<arrow::Int64Array> offsets =
            checked_pointer_cast<arrow::Int64Array>(offset_field);
        if (offsets->null_count() != 0) {
            return Status::Invalid("real-time write offset column contains null");
        }
        const int64_t first_offset = offsets->Value(0);
        if (first_offset < minimum_offset) {
            return Status::Invalid("real-time write offset moved backwards or was duplicated");
        }
        int64_t previous_offset = first_offset;
        for (int64_t row = 1; row < offsets->length(); ++row) {
            const int64_t offset = offsets->Value(row);
            if (offset <= previous_offset) {
                return Status::Invalid("real-time write offsets must be strictly increasing");
            }
            previous_offset = offset;
        }
        if (previous_offset == std::numeric_limits<int64_t>::max()) {
            return Status::Invalid("real-time offset range exceeds INT64_MAX");
        }
        return ValidatedBatch{std::move(data), std::move(offsets),
                              OffsetRange(first_offset, previous_offset + 1)};
    }
};

}  // namespace paimon
