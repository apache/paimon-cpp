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

#include "paimon/common/reader/index_score_batch_utils.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"

namespace paimon {

Result<BatchReader::ReadBatchWithBitmap> IndexScoreBatchUtils::CompleteScoresImpl(
    BatchReader::ReadBatchWithBitmap batch, const std::shared_ptr<arrow::StructArray>& data,
    const std::shared_ptr<arrow::Schema>& output_schema, const ScoreLookups& lookups,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    for (const auto& entry : lookups) {
        const auto& field_name = entry.first;
        const auto indices = output_schema->GetAllFieldIndices(field_name);
        if (indices.empty()) {
            return Status::Invalid("Missing index score field '", field_name, "'");
        }
        if (indices.size() != 1) {
            return Status::Invalid("Duplicate index score field '", field_name, "'");
        }
        if (output_schema->field(indices[0])->type()->id() != arrow::Type::FLOAT) {
            return Status::Invalid("Index score field '", field_name, "' must have float32 type");
        }
    }
    arrow::ArrayVector arrays;
    for (int32_t position = 0; position < output_schema->num_fields(); ++position) {
        const auto& field = output_schema->field(position);
        auto found = lookups.find(field->name());
        if (found == lookups.end()) {
            auto array = position < data->num_fields() &&
                                 data->struct_type()->field(position)->name() == field->name()
                             ? data->field(position)
                             : data->GetFieldByName(field->name());
            if (!array) {
                return Status::Invalid("Missing output field: ", field->name());
            }
            arrays.push_back(std::move(array));
            continue;
        }
        arrow::FloatBuilder builder(arrow_pool.get());
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(data->length()));
        bool all_not_null = (data->length() == batch.second.Cardinality());
        size_t selected = 0;
        for (int64_t row = 0; row < data->length(); ++row) {
            if (!all_not_null && !batch.second.Contains(row)) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(float score, found->second(row, selected++));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(score));
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array, builder.Finish());
        arrays.push_back(std::move(array));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> output,
                                      arrow::StructArray::Make(arrays, output_schema->fields()));
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*output, batch.first.first.get(), batch.first.second.get()));
    PAIMON_RETURN_NOT_OK(
        AddArrowArrayLifetime(batch.first.first.get(), batch.first.second.get(), arrow_pool));
    return batch;
}

}  // namespace paimon
