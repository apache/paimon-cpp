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

#include "paimon/core/io/key_value_meta_projection_consumer.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <functional>
#include <numeric>

#include "arrow/api.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;
Result<std::unique_ptr<KeyValueMetaProjectionConsumer>> KeyValueMetaProjectionConsumer::Create(
    const std::shared_ptr<arrow::Schema>& target_schema, const std::shared_ptr<MemoryPool>& pool) {
    std::vector<int32_t> target_to_src_mapping(target_schema->num_fields() -
                                               SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT);
    std::iota(target_to_src_mapping.begin(), target_to_src_mapping.end(), 0);
    return Create(target_schema, target_to_src_mapping, pool);
}
Result<std::unique_ptr<KeyValueMetaProjectionConsumer>> KeyValueMetaProjectionConsumer::Create(
    const std::shared_ptr<arrow::Schema>& target_schema,
    const std::vector<int32_t>& target_to_src_mapping, const std::shared_ptr<MemoryPool>& pool) {
    if (static_cast<size_t>(target_schema->num_fields() -
                            SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT) !=
        target_to_src_mapping.size()) {
        return Status::Invalid(
            fmt::format("target_schema field count without special fields {} and "
                        "target_to_src_mapping size {} mismatch in KeyValueMetaProjectionConsumer",
                        target_schema->num_fields() - SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT,
                        target_to_src_mapping.size()));
    }

    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(pool);
    // target fields of output array: special fields + value fields
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(
        arrow_pool.get(), arrow::struct_(target_schema->fields()), &array_builder));

    auto struct_builder = checked_pointer_cast<arrow::StructBuilder>(std::move(array_builder));
    auto* sequence_builder = struct_builder->field_builder(0);
    if (!sequence_builder || !sequence_builder->type() ||
        sequence_builder->type()->id() != arrow::Type::INT64) {
        return Status::Invalid("sequence_appender cannot cast to Int64Builder");
    }
    auto* sequence_appender = checked_cast<arrow::Int64Builder*>(sequence_builder);
    auto* value_kind_builder = struct_builder->field_builder(1);
    if (!value_kind_builder || !value_kind_builder->type() ||
        value_kind_builder->type()->id() != arrow::Type::INT8) {
        return Status::Invalid("value_kind_appender cannot cast to Int8Builder");
    }
    auto* value_kind_appender = checked_cast<arrow::Int8Builder*>(value_kind_builder);
    // appenders only contains array_builder of value schema, sequence_appender and
    // value_kind_appender are not in appenders
    std::vector<RowToArrowArrayConverter::AppendValueFunc> appenders;
    appenders.reserve(target_schema->num_fields() - SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT);
    // first is the root struct array, and 2 special fields
    int32_t reserve_count = 1 + SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT;
    for (int32_t i = SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT; i < target_schema->num_fields();
         i++) {
        PAIMON_ASSIGN_OR_RAISE(
            RowToArrowArrayConverter::AppendValueFunc func,
            AppendField(/*use_view=*/true, struct_builder->field_builder(i), &reserve_count));
        appenders.emplace_back(func);
    }
    return std::unique_ptr<KeyValueMetaProjectionConsumer>(new KeyValueMetaProjectionConsumer(
        reserve_count, std::move(appenders), std::move(struct_builder), arrow_pool,
        target_to_src_mapping, sequence_appender, value_kind_appender));
}

Result<KeyValueBatch> KeyValueMetaProjectionConsumer::NextBatch(
    const std::vector<KeyValue>& key_value_vec) {
    if (key_value_vec.empty()) {
        return Status::Invalid("invalid key value batch, cannot be empty");
    }
    KeyValueBatch key_value_batch;
    PAIMON_RETURN_NOT_OK(ResetAndReserve());

    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        array_builder_->AppendValues(key_value_vec.size(), /*valid_bytes=*/nullptr));

    key_value_batch.min_key = key_value_vec[0].key;
    key_value_batch.max_key = key_value_vec.back().key;

    // append special fields
    for (const auto& row : key_value_vec) {
        if (row.value_kind->IsRetract()) {
            key_value_batch.delete_row_count++;
        }
        key_value_batch.min_sequence_number =
            std::min(key_value_batch.min_sequence_number, row.sequence_number);
        key_value_batch.max_sequence_number =
            std::max(key_value_batch.max_sequence_number, row.sequence_number);

        PAIMON_RETURN_NOT_OK_FROM_ARROW(sequence_appender_->Append(row.sequence_number));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            value_kind_appender_->Append(row.value_kind->ToByteValue()));
    }
    // append value fields
    for (size_t i = 0; i < appenders_.size(); i++) {
        for (const auto& row : key_value_vec) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(appenders_[i](*(row.value), target_to_src_mapping_[i]));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch result_batch, FinishAndAccumulate());
    key_value_batch.batch = std::move(result_batch.first);
    ArrowSchemaRelease(result_batch.second.get());
    return std::move(key_value_batch);
}

}  // namespace paimon
