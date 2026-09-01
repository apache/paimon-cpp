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

#include "paimon/core/realtime/realtime_store_read_pipeline.h"

#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/data/shredding/map_shared_shredding_read_plan_factory.h"
#include "paimon/common/data/variant/variant_shredding_read_plan_factory.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/realtime/realtime_offset_batch_reader.h"
#include "paimon/core/utils/nested_projection_utils.h"

namespace paimon {
namespace {

class PhysicalToLogicalBatchReader : public BatchReader {
 public:
    PhysicalToLogicalBatchReader(
        std::unique_ptr<BatchReader>&& reader, const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>>& plans,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : reader_(std::move(reader)),
          logical_schema_(logical_schema),
          plans_(plans),
          arrow_pool_(arrow_pool) {}

    Result<ReadBatch> NextBatch() override {
        return Status::Invalid(
            "paimon inner reader PhysicalToLogicalBatchReader should use "
            "NextBatchWithBitmap");
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap,
                               reader_->NextBatchWithBitmap());
        if (IsEofBatch(batch_with_bitmap)) {
            return batch_with_bitmap;
        }
        PAIMON_ASSIGN_OR_RAISE(ReadBatch transformed,
                               Transform(std::move(batch_with_bitmap.first)));
        batch_with_bitmap.first = std::move(transformed);
        return batch_with_bitmap;
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

 private:
    Result<ReadBatch> Transform(ReadBatch&& batch) const {
        if (IsEofBatch(batch)) {
            return std::move(batch);
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (!array || array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid(
                "real-time physical-to-logical conversion requires a StructArray");
        }
        auto struct_array = checked_pointer_cast<arrow::StructArray>(array);
        const std::string value_kind_name = SpecialFields::ValueKind().Name();
        if (struct_array->num_fields() == 0 ||
            struct_array->struct_type()->field(0)->name() != value_kind_name) {
            return Status::Invalid("real-time query batch must start with _VALUE_KIND");
        }

        arrow::ArrayVector result_arrays = {struct_array->field(0)};
        arrow::FieldVector result_fields = {struct_array->struct_type()->field(0)};
        result_arrays.reserve(logical_schema_->num_fields() + 1);
        result_fields.reserve(logical_schema_->num_fields() + 1);
        for (const std::shared_ptr<arrow::Field>& read_field : logical_schema_->fields()) {
            if (read_field->name() == value_kind_name) {
                continue;
            }
            int32_t source_index = struct_array->struct_type()->GetFieldIndex(read_field->name());
            if (source_index < 0) {
                return Status::Invalid(
                    fmt::format("real-time query batch does not contain requested field {}",
                                read_field->name()));
            }
            std::shared_ptr<arrow::Array> field_array = struct_array->field(source_index);
            auto plan_iter = plans_.find(read_field->name());
            if (plan_iter != plans_.end()) {
                PAIMON_ASSIGN_OR_RAISE(field_array,
                                       plan_iter->second->Assemble(field_array, arrow_pool_.get()));
            }
            PAIMON_ASSIGN_OR_RAISE(field_array,
                                   NestedProjectionUtils::AlignArrayToReadType(
                                       field_array, read_field->type(), arrow_pool_.get()));
            PAIMON_ASSIGN_OR_RAISE(field_array,
                                   NestedProjectionUtils::FilterMapArrayBySelectedKeysRecursively(
                                       field_array, read_field, arrow_pool_.get()));
            result_arrays.push_back(std::move(field_array));
            result_fields.push_back(read_field);
        }

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> result,
                                          arrow::StructArray::Make(result_arrays, result_fields));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*result, c_array.get(), c_schema.get()));
        PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
        return std::move(batch);
    }

    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::Schema> logical_schema_;
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

}  // namespace

Result<std::unique_ptr<RealtimeStoreReadPipeline>> RealtimeStoreReadPipeline::Create(
    const std::shared_ptr<arrow::Schema>& logical_schema,
    const std::shared_ptr<arrow::Schema>& realtime_write_schema,
    const std::shared_ptr<MemoryPool>& memory_pool,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (!logical_schema || !realtime_write_schema || !memory_pool || !arrow_pool) {
        return Status::Invalid("real-time store read pipeline requires schemas and memory pools");
    }

    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans;
    for (const std::shared_ptr<arrow::Field>& read_field : logical_schema->fields()) {
        if (!NestedProjectionUtils::IsMapSharedShreddingAccessField(read_field)) {
            continue;
        }
        std::shared_ptr<arrow::Field> write_field =
            realtime_write_schema->GetFieldByName(read_field->name());
        if (!write_field) {
            return Status::Invalid(
                fmt::format("selected-key MAP field {} does not exist in real-time write schema",
                            read_field->name()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ShreddingColumnReadPlan> plan,
                               MapSharedShreddingReadPlanFactory::CreateDefaultSelectedKeysReadPlan(
                                   write_field, read_field));
        plans.emplace(read_field->name(), std::move(plan));
    }

    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> variant_plans;
    PAIMON_ASSIGN_OR_RAISE(variant_plans, VariantShreddingReadPlanFactory::CreateReadPlans(
                                              logical_schema, realtime_write_schema, memory_pool));
    for (auto& [field_name, plan] : variant_plans) {
        if (!plans.emplace(field_name, std::move(plan)).second) {
            return Status::Invalid(
                fmt::format("multiple real-time read plans exist for field {}", field_name));
        }
    }

    bool needs_conversion = !plans.empty();
    // PK: _VALUE_KIND, _SEQUENCE_NUMBER, _REALTIME_OFFSET, then requested physical fields.
    // Append: _REALTIME_OFFSET, then requested physical fields.
    arrow::FieldVector store_read_fields;
    store_read_fields.reserve(realtime_write_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& write_field : realtime_write_schema->fields()) {
        if (SpecialFields::IsSystemField(write_field->name())) {
            store_read_fields.push_back(write_field);
        }
    }
    for (const std::shared_ptr<arrow::Field>& read_field : logical_schema->fields()) {
        if (SpecialFields::IsSystemField(read_field->name())) {
            continue;
        }
        auto plan_iter = plans.find(read_field->name());
        store_read_fields.push_back(plan_iter == plans.end() ? read_field
                                                             : plan_iter->second->PhysicalField());
        PAIMON_ASSIGN_OR_RAISE(bool has_selected_keys,
                               NestedProjectionUtils::HasMapSelectedKeysRecursively(read_field));
        needs_conversion = needs_conversion || has_selected_keys;
    }
    auto store_read_schema =
        arrow::schema(std::move(store_read_fields), logical_schema->metadata());
    return std::unique_ptr<RealtimeStoreReadPipeline>(
        new RealtimeStoreReadPipeline(logical_schema, std::move(store_read_schema),
                                      std::move(plans), needs_conversion, arrow_pool));
}

RealtimeStoreReadPipeline::RealtimeStoreReadPipeline(
    std::shared_ptr<arrow::Schema> logical_schema, std::shared_ptr<arrow::Schema> store_read_schema,
    std::map<std::string, std::shared_ptr<ShreddingColumnReadPlan>> plans, bool needs_conversion,
    std::shared_ptr<arrow::MemoryPool> arrow_pool)
    : logical_schema_(std::move(logical_schema)),
      store_read_schema_(std::move(store_read_schema)),
      plans_(std::move(plans)),
      needs_conversion_(needs_conversion),
      arrow_pool_(std::move(arrow_pool)) {}

Result<std::unique_ptr<BatchReader>> RealtimeStoreReadPipeline::Wrap(
    std::unique_ptr<BatchReader>&& store_reader, const OffsetRange& visible_offsets) const {
    if (!store_reader) {
        return Status::Invalid("real-time store read pipeline received a null reader");
    }
    std::unique_ptr<BatchReader> reader =
        std::make_unique<RealtimeOffsetBatchReader>(std::move(store_reader), visible_offsets);
    if (!needs_conversion_) {
        return std::move(reader);
    }
    return std::unique_ptr<BatchReader>(
        new PhysicalToLogicalBatchReader(std::move(reader), logical_schema_, plans_, arrow_pool_));
}

}  // namespace paimon
