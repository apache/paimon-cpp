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

#include "paimon/core/realtime/realtime_primary_key_reader.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/utils/nested_projection_utils.h"
#include "paimon/macros.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

namespace {

template <typename Reader>
void CloseReaders(const std::vector<std::unique_ptr<Reader>>& readers) {
    for (const std::unique_ptr<Reader>& reader : readers) {
        if (reader) {
            reader->Close();
        }
    }
}

class RealtimeOffsetCoverage {
 public:
    static Result<std::shared_ptr<RealtimeOffsetCoverage>> Create(const OffsetRange& offsets,
                                                                  size_t reader_count,
                                                                  bool allow_committed_prefix) {
        if (offsets.begin < 0 || offsets.end < offsets.begin) {
            return Status::Invalid("PK real-time store returned an invalid offset range");
        }
        return std::shared_ptr<RealtimeOffsetCoverage>(
            new RealtimeOffsetCoverage(offsets, reader_count, allow_committed_prefix));
    }

    Status Add(const arrow::Int64Array& offsets) {
        for (int64_t row = 0; row < offsets.length(); ++row) {
            const int64_t offset = offsets.Value(row);
            if (allow_committed_prefix_ && offset < 0) {
                return Status::Invalid("PK real-time store reader offset must be non-negative");
            }
            if (allow_committed_prefix_ && offset < offsets_.begin) {
                continue;
            }
            if (offset < offsets_.begin || offset >= offsets_.end) {
                return Status::Invalid(
                    allow_committed_prefix_
                        ? "PK real-time store query reader offset is outside the visible range"
                        : "PK real-time store commit reader offset is outside the sealed range");
            }
            if (!seen_offsets_.CheckedAdd(offset)) {
                return CoverageError();
            }
        }
        return Status::OK();
    }

    Status FinishReader() {
        ++finished_reader_count_;
        if (finished_reader_count_ == reader_count_ &&
            seen_offsets_.Cardinality() != offsets_.Count()) {
            return CoverageError();
        }
        return Status::OK();
    }

 private:
    RealtimeOffsetCoverage(const OffsetRange& offsets, size_t reader_count,
                           bool allow_committed_prefix)
        : offsets_(offsets),
          reader_count_(reader_count),
          allow_committed_prefix_(allow_committed_prefix) {}

    Status CoverageError() const {
        return Status::Invalid(
            allow_committed_prefix_
                ? "PK real-time store query readers did not cover the visible range"
                : "PK real-time store commit readers did not cover the sealed range");
    }

    OffsetRange offsets_;
    size_t reader_count_;
    bool allow_committed_prefix_;
    RoaringBitmap64 seen_offsets_;
    size_t finished_reader_count_ = 0;
};

Status CheckTransportField(const std::shared_ptr<arrow::Schema>& schema, int32_t field_idx,
                           const DataField& expected_field) {
    if (schema->num_fields() <= field_idx) {
        return Status::Invalid(
            fmt::format("realtime primary-key transport schema is missing field {} at index {}",
                        expected_field.Name(), field_idx));
    }
    const std::shared_ptr<arrow::Field>& field = schema->field(field_idx);
    PAIMON_ASSIGN_OR_RAISE(int32_t field_id, NestedProjectionUtils::GetPaimonFieldId(field));
    if (field->name() != expected_field.Name() || !field->type()->Equals(*expected_field.Type()) ||
        field->nullable() || field_id != expected_field.Id()) {
        return Status::Invalid(fmt::format(
            "realtime primary-key transport schema field {} must be non-null {}:{} with field id "
            "{}, got {}:{} nullable={} field id {}",
            field_idx, expected_field.Name(), expected_field.Type()->ToString(),
            expected_field.Id(), field->name(), field->type()->ToString(), field->nullable(),
            field_id));
    }
    return Status::OK();
}

Result<std::vector<int32_t>> ResolveFieldIndexes(
    const std::shared_ptr<arrow::Schema>& transport_schema,
    const std::unordered_map<int32_t, int32_t>& field_indexes,
    const std::shared_ptr<arrow::Schema>& row_schema) {
    std::vector<int32_t> result;
    result.reserve(row_schema->num_fields());
    for (const std::shared_ptr<arrow::Field>& row_field : row_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(int32_t field_id,
                               NestedProjectionUtils::GetPaimonFieldId(row_field));
        auto field_index = field_indexes.find(field_id);
        if (field_index == field_indexes.end()) {
            return Status::Invalid(fmt::format(
                "cannot find field id {} in realtime primary-key transport schema", field_id));
        }
        const std::shared_ptr<arrow::Field>& transport_field =
            transport_schema->field(field_index->second);
        if (!transport_field->type()->Equals(row_field->type())) {
            return Status::Invalid(fmt::format(
                "realtime primary-key transport field id {} type {} does not match row type {}",
                field_id, transport_field->type()->ToString(), row_field->type()->ToString()));
        }
        result.push_back(field_index->second);
    }
    return result;
}

class RealtimePrimaryKeyReaderPlan {
 public:
    static Result<std::shared_ptr<const RealtimePrimaryKeyReaderPlan>> Create(
        const std::shared_ptr<arrow::Schema>& transport_schema,
        const std::shared_ptr<arrow::Schema>& key_schema,
        const std::shared_ptr<arrow::Schema>& value_schema) {
        std::unordered_map<int32_t, int32_t> field_indexes;
        field_indexes.reserve(transport_schema->num_fields() -
                              RealtimePrimaryKeyLayout::kValueStartIndex);
        for (int32_t i = RealtimePrimaryKeyLayout::kValueStartIndex;
             i < transport_schema->num_fields(); ++i) {
            PAIMON_ASSIGN_OR_RAISE(int32_t field_id, NestedProjectionUtils::GetPaimonFieldId(
                                                         transport_schema->field(i)));
            if (!field_indexes.emplace(field_id, i).second) {
                return Status::Invalid(fmt::format(
                    "duplicate field id {} in realtime primary-key transport schema", field_id));
            }
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> key_field_indexes,
                               ResolveFieldIndexes(transport_schema, field_indexes, key_schema));
        PAIMON_ASSIGN_OR_RAISE(std::vector<int32_t> value_field_indexes,
                               ResolveFieldIndexes(transport_schema, field_indexes, value_schema));
        return std::shared_ptr<const RealtimePrimaryKeyReaderPlan>(new RealtimePrimaryKeyReaderPlan(
            transport_schema, std::move(key_field_indexes), std::move(value_field_indexes)));
    }

    const std::shared_ptr<arrow::Schema>& TransportSchema() const {
        return transport_schema_;
    }

    const std::vector<int32_t>& KeyFieldIndexes() const {
        return key_field_indexes_;
    }

    const std::vector<int32_t>& ValueFieldIndexes() const {
        return value_field_indexes_;
    }

 private:
    RealtimePrimaryKeyReaderPlan(const std::shared_ptr<arrow::Schema>& schema,
                                 std::vector<int32_t>&& key_indexes,
                                 std::vector<int32_t>&& value_indexes)
        : transport_schema_(schema),
          key_field_indexes_(std::move(key_indexes)),
          value_field_indexes_(std::move(value_indexes)) {}

    const std::shared_ptr<arrow::Schema> transport_schema_;
    const std::vector<int32_t> key_field_indexes_;
    const std::vector<int32_t> value_field_indexes_;
};

class RealtimePrimaryKeyReader final : public KeyValueRecordReader {
 public:
    RealtimePrimaryKeyReader(std::unique_ptr<BatchReader>&& reader,
                             const std::shared_ptr<const RealtimePrimaryKeyReaderPlan>& plan,
                             const std::optional<OffsetRange>& visible_offsets,
                             const std::shared_ptr<MemoryPool>& pool,
                             const std::shared_ptr<RealtimeOffsetCoverage>& offset_coverage)
        : reader_(std::move(reader)),
          plan_(plan),
          visible_offsets_(visible_offsets),
          pool_(pool),
          offset_coverage_(offset_coverage) {}

    class Iterator final : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(RealtimePrimaryKeyReader* reader) : reader_(reader) {}

        Result<bool> HasNext() const override {
            return cursor_ < reader_->RowCount();
        }

        Result<KeyValue> Next() override {
            if (cursor_ >= reader_->RowCount()) {
                return Status::Invalid("No more realtime primary-key values in current iterator");
            }
            const int64_t row = reader_->RowAt(cursor_);
            std::shared_ptr<InternalRow> key =
                std::make_shared<ColumnarRowRef>(reader_->key_ctx_, row);
            auto value = std::make_unique<ColumnarRowRef>(reader_->value_ctx_, row);
            PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                                   RowKind::FromByteValue(reader_->row_kind_array_->Value(row)));
            int64_t sequence_number = reader_->sequence_number_array_->Value(row);
            ++cursor_;
            return KeyValue(row_kind, sequence_number, KeyValue::UNKNOWN_LEVEL, std::move(key),
                            std::move(value));
        }

     private:
        RealtimePrimaryKeyReader* reader_;
        int64_t cursor_ = 0;
    };

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override {
        return NextBatchImpl();
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        ResetBatchState();
        reader_->Close();
    }

 private:
    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatchImpl() {
        while (true) {
            ResetBatchState();
            PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                                   reader_->NextBatchWithBitmap());
            if (BatchReader::IsEofBatch(batch_with_bitmap)) {
                if (offset_coverage_ && !offset_coverage_finished_) {
                    offset_coverage_finished_ = true;
                    PAIMON_RETURN_NOT_OK(offset_coverage_->FinishReader());
                }
                return std::unique_ptr<KeyValueRecordReader::Iterator>();
            }
            auto& [batch, selection] = batch_with_bitmap;
            auto& [c_array, c_schema] = batch;
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                              arrow::ImportArray(c_array.get(), c_schema.get()));
            if (!arrow_array || arrow_array->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid(
                    "cannot cast realtime primary-key transport batch to StructArray");
            }
            std::shared_ptr<arrow::StructArray> data_batch =
                checked_pointer_cast<arrow::StructArray>(arrow_array);
            PAIMON_RETURN_NOT_OK(ValidateTransportBatch(data_batch));

            std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> offset_array =
                checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                    data_batch->field(RealtimePrimaryKeyLayout::kRealtimeOffsetIndex));
            if (offset_coverage_) {
                PAIMON_RETURN_NOT_OK(offset_coverage_->Add(*offset_array));
            }

            row_kind_array_ = checked_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(
                data_batch->field(RealtimePrimaryKeyLayout::kValueKindIndex));
            sequence_number_array_ = checked_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
                data_batch->field(RealtimePrimaryKeyLayout::kSequenceNumberIndex));
            arrow::ArrayVector key_fields;
            key_fields.reserve(plan_->KeyFieldIndexes().size());
            for (int32_t index : plan_->KeyFieldIndexes()) {
                key_fields.push_back(data_batch->field(index));
            }
            arrow::ArrayVector value_fields;
            value_fields.reserve(plan_->ValueFieldIndexes().size());
            for (int32_t index : plan_->ValueFieldIndexes()) {
                value_fields.push_back(data_batch->field(index));
            }
            key_ctx_ = std::make_shared<ColumnarBatchContext>(key_fields, pool_);
            value_ctx_ = std::make_shared<ColumnarBatchContext>(value_fields, pool_);
            PAIMON_ASSIGN_OR_RAISE(bool has_selected_rows,
                                   SelectRows(*offset_array, std::move(selection)));
            if (!has_selected_rows) {
                continue;
            }
            ArrowUtils::TraverseArray(data_batch);
            return std::make_unique<Iterator>(this);
        }
    }

    Status ValidateTransportBatch(const std::shared_ptr<arrow::StructArray>& data_batch) const {
        if (data_batch->num_fields() != plan_->TransportSchema()->num_fields()) {
            return Status::Invalid(fmt::format(
                "realtime primary-key transport batch field count {} does not match schema field "
                "count {}",
                data_batch->num_fields(), plan_->TransportSchema()->num_fields()));
        }
        const arrow::FieldVector& batch_fields = data_batch->type()->fields();
        for (int32_t i = 0; i < data_batch->num_fields(); ++i) {
            if (!batch_fields[i]->Equals(plan_->TransportSchema()->field(i), true)) {
                return Status::Invalid(fmt::format(
                    "realtime primary-key transport batch field {} does not match declared schema",
                    i));
            }
        }
        if (data_batch->field(RealtimePrimaryKeyLayout::kValueKindIndex)->null_count() != 0 ||
            data_batch->field(RealtimePrimaryKeyLayout::kSequenceNumberIndex)->null_count() != 0 ||
            data_batch->field(RealtimePrimaryKeyLayout::kRealtimeOffsetIndex)->null_count() != 0) {
            return Status::Invalid("realtime primary-key transport columns must not contain nulls");
        }
        return Status::OK();
    }

    Result<bool> SelectRows(const arrow::Int64Array& offsets, RoaringBitmap32&& selection) {
        for (auto iter = selection.Begin(); iter != selection.End(); ++iter) {
            const uint32_t row = *iter;
            if (static_cast<int64_t>(row) >= offsets.length()) {
                return Status::Invalid(
                    fmt::format("selected row id {} is out of bounds for realtime primary-key "
                                "transport batch length {}",
                                row, offsets.length()));
            }
        }
        if (selection.Cardinality() != offsets.length()) {
            return Status::Invalid(
                "PK real-time store reader bitmap must cover every raw "
                "transport row");
        }
        selected_rows_.reserve(offsets.length());
        for (int64_t row = 0; row < offsets.length(); ++row) {
            if (!visible_offsets_.has_value() || (offsets.Value(row) >= visible_offsets_->begin &&
                                                  offsets.Value(row) < visible_offsets_->end)) {
                selected_rows_.push_back(row);
            }
        }
        return !selected_rows_.empty();
    }

    int64_t RowCount() const {
        return static_cast<int64_t>(selected_rows_.size());
    }

    int64_t RowAt(int64_t ordinal) const {
        return selected_rows_[ordinal];
    }

    void ResetBatchState() {
        key_ctx_.reset();
        value_ctx_.reset();
        row_kind_array_.reset();
        sequence_number_array_.reset();
        selected_rows_.clear();
    }

 private:
    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<const RealtimePrimaryKeyReaderPlan> plan_;
    std::optional<OffsetRange> visible_offsets_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<RealtimeOffsetCoverage> offset_coverage_;
    bool offset_coverage_finished_ = false;
    std::shared_ptr<ColumnarBatchContext> key_ctx_;
    std::shared_ptr<ColumnarBatchContext> value_ctx_;
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array_;
    std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> sequence_number_array_;
    std::vector<int64_t> selected_rows_;
};

}  // namespace

std::shared_ptr<arrow::Schema> RealtimePrimaryKeyLayout::CreateSchema(
    const std::vector<std::shared_ptr<arrow::Field>>& value_fields) {
    arrow::FieldVector fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber())
            ->WithNullable(false),
        DataField::ConvertDataFieldToArrowField(SpecialFields::RealtimeOffset())};
    fields.insert(fields.end(), value_fields.begin(), value_fields.end());
    return arrow::schema(std::move(fields));
}

Status RealtimePrimaryKeyLayout::ValidateSchema(
    const std::shared_ptr<arrow::Schema>& transport_schema) {
    if (!transport_schema || transport_schema->num_fields() < kValueStartIndex) {
        return Status::Invalid(
            "realtime primary-key transport schema must contain transport fields");
    }
    PAIMON_RETURN_NOT_OK(
        CheckTransportField(transport_schema, kValueKindIndex, SpecialFields::ValueKind()));
    PAIMON_RETURN_NOT_OK(CheckTransportField(transport_schema, kSequenceNumberIndex,
                                             SpecialFields::SequenceNumber()));
    PAIMON_RETURN_NOT_OK(CheckTransportField(transport_schema, kRealtimeOffsetIndex,
                                             SpecialFields::RealtimeOffset()));
    return Status::OK();
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
RealtimePrimaryKeyReaderFactory::CreateForQuery(
    std::vector<std::unique_ptr<BatchReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& transport_schema, const OffsetRange& visible_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::unique_ptr<KeyValueRecordReader>> adapted_readers;
    ScopeGuard remaining_raw_readers_guard([&readers]() { CloseReaders(readers); });
    if (readers.empty() && visible_offsets.begin < visible_offsets.end) {
        return Status::Invalid(
            "PK real-time store returned no query readers for a non-empty visible range");
    }
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("PK real-time store returned a null query reader");
        }
    }
    PAIMON_RETURN_NOT_OK(RealtimePrimaryKeyLayout::ValidateSchema(transport_schema));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<const RealtimePrimaryKeyReaderPlan> plan,
        RealtimePrimaryKeyReaderPlan::Create(transport_schema, key_schema, value_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeOffsetCoverage> offset_coverage,
                           RealtimeOffsetCoverage::Create(visible_offsets, readers.size(),
                                                          /*allow_committed_prefix=*/true));
    adapted_readers.reserve(readers.size());
    for (std::unique_ptr<BatchReader>& reader : readers) {
        adapted_readers.push_back(std::make_unique<RealtimePrimaryKeyReader>(
            std::move(reader), plan, visible_offsets, memory_pool, offset_coverage));
    }
    remaining_raw_readers_guard.Release();
    return adapted_readers;
}

Result<std::vector<std::unique_ptr<KeyValueRecordReader>>>
RealtimePrimaryKeyReaderFactory::CreateForCommit(
    std::vector<std::unique_ptr<BatchReader>>&& readers,
    const std::shared_ptr<arrow::Schema>& transport_schema, const OffsetRange& sealed_offsets,
    const std::shared_ptr<arrow::Schema>& key_schema,
    const std::shared_ptr<arrow::Schema>& value_schema,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    std::vector<std::unique_ptr<KeyValueRecordReader>> adapted_readers;
    ScopeGuard remaining_raw_readers_guard([&readers]() { CloseReaders(readers); });
    if (readers.empty()) {
        return Status::Invalid(
            "PK real-time store returned no commit readers for a sealed segment");
    }
    for (const std::unique_ptr<BatchReader>& reader : readers) {
        if (!reader) {
            return Status::Invalid("PK real-time store returned a null commit reader");
        }
    }
    PAIMON_RETURN_NOT_OK(RealtimePrimaryKeyLayout::ValidateSchema(transport_schema));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<const RealtimePrimaryKeyReaderPlan> plan,
        RealtimePrimaryKeyReaderPlan::Create(transport_schema, key_schema, value_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RealtimeOffsetCoverage> offset_coverage,
                           RealtimeOffsetCoverage::Create(sealed_offsets, readers.size(),
                                                          /*allow_committed_prefix=*/false));
    adapted_readers.reserve(readers.size());
    for (std::unique_ptr<BatchReader>& reader : readers) {
        adapted_readers.push_back(std::make_unique<RealtimePrimaryKeyReader>(
            std::move(reader), plan, std::nullopt, memory_pool, offset_coverage));
    }
    remaining_raw_readers_guard.Release();
    return adapted_readers;
}

}  // namespace paimon
