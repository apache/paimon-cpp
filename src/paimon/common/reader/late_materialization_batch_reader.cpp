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

#include "paimon/common/reader/late_materialization_batch_reader.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"

namespace paimon {

Result<std::unique_ptr<LateMaterializationBatchReader>> LateMaterializationBatchReader::Create(
    const std::shared_ptr<arrow::Schema>& read_schema,
    const std::shared_ptr<arrow::Schema>& probe_schema,
    std::shared_ptr<arrow::StructArray> probe_data,
    const std::shared_ptr<arrow::Schema>& payload_schema,
    std::unique_ptr<BatchReader>&& payload_reader, int32_t read_batch_size,
    const std::shared_ptr<MemoryPool>& pool, std::unique_ptr<arrow::MemoryPool> arrow_pool) {
    if (!read_schema || !probe_schema || !payload_schema || !probe_data) {
        return Status::Invalid("late materialization reader requires non-null schemas and data");
    }
    if (read_batch_size <= 0) {
        return Status::Invalid("late materialization read batch size should be positive");
    }

    std::vector<FieldSource> field_sources;
    field_sources.reserve(read_schema->num_fields());
    bool needs_payload = false;
    for (const std::shared_ptr<arrow::Field>& read_field : read_schema->fields()) {
        int32_t probe_idx = probe_schema->GetFieldIndex(read_field->name());
        if (probe_idx >= 0) {
            field_sources.push_back({Source::PROBE, probe_idx});
            continue;
        }
        int32_t payload_idx = payload_schema->GetFieldIndex(read_field->name());
        if (payload_idx >= 0) {
            field_sources.push_back({Source::PAYLOAD, payload_idx});
            needs_payload = true;
            continue;
        }
        return Status::Invalid(fmt::format(
            "field {} is missing from both probe and payload schemas", read_field->name()));
    }
    if (needs_payload && !payload_reader) {
        return Status::Invalid(
            "late materialization reader requires a payload reader for payload fields");
    }

    return std::unique_ptr<LateMaterializationBatchReader>(new LateMaterializationBatchReader(
        read_schema, std::move(probe_data), std::move(payload_reader), std::move(field_sources),
        read_batch_size, pool, std::move(arrow_pool)));
}

LateMaterializationBatchReader::LateMaterializationBatchReader(
    const std::shared_ptr<arrow::Schema>& read_schema,
    std::shared_ptr<arrow::StructArray> probe_data, std::unique_ptr<BatchReader>&& payload_reader,
    std::vector<FieldSource>&& field_sources, int32_t read_batch_size,
    const std::shared_ptr<MemoryPool>& pool, std::unique_ptr<arrow::MemoryPool> arrow_pool)
    : arrow_pool_(arrow_pool ? std::move(arrow_pool) : GetArrowPool(pool)),
      read_schema_(read_schema),
      probe_data_(std::move(probe_data)),
      payload_reader_(std::move(payload_reader)),
      field_sources_(std::move(field_sources)),
      read_batch_size_(read_batch_size) {}

Result<BatchReader::ReadBatch> LateMaterializationBatchReader::NextBatch() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap, NextBatchWithBitmap());
    return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool_.get());
}

Result<BatchReader::ReadBatchWithBitmap> LateMaterializationBatchReader::NextBatchWithBitmap() {
    if (payload_reader_) {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap payload_batch_with_bitmap,
                               payload_reader_->NextBatchWithBitmap());
        if (BatchReader::IsEofBatch(payload_batch_with_bitmap)) {
            if (probe_offset_ != probe_data_->length()) {
                return Status::Invalid(fmt::format(
                    "late materialization payload ended at {}, but probe row count is {}",
                    probe_offset_, probe_data_->length()));
            }
            return BatchReader::MakeEofBatchWithBitmap();
        }

        ReadBatchWithBitmap moved_payload_batch = std::move(payload_batch_with_bitmap);
        ReadBatch& payload_batch = moved_payload_batch.first;
        RoaringBitmap32& payload_bitmap = moved_payload_batch.second;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> payload_array,
            arrow::ImportArray(payload_batch.first.get(), payload_batch.second.get()));
        if (payload_bitmap.Cardinality() != payload_array->length()) {
            PAIMON_ASSIGN_OR_RAISE(
                arrow::ArrayVector filtered_payload,
                ReaderUtils::GenerateFilteredArrayVector(payload_array, payload_bitmap));
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                payload_array, arrow::Concatenate(filtered_payload, arrow_pool_.get()));
        }
        std::shared_ptr<arrow::StructArray> payload_struct =
            arrow::internal::checked_pointer_cast<arrow::StructArray>(payload_array);
        return MakeBatch(payload_struct);
    }

    if (probe_offset_ >= probe_data_->length()) {
        return BatchReader::MakeEofBatchWithBitmap();
    }
    return MakeBatch(/*payload_data=*/nullptr);
}

Result<BatchReader::ReadBatchWithBitmap> LateMaterializationBatchReader::MakeBatch(
    const std::shared_ptr<arrow::StructArray>& payload_data) {
    int64_t length =
        payload_data ? payload_data->length()
                     : std::min<int64_t>(read_batch_size_, probe_data_->length() - probe_offset_);
    if (probe_offset_ + length > probe_data_->length()) {
        return Status::Invalid(fmt::format(
            "late materialization payload row count exceeds probe row count: offset {}, length {}, "
            "probe {}",
            probe_offset_, length, probe_data_->length()));
    }

    std::shared_ptr<arrow::StructArray> probe_slice =
        arrow::internal::checked_pointer_cast<arrow::StructArray>(
            probe_data_->Slice(probe_offset_, length));
    arrow::ArrayVector arrays;
    arrays.reserve(field_sources_.size());
    for (const FieldSource& field_source : field_sources_) {
        if (field_source.source == Source::PROBE) {
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> normalized_probe_field,
                arrow::Concatenate({probe_slice->field(field_source.index)}, arrow_pool_.get()));
            arrays.push_back(std::move(normalized_probe_field));
        } else {
            if (!payload_data) {
                return Status::Invalid(
                    "late materialization payload field requested without "
                    "payload reader");
            }
            arrays.push_back(payload_data->field(field_source.index));
        }
    }

    std::shared_ptr<arrow::StructArray> struct_array = std::make_shared<arrow::StructArray>(
        arrow::struct_(read_schema_->fields()), length, arrays);
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*struct_array, c_array.get(), c_schema.get()));

    RoaringBitmap32 bitmap;
    bitmap.AddRange(0, static_cast<int32_t>(length));
    probe_offset_ += length;
    return std::make_pair(std::make_pair(std::move(c_array), std::move(c_schema)),
                          std::move(bitmap));
}

std::shared_ptr<Metrics> LateMaterializationBatchReader::GetReaderMetrics() const {
    if (payload_reader_) {
        return payload_reader_->GetReaderMetrics();
    }
    return std::make_shared<MetricsImpl>();
}

void LateMaterializationBatchReader::Close() {
    if (payload_reader_) {
        payload_reader_->Close();
    }
}

}  // namespace paimon
