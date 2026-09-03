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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/realtime/realtime_offset_batch_reader.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/metrics.h"
#include "paimon/status.h"

namespace paimon {
RealtimeOffsetBatchReader::RealtimeOffsetBatchReader(std::unique_ptr<BatchReader>&& reader,
                                                     const OffsetRange& visible_offsets)
    : reader_(std::move(reader)), visible_offsets_(visible_offsets) {}

Result<BatchReader::ReadBatch> RealtimeOffsetBatchReader::NextBatch() {
    return Status::Invalid(
        "paimon inner reader RealtimeOffsetBatchReader should use NextBatchWithBitmap");
}

Result<BatchReader::ReadBatchWithBitmap> RealtimeOffsetBatchReader::NextBatchWithBitmap() {
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap,
                               reader_->NextBatchWithBitmap());
        if (IsEofBatch(batch_with_bitmap)) {
            return batch_with_bitmap;
        }
        auto& [batch, input_bitmap] = batch_with_bitmap;
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (!arrow_array || arrow_array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("realtime query batch must be a StructArray");
        }
        std::shared_ptr<arrow::StructArray> struct_array =
            checked_pointer_cast<arrow::StructArray>(arrow_array);
        std::shared_ptr<arrow::Array> offset_field =
            struct_array->GetFieldByName(SpecialFields::RealtimeOffset().Name());
        if (!offset_field || offset_field->type_id() != arrow::Type::INT64) {
            return Status::Invalid("realtime query batch must contain int64 _REALTIME_OFFSET");
        }
        std::shared_ptr<arrow::Int64Array> offsets =
            checked_pointer_cast<arrow::Int64Array>(offset_field);
        if (offsets->null_count() != 0) {
            return Status::Invalid("realtime query offset column contains null");
        }

        RoaringBitmap32 output_bitmap;
        for (auto iter = input_bitmap.Begin(); iter != input_bitmap.End(); ++iter) {
            const uint32_t row = *iter;
            if (static_cast<int64_t>(row) >= offsets->length()) {
                return Status::Invalid(fmt::format(
                    "selected row id {} is out of bounds for realtime query batch length {}", row,
                    offsets->length()));
            }
            const int64_t offset = offsets->Value(row);
            if (offset >= visible_offsets_.begin && offset < visible_offsets_.end) {
                output_bitmap.Add(row);
            }
        }
        if (static_cast<int64_t>(input_bitmap.Cardinality()) != offsets->length()) {
            return Status::Invalid(
                "real-time store reader bitmap must cover every raw transport row");
        }
        if (output_bitmap.IsEmpty()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::StructArray> output,
                               ArrowUtils::RemoveFieldFromStructArray(
                                   struct_array, SpecialFields::RealtimeOffset().Name()));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*output, c_array.get(), c_schema.get()));
        return ReadBatchWithBitmap(std::move(batch), std::move(output_bitmap));
    }
}

std::shared_ptr<Metrics> RealtimeOffsetBatchReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void RealtimeOffsetBatchReader::Close() {
    reader_->Close();
}

}  // namespace paimon
