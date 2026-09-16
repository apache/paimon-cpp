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

#include "paimon/indexer/lumina/lumina_index_accumulator.h"

#include <utility>

#include "fmt/format.h"
#include "lumina/extensions/experimental/BuildCombinedExtensionV0.h"
#include "paimon/indexer/lumina/lumina_dataset.h"
#include "paimon/indexer/lumina/lumina_utils.h"

namespace paimon::lumina {

Status LuminaIndexAccumulator::AddBatch(const std::shared_ptr<arrow::StructArray>& struct_array,
                                        const std::shared_ptr<arrow::ListArray>& vectors,
                                        uint32_t dimension,
                                        const std::vector<LuminaTagField>& tag_fields,
                                        int64_t first_row_id) {
    // Split into contiguous non-null segments, skipping null rows in the list field.
    int64_t segment_start = -1;
    for (int64_t i = 0; i <= vectors->length(); ++i) {
        bool is_null = i < vectors->length() && vectors->IsNull(i);
        bool is_end = i == vectors->length();
        if (!is_null && !is_end && segment_start < 0) {
            segment_start = i;
        }
        if ((is_null || is_end) && segment_start >= 0) {
            int64_t segment_length = i - segment_start;
            // Use value_offset to precisely locate the float range for this segment.
            int64_t value_start = vectors->value_offset(segment_start);
            int64_t value_end = vectors->value_offset(i);
            std::shared_ptr<arrow::FloatArray> values =
                std::dynamic_pointer_cast<arrow::FloatArray>(
                    vectors->values()->Slice(value_start, value_end - value_start));
            if (!values) {
                return Status::Invalid(
                    "invalid sliced value array in LuminaIndexWriter, must be float array");
            }
            if (values->null_count() != 0) {
                return Status::Invalid(
                    "field value array in LuminaIndexWriter is invalid, must not null");
            }
            for (int64_t row = segment_start; row < i; ++row) {
                int64_t vector_length = vectors->value_offset(row + 1) - vectors->value_offset(row);
                if (vector_length != static_cast<int64_t>(dimension)) {
                    return Status::Invalid(fmt::format(
                        "invalid input array in LuminaIndexWriter, vector at row [{}] has length "
                        "[{}], expected dimension [{}]",
                        row, vector_length, dimension));
                }
            }
            if (!tag_fields.empty()) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::vector<::lumina::extensions::experimental::TagDimensionData> tag_data,
                    LuminaTagUtils::ExtractTagDataForSegment(struct_array, tag_fields,
                                                             segment_start, segment_length));
                tag_data_vec_.push_back(std::move(tag_data));
            }
            arrays_.push_back(std::move(values));
            array_start_ids_.push_back(first_row_id + segment_start);
            indexed_count_ += segment_length;
            segment_start = -1;
        }
    }
    return Status::OK();
}

Result<::lumina::api::LuminaBuilder> LuminaIndexAccumulator::Build(
    const ::lumina::api::BuilderOptions& builder_options, uint32_t dimension, bool with_tag,
    LuminaMemoryPool* pool) {
    ::lumina::core::MemoryResourceConfig memory_resource(pool);
    PAIMON_ASSIGN_OR_RAISE_FROM_LUMINA(
        ::lumina::api::LuminaBuilder builder,
        ::lumina::api::LuminaBuilder::Create(builder_options, memory_resource));

    // Pretrain before inserting the accumulated vectors.
    LuminaDataset pretrain_data(indexed_count_, dimension, arrays_, array_start_ids_);
    PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.PretrainFrom(pretrain_data));

    // insert data
    if (!with_tag) {
        LuminaDataset insert_data(indexed_count_, dimension, arrays_, array_start_ids_);
        std::vector<std::shared_ptr<arrow::FloatArray>>().swap(arrays_);
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.InsertFrom(insert_data));
    } else {
        ::lumina::extensions::experimental::BuildWithTagExtension tag_extension;
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(builder.Attach(tag_extension));
        LuminaDatasetWithTag insert_data(indexed_count_, dimension, arrays_, array_start_ids_,
                                         tag_data_vec_);
        std::vector<std::shared_ptr<arrow::FloatArray>>().swap(arrays_);
        std::vector<std::vector<::lumina::extensions::experimental::TagDimensionData>>().swap(
            tag_data_vec_);
        PAIMON_RETURN_NOT_OK_FROM_LUMINA(tag_extension.InsertFromWithTag(insert_data));
    }
    return std::move(builder);
}

}  // namespace paimon::lumina
