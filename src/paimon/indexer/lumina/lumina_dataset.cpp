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

#include "paimon/indexer/lumina/lumina_dataset.h"

#include <cstring>
#include <numeric>
#include <utility>

namespace paimon::lumina {

LuminaDataset::LuminaDataset(int64_t element_count, uint32_t dimension,
                             const std::vector<std::shared_ptr<arrow::FloatArray>>& arrays,
                             const std::vector<int64_t>& start_ids)
    : element_count_(element_count),
      dimension_(dimension),
      arrays_(arrays),
      start_ids_(start_ids) {}

uint32_t LuminaDataset::Dim() const noexcept {
    return dimension_;
}

uint64_t LuminaDataset::TotalSize() const noexcept {
    return static_cast<uint64_t>(element_count_);
}

::lumina::core::Result<uint64_t> LuminaDataset::GetNextBatch(
    std::vector<float>& vector_buffer,
    std::vector<::lumina::core::vector_id_t>& id_buffer) noexcept {
    if (cursor_ >= arrays_.size()) {
        return ::lumina::core::Result<uint64_t>::Ok(0);
    }
    std::shared_ptr<arrow::FloatArray>& values = arrays_[cursor_];
    int64_t value_count = values->length();
    int64_t vector_count = value_count / dimension_;
    vector_buffer.resize(static_cast<size_t>(value_count));
    std::memcpy(vector_buffer.data(), values->raw_values(),
                sizeof(float) * static_cast<size_t>(value_count));
    id_buffer.resize(static_cast<size_t>(vector_count));
    std::iota(id_buffer.begin(), id_buffer.end(),
              static_cast<::lumina::core::vector_id_t>(start_ids_[cursor_]));

    // release the array when copy to vector_buffer
    values.reset();
    ++cursor_;
    return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(vector_count));
}

LuminaDatasetWithTag::LuminaDatasetWithTag(
    int64_t element_count, uint32_t dimension,
    const std::vector<std::shared_ptr<arrow::FloatArray>>& arrays,
    const std::vector<int64_t>& start_ids,
    const std::vector<std::vector<TagDimensionData>>& tag_data)
    : element_count_(element_count),
      dimension_(dimension),
      arrays_(arrays),
      start_ids_(start_ids),
      tag_data_(tag_data) {}

uint32_t LuminaDatasetWithTag::Dim() const noexcept {
    return dimension_;
}

uint64_t LuminaDatasetWithTag::TotalSize() const noexcept {
    return static_cast<uint64_t>(element_count_);
}

::lumina::core::Result<uint64_t> LuminaDatasetWithTag::GetNextBatch(
    std::vector<float>& vector_buffer, std::vector<::lumina::core::vector_id_t>& id_buffer,
    std::vector<TagDimensionData>& tag_dimensions_data) noexcept {
    if (cursor_ >= arrays_.size()) {
        return ::lumina::core::Result<uint64_t>::Ok(0);
    }
    std::shared_ptr<arrow::FloatArray>& values = arrays_[cursor_];
    int64_t value_count = values->length();
    int64_t vector_count = value_count / dimension_;
    vector_buffer.resize(static_cast<size_t>(value_count));
    std::memcpy(vector_buffer.data(), values->raw_values(),
                sizeof(float) * static_cast<size_t>(value_count));
    id_buffer.resize(static_cast<size_t>(vector_count));
    std::iota(id_buffer.begin(), id_buffer.end(),
              static_cast<::lumina::core::vector_id_t>(start_ids_[cursor_]));
    tag_dimensions_data = std::move(tag_data_[cursor_]);
    values.reset();
    ++cursor_;
    return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(vector_count));
}

}  // namespace paimon::lumina
