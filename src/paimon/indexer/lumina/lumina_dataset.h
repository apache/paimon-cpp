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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "lumina/api/Dataset.h"
#include "lumina/extensions/experimental/DatasetWithTag.h"

namespace paimon::lumina {

class LuminaDataset final : public ::lumina::api::Dataset {
 public:
    LuminaDataset(int64_t element_count, uint32_t dimension,
                  const std::vector<std::shared_ptr<arrow::FloatArray>>& arrays,
                  const std::vector<int64_t>& start_ids);

    uint32_t Dim() const noexcept override;

    uint64_t TotalSize() const noexcept override;

    ::lumina::core::Result<uint64_t> GetNextBatch(
        std::vector<float>& vector_buffer,
        std::vector<::lumina::core::vector_id_t>& id_buffer) noexcept override;

 private:
    int64_t element_count_;
    uint32_t dimension_;
    std::vector<std::shared_ptr<arrow::FloatArray>> arrays_;
    std::vector<int64_t> start_ids_;
    size_t cursor_ = 0;
};

class LuminaDatasetWithTag final : public ::lumina::extensions::experimental::DatasetWithTag {
 public:
    using TagDimensionData = ::lumina::extensions::experimental::TagDimensionData;

    LuminaDatasetWithTag(int64_t element_count, uint32_t dimension,
                         const std::vector<std::shared_ptr<arrow::FloatArray>>& arrays,
                         const std::vector<int64_t>& start_ids,
                         const std::vector<std::vector<TagDimensionData>>& tag_data);

    uint32_t Dim() const noexcept override;

    uint64_t TotalSize() const noexcept override;

    ::lumina::core::Result<uint64_t> GetNextBatch(
        std::vector<float>& vector_buffer, std::vector<::lumina::core::vector_id_t>& id_buffer,
        std::vector<TagDimensionData>& tag_dimensions_data) noexcept override;

 private:
    int64_t element_count_;
    uint32_t dimension_;
    std::vector<std::shared_ptr<arrow::FloatArray>> arrays_;
    std::vector<int64_t> start_ids_;
    std::vector<std::vector<TagDimensionData>> tag_data_;
    size_t cursor_ = 0;
};

}  // namespace paimon::lumina
