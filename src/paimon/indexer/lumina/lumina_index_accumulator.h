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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "lumina/api/LuminaBuilder.h"
#include "lumina/api/Options.h"
#include "lumina/extensions/experimental/DatasetWithTag.h"
#include "paimon/indexer/lumina/lumina_memory_pool.h"
#include "paimon/indexer/lumina/lumina_tag_utils.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon::lumina {

/// Accumulates the non-null vector segments shared by Lumina Global Index and File Index writers.
class LuminaIndexAccumulator {
 public:
    Status AddBatch(const std::shared_ptr<arrow::StructArray>& struct_array,
                    const std::shared_ptr<arrow::ListArray>& vectors, uint32_t dimension,
                    const std::vector<LuminaTagField>& tag_fields, int64_t first_row_id);

    Result<::lumina::api::LuminaBuilder> Build(const ::lumina::api::BuilderOptions& builder_options,
                                               uint32_t dimension, bool with_tag,
                                               LuminaMemoryPool* pool);

    int64_t IndexedCount() const {
        return indexed_count_;
    }

 private:
    int64_t indexed_count_ = 0;
    std::vector<std::shared_ptr<arrow::FloatArray>> arrays_;
    std::vector<int64_t> array_start_ids_;
    std::vector<std::vector<::lumina::extensions::experimental::TagDimensionData>> tag_data_vec_;
};

}  // namespace paimon::lumina
