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

#pragma once

#include <memory>

#include "arrow/api.h"
#include "paimon/common/data/shredding/map_shared_shredding_utils.h"
#include "paimon/common/data/shredding/shredding_read_plan.h"

namespace paimon {

/// Builds per-column read plans for shared-shredding MAP columns and selected-key MAP access.
class MapSharedShreddingReadPlanFactory {
 public:
    MapSharedShreddingReadPlanFactory() = delete;
    ~MapSharedShreddingReadPlanFactory() = delete;

    static Result<std::shared_ptr<ShreddingColumnReadPlan>> CreateMapReadPlan(
        const std::shared_ptr<arrow::Field>& logical_map_field,
        const MapSharedShreddingFieldMeta& meta);

    static Result<std::shared_ptr<ShreddingColumnReadPlan>> CreateSharedSelectedKeysReadPlan(
        const std::shared_ptr<arrow::Field>& selected_keys_field,
        const MapSharedShreddingFieldMeta& meta);

    static Result<std::shared_ptr<ShreddingColumnReadPlan>> CreateDefaultSelectedKeysReadPlan(
        const std::shared_ptr<arrow::Field>& file_map_field,
        const std::shared_ptr<arrow::Field>& selected_keys_field);
};

}  // namespace paimon
