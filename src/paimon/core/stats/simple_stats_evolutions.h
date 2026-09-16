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

#include <cstdint>
#include <memory>

#include "paimon/common/utils/generic_lru_cache.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/stats/simple_stats_evolution.h"

namespace paimon {
class SimpleStatsEvolutions {
 public:
    SimpleStatsEvolutions(const std::shared_ptr<TableSchema>& table_schema,
                          const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool), table_schema_(table_schema) {}

    Result<std::shared_ptr<SimpleStatsEvolution>> GetOrCreate(
        const std::shared_ptr<TableSchema>& data_schema) {
        return evolutions_.Get(
            data_schema->Id(),
            [this,
             &data_schema](const int64_t& id) -> Result<std::shared_ptr<SimpleStatsEvolution>> {
                return std::make_shared<SimpleStatsEvolution>(data_schema->Fields(),
                                                              table_schema_->Fields(),
                                                              id != table_schema_->Id(), pool_);
            });
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<TableSchema> table_schema_;
    static constexpr int64_t kEvolutionCacheCapacity = 64;
    using EvolutionCache = GenericLruCache<int64_t, std::shared_ptr<SimpleStatsEvolution>>;
    EvolutionCache evolutions_{EvolutionCache::Options{/*max_weight=*/kEvolutionCacheCapacity}};
};
}  // namespace paimon
