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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/concurrent_hash_map.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/result.h"

namespace paimon {
class BinaryArray;
class GenericRow;
class InternalArray;
class InternalRow;
class MemoryPool;
class SimpleStats;

/// Converter for array of `SimpleStats`.
class SimpleStatsEvolution {
 public:
    SimpleStatsEvolution(const std::vector<DataField>& data_fields,
                         const std::vector<DataField>& table_fields, bool need_mapping,
                         const std::shared_ptr<MemoryPool>& pool);

    struct EvolutionStats {
        EvolutionStats() = default;

        EvolutionStats(const std::shared_ptr<InternalRow>& _min_values,
                       const std::shared_ptr<InternalRow>& _max_values,
                       const std::shared_ptr<InternalArray>& _null_counts)
            : min_values(_min_values), max_values(_max_values), null_counts(_null_counts) {}
        std::shared_ptr<InternalRow> min_values;
        std::shared_ptr<InternalRow> max_values;
        std::shared_ptr<InternalArray> null_counts;
    };

    Result<EvolutionStats> Evolution(const SimpleStats& stats, int64_t row_count,
                                     const std::optional<std::vector<std::string>>& dense_fields);

    const std::map<int32_t, std::pair<int32_t, DataField>>& GetFieldIdToDataField() const {
        return id_to_data_fields_;
    }

    const std::map<std::string, DataField>& GetFieldNameToTableField() const {
        return name_to_table_fields_;
    }

 private:
    Result<SimpleStatsEvolution::EvolutionStats> MappingStats(
        int64_t row_count, const SimpleStatsEvolution::EvolutionStats& src_stats) const;

 private:
    bool need_mapping_ = false;
    std::vector<DataField> data_fields_;
    std::vector<DataField> table_fields_;
    std::shared_ptr<GenericRow> empty_values_;
    std::shared_ptr<BinaryArray> empty_null_counts_;

    std::map<int32_t, std::pair<int32_t, DataField>> id_to_data_fields_;
    std::map<std::string, DataField> name_to_table_fields_;

    // dense field names -> idx mapping
    ConcurrentHashMap<std::vector<std::string>, std::vector<int32_t>, VectorStringHashCompare>
        dense_fields_mapping_;
};
}  // namespace paimon
