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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/format/mosaic/mosaic_ffi.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::mosaic {

class MosaicInputContext;

class MosaicStatsUtils {
 public:
    struct ColumnStatistics {
        uint64_t null_count;
        std::optional<std::vector<uint8_t>> min;
        std::optional<std::vector<uint8_t>> max;
    };

    using RowGroupStatistics = std::unordered_map<std::string, ColumnStatistics>;

    MosaicStatsUtils() = delete;
    ~MosaicStatsUtils() = delete;

    static Result<RowGroupStatistics> ReadRowGroupStatistics(
        uint32_t row_group, const MosaicInputContext* input_context, MosaicReaderHandle* reader);

    static Result<ColumnStatsVector> ConvertColumnStatistics(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::vector<RowGroupStatistics>& row_group_stats, bool missing_null_count_is_zero);
};

}  // namespace paimon::mosaic
