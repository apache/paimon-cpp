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
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/format/column_stats.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class BinaryRow;
class ColumnStats;

class SimpleStatsCollector {
 public:
    explicit SimpleStatsCollector(const std::shared_ptr<arrow::Schema>& schema);
    Status Collect(const BinaryRow& row);
    Result<std::vector<std::shared_ptr<ColumnStats>>> GetResult() const {
        std::vector<std::shared_ptr<ColumnStats>> stats;
        for (const auto& col_stats : column_stats_) {
            if (col_stats == nullptr) {
                return Status::Invalid("column stats is nullptr");
            }
            stats.push_back(col_stats);
        }
        return stats;
    }

 private:
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<ColumnStats>> column_stats_;
};

}  // namespace paimon
