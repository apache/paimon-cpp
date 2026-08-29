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

#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/table/source/table_read.h"

namespace arrow {
class MemoryPool;
}

namespace paimon {
class DataSplit;

class FallbackTableRead : public TableRead {
 public:
    FallbackTableRead(std::unique_ptr<TableRead> main_table,
                      std::unique_ptr<TableRead> fallback_table,
                      const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
        : main_table_(std::move(main_table)),
          fallback_table_(std::move(fallback_table)),
          arrow_pool_(arrow_pool) {}

    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;
    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override;

 private:
    std::unique_ptr<TableRead> main_table_;
    std::unique_ptr<TableRead> fallback_table_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

}  // namespace paimon
