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
#include <string>
#include <utility>

#include "paimon/format/format_stats_extractor.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::mosaic {

class MosaicStatsExtractor : public FormatStatsExtractor {
 public:
    explicit MosaicStatsExtractor(const std::shared_ptr<arrow::Schema>& schema) : schema_(schema) {}

    Result<ColumnStatsVector> Extract(const std::shared_ptr<FileSystem>& file_system,
                                      const std::string& path,
                                      const std::shared_ptr<MemoryPool>& pool) override {
        using ExtractResult = std::pair<ColumnStatsVector, FileInfo>;
        PAIMON_ASSIGN_OR_RAISE(ExtractResult result, ExtractWithFileInfo(file_system, path, pool));
        return result.first;
    }

    Result<std::pair<ColumnStatsVector, FileInfo>> ExtractWithFileInfo(
        const std::shared_ptr<FileSystem>& file_system, const std::string& path,
        const std::shared_ptr<MemoryPool>& pool) override;

 private:
    std::shared_ptr<arrow::Schema> schema_;
};

}  // namespace paimon::mosaic
