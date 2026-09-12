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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <map>
#include <memory>
#include <string>

#include "arrow/type_fwd.h"
#include "paimon/format/format_stats_extractor.h"

namespace paimon::lance {

class LanceStatsExtractor : public FormatStatsExtractor {
 public:
    LanceStatsExtractor(const std::shared_ptr<arrow::Schema>& schema,
                        const std::map<std::string, std::string>& options)
        : schema_(schema), options_(options) {}

    Result<ColumnStatsVector> Extract(const std::shared_ptr<FileSystem>& file_system,
                                      const std::string& path,
                                      const std::shared_ptr<MemoryPool>& pool) override;
    Result<std::pair<ColumnStatsVector, FileInfo>> ExtractWithFileInfo(
        const std::shared_ptr<FileSystem>& file_system, const std::string& path,
        const std::shared_ptr<MemoryPool>& pool) override;

 private:
    Result<std::unique_ptr<ColumnStats>> CreateEmptyStats(
        const std::shared_ptr<arrow::DataType>& type) const;

    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::lance
