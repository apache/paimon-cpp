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

#include "paimon/testing/mock/mock_stats_extractor.h"

#include <optional>

#include "paimon/format/column_stats.h"

namespace paimon {
class FileSystem;
class MemoryPool;
}  // namespace paimon

namespace paimon::test {
Result<std::vector<std::shared_ptr<ColumnStats>>> MockStatsExtractor::Extract(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(auto result, ExtractWithFileInfo(file_system, path, pool));
    return result.first;
}

Result<std::pair<std::vector<std::shared_ptr<ColumnStats>>, FormatStatsExtractor::FileInfo>>
MockStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                        const std::string& path,
                                        const std::shared_ptr<MemoryPool>& pool) {
    FormatStatsExtractor::FileInfo file_info(1000);
    std::shared_ptr<ColumnStats> col0 = ColumnStats::CreateIntColumnStats(0, 100, 10);
    std::shared_ptr<ColumnStats> col1 = ColumnStats::CreateDoubleColumnStats(0.1, 100.1, 10);
    std::shared_ptr<ColumnStats> col2 = ColumnStats::CreateStringColumnStats("abc", "def", 10);
    std::vector<std::shared_ptr<ColumnStats>> stats = {col0, col1, col2};
    return std::make_pair(stats, file_info);
}

}  // namespace paimon::test
