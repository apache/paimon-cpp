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
#include <string>
#include <utility>

#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/column_stats.h"
#include "paimon/format/format_stats_extractor.h"
#include "paimon/format/parquet/parquet_schema_util.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class FileSystem;
class MemoryPool;
}  // namespace paimon

namespace parquet::schema {
class GroupNode;
class Node;
class PrimitiveNode;
}  // namespace parquet::schema

namespace paimon::parquet {

class ParquetStatsExtractor : public FormatStatsExtractor {
 public:
    explicit ParquetStatsExtractor(const std::shared_ptr<arrow::Schema>& write_schema)
        : write_schema_(write_schema) {}

    Result<ColumnStatsVector> Extract(const std::shared_ptr<FileSystem>& file_system,
                                      const std::string& path,
                                      const std::shared_ptr<MemoryPool>& pool) override {
        PAIMON_ASSIGN_OR_RAISE(auto result, ExtractWithFileInfo(file_system, path, pool));
        return result.first;
    }

    Result<std::pair<ColumnStatsVector, FileInfo>> ExtractWithFileInfo(
        const std::shared_ptr<FileSystem>& file_system, const std::string& path,
        const std::shared_ptr<MemoryPool>& pool) override;

 private:
    void PrintConvertedType(const ::parquet::schema::Node* node);

    void Visit(const ::parquet::schema::Node* node);
    void Visit(const ::parquet::schema::PrimitiveNode* node);
    void Visit(const ::parquet::schema::GroupNode* node);

 private:
    int64_t row_count_ = -1;
    std::shared_ptr<arrow::Schema> write_schema_;
};

}  // namespace paimon::parquet
