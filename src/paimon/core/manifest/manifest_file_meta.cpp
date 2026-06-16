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
#include "paimon/core/manifest/manifest_file_meta.h"

#include "arrow/type_fwd.h"
#include "fmt/format.h"

namespace arrow {
class DataType;
}  // namespace arrow

namespace paimon {

ManifestFileMeta::ManifestFileMeta(
    const std::string& file_name, int64_t file_size, int64_t num_added_files,
    int64_t num_deleted_files, const SimpleStats& partition_stats, int64_t schema_id,
    const std::optional<int32_t>& min_bucket, const std::optional<int32_t>& max_bucket,
    const std::optional<int32_t>& min_level, const std::optional<int32_t>& max_level,
    const std::optional<int64_t>& min_row_id, const std::optional<int64_t>& max_row_id)
    : file_name_(file_name),
      file_size_(file_size),
      num_added_files_(num_added_files),
      num_deleted_files_(num_deleted_files),
      partition_stats_(partition_stats),
      schema_id_(schema_id),
      min_bucket_(min_bucket),
      max_bucket_(max_bucket),
      min_level_(min_level),
      max_level_(max_level),
      min_row_id_(min_row_id),
      max_row_id_(max_row_id) {}

bool ManifestFileMeta::operator==(const ManifestFileMeta& other) const {
    if (this == &other) {
        return true;
    }
    return file_name_ == other.file_name_ && file_size_ == other.file_size_ &&
           num_added_files_ == other.num_added_files_ &&
           num_deleted_files_ == other.num_deleted_files_ &&
           partition_stats_ == other.partition_stats_ && schema_id_ == other.schema_id_ &&
           min_bucket_ == other.min_bucket_ && max_bucket_ == other.max_bucket_ &&
           min_level_ == other.min_level_ && max_level_ == other.max_level_ &&
           min_row_id_ == other.min_row_id_ && max_row_id_ == other.max_row_id_;
}

const std::shared_ptr<arrow::DataType>& ManifestFileMeta::DataType() {
    static std::shared_ptr<arrow::DataType> data_type = arrow::struct_({
        arrow::field("_FILE_NAME", arrow::utf8(), /*nullable=*/false),
        arrow::field("_FILE_SIZE", arrow::int64(), /*nullable=*/false),
        arrow::field("_NUM_ADDED_FILES", arrow::int64(), /*nullable=*/false),
        arrow::field("_NUM_DELETED_FILES", arrow::int64(), /*nullable=*/false),
        arrow::field("_PARTITION_STATS", SimpleStats::DataType(), /*nullable=*/false),
        arrow::field("_SCHEMA_ID", arrow::int64(), /*nullable=*/false),
        arrow::field("_MIN_BUCKET", arrow::int32(), /*nullable=*/true),
        arrow::field("_MAX_BUCKET", arrow::int32(), /*nullable=*/true),
        arrow::field("_MIN_LEVEL", arrow::int32(), /*nullable=*/true),
        arrow::field("_MAX_LEVEL", arrow::int32(), /*nullable=*/true),
        arrow::field("_MIN_ROW_ID", arrow::int64(), /*nullable=*/true),
        arrow::field("_MAX_ROW_ID", arrow::int64(), /*nullable=*/true),
    });
    return data_type;
}

std::string ManifestFileMeta::ToString() const {
    std::string min_bucket_str =
        min_bucket_ != std::nullopt ? std::to_string(min_bucket_.value()) : "null";
    std::string max_bucket_str =
        max_bucket_ != std::nullopt ? std::to_string(max_bucket_.value()) : "null";

    std::string min_level_str =
        min_level_ != std::nullopt ? std::to_string(min_level_.value()) : "null";
    std::string max_level_str =
        max_level_ != std::nullopt ? std::to_string(max_level_.value()) : "null";

    std::string min_row_id_str =
        min_row_id_ != std::nullopt ? std::to_string(min_row_id_.value()) : "null";
    std::string max_row_id_str =
        max_row_id_ != std::nullopt ? std::to_string(max_row_id_.value()) : "null";

    return fmt::format("{{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}}", file_name_, file_size_,
                       num_added_files_, num_deleted_files_, partition_stats_.ToString(),
                       schema_id_, min_bucket_str, max_bucket_str, min_level_str, max_level_str,
                       min_row_id_str, max_row_id_str);
}

}  // namespace paimon
