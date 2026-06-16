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
#include "paimon/core/manifest/manifest_entry_writer.h"

#include <algorithm>
#include <cassert>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/stats/simple_stats_converter.h"

namespace paimon {
class ColumnStats;

Status ManifestEntryWriter::Write(const ManifestEntry& entry) {
    PAIMON_RETURN_NOT_OK(SingleFileWriter::Write(entry));
    if (entry.Kind() == FileKind::Add()) {
        num_added_files_++;
    } else if (entry.Kind() == FileKind::Delete()) {
        num_deleted_files_++;
    } else {
        return Status::NotImplemented(fmt::format(
            "Unknown entry kind: {}", static_cast<int32_t>(entry.Kind().ToByteValue())));
    }
    schema_id_ = std::max(schema_id_, entry.File()->schema_id);
    min_bucket_ = std::min(min_bucket_, entry.Bucket());
    max_bucket_ = std::max(max_bucket_, entry.Bucket());
    min_level_ = std::min(min_level_, entry.Level());
    max_level_ = std::max(max_level_, entry.Level());

    if (row_id_stats_) {
        std::optional<int64_t> first_row_id = entry.File()->first_row_id;
        if (first_row_id) {
            row_id_stats_.value().Collect(first_row_id.value(), entry.File()->row_count);
        } else {
            row_id_stats_ = std::nullopt;
        }
    }

    assert(partition_stats_collector_);
    PAIMON_RETURN_NOT_OK(partition_stats_collector_->Collect(entry.Partition()));
    return Status::OK();
}

Result<ManifestFileMeta> ManifestEntryWriter::GetResult() {
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<ColumnStats>> col_stats,
                           partition_stats_collector_->GetResult());
    PAIMON_ASSIGN_OR_RAISE(SimpleStats stats,
                           SimpleStatsConverter::ToBinary(col_stats, pool_.get()));
    return ManifestFileMeta(
        PathUtil::GetName(path_), output_bytes_, num_added_files_, num_deleted_files_, stats,
        schema_id_, min_bucket_, max_bucket_, min_level_, max_level_,
        (row_id_stats_ == std::nullopt ? std::nullopt
                                       : std::optional<int64_t>(row_id_stats_.value().min_row_id)),
        (row_id_stats_ == std::nullopt ? std::nullopt
                                       : std::optional<int64_t>(row_id_stats_.value().max_row_id)));
}

}  // namespace paimon
