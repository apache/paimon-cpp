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

#include "paimon/core/utils/data_evolution_utils.h"

#include <algorithm>
#include <limits>
#include <optional>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/vector_store_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/status.h"

namespace paimon {

bool DataEvolutionUtils::IsNormalFile(const std::string& file_name) {
    return !BlobUtils::IsBlobFile(file_name) && !VectorStoreUtils::IsVectorStoreFile(file_name);
}

bool DataEvolutionUtils::HasNormalFile(const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    return std::any_of(files.begin(), files.end(), [](const std::shared_ptr<DataFileMeta>& file) {
        return file != nullptr && IsNormalFile(file->file_name);
    });
}

bool DataEvolutionUtils::IsMaterializedCompaction(
    const std::vector<std::shared_ptr<DataFileMeta>>& before,
    const std::vector<std::shared_ptr<DataFileMeta>>& after) {
    if (!HasNormalFile(before)) {
        return false;
    }
    // Every output is looked at, dedicated files included: a materialized rewrite assigns no
    // row id to any of them.
    return std::all_of(after.begin(), after.end(), [](const std::shared_ptr<DataFileMeta>& file) {
        return file != nullptr && file->first_row_id == std::nullopt;
    });
}

Result<std::shared_ptr<DataFileMeta>> DataEvolutionUtils::RetrieveAnchorFile(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    std::shared_ptr<DataFileMeta> anchor;
    for (const auto& file : files) {
        if (!IsNormalFile(file->file_name)) {
            continue;
        }
        if (anchor == nullptr || file->max_sequence_number < anchor->max_sequence_number ||
            (file->max_sequence_number == anchor->max_sequence_number &&
             file->file_name < anchor->file_name)) {
            anchor = file;
        }
    }
    if (anchor == nullptr) {
        return Status::Invalid(
            "Data-evolution deletion vectors should have a normal anchor file in each row range "
            "group.");
    }
    return anchor;
}

Result<Range> DataEvolutionUtils::CheckContiguousRowRange(
    const std::vector<std::shared_ptr<DataFileMeta>>& files) {
    if (files.empty()) {
        return Status::Invalid("Data evolution compact files should not be empty.");
    }
    std::vector<Range> ranges;
    ranges.reserve(files.size());
    for (const auto& file : files) {
        // A public entry, so the row id range is validated here as well, not only by the
        // compaction planner's input checks.
        if (file == nullptr) {
            return Status::Invalid("Data evolution compact files must not be null.");
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t first_row_id, file->NonNullFirstRowId());
        if (file->row_count <= 0 || first_row_id < 0 ||
            first_row_id > std::numeric_limits<int64_t>::max() - file->row_count) {
            return Status::Invalid(
                fmt::format("Invalid data evolution compact input {}: first row id {} and row "
                            "count {} must form a valid row id range.",
                            file->file_name, first_row_id, file->row_count));
        }
        ranges.emplace_back(first_row_id, first_row_id + file->row_count - 1);
    }
    std::vector<Range> merged = Range::SortAndMergeOverlap(ranges, /*adjacent=*/true);
    if (merged.size() != 1) {
        std::vector<std::string> range_strings;
        range_strings.reserve(merged.size());
        for (const auto& range : merged) {
            range_strings.push_back(range.ToString());
        }
        return Status::Invalid(
            fmt::format("Data evolution compact files should have a contiguous row range, "
                        "but got [{}].",
                        fmt::join(range_strings, ", ")));
    }
    return merged[0];
}

}  // namespace paimon
