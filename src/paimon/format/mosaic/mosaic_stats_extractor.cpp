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

#include "paimon/format/mosaic/mosaic_stats_extractor.h"

#include <limits>
#include <memory>
#include <vector>

#include "paimon/common/utils/math.h"
#include "paimon/format/mosaic/mosaic_ffi.h"
#include "paimon/format/mosaic/mosaic_stats.h"
#include "paimon/format/mosaic/mosaic_stream.h"
#include "paimon/fs/file_system.h"

namespace paimon::mosaic {

Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
MosaicStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                          const std::string& path,
                                          const std::shared_ptr<MemoryPool>& pool) {
    if (file_system == nullptr || pool == nullptr) {
        return Status::Invalid("Mosaic stats extractor requires file system and memory pool");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system->Open(path));
    PAIMON_ASSIGN_OR_RAISE(int64_t signed_length, input->Length());
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(signed_length, "Mosaic input length"));
    auto input_context =
        std::make_unique<MosaicInputContext>(input, static_cast<uint64_t>(signed_length));
    MosaicInputFile input_file = {};
    input_file.ctx = input_context.get();
    input_file.read_at_fn = MosaicInputContext::ReadAt;
    input_file.length_fn = MosaicInputContext::Length;
    std::unique_ptr<MosaicReaderHandle, decltype(&mosaic_reader_free)> reader(
        mosaic_reader_open(input_file), mosaic_reader_free);
    if (reader == nullptr) {
        return MosaicFfiError("open Mosaic reader", input_context->GetCallbackStatus());
    }

    uint32_t num_row_groups = 0;
    if (mosaic_reader_num_row_groups(reader.get(), &num_row_groups) != 0) {
        return MosaicFfiError("read Mosaic row group count", input_context->GetCallbackStatus());
    }
    int64_t row_count = 0;
    std::vector<MosaicStatsUtils::RowGroupStatistics> row_group_stats;
    row_group_stats.reserve(num_row_groups);
    for (uint32_t row_group = 0; row_group < num_row_groups; ++row_group) {
        uint32_t row_group_row_count = 0;
        if (mosaic_reader_row_group_num_rows(reader.get(), row_group, &row_group_row_count) != 0) {
            return MosaicFfiError("read Mosaic row count", input_context->GetCallbackStatus());
        }
        if (row_group_row_count >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max() - row_count)) {
            return Status::Invalid("Mosaic row count exceeds int64 range");
        }
        row_count += row_group_row_count;
        PAIMON_ASSIGN_OR_RAISE(
            MosaicStatsUtils::RowGroupStatistics stats,
            MosaicStatsUtils::ReadRowGroupStatistics(row_group, input_context.get(), reader.get()));
        row_group_stats.push_back(std::move(stats));
    }
    PAIMON_ASSIGN_OR_RAISE(ColumnStatsVector stats,
                           MosaicStatsUtils::ConvertColumnStatistics(
                               schema_, row_group_stats, /*missing_null_count_is_zero=*/false));
    return std::make_pair(std::move(stats), FileInfo(row_count));
}

}  // namespace paimon::mosaic
