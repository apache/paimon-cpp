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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/format/mosaic/mosaic_writer_builder.h"

#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"
#include "paimon/format/mosaic/mosaic_format_defs.h"

namespace paimon::mosaic {

Result<std::unique_ptr<FormatWriter>> MosaicWriterBuilder::Build(
    const std::shared_ptr<OutputStream>& output, const std::string& compression) {
    if (pool_ == nullptr) {
        return Status::Invalid("Mosaic writer memory pool is nullptr");
    }
    std::string normalized = StringUtils::ToLowerCase(compression);
    uint8_t compression_id = 0;
    if (normalized == "zstd" || normalized == "zstandard") {
        compression_id = 1;
    } else if (normalized != "none" && normalized != "null" && normalized != "uncompressed") {
        return Status::Invalid("unknown Mosaic compression ", compression);
    }
    MosaicWriterOptions writer_options = mosaic_writer_options_default();
    writer_options.compression = compression_id;
    if (compression_id == 1) {
        PAIMON_ASSIGN_OR_RAISE(
            writer_options.zstd_level,
            OptionsUtils::GetValueFromMap<int32_t>(options_, Options::FILE_COMPRESSION_ZSTD_LEVEL,
                                                   writer_options.zstd_level));
    }
    PAIMON_ASSIGN_OR_RAISE(writer_options.num_buckets,
                           OptionsUtils::GetValueFromMap<uint32_t>(options_, MOSAIC_NUM_BUCKETS,
                                                                   writer_options.num_buckets));
    auto block_size = options_.find(Options::FILE_BLOCK_SIZE);
    if (block_size != options_.end()) {
        PAIMON_ASSIGN_OR_RAISE(int64_t row_group_max_size,
                               MemorySize::ParseBytes(block_size->second));
        PAIMON_RETURN_NOT_OK(
            ValidateValueInRange<uint64_t>(row_group_max_size, "Mosaic row group max size"));
        writer_options.row_group_max_size = static_cast<uint64_t>(row_group_max_size);
    }
    writer_options.stats_columns = nullptr;
    writer_options.num_stats_columns = 0;
    return MosaicFormatWriter::Create(output, schema_, writer_options);
}

}  // namespace paimon::mosaic
