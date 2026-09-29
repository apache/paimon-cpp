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

#include "paimon/format/vortex/vortex_stats_extractor.h"

#include <utility>

#include "paimon/common/utils/math.h"
#include "paimon/format/vortex/vortex_ffi.h"
#include "paimon/format/vortex/vortex_ffi_util.h"
#include "paimon/format/vortex/vortex_io_callbacks.h"
#include "paimon/fs/file_system.h"

namespace paimon::vortex {

Result<std::pair<ColumnStatsVector, FormatStatsExtractor::FileInfo>>
VortexStatsExtractor::ExtractWithFileInfo(const std::shared_ptr<FileSystem>& file_system,
                                          const std::string& path,
                                          const std::shared_ptr<MemoryPool>& pool) {
    if (file_system == nullptr) {
        return Status::Invalid("Vortex stats extractor requires a file system");
    }
    if (schema_ == nullptr) {
        return Status::Invalid("Vortex stats extractor has no schema");
    }
    (void)pool;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input, file_system->Open(path));
    // Only the row count is needed, and Vortex keeps it in the file footer. Reading through
    // callbacks means just those ranges are fetched instead of the whole file.
    PAIMON_ASSIGN_OR_RAISE(int64_t signed_length, input->Length());
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(signed_length, "Vortex input length"));
    auto length = static_cast<uint64_t>(signed_length);
    auto input_context = std::make_shared<VortexInputContext>(input);

    VxSessionPtr session(vx_session_new(), vx_session_free);
    if (session == nullptr) {
        return Status::IOError("failed to create Vortex session");
    }
    vx_error* error = nullptr;
    VxDataSourcePtr data_source(
        vx_data_source_new_callback(session.get(), VortexInputContext::MakeCallbacks(input_context),
                                    length, &error),
        vx_data_source_free);
    if (data_source == nullptr) {
        return VortexCallbackError("open Vortex file for stats", error,
                                   input_context->GetCallbackStatus());
    }
    vx_estimate row_count{};
    vx_data_source_get_row_count(data_source.get(), &row_count);
    if (row_count.type == VX_ESTIMATE_UNKNOWN) {
        return Status::Invalid("Vortex file did not report a row count");
    }
    const auto rows = static_cast<int64_t>(row_count.estimate);
    // Empty column stats: Vortex keeps its statistics internal and does not surface them to Paimon.
    return std::make_pair(ColumnStatsVector(), FileInfo(rows));
}

}  // namespace paimon::vortex
