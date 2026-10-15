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

#include <memory>

#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

/// Optionally implemented by a `FormatWriter` that can produce the column statistics of the file it
/// has finished from the metadata it still holds in memory. The data file writers then take the
/// statistics from it instead of reopening the file through `FormatStatsExtractor` to read them
/// back.
///
/// It is implemented alongside `FormatWriter` rather than added to it, so `FormatWriter` keeps its
/// ABI and format writers that do not implement it are unaffected.
class PAIMON_EXPORT WrittenFileStatsProvider {
 public:
    virtual ~WrittenFileStatsProvider() = default;

    /// Extracts the statistics of each top-level column of the finished file. They must equal
    /// what `FormatStatsExtractor::Extract()` reads back from that file.
    ///
    /// @param pool Memory pool used to build the statistics.
    /// @return The statistics, or an error status if the file has not been finished successfully.
    virtual Result<ColumnStatsVector> ExtractWrittenFileStats(
        const std::shared_ptr<MemoryPool>& pool) const = 0;
};

}  // namespace paimon
