/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <optional>

#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"

namespace paimon {
/// Selects complete compacted data files for source-backed primary-key indexes.
///
/// Only files produced by compaction on a positive data level are eligible: level 0 files
/// and appended files may still be rewritten or merged, so payloads built over them could
/// not maintain the exact per-level coverage contract.
class PrimaryKeyIndexSourcePolicy {
 public:
    PrimaryKeyIndexSourcePolicy() = delete;
    ~PrimaryKeyIndexSourcePolicy() = delete;

    static bool ShouldWrite(const FileSource& file_source, int32_t level) {
        return file_source == FileSource::Compact() && level > 0;
    }

    static bool ShouldRead(const DataFileMeta& file) {
        if (file.file_source == std::nullopt) {
            return false;
        }
        return ShouldWrite(file.file_source.value(), file.level);
    }
};

}  // namespace paimon
