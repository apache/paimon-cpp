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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/core/index/pk/primary_key_index_source_meta.h"

namespace paimon {
/// The single validated payload group which indexes one complete data level.
///
/// A group is only created when the payload provably covers the current active source set
/// of its data level: exactly one payload, unique source names, source order / file names /
/// row counts identical to the expected level sources, matching index type and field id,
/// a row range of exactly `[0, total source rows - 1]` and a payload row count equal to the
/// source row count sum. Anything else must be treated as uncovered.
class PkSortedIndexGroup {
 public:
    /// Validates one payload against the expected level sources; returns `std::nullopt`
    /// when any coverage condition fails.
    static std::optional<PkSortedIndexGroup> Create(
        int32_t field_id, const std::string& index_type,
        const std::vector<PrimaryKeyIndexSourceFile>& expected_sources,
        const std::shared_ptr<IndexFileMeta>& payload,
        const PrimaryKeyIndexSourceMeta& payload_source_meta);

    int32_t DataLevel() const {
        return data_level_;
    }

    const std::vector<PrimaryKeyIndexSourceFile>& SourceFiles() const {
        return source_files_;
    }

    const std::shared_ptr<IndexFileMeta>& Payload() const {
        return payload_;
    }

    int64_t TotalSourceRowCount() const {
        return total_source_row_count_;
    }

 private:
    PkSortedIndexGroup(int32_t data_level, std::vector<PrimaryKeyIndexSourceFile> source_files,
                       std::shared_ptr<IndexFileMeta> payload, int64_t total_source_row_count)
        : data_level_(data_level),
          source_files_(std::move(source_files)),
          payload_(std::move(payload)),
          total_source_row_count_(total_source_row_count) {}

    int32_t data_level_;
    std::vector<PrimaryKeyIndexSourceFile> source_files_;
    std::shared_ptr<IndexFileMeta> payload_;
    int64_t total_source_row_count_;
};

}  // namespace paimon
