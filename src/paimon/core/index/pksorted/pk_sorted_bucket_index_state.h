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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/pk/primary_key_index_source_file.h"
#include "paimon/core/index/pksorted/pk_sorted_index_group.h"
#include "paimon/core/io/data_file_meta.h"

namespace paimon {
/// Immutable source-backed sorted-index state for one field and bucket.
///
/// Derives the eligible source sets from the active data files and keeps validated,
/// payload groups. A group retains its complete immutable source list for ordinal
/// localization while covering only the listed files which are still active at its
/// metadata-declared level. A payload is rejected when its metadata is invalid, it has no
/// active source at that level, an active source has a different row count, its source
/// order is not canonical, or another payload exists for the same level. Active files
/// without an accepted group remain uncovered and must be scanned normally.
class PkSortedBucketIndexState {
 public:
    static PkSortedBucketIndexState FromActiveDataFiles(
        int32_t field_id, const std::string& index_type,
        const std::vector<std::shared_ptr<DataFileMeta>>& active_data_files,
        const std::vector<std::shared_ptr<IndexFileMeta>>& active_payloads);

    const std::vector<std::shared_ptr<PkSortedIndexGroup>>& Groups() const {
        return groups_;
    }

    const std::vector<PrimaryKeyIndexSourceFile>& CoveredSourceFiles() const {
        return covered_source_files_;
    }

    const std::vector<PrimaryKeyIndexSourceFile>& UncoveredSourceFiles() const {
        return uncovered_source_files_;
    }

    const std::vector<std::shared_ptr<IndexFileMeta>>& RejectedPayloads() const {
        return rejected_payloads_;
    }

 private:
    PkSortedBucketIndexState(std::vector<std::shared_ptr<PkSortedIndexGroup>> groups,
                             std::vector<PrimaryKeyIndexSourceFile> covered_source_files,
                             std::vector<PrimaryKeyIndexSourceFile> uncovered_source_files,
                             std::vector<std::shared_ptr<IndexFileMeta>> rejected_payloads)
        : groups_(std::move(groups)),
          covered_source_files_(std::move(covered_source_files)),
          uncovered_source_files_(std::move(uncovered_source_files)),
          rejected_payloads_(std::move(rejected_payloads)) {}

    std::vector<std::shared_ptr<PkSortedIndexGroup>> groups_;
    std::vector<PrimaryKeyIndexSourceFile> covered_source_files_;
    std::vector<PrimaryKeyIndexSourceFile> uncovered_source_files_;
    std::vector<std::shared_ptr<IndexFileMeta>> rejected_payloads_;
};

}  // namespace paimon
