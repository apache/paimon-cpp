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

#include "paimon/core/index/pksorted/pk_sorted_index_group.h"

#include <set>
#include <utility>

namespace paimon {
std::shared_ptr<PkSortedIndexGroup> PkSortedIndexGroup::Create(
    int32_t field_id, const std::string& index_type,
    const std::vector<PrimaryKeyIndexSourceFile>& expected_sources,
    const std::shared_ptr<IndexFileMeta>& payload,
    const PrimaryKeyIndexSourceMeta& payload_source_meta) {
    if (payload == nullptr || expected_sources.empty()) {
        return nullptr;
    }
    int64_t source_row_count = 0;
    std::set<std::string> source_names;
    for (const PrimaryKeyIndexSourceFile& source_file : expected_sources) {
        if (!source_names.insert(source_file.file_name).second) {
            return nullptr;
        }
        if (__builtin_add_overflow(source_row_count, source_file.row_count, &source_row_count)) {
            return nullptr;
        }
    }

    const std::optional<GlobalIndexMeta>& meta = payload->GetGlobalIndexMeta();
    if (payload_source_meta.SourceFiles() != expected_sources ||
        index_type != payload->IndexType() || meta == std::nullopt ||
        meta.value().index_field_id != field_id || meta.value().row_range_start != 0 ||
        meta.value().row_range_end != source_row_count - 1 ||
        payload->RowCount() != source_row_count) {
        return nullptr;
    }
    return std::shared_ptr<PkSortedIndexGroup>(new PkSortedIndexGroup(
        payload_source_meta.DataLevel(), expected_sources, payload, source_row_count));
}

}  // namespace paimon
