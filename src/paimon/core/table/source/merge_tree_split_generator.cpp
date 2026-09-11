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

#include "paimon/core/table/source/merge_tree_split_generator.h"

#include <algorithm>
#include <functional>
#include <optional>
#include <set>

#include "paimon/common/utils/bin_packing.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/mergetree/sorted_run.h"
#include "paimon/core/options/merge_engine.h"

namespace paimon {
class FieldsComparator;

MergeTreeSplitGenerator::MergeTreeSplitGenerator(
    int64_t target_split_size, int64_t open_file_cost, bool deletion_vectors_enabled,
    const MergeEngine& merge_engine, const std::shared_ptr<FieldsComparator>& key_comparator)
    : target_split_size_(target_split_size),
      open_file_cost_(open_file_cost),
      deletion_vectors_enabled_(deletion_vectors_enabled),
      merge_engine_(merge_engine),
      key_comparator_(key_comparator) {}

Result<std::vector<SplitGenerator::SplitGroup>> MergeTreeSplitGenerator::SplitForBatch(
    std::vector<std::shared_ptr<DataFileMeta>>&& input) const {
    bool raw_convertible = true;
    std::set<int32_t> all_levels;
    for (const auto& meta : input) {
        if (meta->level == 0 || !WithoutDeleteRow(meta)) {
            raw_convertible = false;
        }
        all_levels.insert(meta->level);
    }
    bool one_level = (all_levels.size() == 1);

    if (raw_convertible &&
        (deletion_vectors_enabled_ || merge_engine_ == MergeEngine::FIRST_ROW || one_level)) {
        auto weight_func = [open_file_cost = open_file_cost_](
                               const std::shared_ptr<DataFileMeta>& meta) -> int64_t {
            return std::max(meta->file_size, open_file_cost);
        };
        auto packed = BinPacking::PackForOrdered<std::shared_ptr<DataFileMeta>>(
            std::move(input), weight_func, target_split_size_);
        std::vector<SplitGenerator::SplitGroup> ret;
        ret.reserve(packed.size());
        for (auto& pack : packed) {
            ret.push_back(SplitGenerator::SplitGroup::RawConvertibleGroup(std::move(pack)));
        }
        return ret;
    }

    /*
     * The generator aims to parallel the scan execution by slicing the files of each bucket
     * into multiple splits. The generation has one constraint: files with intersected key
     * ranges (within one section) must go to the same split. Therefore, the files are first to
     * go through the interval partition algorithm to generate sections and then through the
     * OrderedPack algorithm. Note that the item to be packed here is each section, the capacity
     * is denoted as the targetSplitSize, and the final number of the bins is the number of
     * splits generated.
     *
     * For instance, there are files: [1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [210, 700]
     * with targetSplitSize 128M. After interval partition, there are four sections:
     * - section1: [1, 2]
     * - section2: [3, 4]
     * - section3: [5, 180], [5, 190]
     * - section4: [200, 600], [210, 700]
     *
     * After OrderedPack, section1 and section2 will be put into one bin (split), so the final
     * result will be:
     * - split1: [1, 2] [3, 4]
     * - split2: [5, 180] [5,190]
     * - split3: [200, 600] [210, 700]
     */
    std::vector<std::vector<SortedRun>> sorted_runs_vec =
        IntervalPartition(std::move(input), key_comparator_).Partition();
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> sections;
    sections.reserve(sorted_runs_vec.size());
    for (auto& sorted_runs : sorted_runs_vec) {
        auto files = FlatRun(std::move(sorted_runs));
        sections.push_back(std::move(files));
    }

    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> metas_vec =
        PackSplits(std::move(sections));
    std::vector<SplitGenerator::SplitGroup> split_groups;
    split_groups.reserve(metas_vec.size());
    for (auto& metas : metas_vec) {
        bool packed_raw_convertible = true;
        std::set<int32_t> packed_levels;
        for (const std::shared_ptr<DataFileMeta>& meta : metas) {
            if (meta->level == 0 || !WithoutDeleteRow(meta)) {
                packed_raw_convertible = false;
            }
            packed_levels.insert(meta->level);
        }
        const bool single_file_raw_convertible =
            metas.size() == 1 && WithoutDeleteRow(metas[0]) &&
            (!deletion_vectors_enabled_ || metas[0]->level > 0);
        if (single_file_raw_convertible ||
            (packed_raw_convertible &&
             (deletion_vectors_enabled_ || merge_engine_ == MergeEngine::FIRST_ROW ||
              packed_levels.size() == 1))) {
            split_groups.push_back(
                SplitGenerator::SplitGroup::RawConvertibleGroup(std::move(metas)));
        } else {
            split_groups.push_back(
                SplitGenerator::SplitGroup::NonRawConvertibleGroup(std::move(metas)));
        }
    }
    return split_groups;
}

std::vector<std::vector<std::shared_ptr<DataFileMeta>>> MergeTreeSplitGenerator::PackSplits(
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>>&& sections) const {
    auto total_size = [](const std::vector<std::shared_ptr<DataFileMeta>>& section) -> int64_t {
        int64_t ret = 0;
        for (const auto& meta : section) {
            ret += meta->file_size;
        }
        return ret;
    };
    auto weight_func = [open_file_cost = open_file_cost_, total_size](
                           const std::vector<std::shared_ptr<DataFileMeta>>& metas) -> int64_t {
        return std::max(total_size(metas), open_file_cost);
    };
    auto packed = BinPacking::PackForOrdered<std::vector<std::shared_ptr<DataFileMeta>>>(
        std::move(sections), weight_func, target_split_size_);
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>> result;
    result.reserve(packed.size());
    for (auto& pa : packed) {
        auto flat_files = MergeTreeSplitGenerator::FlatFiles(std::move(pa));
        result.push_back(std::move(flat_files));
    }
    return result;
}

std::vector<std::shared_ptr<DataFileMeta>> MergeTreeSplitGenerator::FlatFiles(
    std::vector<std::vector<std::shared_ptr<DataFileMeta>>>&& section) {
    std::vector<std::shared_ptr<DataFileMeta>> result;
    for (auto& sec : section) {
        for (auto& file : sec) {
            result.push_back(std::move(file));
        }
    }
    return result;
}

std::vector<std::shared_ptr<DataFileMeta>> MergeTreeSplitGenerator::FlatRun(
    std::vector<SortedRun>&& section) {
    std::vector<std::shared_ptr<DataFileMeta>> result;
    for (auto& run : section) {
        for (auto& file : run.Files()) {
            result.push_back(std::move(file));
        }
    }
    return result;
}

bool MergeTreeSplitGenerator::WithoutDeleteRow(const std::shared_ptr<DataFileMeta>& meta) {
    // null to true to be compatible with old version
    if (meta->delete_row_count == std::nullopt) {
        return true;
    }
    return meta->delete_row_count.value() == 0l;
}

}  // namespace paimon
