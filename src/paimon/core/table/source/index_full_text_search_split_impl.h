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
#include "paimon/result.h"
#include "paimon/table/source/full_text_search_split.h"
#include "paimon/utils/range.h"

namespace paimon {
/// The implementation of `IndexFullTextSearchSplit`, which also holds the full-text index files of
/// the shard.
class IndexFullTextSearchSplitImpl : public IndexFullTextSearchSplit {
 public:
    /// "PFTSPLT1" in ASCII.
    static constexpr int64_t MAGIC = 0x50465453504C5431L;
    /// TODO: once row filters (#405) are supported, add the scalar index files that pre-filter rows
    /// in a new version, as version 2 of Java `IndexFullTextSearchSplit` does.
    static constexpr int32_t VERSION = 1;

    /// Creates a split.
    /// @param snapshot_id       The snapshot the split is planned from.
    /// @param column_name       The searched table column; not empty and at most 65535 bytes.
    /// @param row_range_start   The first global row id of the index shard; not negative.
    /// @param row_range_end     The last global row id of the index shard, inclusive; the row
    ///                          count of the shard must fit in int64.
    /// @param full_text_index_files The files of the shard: at least one, all of one index
    ///                          (`DataEvolutionFullTextScan::IndexIdentity()`), and each with a
    ///                          global index meta of the shard range.
    /// @param search_row_ranges The sorted, disjoint and non-adjacent row id ranges to rank, at
    ///                          least one and all within the shard range.
    /// @return The split, or `Invalid` if an argument breaks these rules.
    static Result<std::shared_ptr<IndexFullTextSearchSplitImpl>> Create(
        int64_t snapshot_id, const std::string& column_name, int64_t row_range_start,
        int64_t row_range_end, std::vector<std::shared_ptr<IndexFileMeta>> full_text_index_files,
        std::vector<Range> search_row_ranges);

    int64_t SnapshotId() const override {
        return snapshot_id_;
    }

    const std::string& ColumnName() const override {
        return column_name_;
    }

    int64_t RowRangeStart() const override {
        return row_range_start_;
    }

    int64_t RowRangeEnd() const override {
        return row_range_end_;
    }

    const std::vector<Range>& SearchRowRanges() const override {
        return search_row_ranges_;
    }

    const std::vector<std::shared_ptr<IndexFileMeta>>& FullTextIndexFiles() const {
        return full_text_index_files_;
    }

    bool operator==(const IndexFullTextSearchSplitImpl& other) const;

    std::string ToString() const;

 private:
    IndexFullTextSearchSplitImpl(int64_t snapshot_id, const std::string& column_name,
                                 int64_t row_range_start, int64_t row_range_end,
                                 std::vector<std::shared_ptr<IndexFileMeta>> full_text_index_files,
                                 std::vector<Range> search_row_ranges)
        : snapshot_id_(snapshot_id),
          column_name_(column_name),
          row_range_start_(row_range_start),
          row_range_end_(row_range_end),
          full_text_index_files_(std::move(full_text_index_files)),
          search_row_ranges_(std::move(search_row_ranges)) {}

    int64_t snapshot_id_;
    std::string column_name_;
    int64_t row_range_start_;
    int64_t row_range_end_;
    std::vector<std::shared_ptr<IndexFileMeta>> full_text_index_files_;
    std::vector<Range> search_row_ranges_;
};

}  // namespace paimon
