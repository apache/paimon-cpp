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

#include "paimon/format/parquet/file_reader_wrapper.h"

#include <cassert>
#include <cstddef>

#include "arrow/record_batch.h"
#include "arrow/util/range.h"
#include "fmt/format.h"
#include "paimon/macros.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"

namespace paimon::parquet {

Result<std::unique_ptr<FileReaderWrapper>> FileReaderWrapper::Create(
    std::unique_ptr<::parquet::arrow::FileReader>&& file_reader) {
    if (file_reader == nullptr) {
        return Status::Invalid("file reader wrapper create failed. file reader is nullptr");
    }
    std::vector<std::pair<uint64_t, uint64_t>> all_row_group_ranges;
    auto meta_data = file_reader->parquet_reader()->metadata();
    // prepare [start_row_idx, end_row_idx) for all row groups
    uint64_t start_row_idx = 0;
    for (int32_t i = 0; i < meta_data->num_row_groups(); i++) {
        uint64_t end_row_idx = start_row_idx + meta_data->RowGroup(i)->num_rows();
        all_row_group_ranges.emplace_back(start_row_idx, end_row_idx);
        start_row_idx = end_row_idx;
    }
    uint64_t num_rows = file_reader->parquet_reader()->metadata()->num_rows();
    if (start_row_idx != num_rows) {
        assert(false);
        return Status::Invalid(
            fmt::format("unexpected error. row group ranges not match with num rows {}", num_rows));
    }
    std::vector<int32_t> row_groups_indices = arrow::internal::Iota(file_reader->num_row_groups());
    std::vector<int32_t> columns_indices =
        arrow::internal::Iota(file_reader->parquet_reader()->metadata()->num_columns());
    auto file_reader_wrapper = std::unique_ptr<FileReaderWrapper>(
        new FileReaderWrapper(std::move(file_reader), all_row_group_ranges, num_rows));
    PAIMON_RETURN_NOT_OK(file_reader_wrapper->PrepareForReadingLazy(
        std::set<int32_t>(row_groups_indices.begin(), row_groups_indices.end()), columns_indices));
    return file_reader_wrapper;
}

FileReaderWrapper::FileReaderWrapper(
    std::unique_ptr<::parquet::arrow::FileReader>&& file_reader,
    const std::vector<std::pair<uint64_t, uint64_t>>& all_row_group_ranges, uint64_t num_rows)
    : file_reader_(std::move(file_reader)),
      all_row_group_ranges_(all_row_group_ranges),
      num_rows_(num_rows) {}

Status FileReaderWrapper::SeekToRow(uint64_t row_number) {
    for (uint64_t i = 0; i < target_row_groups_.size(); i++) {
        if (row_number > target_row_groups_[i].first && row_number < target_row_groups_[i].second) {
            return Status::Invalid(fmt::format(
                "seek to row failed. row number {} should not be in the middle of readable range",
                row_number));
        }
        if (target_row_groups_[i].first >= row_number) {
            current_row_group_idx_ = i;
            next_row_to_read_ = target_row_groups_[i].first;
            std::vector<int32_t> target_row_group_indices;
            for (uint64_t j = i; j < target_row_groups_.size(); j++) {
                PAIMON_ASSIGN_OR_RAISE(int32_t row_group_id, GetRowGroupId(target_row_groups_[j]));
                target_row_group_indices.push_back(row_group_id);
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetRecordBatchReader(
                target_row_group_indices, target_column_indices_, &batch_reader_));
            return Status::OK();
        }
    }
    next_row_to_read_ = num_rows_;
    current_row_group_idx_ = target_row_groups_.size();
    return Status::OK();
}

Result<std::shared_ptr<arrow::RecordBatch>> FileReaderWrapper::Next() {
    if (PAIMON_UNLIKELY(!reader_initialized_)) {
        PAIMON_RETURN_NOT_OK(PrepareForReading(target_row_group_indices_, target_column_indices_));
    }
    std::shared_ptr<arrow::RecordBatch> record_batch;
    if (current_row_group_idx_ < target_row_groups_.size()) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(record_batch, batch_reader_->Next());
    }
    if (record_batch) {
        int64_t num_rows = record_batch->num_rows();
        previous_first_row_ = next_row_to_read_;
        if (next_row_to_read_ + num_rows < target_row_groups_[current_row_group_idx_].second) {
            next_row_to_read_ += num_rows;
        } else if (next_row_to_read_ + num_rows ==
                   target_row_groups_[current_row_group_idx_].second) {
            if (current_row_group_idx_ == target_row_groups_.size() - 1) {
                // current row group is the last.
                next_row_to_read_ = num_rows_;
            } else {
                current_row_group_idx_++;
                next_row_to_read_ = target_row_groups_[current_row_group_idx_].first;
            }
        } else {
            return Status::Invalid(fmt::format(
                "Next failed. Unexpected error, next row to read {} + num rows just read {} "
                "should always be within current row group range or exactly equals to current "
                "row group end {}",
                next_row_to_read_, num_rows, target_row_groups_[current_row_group_idx_].second));
        }
    } else {
        previous_first_row_ = next_row_to_read_;
    }
    return record_batch;
}

Result<std::vector<std::pair<uint64_t, uint64_t>>> FileReaderWrapper::GetRowGroupRanges(
    const std::set<int32_t>& row_group_indices) const {
    std::vector<std::pair<uint64_t, uint64_t>> row_group_ranges;
    for (auto row_group_index : row_group_indices) {
        if (static_cast<size_t>(row_group_index) >= all_row_group_ranges_.size()) {
            return Status::Invalid(fmt::format("row group index {} is out of bound {}",
                                               row_group_index, all_row_group_ranges_.size()));
        }
        row_group_ranges.push_back(all_row_group_ranges_[row_group_index]);
    }
    return row_group_ranges;
}

Status FileReaderWrapper::PrepareForReadingLazy(const std::set<int32_t>& target_row_group_indices,
                                                const std::vector<int32_t>& column_indices) {
    target_row_group_indices_ = target_row_group_indices;
    target_column_indices_ = column_indices;
    reader_initialized_ = false;
    return Status::OK();
}

Status FileReaderWrapper::PrepareForReading(const std::set<int32_t>& target_row_group_indices,
                                            const std::vector<int32_t>& column_indices) {
    std::vector<std::pair<uint64_t, uint64_t>> target_row_groups;
    PAIMON_ASSIGN_OR_RAISE(target_row_groups, GetRowGroupRanges(target_row_group_indices));
    std::unique_ptr<arrow::RecordBatchReader> batch_reader;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(file_reader_->GetRecordBatchReader(
        std::vector<int32_t>(target_row_group_indices.begin(), target_row_group_indices.end()),
        column_indices, &batch_reader));
    target_row_groups_ = target_row_groups;
    target_column_indices_ = column_indices;
    batch_reader_ = std::move(batch_reader);
    if (target_row_groups_.empty()) {
        next_row_to_read_ = num_rows_;
    } else {
        next_row_to_read_ = target_row_groups_[0].first;
    }
    previous_first_row_ = std::numeric_limits<uint64_t>::max();
    current_row_group_idx_ = 0;
    reader_initialized_ = true;
    return Status::OK();
}

Result<std::set<int32_t>> FileReaderWrapper::FilterRowGroupsByReadRanges(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges,
    const std::vector<int32_t>& src_row_groups) const {
    std::set<int32_t> target_row_groups;
    PAIMON_ASSIGN_OR_RAISE(std::set<int32_t> row_groups_to_read,
                           ReadRangesToRowGroupIds(read_ranges));
    for (const auto& row_group_id : src_row_groups) {
        if (row_groups_to_read.find(row_group_id) != row_groups_to_read.end()) {
            target_row_groups.emplace(row_group_id);
        }
    }
    return target_row_groups;
}

Result<std::set<int32_t>> FileReaderWrapper::ReadRangesToRowGroupIds(
    const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) const {
    std::set<int32_t> selected_row_group_ids;
    for (const auto& read_range : read_ranges) {
        PAIMON_ASSIGN_OR_RAISE(int32_t row_group_id, GetRowGroupId(read_range));
        selected_row_group_ids.emplace(row_group_id);
    }
    return selected_row_group_ids;
}

Result<int32_t> FileReaderWrapper::GetRowGroupId(std::pair<uint64_t, uint64_t> target_range) const {
    for (size_t i = 0; i < all_row_group_ranges_.size(); i++) {
        if (all_row_group_ranges_[i] == target_range) {
            return i;
        }
    }
    return Status::Invalid(fmt::format(
        "not expected failure. target range bound '{},{}' not match with row group range bound",
        target_range.first, target_range.second));
}

}  // namespace paimon::parquet
