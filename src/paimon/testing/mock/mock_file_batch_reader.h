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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/reader/prefetch_file_batch_reader.h"

namespace paimon::test {

class MockFileBatchReader : public PrefetchFileBatchReader {
 public:
    MockFileBatchReader(const std::shared_ptr<arrow::Array>& data,
                        const std::shared_ptr<arrow::DataType>& file_schema,
                        int32_t read_batch_size)
        : data_(data),
          file_schema_(file_schema),
          read_schema_(arrow::schema(file_schema->fields())),
          batch_size_(read_batch_size) {
        assert(read_batch_size > 0);
        // add all valid bitmap
        int64_t data_length = data_ ? data_->length() : 0;
        assert(data_length >= 0);
        assert(data_length <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()));
        bitmap_ = RoaringBitmap32();
        bitmap_.AddRange(0, static_cast<int32_t>(data_length));
        read_end_pos_ = static_cast<int32_t>(data_length);
    }

    MockFileBatchReader(const std::shared_ptr<arrow::Array>& data,
                        const std::shared_ptr<arrow::DataType>& schema,
                        const RoaringBitmap32& bitmap, int32_t read_batch_size)
        : MockFileBatchReader(data, schema, read_batch_size) {
        bitmap_ = bitmap;
    }

    Result<std::unique_ptr<::ArrowSchema>> GetFileSchema() const override {
        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportType(*file_schema_, c_schema.get()));
        return c_schema;
    }

    void SetNextBatchStatus(const Status& status) {
        next_batch_status_ = status;
    }

    void EnableRandomizeBatchSize(bool enabled) {
        enable_randomize_batch_size_ = enabled;
    }

    Status SetReadSchema(::ArrowSchema* read_schema, const std::shared_ptr<Predicate>& predicate,
                         const std::optional<RoaringBitmap32>& selection_bitmap) override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                          arrow::ImportSchema(read_schema));
        read_schema_ = arrow_schema;
        // A real FileBatchReader restarts from the first row and drops its assigned read ranges
        // when the read schema is (re)set. Readers that switch schemas mid-file, such as the
        // late-materialization reader moving from its probe pass to its payload pass, rely on it.
        current_pos_ = 0;
        previous_batch_first_row_num_ = std::numeric_limits<uint64_t>::max();
        return Status::OK();
    }

    Result<std::vector<std::pair<uint64_t, uint64_t>>> GenReadRanges(
        bool* need_prefetch) const override {
        uint64_t begin_row_num = 0;
        PAIMON_ASSIGN_OR_RAISE(uint64_t end_row_num, GetNumberOfRows());
        std::vector<std::pair<uint64_t, uint64_t>> read_ranges;
        for (uint64_t begin = begin_row_num; begin < end_row_num; begin += batch_size_) {
            uint64_t end = std::min(begin + batch_size_, end_row_num);
            read_ranges.emplace_back(begin, end);
        }
        *need_prefetch = true;
        return read_ranges;
    }

    Status SeekToRow(uint64_t row_number) override {
        assert(row_number <= static_cast<uint64_t>(std::numeric_limits<int32_t>::max()));
        current_pos_ = static_cast<int32_t>(row_number);
        return Status::OK();
    }

    Status SetReadRanges(const std::vector<std::pair<uint64_t, uint64_t>>& read_ranges) override {
        read_ranges_ = read_ranges;
        return Status::OK();
    }

    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatchWithBitmap batch_with_bitmap, NextBatchWithBitmap());
        std::shared_ptr<arrow::MemoryPool> arrow_pool(arrow::default_memory_pool(),
                                                      [](arrow::MemoryPool*) {});
        return ReaderUtils::ApplyBitmapToReadBatch(std::move(batch_with_bitmap), arrow_pool);
    }

    Result<ReadBatchWithBitmap> NextBatchWithBitmap() override {
        while (true) {
            PAIMON_RETURN_NOT_OK(next_batch_status_);
            int32_t begin_pos = current_pos_;
            int32_t range_end_pos = read_end_pos_;
            if (!read_ranges_.empty()) {
                // Reading is restricted to the assigned ranges (ascending and half-open), like a
                // real format reader, so that a prefetch reader may dispatch disjoint ranges to
                // parallel readers. An empty range set means the whole file may be read.
                const std::pair<uint64_t, uint64_t>* selected = nullptr;
                for (const auto& range : read_ranges_) {
                    if (static_cast<int64_t>(range.second) > begin_pos) {
                        selected = &range;
                        break;
                    }
                }
                if (selected == nullptr) {
                    previous_batch_first_row_num_ = ToReaderRowNumber(begin_pos);
                    return BatchReader::MakeEofBatchWithBitmap();
                }
                // Skip the gap in front of the first range that has not been read yet.
                begin_pos = std::max(begin_pos, static_cast<int32_t>(selected->first));
                range_end_pos = std::min(range_end_pos, static_cast<int32_t>(selected->second));
            }
            if (begin_pos >= read_end_pos_) {
                previous_batch_first_row_num_ = ToReaderRowNumber(begin_pos);
                return BatchReader::MakeEofBatchWithBitmap();
            }
            int32_t actual_batch_size = batch_size_;
            if (enable_randomize_batch_size_) {
                std::uniform_int_distribution<int32_t> distribution(1, batch_size_);
                actual_batch_size = distribution(random_engine_);
            }
            int32_t batch_end_pos =
                std::min({read_end_pos_, range_end_pos, begin_pos + actual_batch_size});
            auto slice = data_->Slice(begin_pos, batch_end_pos - begin_pos);
            PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                std::shared_ptr<arrow::Array> concat_slice,
                arrow::Concatenate({slice}, arrow::default_memory_pool()));
            RoaringBitmap32 bitmap;
            for (auto iter = bitmap_.EqualOrLarger(begin_pos);
                 iter != bitmap_.End() && *iter < batch_end_pos; ++iter) {
                bitmap.Add(*iter - begin_pos);
            }
            previous_batch_first_row_num_ = ToReaderRowNumber(begin_pos);
            current_pos_ = batch_end_pos;
            if (bitmap.IsEmpty()) {
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(concat_slice, ProjectBatch(concat_slice));
            std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
            std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                arrow::ExportArray(*concat_slice, c_array.get(), c_schema.get()));
            return std::make_pair(std::make_pair(std::move(c_array), std::move(c_schema)),
                                  std::move(bitmap));
        }
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        auto metrics = std::make_shared<MetricsImpl>();
        metrics->SetCounter("mock.number.of.rows", GetNumberOfRows().value_or(-1));
        return metrics;
    }

    Result<uint64_t> GetPreviousBatchFileRowId(uint64_t batch_row_id) const override {
        if (previous_batch_first_row_num_ == std::numeric_limits<uint64_t>::max()) {
            return Status::Invalid("No batch has been read yet");
        }
        return previous_batch_first_row_num_ + batch_row_id;
    }

    Result<uint64_t> GetNumberOfRows() const override {
        return ToReaderRowNumber(read_end_pos_);
    }
    Result<uint64_t> GetNextRowToRead() const override {
        return ToReaderRowNumber(current_pos_);
    }
    void Close() override {}

    std::vector<std::pair<uint64_t, uint64_t>> GetReadRanges() const {
        return read_ranges_;
    }

    bool SupportPreciseBitmapSelection() const override {
        return false;
    }

 protected:
    static uint64_t ToReaderRowNumber(int32_t row_number) {
        if (row_number < 0) {
            return std::numeric_limits<uint64_t>::max();
        }
        return static_cast<uint64_t>(row_number);
    }

    /// Pick the columns requested by `read_schema_` out of `batch`, in the requested order.
    ///
    /// `batch` is returned as is unless the requested schema is a genuine re-selection of the
    /// columns this file has. Requesting a field the file does not have means the read schema is a
    /// logical view over some other physical layout, as the shredding and the row tracking readers
    /// do, and those map the raw batch themselves.
    /// `batch` is expected to have a zero offset, so its validity buffer can be reused as is.
    Result<std::shared_ptr<arrow::Array>> ProjectBatch(
        const std::shared_ptr<arrow::Array>& batch) const {
        auto struct_batch = std::dynamic_pointer_cast<arrow::StructArray>(batch);
        if (struct_batch == nullptr) {
            return batch;
        }
        arrow::ArrayVector children;
        arrow::FieldVector fields;
        for (const auto& field : read_schema_->fields()) {
            std::shared_ptr<arrow::Array> column = struct_batch->GetFieldByName(field->name());
            if (column == nullptr) {
                return batch;
            }
            children.push_back(column);
            fields.push_back(field);
        }
        const arrow::FieldVector& batch_fields = struct_batch->type()->fields();
        bool keeps_every_column = fields.size() == batch_fields.size();
        for (size_t i = 0; keeps_every_column && i < fields.size(); i++) {
            keeps_every_column = fields[i]->name() == batch_fields[i]->name();
        }
        if (keeps_every_column) {
            return batch;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> projected,
            arrow::StructArray::Make(children, fields, struct_batch->null_bitmap(),
                                     struct_batch->null_count()));
        return projected;
    }

    std::shared_ptr<arrow::Array> data_;
    std::shared_ptr<arrow::DataType> file_schema_;
    std::shared_ptr<arrow::Schema> read_schema_;
    RoaringBitmap32 bitmap_;
    int32_t batch_size_ = 0;
    int32_t current_pos_ = 0;
    int32_t read_end_pos_ = 0;
    uint64_t previous_batch_first_row_num_ = std::numeric_limits<uint64_t>::max();
    Status next_batch_status_;
    bool enable_randomize_batch_size_ = true;
    std::vector<std::pair<uint64_t, uint64_t>> read_ranges_;
    std::mt19937 random_engine_{std::random_device{}()};  // NOLINT(whitespace/braces)
};

}  // namespace paimon::test
