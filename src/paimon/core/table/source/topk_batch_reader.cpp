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

#include "paimon/core/table/source/topk_batch_reader.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/fields_comparator.h"

namespace paimon {

TopKBatchReader::TopKBatchReader(std::unique_ptr<BatchReader> reader,
                                 const std::string& score_field_name, ScoreOrder score_order,
                                 int32_t limit, int32_t batch_size,
                                 const std::shared_ptr<arrow::Schema>& read_schema,
                                 const std::shared_ptr<arrow::MemoryPool>& pool)
    : pool_(pool),
      reader_(std::move(reader)),
      score_name_(score_field_name),
      score_order_(score_order),
      limit_(limit),
      batch_size_(batch_size),
      read_type_(arrow::struct_(read_schema->fields())),
      remove_score_(read_schema->GetFieldIndex(score_field_name) < 0) {}

Result<BatchReader::ReadBatch> TopKBatchReader::NextBatch() {
    if (!initialized_) {
        initialized_ = true;
        PAIMON_RETURN_NOT_OK(ReadTopK());
    }
    if (!output_ || position_ == output_->length()) {
        return MakeEofBatch();
    }
    return ReadOutput();
}

std::shared_ptr<Metrics> TopKBatchReader::GetReaderMetrics() const {
    return reader_->GetReaderMetrics();
}

void TopKBatchReader::Close() {
    reader_->Close();
    output_.reset();
}

Status TopKBatchReader::CompactCandidates(std::vector<Candidate>* candidates) const {
    if (candidates->empty()) {
        return Status::OK();
    }
    arrow::ArrayVector rows;
    rows.reserve(candidates->size());
    for (const auto& candidate : *candidates) {
        rows.push_back(candidate.values->Slice(candidate.row, 1));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> compact,
                                      arrow::Concatenate(rows, pool_.get()));
    for (size_t i = 0; i < candidates->size(); ++i) {
        (*candidates)[i].values = compact;
        (*candidates)[i].row = static_cast<int64_t>(i);
    }
    return Status::OK();
}

Status TopKBatchReader::ReadTopK() {
    std::vector<Candidate> candidates;
    uint64_t ordinal = 0;
    bool ascending = score_order_ == ScoreOrder::ASCENDING;
    auto better = [ascending](const Candidate& left, const Candidate& right) {
        int32_t comparison = FieldsComparator::CompareFloatingPoint(left.score, right.score);
        if (comparison == 0) {
            return left.ordinal < right.ordinal;
        }
        return ascending ? comparison < 0 : comparison > 0;
    };
    while (true) {
        // NextBatch applies all row-selection bitmaps, including deletion vectors.
        PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, reader_->NextBatch());
        if (IsEofBatch(batch)) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::Array> array,
            arrow::ImportArray(batch.first.get(), batch.second.get()));
        if (array->type_id() != arrow::Type::STRUCT) {
            return Status::Invalid("TopK reader requires a StructArray");
        }
        if (array->length() == 0) {
            continue;
        }
        auto values = checked_pointer_cast<arrow::StructArray>(array);
        PAIMON_ASSIGN_OR_RAISE(values, ArrowUtils::FlattenUnresolvableDictionaries(
                                           values, read_type_, pool_.get(),
                                           /*preserve_layout_recoverable_dictionaries=*/false));
        auto scores =
            std::dynamic_pointer_cast<arrow::FloatArray>(values->GetFieldByName(score_name_));
        if (!scores || scores->null_count() != 0) {
            return Status::Invalid("TopK reader requires non-null float search scores");
        }
        for (int64_t row = 0; row < values->length(); ++row) {
            const float score = scores->Value(row);
            Candidate candidate{score, ordinal++, values, row};
            if (candidates.size() < static_cast<size_t>(limit_)) {
                candidates.push_back(std::move(candidate));
                std::push_heap(candidates.begin(), candidates.end(), better);
            } else if (better(candidate, candidates.front())) {
                std::pop_heap(candidates.begin(), candidates.end(), better);
                candidates.back() = std::move(candidate);
                std::push_heap(candidates.begin(), candidates.end(), better);
            }
        }
        PAIMON_RETURN_NOT_OK(CompactCandidates(&candidates));
    }
    if (candidates.empty()) {
        return Status::OK();
    }
    std::sort(candidates.begin(), candidates.end(), better);
    PAIMON_RETURN_NOT_OK(CompactCandidates(&candidates));
    output_ = checked_pointer_cast<arrow::StructArray>(candidates.front().values);
    if (remove_score_) {
        PAIMON_ASSIGN_OR_RAISE(output_,
                               ArrowUtils::RemoveFieldFromStructArray(output_, score_name_));
    }
    return Status::OK();
}

Result<BatchReader::ReadBatch> TopKBatchReader::ReadOutput() {
    const int64_t length = std::min<int64_t>(batch_size_, output_->length() - position_);
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Array> array,
        ArrowUtils::NormalizeArrayOffsets(output_->Slice(position_, length), pool_.get()));
    auto c_array = std::make_unique<ArrowArray>();
    auto c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), pool_));
    position_ += length;
    return std::make_pair(std::move(c_array), std::move(c_schema));
}

}  // namespace paimon
