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

#include "paimon/common/reader/blob_fallback_batch_reader.h"

#include <algorithm>
#include <string_view>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/reader_utils.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/scope_guard.h"

namespace paimon {

Result<std::unique_ptr<BlobFallbackBatchReader>> BlobFallbackBatchReader::Create(
    std::vector<std::vector<Segment>>&& sequence_groups,
    const std::shared_ptr<arrow::Schema>& read_schema, int32_t read_batch_size,
    const std::shared_ptr<arrow::MemoryPool>& arrow_pool) {
    if (sequence_groups.empty()) {
        return Status::Invalid("Blob fallback needs at least one sequence group.");
    }
    if (read_schema == nullptr) {
        return Status::Invalid("Blob fallback read schema cannot be nullptr.");
    }
    if (read_batch_size <= 0) {
        return Status::Invalid(fmt::format(
            "Blob fallback read batch size '{}' should be larger than zero", read_batch_size));
    }
    int32_t blob_field_idx = -1;
    for (int32_t i = 0; i < read_schema->num_fields(); i++) {
        if (BlobUtils::IsAnyBlobField(read_schema->field(i))) {
            if (blob_field_idx != -1) {
                return Status::Invalid(
                    "Blob fallback read schema should contain exactly one blob field.");
            }
            blob_field_idx = i;
        }
    }
    if (blob_field_idx == -1) {
        return Status::Invalid("Blob fallback read schema should contain a blob field.");
    }
    int32_t row_id_field_idx = read_schema->GetFieldIndex(SpecialFields::RowId().Name());
    int32_t seq_num_field_idx = read_schema->GetFieldIndex(SpecialFields::SequenceNumber().Name());
    std::vector<GroupCursor> groups;
    groups.reserve(sequence_groups.size());
    for (auto& segments : sequence_groups) {
        if (segments.empty()) {
            return Status::Invalid("Blob fallback sequence group should not be empty.");
        }
        for (const auto& segment : segments) {
            if (segment.reader != nullptr && segment.blob_context == nullptr) {
                return Status::Invalid("Blob fallback file segment needs a blob read context.");
            }
            if (segment.reader == nullptr && segment.gap_selected_ranges.empty()) {
                return Status::Invalid(
                    "Blob fallback gap segment should cover at least one selected row id.");
            }
        }
        GroupCursor cursor;
        cursor.segments = std::move(segments);
        groups.push_back(std::move(cursor));
    }
    return std::unique_ptr<BlobFallbackBatchReader>(new BlobFallbackBatchReader(
        std::move(groups), read_schema, blob_field_idx, row_id_field_idx, seq_num_field_idx,
        read_batch_size, arrow_pool));
}

BlobFallbackBatchReader::BlobFallbackBatchReader(
    std::vector<GroupCursor>&& groups, const std::shared_ptr<arrow::Schema>& read_schema,
    int32_t blob_field_idx, int32_t row_id_field_idx, int32_t seq_num_field_idx,
    int32_t read_batch_size, const std::shared_ptr<arrow::MemoryPool>& arrow_pool)
    : groups_(std::move(groups)),
      read_schema_(read_schema),
      blob_field_idx_(blob_field_idx),
      row_id_field_idx_(row_id_field_idx),
      seq_num_field_idx_(seq_num_field_idx),
      read_batch_size_(read_batch_size),
      arrow_pool_(arrow_pool),
      finished_reader_metrics_(std::make_shared<MetricsImpl>()) {}

Result<BatchReader::ReadBatchWithBitmap> BlobFallbackBatchReader::ReadSegmentBatch(
    const Segment& segment, int32_t max_rows, bool read_payload) {
    const std::optional<BlobBatchRequest> previous_request = segment.blob_context->request;
    segment.blob_context->request = BlobBatchRequest{max_rows, read_payload};
    ScopeGuard reset_request([context = segment.blob_context, previous_request]() {
        context->request = previous_request;
    });
    return segment.reader->NextBatchWithBitmap();
}

Result<int64_t> BlobFallbackBatchReader::FillWindow(size_t group_idx, int64_t want,
                                                    const std::vector<int32_t>* group_choice,
                                                    std::vector<Chunk>* chunks) {
    GroupCursor& cursor = groups_[group_idx];
    int64_t collected = 0;
    int64_t run_end = 0;
    bool materialize = true;
    while (collected < want) {
        // Split the window into contiguous read/skip runs, grouping rows with the same operation.
        if (collected == run_end) {
            materialize = !group_choice || (*group_choice)[collected] == -1;
            run_end = collected + 1;
            if (!group_choice) {
                run_end = want;
            } else {
                while (run_end < want && ((*group_choice)[run_end] == -1) == materialize) {
                    run_end++;
                }
            }
        }
        if (cursor.segment_idx >= cursor.segments.size()) {
            // group exhausted; only the first group may define a shorter window
            break;
        }
        Segment& segment = cursor.segments[cursor.segment_idx];
        if (segment.reader == nullptr) {
            // gap segment: all rows are placeholders, stepped range by range so the row ids
            // stay available for all-placeholder rows
            if (cursor.gap_range_idx >= segment.gap_selected_ranges.size()) {
                cursor.segment_idx++;
                cursor.gap_range_idx = 0;
                cursor.gap_range_pos = 0;
                continue;
            }
            const Range& range = segment.gap_selected_ranges[cursor.gap_range_idx];
            int64_t remaining = range.Count() - cursor.gap_range_pos;
            int64_t take = std::min(remaining, run_end - collected);
            Chunk chunk{nullptr, take, {}};
            if (group_idx == 0 && row_id_field_idx_ >= 0) {
                chunk.gap_row_ids.reserve(take);
                for (int64_t k = 0; k < take; k++) {
                    chunk.gap_row_ids.push_back(range.from + cursor.gap_range_pos + k);
                }
            }
            chunks->push_back(std::move(chunk));
            cursor.gap_range_pos += take;
            collected += take;
            if (cursor.gap_range_pos == range.Count()) {
                cursor.gap_range_idx++;
                cursor.gap_range_pos = 0;
            }
            continue;
        }
        const int64_t remaining = run_end - collected;
        // Bound selected input rows by this run. Outer filtering can return fewer valid rows,
        // but must not read ahead into a run requiring a different payload request.
        PAIMON_ASSIGN_OR_RAISE(
            ReadBatchWithBitmap batch_with_bitmap,
            ReadSegmentBatch(segment, static_cast<int32_t>(remaining), materialize));
        if (BatchReader::IsEofBatch(batch_with_bitmap)) {
            segment.reader->Close();
            finished_reader_metrics_->Merge(segment.reader->GetReaderMetrics());
            segment.reader.reset();
            cursor.segment_idx++;
            continue;
        }
        auto& [read_batch, bitmap] = batch_with_bitmap;
        auto& [c_array, c_schema] = read_batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> src_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        if (src_array->length() > remaining) {
            return Status::Invalid(
                fmt::format("Blob fallback reader returned {} rows exceeding requested limit {}",
                            src_array->length(), remaining));
        }
        PAIMON_ASSIGN_OR_RAISE(arrow::ArrayVector selected_array_vec,
                               ReaderUtils::GenerateFilteredArrayVector(src_array, bitmap));
        for (const auto& selected_array : selected_array_vec) {
            if (!selected_array || selected_array->type_id() != arrow::Type::STRUCT) {
                return Status::Invalid("Blob fallback expects file readers to emit struct arrays.");
            }
            if (selected_array->length() == 0) {
                continue;
            }
            const int64_t count = selected_array->length();
            auto struct_array = checked_pointer_cast<arrow::StructArray>(selected_array);
            chunks->push_back(Chunk{materialize ? struct_array : nullptr, count, {}});
            collected += count;
        }
    }
    return collected;
}

Result<std::vector<bool>> BlobFallbackBatchReader::ComputePlaceholderFlags(
    const std::vector<Chunk>& chunks, int64_t row_count) const {
    std::vector<bool> flags(row_count, false);
    int64_t pos = 0;
    for (const auto& chunk : chunks) {
        if (chunk.array == nullptr) {
            // gap rows stand for placeholders
            std::fill(flags.begin() + pos, flags.begin() + pos + chunk.length, true);
        } else {
            std::shared_ptr<arrow::Array> blob_col = chunk.array->field(blob_field_idx_);
            if (!blob_col) {
                return Status::Invalid("Blob fallback got a null blob column.");
            }
            if (blob_col->type_id() == arrow::Type::MAP) {
                auto map_col = checked_pointer_cast<arrow::MapArray>(blob_col);
                for (int64_t k = 0; k < chunk.length; k++) {
                    flags[pos + k] = BlobUtils::IsMapBlobPlaceholder(*map_col, k);
                }
                pos += chunk.length;
                continue;
            }
            if (blob_col->type_id() == arrow::Type::LIST) {
                auto list_col = checked_pointer_cast<arrow::ListArray>(blob_col);
                for (int64_t k = 0; k < chunk.length; k++) {
                    flags[pos + k] = BlobUtils::IsArrayBlobPlaceholder(*list_col, k);
                }
                pos += chunk.length;
                continue;
            }
            if (blob_col->type_id() != arrow::Type::LARGE_BINARY) {
                return Status::Invalid(fmt::format(
                    "Blob fallback expects a BLOB, ARRAY<BLOB> or MAP<..., BLOB> column, but "
                    "got {}",
                    blob_col->type()->ToString()));
            }
            auto binary_col = checked_pointer_cast<arrow::LargeBinaryArray>(blob_col);
            for (int64_t k = 0; k < chunk.length; k++) {
                if (binary_col->IsNull(k)) {
                    continue;
                }
                std::string_view value = binary_col->GetView(k);
                if (BlobDefs::IsPlaceholderSentinel(value.data(), value.size())) {
                    flags[pos + k] = true;
                }
            }
        }
        pos += chunk.length;
    }
    return flags;
}

Result<std::shared_ptr<arrow::Array>> BlobFallbackBatchReader::AssembleRowIdRun(
    const std::vector<Chunk>& chunks, int64_t run_start, int64_t run_end,
    ChunkCursor* cursor) const {
    arrow::ArrayVector pieces;
    while (cursor->chunk_idx < chunks.size() && cursor->chunk_start < run_end) {
        const auto& chunk = chunks[cursor->chunk_idx];
        const int64_t pos = cursor->chunk_start;
        int64_t overlap_start = std::max(run_start, pos);
        int64_t overlap_end = std::min(run_end, pos + chunk.length);
        if (overlap_start < overlap_end) {
            if (chunk.array != nullptr) {
                std::shared_ptr<arrow::Array> column = chunk.array->field(row_id_field_idx_);
                pieces.push_back(column->Slice(overlap_start - pos, overlap_end - overlap_start));
            } else {
                arrow::Int64Builder builder(arrow_pool_.get());
                for (int64_t r = overlap_start; r < overlap_end; r++) {
                    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(chunk.gap_row_ids[r - pos]));
                }
                std::shared_ptr<arrow::Array> piece;
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&piece));
                pieces.push_back(std::move(piece));
            }
        }
        if (pos + chunk.length > run_end) {
            break;
        }
        cursor->chunk_start += chunk.length;
        cursor->chunk_idx++;
    }
    if (pieces.size() == 1) {
        return pieces[0];
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> concat_array,
                                      arrow::Concatenate(pieces, arrow_pool_.get()));
    return concat_array;
}

Result<std::shared_ptr<arrow::Array>> BlobFallbackBatchReader::AssembleColumn(
    int32_t field_idx, const std::vector<int32_t>& group_choice,
    const std::vector<std::vector<Chunk>>& group_chunks) const {
    const auto row_count = static_cast<int64_t>(group_choice.size());
    arrow::ArrayVector pieces;
    std::vector<ChunkCursor> cursors(group_chunks.size());
    int64_t run_start = 0;
    while (run_start < row_count) {
        const int32_t group = group_choice[run_start];
        int64_t run_end = run_start + 1;
        while (run_end < row_count && group_choice[run_end] == group) {
            run_end++;
        }
        if (group < 0) {
            // placeholder in every layer: the blob degrades to null; the row keeps its row id
            // (taken from the newest group, which steps in lockstep), reports -1 as its
            // sequence number, and returns null for every other field
            if (field_idx == row_id_field_idx_) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<arrow::Array> row_id_piece,
                    AssembleRowIdRun(group_chunks[0], run_start, run_end, &cursors[0]));
                pieces.push_back(std::move(row_id_piece));
            } else if (field_idx == seq_num_field_idx_) {
                arrow::Int64Scalar seq_scalar(-1);
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> seq_piece,
                    arrow::MakeArrayFromScalar(seq_scalar, run_end - run_start, arrow_pool_.get()));
                pieces.push_back(std::move(seq_piece));
            } else {
                PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
                    std::shared_ptr<arrow::Array> null_piece,
                    arrow::MakeArrayOfNull(read_schema_->field(field_idx)->type(),
                                           run_end - run_start, arrow_pool_.get()));
                pieces.push_back(std::move(null_piece));
            }
        } else {
            ChunkCursor& cursor = cursors[group];
            const auto& chunks = group_chunks[group];
            while (cursor.chunk_idx < chunks.size() && cursor.chunk_start < run_end) {
                const auto& chunk = chunks[cursor.chunk_idx];
                const int64_t pos = cursor.chunk_start;
                int64_t overlap_start = std::max(run_start, pos);
                int64_t overlap_end = std::min(run_end, pos + chunk.length);
                if (overlap_start < overlap_end) {
                    if (chunk.array == nullptr) {
                        return Status::Invalid(
                            "Unexpected: a gap or skipped row was chosen as a blob fallback "
                            "result.");
                    }
                    std::shared_ptr<arrow::Array> column = chunk.array->field(field_idx);
                    pieces.push_back(
                        column->Slice(overlap_start - pos, overlap_end - overlap_start));
                }
                // Keep a partially consumed chunk for a later run choosing the same group.
                if (pos + chunk.length > run_end) {
                    break;
                }
                cursor.chunk_start += chunk.length;
                cursor.chunk_idx++;
            }
        }
        run_start = run_end;
    }
    if (pieces.size() == 1) {
        return pieces[0];
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> concat_array,
                                      arrow::Concatenate(pieces, arrow_pool_.get()));
    return concat_array;
}

Result<BatchReader::ReadBatch> BlobFallbackBatchReader::NextBatch() {
    if (closed_) {
        return Status::Invalid("blob fallback batch reader is closed");
    }
    std::vector<std::vector<Chunk>> group_chunks(groups_.size());
    // The newest group defines the window. Resolve rows before opening each older group's
    // payloads, while still advancing every group by the same number of selected rows.
    PAIMON_ASSIGN_OR_RAISE(int64_t row_count,
                           FillWindow(0, read_batch_size_, nullptr, &group_chunks[0]));
    std::vector<int32_t> group_choice(row_count, -1);
    const std::vector<int32_t> eof_choice(1, 0);
    for (size_t g = 0; g < groups_.size(); g++) {
        if (g > 0) {
            // At EOF of the newest group, check that older groups are also exhausted without
            // reading payloads.
            const int64_t want = std::max<int64_t>(row_count, 1);
            PAIMON_ASSIGN_OR_RAISE(int64_t got,
                                   FillWindow(g, want, row_count == 0 ? &eof_choice : &group_choice,
                                              &group_chunks[g]));
            if (got != row_count) {
                return Status::Invalid(fmt::format(
                    "All sequence groups of a blob fallback read should have the same number of "
                    "rows: group {} yielded {} rows in a window of {}",
                    g, got, row_count));
            }
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<bool> placeholder_flags,
                               ComputePlaceholderFlags(group_chunks[g], row_count));
        for (int64_t r = 0; r < row_count; r++) {
            if (group_choice[r] == -1 && !placeholder_flags[r]) {
                group_choice[r] = static_cast<int32_t>(g);
            }
        }
    }
    if (row_count == 0) {
        return BatchReader::MakeEofBatch();
    }

    arrow::ArrayVector columns;
    columns.reserve(read_schema_->num_fields());
    for (int32_t field_idx = 0; field_idx < read_schema_->num_fields(); field_idx++) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> column,
                               AssembleColumn(field_idx, group_choice, group_chunks));
        columns.push_back(std::move(column));
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> array,
                                      arrow::StructArray::Make(columns, read_schema_->fields()));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> normalized_array,
                           ArrowUtils::NormalizeArrayOffsets(array, arrow_pool_.get()));
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        arrow::ExportArray(*normalized_array, c_array.get(), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
    return std::make_pair(std::move(c_array), std::move(c_schema));
}

Result<BatchReader::ReadBatchWithBitmap> BlobFallbackBatchReader::NextBatchWithBitmap() {
    PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, NextBatch());
    if (BatchReader::IsEofBatch(batch)) {
        return BatchReader::MakeEofBatchWithBitmap();
    }
    return ReaderUtils::AddAllValidBitmap(std::move(batch));
}

void BlobFallbackBatchReader::Close() {
    for (auto& group : groups_) {
        for (auto& segment : group.segments) {
            if (segment.reader) {
                segment.reader->Close();
                finished_reader_metrics_->Merge(segment.reader->GetReaderMetrics());
                segment.reader.reset();
            }
        }
    }
    closed_ = true;
}

std::shared_ptr<Metrics> BlobFallbackBatchReader::GetReaderMetrics() const {
    auto metrics = std::make_shared<MetricsImpl>();
    metrics->Merge(finished_reader_metrics_);
    for (const auto& group : groups_) {
        for (const auto& segment : group.segments) {
            if (segment.reader) {
                auto reader_metrics = segment.reader->GetReaderMetrics();
                if (reader_metrics) {
                    metrics->Merge(reader_metrics);
                }
            }
        }
    }
    return metrics;
}

}  // namespace paimon
