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

#include "paimon/format/blob/video_file_batch_reader.h"

#include <algorithm>
#include <array>
#include <limits>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/blob_defs.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/data/video_frame_descriptor.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/reader/blob_read_context.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/delta_varint_compressor.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"

namespace paimon::blob {
namespace {

// Encoded video payloads, six delta varint indexes, then the footer.
// The footer stores six little endian int32 index lengths followed by the magic and version.
constexpr int64_t kFooterSize = 29;
constexpr int32_t kMagic = 0x4F454449;
constexpr int8_t kVersion = 1;
constexpr uint64_t kMaxIndexMemoryBytes = 64 * 1024 * 1024;
constexpr int64_t kNullReference = -1;
constexpr int64_t kPlaceholderReference = -2;
constexpr char kLegacyVideoHint[] = ". Legacy five-index .video files require reimport";

Status ReadAtFully(InputStream* input, int64_t offset, char* data, int64_t size) {
    for (int64_t read = 0; read < size;) {
        PAIMON_ASSIGN_OR_RAISE(int64_t n, input->Read(data + read, size - read, offset + read));
        if (n <= 0) {
            return Status::Invalid("truncated video file");
        }
        read += n;
    }
    return Status::OK();
}

}  // namespace

VideoFileBatchReader::VideoFileBatchReader(std::string uri, int32_t batch_size,
                                           std::shared_ptr<MemoryPool> pool,
                                           bool emit_placeholder_sentinel,
                                           const std::shared_ptr<BlobReadContext>& blob_context)
    : uri_(std::move(uri)),
      batch_size_(batch_size),
      pool_(std::move(pool)),
      arrow_pool_(GetArrowPool(pool_)),
      metrics_(std::make_shared<MetricsImpl>()),
      emit_placeholder_sentinel_(emit_placeholder_sentinel),
      blob_context_(blob_context) {}

Result<std::unique_ptr<VideoFileBatchReader>> VideoFileBatchReader::Create(
    const std::shared_ptr<InputStream>& input, int32_t batch_size,
    const std::shared_ptr<MemoryPool>& pool, bool emit_placeholder_sentinel,
    const std::shared_ptr<BlobReadContext>& blob_context) {
    if (!input || !pool || batch_size <= 0) {
        return Status::Invalid("invalid video reader input, pool, or batch size");
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t size, input->Length());
    if (size < kFooterSize) {
        return Status::Invalid("video file is smaller than its footer", kLegacyVideoHint);
    }
    char footer[kFooterSize];
    PAIMON_RETURN_NOT_OK(ReadAtFully(input.get(), size - kFooterSize, footer, sizeof(footer)));
    DataInputStream footer_input(std::make_shared<ByteArrayInputStream>(footer, sizeof(footer)));
    footer_input.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    std::array<int32_t, 6> index_lengths;
    int64_t index_start = size - kFooterSize;
    uint64_t decoded_index_bytes = 0;
    uint64_t decode_peak_bytes = 0;
    for (int32_t& index_length : index_lengths) {
        PAIMON_ASSIGN_OR_RAISE(index_length, footer_input.ReadValue<int32_t>());
        if (index_length < 0 || index_length > index_start) {
            return Status::Invalid("video indexes exceed the file size", kLegacyVideoHint);
        }
        index_start -= index_length;
        // A varint can be one byte. The decoder holds both deltas and decoded values.
        const uint64_t decoded_bytes = static_cast<uint64_t>(index_length) * sizeof(int64_t);
        decode_peak_bytes =
            std::max(decode_peak_bytes, decoded_index_bytes + index_length + 2 * decoded_bytes);
        decoded_index_bytes += decoded_bytes;
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t magic, footer_input.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int8_t version, footer_input.ReadValue<int8_t>());
    if (magic != kMagic || version != kVersion) {
        return Status::Invalid("invalid or unsupported video file footer");
    }
    // Each group has equal entry counts, bounded by its shortest encoded index.
    const uint64_t video_count = std::min({index_lengths[0], index_lengths[1], index_lengths[2]});
    const uint64_t run_count = std::min({index_lengths[3], index_lengths[4], index_lengths[5]});
    const uint64_t build_peak_bytes =
        decoded_index_bytes + video_count * sizeof(VideoRange) + run_count * sizeof(FrameRun);
    if (std::max(decode_peak_bytes, build_peak_bytes) > kMaxIndexMemoryBytes) {
        return Status::CapacityError("video indexes exceed the memory budget of ",
                                     kMaxIndexMemoryBytes, " bytes");
    }
    std::array<std::vector<int64_t>, 6> indexes;
    int64_t index_offset = index_start;
    for (size_t i = 0; i < indexes.size(); ++i) {
        std::vector<char> bytes(index_lengths[i]);
        PAIMON_RETURN_NOT_OK(ReadAtFully(input.get(), index_offset, bytes.data(), bytes.size()));
        PAIMON_ASSIGN_OR_RAISE(indexes[i], DeltaVarintCompressor::Decompress(bytes));
        index_offset += index_lengths[i];
    }

    const auto& video_lengths = indexes[0];
    const auto& moov_offsets = indexes[1];
    const auto& moov_lengths = indexes[2];
    const auto& run_lengths = indexes[3];
    const auto& references = indexes[4];
    const auto& first_frames = indexes[5];
    if (video_lengths.size() != moov_offsets.size() ||
        video_lengths.size() != moov_lengths.size() || run_lengths.size() != references.size() ||
        run_lengths.size() != first_frames.size()) {
        return Status::Invalid("video indexes have inconsistent entry counts", kLegacyVideoHint);
    }

    PAIMON_ASSIGN_OR_RAISE(std::string uri, input->GetUri());
    auto reader = std::unique_ptr<VideoFileBatchReader>(new VideoFileBatchReader(
        std::move(uri), batch_size, pool, emit_placeholder_sentinel, blob_context));
    int64_t payload_offset = 0;
    // Both layouts use v1. For a five-index file, the moov indexes below would actually
    // contain the old video and keyframe lengths. Their sum covers the old payload region,
    // which ends at least four bytes after index_start. The range and exact coverage checks
    // together therefore reject the old layout even if its footer parses.
    reader->videos_.reserve(video_lengths.size());
    for (size_t i = 0; i < video_lengths.size(); ++i) {
        const int64_t length = video_lengths[i];
        if (length <= 0 || length > index_start - payload_offset) {
            return Status::Invalid("invalid video payload length", kLegacyVideoHint);
        }
        const int64_t moov_offset = moov_offsets[i];
        const int64_t moov_length = moov_lengths[i];
        if (moov_offset < 0 || moov_length < 0 || moov_offset > length ||
            moov_length > length - moov_offset || (moov_length == 0 && moov_offset != 0)) {
            return Status::Invalid("invalid moov range in video payload", kLegacyVideoHint);
        }
        reader->videos_.push_back({payload_offset, length, moov_offset, moov_length});
        payload_offset += length;
    }
    if (payload_offset != index_start) {
        return Status::Invalid("video payload length does not match its index", kLegacyVideoHint);
    }

    int64_t rows = 0;
    reader->runs_.reserve(run_lengths.size());
    for (size_t i = 0; i < run_lengths.size(); ++i) {
        const int64_t length = run_lengths[i];
        const int64_t ref = references[i];
        const int64_t first = first_frames[i];
        if (length <= 0 || length > std::numeric_limits<int32_t>::max() - rows ||
            (ref != kNullReference && ref != kPlaceholderReference &&
             (ref < 0 || static_cast<uint64_t>(ref) >= video_lengths.size())) ||
            (ref >= 0 &&
             (first < 0 || first > std::numeric_limits<int64_t>::max() - (length - 1)))) {
            return Status::Invalid("invalid video frame run");
        }
        rows += length;
        reader->runs_.push_back({static_cast<uint64_t>(rows), ref, first});
    }
    reader->row_count_ = rows;
    return reader;
}

Result<std::unique_ptr<::ArrowSchema>> VideoFileBatchReader::GetFileSchema() const {
    return Status::NotImplemented("video files have no self-describing schema");
}

Status VideoFileBatchReader::SetReadSchema(::ArrowSchema* schema, const std::shared_ptr<Predicate>&,
                                           const std::optional<RoaringBitmap32>& selection) {
    if (!schema) {
        return Status::Invalid("video read schema is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(schema));
    if (arrow_schema->num_fields() != 1 || !BlobUtils::IsBlobField(arrow_schema->field(0))) {
        return Status::Invalid("video reader requires one scalar BLOB field");
    }
    // Create() bounds row_count_ by INT32_MAX. Seek once to check the entire upper range,
    // including bitmap entries above INT32_MAX, which appear negative when dereferenced.
    if (selection &&
        selection->EqualOrLarger(static_cast<int32_t>(row_count_)) != selection->End()) {
        return Status::Invalid("video selection row exceeds row count");
    }

    selection_ = selection;
    read_row_count_ = selection ? selection->Cardinality() : row_count_;
    previous_selected_rows_.clear();
    field_ = arrow_schema->field(0);
    current_ = 0;
    previous_batch_size_ = 0;
    return Status::OK();
}

Result<uint64_t> VideoFileBatchReader::GetPreviousBatchFileRowId(uint64_t batch_row_id) const {
    if (batch_row_id >= previous_batch_size_) {
        return Status::Invalid("video batch row ID is out of range");
    }
    return selection_ ? previous_selected_rows_[batch_row_id]
                      : current_ - previous_batch_size_ + batch_row_id;
}

Result<uint64_t> VideoFileBatchReader::GetNumberOfRows() const {
    return row_count_;
}

std::shared_ptr<Metrics> VideoFileBatchReader::GetReaderMetrics() const {
    return metrics_;
}

void VideoFileBatchReader::Close() {
    closed_ = true;
}

Result<BatchReader::ReadBatch> VideoFileBatchReader::NextBatch() {
    if (closed_ || !field_) {
        return Status::Invalid("video reader is closed or read schema is unset");
    }
    if (current_ >= read_row_count_) {
        previous_batch_size_ = 0;
        return BatchReader::MakeEofBatch();
    }
    const BlobBatchRequest* request =
        blob_context_ && blob_context_->request ? &blob_context_->request.value() : nullptr;
    if (request && request->max_rows <= 0) {
        return Status::Invalid("video read batch row limit must be positive");
    }
    const uint64_t batch_limit = request ? std::min(batch_size_, request->max_rows) : batch_size_;
    const uint64_t count = std::min(batch_limit, read_row_count_ - current_);
    std::vector<int32_t> selected_rows;
    if (selection_) {
        const int32_t start =
            previous_selected_rows_.empty() ? 0 : previous_selected_rows_.back() + 1;
        auto selected = selection_->EqualOrLarger(start);
        selected_rows.reserve(count);
        for (uint64_t i = 0; i < count; ++i, ++selected) {
            selected_rows.push_back(*selected);
        }
    }
    arrow::LargeBinaryBuilder builder(arrow_pool_.get());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(count));
    for (uint64_t i = 0; i < count; ++i) {
        if (request && !request->read_payload) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
            continue;
        }
        const uint64_t row = selection_ ? selected_rows[i] : current_ + i;
        const auto run =
            std::upper_bound(runs_.begin(), runs_.end(), row,
                             [](uint64_t row, const FrameRun& run) { return row < run.end; });
        if (run->reference == kNullReference) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
        } else if (run->reference == kPlaceholderReference) {
            if (!emit_placeholder_sentinel_) {
                return Status::Invalid("video placeholder requires the blob fallback read path");
            }
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(BlobDefs::PlaceholderSentinelView()));
        } else {
            const auto& video = videos_[run->reference];
            const uint64_t run_start = run == runs_.begin() ? 0 : (run - 1)->end;
            const int64_t frame = run->first_frame + static_cast<int64_t>(row - run_start);
            PAIMON_ASSIGN_OR_RAISE(
                std::unique_ptr<VideoFrameDescriptor> descriptor,
                VideoFrameDescriptor::Create(uri_, video.offset, video.length, frame,
                                             video.moov_offset, video.moov_length));
            auto bytes = descriptor->Serialize(pool_);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(bytes->data(), bytes->size()));
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> values, builder.Finish());
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> array,
                                      arrow::StructArray::Make({values}, {field_}));
    auto c_array = std::make_unique<::ArrowArray>();
    auto c_schema = std::make_unique<::ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
    PAIMON_RETURN_NOT_OK(AddArrowArrayLifetime(c_array.get(), c_schema.get(), arrow_pool_));
    current_ += count;
    previous_batch_size_ = count;
    previous_selected_rows_ = std::move(selected_rows);
    return std::make_pair(std::move(c_array), std::move(c_schema));
}

}  // namespace paimon::blob
