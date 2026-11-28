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
constexpr int64_t kLegacyFooterSize = 25;
constexpr int32_t kMagic = 0x4F454449;
constexpr int8_t kVersion = 1;
constexpr uint64_t kMaxIndexMemoryBytes = 64 * 1024 * 1024;
constexpr int64_t kNullReference = -1;
constexpr int64_t kPlaceholderReference = -2;

using IndexLengths = std::array<int32_t, 6>;

struct IndexLayout {
    int64_t start;
    uint64_t decoded_bytes = 0;
    uint64_t decode_peak_bytes = 0;
};

Status ReadAtFully(InputStream* input, int64_t offset, char* data, int64_t size) {
    for (int64_t read = 0; read < size;) {
        const Result<int64_t> n = input->Read(data + read, size - read, offset + read);
        // An I/O failure leaves the layout undetermined.
        if (!n.ok()) {
            return n.status().IsInvalid()
                       ? Status::IOError("failed to read video file. ", n.status().ToString())
                       : n.status();
        }
        if (n.value() <= 0) {
            return Status::IOError("truncated video file");
        }
        read += n.value();
    }
    return Status::OK();
}

Result<IndexLayout> PlanIndexes(const IndexLengths& lengths, int64_t footer_start) {
    if (footer_start < 0) {
        return Status::Invalid("video file is smaller than its footer");
    }
    IndexLayout layout{footer_start};
    for (int32_t length : lengths) {
        if (length < 0 || length > layout.start) {
            return Status::Invalid("video indexes exceed the file size");
        }
        layout.start -= length;
        // A varint can be one byte. The decoder holds both deltas and decoded values.
        const uint64_t decoded_bytes = static_cast<uint64_t>(length) * sizeof(int64_t);
        layout.decode_peak_bytes =
            std::max(layout.decode_peak_bytes, layout.decoded_bytes + length + 2 * decoded_bytes);
        layout.decoded_bytes += decoded_bytes;
    }
    return layout;
}

Status CheckIndexBudget(const IndexLayout& layout, uint64_t materialized_bytes) {
    if (std::max(layout.decode_peak_bytes, layout.decoded_bytes + materialized_bytes) >
        kMaxIndexMemoryBytes) {
        return Status::CapacityError("video indexes exceed the memory budget of ",
                                     kMaxIndexMemoryBytes, " bytes");
    }
    return Status::OK();
}

Result<std::vector<int64_t>> ReadIndex(InputStream* input, int64_t* offset, int32_t length) {
    std::vector<char> bytes(length);
    PAIMON_RETURN_NOT_OK(ReadAtFully(input, *offset, bytes.data(), length));
    *offset += length;
    return DeltaVarintCompressor::Decompress(bytes);
}

bool IsValidFrameRun(int64_t length, int64_t reference, int64_t first, size_t video_count,
                     int64_t rows) {
    return length > 0 && length <= std::numeric_limits<int32_t>::max() - rows &&
           (reference == kNullReference || reference == kPlaceholderReference ||
            (reference >= 0 && static_cast<uint64_t>(reference) < video_count && first >= 0));
}

Status ValidateLegacyLayout(InputStream* input, const IndexLengths& lengths, int64_t index_start) {
    // Before apache/paimon#10905, the payload area contained videos followed by keyframe
    // indexes. The footer had two physical indexes and three run indexes.
    int64_t offset = index_start;
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> videos, ReadIndex(input, &offset, lengths[1]));
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> keyframes, ReadIndex(input, &offset, lengths[2]));
    if (videos.size() != keyframes.size()) {
        return Status::Invalid("legacy video and keyframe indexes have different counts");
    }
    int64_t remaining = index_start;
    for (size_t i = 0; i < videos.size(); ++i) {
        if (videos[i] <= 0 || videos[i] > remaining || keyframes[i] < 0 ||
            keyframes[i] > remaining - videos[i]) {
            return Status::Invalid("invalid legacy video or keyframe length");
        }
        remaining -= videos[i] + keyframes[i];
    }
    if (remaining != 0) {
        return Status::Invalid("legacy payload length does not match its indexes");
    }

    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> runs, ReadIndex(input, &offset, lengths[3]));
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> references, ReadIndex(input, &offset, lengths[4]));
    PAIMON_ASSIGN_OR_RAISE(std::vector<int64_t> first_frames,
                           ReadIndex(input, &offset, lengths[5]));
    if (runs.size() != references.size() || runs.size() != first_frames.size()) {
        return Status::Invalid("legacy video run indexes have different counts");
    }
    int64_t rows = 0;
    for (size_t i = 0; i < runs.size(); ++i) {
        if (!IsValidFrameRun(runs[i], references[i], first_frames[i], videos.size(), rows)) {
            return Status::Invalid("invalid legacy video frame run");
        }
        rows += runs[i];
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
    if (size < kLegacyFooterSize) {
        return Status::Invalid(
            "corrupt or unsupported .video layout. File is smaller than its footer");
    }
    // Both layouts end with the same five lengths, magic and version. The index data
    // for those lengths starts four bytes earlier in the six-index interpretation.
    char footer[kFooterSize] = {};
    const int64_t footer_size = std::min(size, kFooterSize);
    PAIMON_RETURN_NOT_OK(ReadAtFully(input.get(), size - footer_size,
                                     footer + kFooterSize - footer_size, footer_size));
    DataInputStream footer_input(std::make_shared<ByteArrayInputStream>(footer, sizeof(footer)));
    footer_input.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    IndexLengths index_lengths;
    for (int32_t& index_length : index_lengths) {
        PAIMON_ASSIGN_OR_RAISE(index_length, footer_input.ReadValue<int32_t>());
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t magic, footer_input.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int8_t version, footer_input.ReadValue<int8_t>());
    if (magic != kMagic || version != kVersion) {
        return Status::Invalid("corrupt or unsupported .video footer");
    }

    // Check both budgets before reading index data. The legacy probe frees its decoded
    // indexes before the six-index reader allocates its own.
    const Result<IndexLayout> modern_layout = PlanIndexes(index_lengths, size - kFooterSize);
    if (modern_layout.ok()) {
        // Each group has equal entry counts, bounded by its shortest encoded index.
        const uint64_t videos = std::min({index_lengths[0], index_lengths[1], index_lengths[2]});
        const uint64_t runs = std::min({index_lengths[3], index_lengths[4], index_lengths[5]});
        PAIMON_RETURN_NOT_OK(CheckIndexBudget(
            modern_layout.value(), videos * sizeof(VideoRange) + runs * sizeof(FrameRun)));
    }
    IndexLengths legacy_lengths = index_lengths;
    legacy_lengths[0] = 0;
    const Result<IndexLayout> legacy_layout = PlanIndexes(legacy_lengths, size - kLegacyFooterSize);
    Status legacy_status = legacy_layout.status();
    if (legacy_layout.ok()) {
        PAIMON_RETURN_NOT_OK(CheckIndexBudget(legacy_layout.value(), 0));
        legacy_status =
            ValidateLegacyLayout(input.get(), legacy_lengths, legacy_layout.value().start);
        if (!legacy_status.ok() && !legacy_status.IsInvalid()) {
            return legacy_status;
        }
    }

    std::unique_ptr<VideoFileBatchReader> reader;
    Status modern_status = modern_layout.status();
    if (modern_layout.ok()) {
        PAIMON_ASSIGN_OR_RAISE(std::string uri, input->GetUri());
        reader.reset(new VideoFileBatchReader(std::move(uri), batch_size, pool,
                                              emit_placeholder_sentinel, blob_context));
        modern_status =
            reader->ReadSixIndexLayout(input.get(), index_lengths, modern_layout.value().start);
        if (!modern_status.ok() && !modern_status.IsInvalid()) {
            return modern_status;
        }
    }
    if (legacy_status.ok()) {
        if (modern_status.ok()) {
            return Status::Invalid(
                "ambiguous v1 .video layout. Both five-index and six-index "
                "validation passed. Reimport the file");
        }
        return Status::Invalid("Legacy five-index .video files require reimport");
    }
    if (!modern_status.ok()) {
        return Status::Invalid("corrupt or unsupported .video layout. ", modern_status.ToString());
    }
    return reader;
}

Status VideoFileBatchReader::ReadSixIndexLayout(InputStream* input,
                                                const IndexLengths& index_lengths,
                                                int64_t index_start) {
    std::array<std::vector<int64_t>, 6> indexes;
    int64_t index_offset = index_start;
    for (size_t i = 0; i < indexes.size(); ++i) {
        PAIMON_ASSIGN_OR_RAISE(indexes[i], ReadIndex(input, &index_offset, index_lengths[i]));
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
        return Status::Invalid("video indexes have inconsistent entry counts");
    }

    int64_t payload_offset = 0;
    videos_.reserve(video_lengths.size());
    for (size_t i = 0; i < video_lengths.size(); ++i) {
        const int64_t length = video_lengths[i];
        if (length <= 0 || length > index_start - payload_offset) {
            return Status::Invalid("invalid video payload length");
        }
        const int64_t moov_offset = moov_offsets[i];
        const int64_t moov_length = moov_lengths[i];
        if (moov_offset < 0 || moov_length < 0 || moov_offset > length ||
            moov_length > length - moov_offset || (moov_length == 0 && moov_offset != 0)) {
            return Status::Invalid("invalid moov range in video payload");
        }
        videos_.push_back({payload_offset, length, moov_offset, moov_length});
        payload_offset += length;
    }
    if (payload_offset != index_start) {
        return Status::Invalid("video payload length does not match its index");
    }

    int64_t rows = 0;
    runs_.reserve(run_lengths.size());
    for (size_t i = 0; i < run_lengths.size(); ++i) {
        const int64_t length = run_lengths[i];
        const int64_t ref = references[i];
        const int64_t first = first_frames[i];
        if (!IsValidFrameRun(length, ref, first, video_lengths.size(), rows) ||
            (ref >= 0 && first > std::numeric_limits<int64_t>::max() - (length - 1))) {
            return Status::Invalid("invalid video frame run");
        }
        rows += length;
        runs_.push_back({static_cast<uint64_t>(rows), ref, first});
    }
    row_count_ = rows;
    return Status::OK();
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
