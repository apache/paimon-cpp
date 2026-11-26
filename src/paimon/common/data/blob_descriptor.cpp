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

#include "paimon/data/blob_descriptor.h"

#include <limits>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/byte_order.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
namespace {

constexpr int64_t kMagic = 0x424C4F4244455343l;
constexpr int64_t kVideoFrameMagic = 0x564944454F46524D;
constexpr int8_t kVideoFrameVersion = 1;
// One byte for version, eight bytes for magic number.
constexpr uint64_t kMinDescriptorLength = 9;

}  // namespace

Result<std::unique_ptr<BlobDescriptor>> BlobDescriptor::Create(const std::string& uri,
                                                               int64_t offset, int64_t length) {
    return Create(kCurrentVersion, uri, offset, length);
}

Result<std::unique_ptr<BlobDescriptor>> BlobDescriptor::Create(int8_t version,
                                                               const std::string& uri,
                                                               int64_t offset, int64_t length) {
    if (offset < 0) {
        return Status::Invalid(fmt::format("offset {} is less than 0", offset));
    }
    // length == -1 means it's dynamic length
    if (length < -1) {
        return Status::Invalid(fmt::format("length {} is less than -1", length));
    }
    return std::unique_ptr<BlobDescriptor>(new BlobDescriptor(version, uri, offset, length));
}

Result<std::unique_ptr<BlobDescriptor>> BlobDescriptor::CreateVideoFrame(
    const std::string& uri, int64_t offset, int64_t length, int64_t frame_index,
    int64_t moov_offset, int64_t moov_length) {
    if (frame_index < 0 || moov_offset < 0 || moov_length < 0 ||
        uri.size() > std::numeric_limits<int32_t>::max() ||
        (moov_length == 0 ? moov_offset != 0
                          : moov_offset > length || moov_length > length - moov_offset)) {
        return Status::Invalid("invalid video frame descriptor range");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobDescriptor> descriptor,
                           Create(kVideoFrameVersion, uri, offset, length));
    descriptor->frame_index_ = frame_index;
    descriptor->moov_offset_ = moov_offset;
    descriptor->moov_length_ = moov_length;
    return descriptor;
}

PAIMON_UNIQUE_PTR<Bytes> BlobDescriptor::Serialize(const std::shared_ptr<MemoryPool>& pool) const {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    out.WriteValue<int8_t>(IsVideoFrame() ? kVideoFrameVersion : kCurrentVersion);
    out.WriteValue<int64_t>(IsVideoFrame() ? kVideoFrameMagic : kMagic);
    out.WriteValue<int32_t>(static_cast<int32_t>(uri_.size()));

    auto uri_bytes = std::make_shared<Bytes>(uri_, pool.get());
    out.WriteBytes(uri_bytes);
    out.WriteValue<int64_t>(offset_);
    out.WriteValue<int64_t>(length_);
    if (IsVideoFrame()) {
        out.WriteValue<int64_t>(frame_index_);
        out.WriteValue<int64_t>(moov_offset_);
        out.WriteValue<int64_t>(moov_length_);
    }
    return MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
}

Result<std::unique_ptr<BlobDescriptor>> BlobDescriptor::Deserialize(const char* buffer,
                                                                    uint64_t size) {
    if (buffer == nullptr || size == 0) {
        return Status::Invalid("empty blob descriptor");
    }
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, size);
    DataInputStream in(std::move(input_stream));
    in.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    PAIMON_ASSIGN_OR_RAISE(int8_t version, in.ReadValue<int8_t>());
    if (version > kCurrentVersion) {
        return Status::Invalid(fmt::format(
            "Expecting BlobDescriptor version to be less than or equal to {}, but found {}.",
            kCurrentVersion, version));
    }
    bool video_frame = false;
    if (version == kVideoFrameVersion && size >= kMinDescriptorLength) {
        PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
        video_frame = magic == kVideoFrameMagic;
        if (!video_frame) {
            PAIMON_RETURN_NOT_OK(in.Seek(1));
        }
    } else if (version > 1) {
        PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
        if (kMagic != magic) {
            return Status::Invalid(fmt::format(
                "Invalid BlobDescriptor: missing magic header. Expected magic: {}, but found {}",
                kMagic, magic));
        }
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t uri_length, in.ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t uri_start, in.GetPos());
    const uint64_t fixed_tail_size = (video_frame ? 5 : 2) * sizeof(int64_t);
    const uint64_t remaining_size = size - uri_start;
    if (uri_length < 0 || remaining_size < fixed_tail_size ||
        static_cast<uint64_t>(uri_length) > remaining_size - fixed_tail_size ||
        (video_frame && static_cast<uint64_t>(uri_length) != remaining_size - fixed_tail_size)) {
        return Status::Invalid("invalid blob descriptor length");
    }
    std::string uri(uri_length, '\0');
    PAIMON_RETURN_NOT_OK(in.Read(uri.data(), uri.size()));
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, in.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t length, in.ReadValue<int64_t>());
    if (video_frame) {
        PAIMON_ASSIGN_OR_RAISE(int64_t frame_index, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t moov_offset, in.ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t moov_length, in.ReadValue<int64_t>());
        return CreateVideoFrame(uri, offset, length, frame_index, moov_offset, moov_length);
    }
    return BlobDescriptor::Create(version, uri, offset, length);
}

Result<bool> BlobDescriptor::IsBlobDescriptor(const char* buffer, uint64_t size) {
    if (buffer == nullptr || size < kMinDescriptorLength) {
        return false;
    }
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, size);
    DataInputStream in(std::move(input_stream));
    in.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    PAIMON_ASSIGN_OR_RAISE(int8_t version, in.ReadValue<int8_t>());
    if (version > kCurrentVersion) {
        return false;
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t magic, in.ReadValue<int64_t>());
    return kMagic == magic || (version == kVideoFrameVersion && kVideoFrameMagic == magic);
}

std::string BlobDescriptor::ToString() const {
    return fmt::format("BlobDescriptor{{version={}, uri='{}', offset={}, length={}}}", version_,
                       uri_, offset_, length_);
}

}  // namespace paimon
