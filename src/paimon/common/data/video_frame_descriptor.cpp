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

#include "paimon/common/data/video_frame_descriptor.h"

#include <algorithm>
#include <limits>
#include <string_view>
#include <utility>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"

namespace paimon {
namespace {

constexpr int8_t kVersion = 1;
constexpr std::string_view kMagic = "MRFOEDIV";  // 0x564944454F46524D in little endian
constexpr uint64_t kHeaderSize = sizeof(kVersion) + kMagic.size();
constexpr uint64_t kFixedSize = kHeaderSize + sizeof(int32_t) + 5 * sizeof(int64_t);

}  // namespace

VideoFrameDescriptor::VideoFrameDescriptor(std::unique_ptr<BlobDescriptor> payload,
                                           int64_t frame_index, int64_t moov_offset,
                                           int64_t moov_length)
    : payload_(std::move(payload)),
      frame_index_(frame_index),
      moov_offset_(moov_offset),
      moov_length_(moov_length) {}

Result<std::unique_ptr<VideoFrameDescriptor>> VideoFrameDescriptor::Create(
    const std::string& uri, int64_t offset, int64_t length, int64_t frame_index,
    int64_t moov_offset, int64_t moov_length) {
    if (frame_index < 0 || moov_offset < 0 || moov_length < 0 ||
        uri.size() > std::numeric_limits<int32_t>::max() ||
        (moov_length == 0 ? moov_offset != 0
                          : moov_offset > length || moov_length > length - moov_offset)) {
        return Status::Invalid("invalid video frame descriptor range");
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BlobDescriptor> payload,
                           BlobDescriptor::Create(uri, offset, length));
    return std::unique_ptr<VideoFrameDescriptor>(
        new VideoFrameDescriptor(std::move(payload), frame_index, moov_offset, moov_length));
}

bool VideoFrameDescriptor::HasMagic(const char* buffer, uint64_t size) {
    return buffer != nullptr && size >= kHeaderSize &&
           std::string_view(buffer + sizeof(kVersion), kMagic.size()) == kMagic;
}

Result<std::unique_ptr<VideoFrameDescriptor>> VideoFrameDescriptor::Deserialize(const char* buffer,
                                                                                uint64_t size) {
    if (size < kFixedSize || !HasMagic(buffer, size) || buffer[0] != kVersion) {
        return Status::Invalid("invalid or unsupported video frame descriptor header");
    }
    DataInputStream input(std::make_shared<ByteArrayInputStream>(buffer, size));
    input.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    PAIMON_RETURN_NOT_OK(input.Seek(kHeaderSize));
    PAIMON_ASSIGN_OR_RAISE(int32_t uri_length, input.ReadValue<int32_t>());
    if (uri_length < 0 || static_cast<uint64_t>(uri_length) != size - kFixedSize) {
        return Status::Invalid("invalid video frame descriptor length");
    }
    std::string uri(uri_length, '\0');
    PAIMON_RETURN_NOT_OK(input.Read(uri.data(), uri_length));
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, input.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t length, input.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t frame_index, input.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t moov_offset, input.ReadValue<int64_t>());
    PAIMON_ASSIGN_OR_RAISE(int64_t moov_length, input.ReadValue<int64_t>());
    return Create(uri, offset, length, frame_index, moov_offset, moov_length);
}

PAIMON_UNIQUE_PTR<Bytes> VideoFrameDescriptor::Serialize(
    const std::shared_ptr<MemoryPool>& pool) const {
    const std::string& uri = payload_->Uri();
    const int32_t segment_size = std::min<uint64_t>(
        kFixedSize + uri.size(), MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE);
    MemorySegmentOutputStream output(segment_size, pool);
    output.SetOrder(ByteOrder::PAIMON_LITTLE_ENDIAN);
    output.WriteValue<int8_t>(kVersion);
    output.Write(kMagic.data(), kMagic.size());
    output.WriteValue<int32_t>(uri.size());
    output.Write(uri.data(), uri.size());
    output.WriteValue<int64_t>(payload_->Offset());
    output.WriteValue<int64_t>(payload_->Length());
    output.WriteValue<int64_t>(frame_index_);
    output.WriteValue<int64_t>(moov_offset_);
    output.WriteValue<int64_t>(moov_length_);
    return MemorySegmentUtils::CopyToBytes(output.Segments(), 0, output.CurrentSize(), pool.get());
}

}  // namespace paimon
