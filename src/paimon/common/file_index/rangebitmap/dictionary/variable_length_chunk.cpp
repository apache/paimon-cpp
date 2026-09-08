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

#include "paimon/common/file_index/rangebitmap/dictionary/variable_length_chunk.h"

#include <limits>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {

Result<bool> VariableLengthChunk::TryAdd(const Literal& key) {
    PAIMON_ASSIGN_OR_RAISE(int32_t key_length, LiteralSerDeUtils::GetSerializedSizeInBytes(key));
    if (key_length > remaining_keys_size_ ||
        static_cast<int32_t>(sizeof(int32_t)) > remaining_offsets_size_) {
        return false;
    }
    offsets_stream_out_->WriteValue<int32_t>(static_cast<int32_t>(keys_stream_out_->CurrentSize()));
    PAIMON_RETURN_NOT_OK(serializer_(keys_stream_out_, key));
    remaining_offsets_size_ -= sizeof(int32_t);
    remaining_keys_size_ -= key_length;
    ++size_;
    return true;
}

Result<int32_t> VariableLengthChunk::CompareKey(const Literal& lhs, const Literal& rhs) {
    return factory_->CompareLiteral(lhs, rhs);
}

Status VariableLengthChunk::LoadKeys() {
    if (offsets_ != nullptr && keys_ != nullptr) {
        return Status::OK();
    }
    if (offsets_length_ < 0 || keys_length_ < 0 ||
        offsets_length_ > std::numeric_limits<int32_t>::max() - keys_length_) {
        return Status::Invalid("Invalid variable length chunk payload length");
    }
    PAIMON_RETURN_NOT_OK(input_stream_->Seek(keys_base_offset_ + offset_, FS_SEEK_SET));
    offsets_ = Bytes::AllocateBytes(offsets_length_, pool_.get());
    PAIMON_ASSIGN_OR_RAISE(int64_t offsets_read,
                           input_stream_->Read(offsets_->data(), offsets_length_));
    if (offsets_read != offsets_length_) {
        return Status::Invalid(fmt::format(
            "Failed to read variable length chunk offsets, expected {} bytes but got {}",
            offsets_length_, offsets_read));
    }
    keys_ = Bytes::AllocateBytes(keys_length_, pool_.get());
    PAIMON_ASSIGN_OR_RAISE(int64_t keys_read, input_stream_->Read(keys_->data(), keys_length_));
    if (keys_read != keys_length_) {
        return Status::Invalid(
            fmt::format("Failed to read variable length chunk keys, expected {} bytes but got {}",
                        keys_length_, keys_read));
    }
    PAIMON_ASSIGN_OR_RAISE(deserializer_,
                           LiteralSerDeUtils::CreateValueReader(factory_->GetFieldType()));
    return Status::OK();
}

Result<Literal> VariableLengthChunk::GetKey(int32_t index) {
    if (index < 0 || index >= size_) {
        return Status::Invalid("Index out of bounds");
    }
    PAIMON_RETURN_NOT_OK(LoadKeys());
    auto offsets_in = std::make_shared<DataInputStream>(
        std::make_shared<ByteArrayInputStream>(offsets_->data(), offsets_->size()));
    PAIMON_RETURN_NOT_OK(offsets_in->Seek(static_cast<int64_t>(index) * sizeof(int32_t)));
    PAIMON_ASSIGN_OR_RAISE(int32_t key_offset, offsets_in->ReadValue<int32_t>());
    if (key_offset < 0 || key_offset >= keys_length_) {
        return Status::Invalid("Invalid key offset in variable length chunk");
    }
    auto keys_in = std::make_shared<DataInputStream>(
        std::make_shared<ByteArrayInputStream>(keys_->data(), keys_->size()));
    PAIMON_RETURN_NOT_OK(keys_in->Seek(key_offset));
    return deserializer_(keys_in, pool_.get());
}

Result<PAIMON_UNIQUE_PTR<Bytes>> VariableLengthChunk::SerializeChunk() const {
    const auto data_out = std::make_shared<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    data_out->WriteValue<int8_t>(kCurrentVersion);
    PAIMON_RETURN_NOT_OK(serializer_(data_out, key_));
    data_out->WriteValue<int32_t>(code_);
    data_out->WriteValue<int32_t>(offset_);
    data_out->WriteValue<int32_t>(size_);
    data_out->WriteValue<int32_t>(static_cast<int32_t>(offsets_stream_out_->CurrentSize()));
    data_out->WriteValue<int32_t>(static_cast<int32_t>(keys_stream_out_->CurrentSize()));
    return MemorySegmentUtils::CopyToBytes(
        data_out->Segments(), 0, static_cast<int32_t>(data_out->CurrentSize()), pool_.get());
}

Result<PAIMON_UNIQUE_PTR<Bytes>> VariableLengthChunk::SerializeKeys() const {
    const auto data_out = std::make_shared<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    PAIMON_RETURN_NOT_OK(MemorySegmentUtils::CopyToStream(
        offsets_stream_out_->Segments(), 0,
        static_cast<int32_t>(offsets_stream_out_->CurrentSize()), data_out.get()));
    PAIMON_RETURN_NOT_OK(MemorySegmentUtils::CopyToStream(
        keys_stream_out_->Segments(), 0, static_cast<int32_t>(keys_stream_out_->CurrentSize()),
        data_out.get()));
    return MemorySegmentUtils::CopyToBytes(
        data_out->Segments(), 0, static_cast<int32_t>(data_out->CurrentSize()), pool_.get());
}

/// Read path
VariableLengthChunk::VariableLengthChunk(Literal key, int32_t code, int32_t offset, int32_t size,
                                         const std::shared_ptr<KeyFactory>& factory,
                                         const std::shared_ptr<InputStream>& input_stream,
                                         int32_t keys_base_offset, int32_t offsets_length,
                                         int32_t keys_length,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      key_(std::move(key)),
      code_(code),
      offset_(offset),
      size_(size),
      factory_(factory),
      input_stream_(input_stream),
      keys_base_offset_(keys_base_offset),
      offsets_length_(offsets_length),
      keys_length_(keys_length),
      deserializer_({}),
      serializer_({}),
      remaining_offsets_size_(0),
      remaining_keys_size_(0) {}

/// Write path
VariableLengthChunk::VariableLengthChunk(Literal key, int32_t code, int32_t keys_length_limit,
                                         const std::shared_ptr<KeyFactory>& factory,
                                         const LiteralSerDeUtils::Serializer& serializer,
                                         const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      key_(std::move(key)),
      code_(code),
      offset_(0),
      size_(0),
      factory_(factory),
      keys_base_offset_(0),
      offsets_length_(0),
      keys_length_(0),
      deserializer_({}),
      serializer_(serializer),
      offsets_stream_out_(std::make_shared<MemorySegmentOutputStream>(
          MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool)),
      keys_stream_out_(std::make_shared<MemorySegmentOutputStream>(
          MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool)),
      remaining_offsets_size_(keys_length_limit),
      remaining_keys_size_(keys_length_limit) {}

}  // namespace paimon
