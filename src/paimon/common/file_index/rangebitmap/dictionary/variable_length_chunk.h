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

#include <memory>

#include "paimon/common/file_index/rangebitmap/dictionary/chunk.h"
#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"
#include "paimon/fs/file_system.h"

namespace paimon {

class DataInputStream;
class InputStream;
class KeyFactory;
class MemoryPool;
class MemorySegmentOutputStream;

class VariableLengthChunk final : public Chunk {
 public:
    Result<bool> TryAdd(const Literal& key) override;
    Result<int32_t> CompareKey(const Literal& lhs, const Literal& rhs) override;
    Result<Literal> GetKey(int32_t index) override;

    const Literal& Key() const override {
        return key_;
    }
    int32_t Code() const override {
        return code_;
    }
    int32_t Offset() const override {
        return offset_;
    }
    void SetOffset(int32_t offset) override {
        offset_ = offset;
    }
    int32_t Size() const override {
        return size_;
    }

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeChunk() const override;
    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializeKeys() const override;

    // For Read Path
    VariableLengthChunk(Literal key, int32_t code, int32_t offset, int32_t size,
                        const std::shared_ptr<KeyFactory>& factory,
                        const std::shared_ptr<InputStream>& input_stream, int32_t keys_base_offset,
                        int32_t offsets_length, int32_t keys_length,
                        const std::shared_ptr<MemoryPool>& pool);

    // For Write Path
    VariableLengthChunk(Literal key, int32_t code, int32_t keys_length_limit,
                        const std::shared_ptr<KeyFactory>& factory,
                        const LiteralSerDeUtils::Serializer& serializer,
                        const std::shared_ptr<MemoryPool>& pool);

 public:
    static constexpr int8_t kCurrentVersion = 1;

 private:
    Status LoadKeys();

    std::shared_ptr<MemoryPool> pool_;
    Literal key_;     // representative key for binary search
    int32_t code_;    // first code in this chunk
    int32_t offset_;  // offset of this chunk
    int32_t size_;    // number of keys in this chunk
    std::shared_ptr<KeyFactory> factory_;

    // For read path lazy keys loading
    std::shared_ptr<InputStream> input_stream_;
    int32_t keys_base_offset_;
    int32_t offsets_length_;
    int32_t keys_length_;
    PAIMON_UNIQUE_PTR<Bytes> offsets_;
    PAIMON_UNIQUE_PTR<Bytes> keys_;
    LiteralSerDeUtils::Deserializer deserializer_;

    // For write path
    LiteralSerDeUtils::Serializer serializer_;
    std::shared_ptr<MemorySegmentOutputStream> offsets_stream_out_;
    std::shared_ptr<MemorySegmentOutputStream> keys_stream_out_;
    int32_t remaining_offsets_size_;
    int32_t remaining_keys_size_;
};

}  // namespace paimon
