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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/data/blob_descriptor.h"
#include "paimon/visibility.h"

namespace paimon {

/// Wire representation of a frame in an encoded video. The moov range is relative to its payload.
class PAIMON_EXPORT VideoFrameDescriptor {
 public:
    static Result<std::unique_ptr<VideoFrameDescriptor>> Create(const std::string& uri,
                                                                int64_t offset, int64_t length,
                                                                int64_t frame_index,
                                                                int64_t moov_offset,
                                                                int64_t moov_length);

    static bool HasMagic(const char* buffer, uint64_t size);
    static Result<std::unique_ptr<VideoFrameDescriptor>> Deserialize(const char* buffer,
                                                                     uint64_t size);

    PAIMON_UNIQUE_PTR<Bytes> Serialize(const std::shared_ptr<MemoryPool>& pool) const;

    const BlobDescriptor& Payload() const {
        return *payload_;
    }

    int64_t FrameIndex() const {
        return frame_index_;
    }

    int64_t MoovOffset() const {
        return moov_offset_;
    }

    int64_t MoovLength() const {
        return moov_length_;
    }

 private:
    VideoFrameDescriptor(std::unique_ptr<BlobDescriptor> payload, int64_t frame_index,
                         int64_t moov_offset, int64_t moov_length);

    std::unique_ptr<BlobDescriptor> payload_;
    int64_t frame_index_;
    int64_t moov_offset_;
    int64_t moov_length_;
};

}  // namespace paimon
