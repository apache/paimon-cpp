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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/common/io/byte_array_output_stream.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <utility>
#include <vector>

#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

ByteArrayOutputStream::ByteArrayOutputStream(std::unique_ptr<MemorySegmentOutputStream>&& output)
    : output_(std::move(output)) {
    assert(output_);
}

Result<int64_t> ByteArrayOutputStream::Write(const char* buffer, int64_t size) {
    if (closed_) {
        return Status::Invalid("Byte array output stream is closed");
    }
    PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(size, "write length"));
    if (buffer == nullptr && size > 0) {
        return Status::Invalid("Write buffer must not be null when size is positive");
    }
    int64_t remaining = size;
    while (remaining > 0) {
        uint32_t to_write = static_cast<uint32_t>(std::min<int64_t>(
            remaining, static_cast<int64_t>(std::numeric_limits<uint32_t>::max())));
        output_->Write(buffer, to_write);
        buffer += to_write;
        remaining -= to_write;
    }
    return size;
}

Status ByteArrayOutputStream::Close() {
    closed_ = true;
    return Status::OK();
}

Result<std::shared_ptr<Bytes>> ByteArrayOutputStream::Finish(MemoryPool* pool) {
    assert(pool);
    PAIMON_RETURN_NOT_OK(Close());
    if (result_) {
        return result_;
    }
    // TODO(jinli.zjw): Support int64_t lengths in MemorySegmentUtils::CopyToBytes and remove this
    // limit.
    const int64_t size = output_->CurrentSize();
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(size, "byte array output stream size"));
    const std::vector<MemorySegment>& segments = output_->Segments();
    result_ = std::make_shared<Bytes>(static_cast<size_t>(size), pool);
    MemorySegmentUtils::CopyToBytes(segments, /*offset=*/0, result_.get(),
                                    /*bytes_offset=*/0, static_cast<int32_t>(size));
    return result_;
}

}  // namespace paimon
