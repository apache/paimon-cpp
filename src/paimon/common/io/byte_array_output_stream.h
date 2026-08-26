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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class Bytes;
class MemoryPool;

/// An in-memory output stream backed by segments allocated from a Paimon MemoryPool.
class ByteArrayOutputStream : public OutputStream {
 public:
    /// Takes ownership of an initialized segmented output stream.
    explicit ByteArrayOutputStream(std::unique_ptr<MemorySegmentOutputStream>&& output);

    ~ByteArrayOutputStream() override = default;

    Result<int64_t> Write(const char* buffer, int64_t size) override;

    Status Flush() override {
        return Status::OK();
    }

    Result<int64_t> GetPos() const override {
        return output_->CurrentSize();
    }

    Result<std::string> GetUri() const override {
        return std::string();
    }

    Status Close() override;

    /// Closes the stream and returns its contents as an exactly-sized contiguous byte array.
    /// @note The caller must keep `pool` alive until the returned bytes are destroyed.
    Result<std::shared_ptr<Bytes>> Finish(MemoryPool* pool);

 private:
    std::unique_ptr<MemorySegmentOutputStream> output_;
    std::shared_ptr<Bytes> result_;
    bool closed_ = false;
};

}  // namespace paimon
