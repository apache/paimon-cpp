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

#include "paimon/fs/file_system.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class ByteArrayOutputStream;
class Bytes;
class MemoryPool;

/// Keeps a small file index in memory and spills to an external file above the threshold.
class SpillableIndexOutputStream : public OutputStream {
 public:
    SpillableIndexOutputStream(const std::shared_ptr<FileSystem>& file_system,
                               const std::string& path, int64_t threshold,
                               const std::shared_ptr<MemoryPool>& pool);

    Result<int64_t> Write(const char* buffer, int64_t size) override;
    Result<int64_t> GetPos() const override;
    Result<std::string> GetUri() const override;
    Status Flush() override;
    Status Close() override;

    bool Spilled() const {
        return external_ != nullptr;
    }

    /// Closes the stream and deletes any unpublished external index, even if Close() fails.
    Status Abort();

    /// Returns the buffered index, or null if it has spilled to an external file.
    Result<std::shared_ptr<Bytes>> EmbeddedBytes();

 private:
    int64_t threshold_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> file_system_;
    std::string path_;
    std::shared_ptr<ByteArrayOutputStream> memory_;
    std::shared_ptr<OutputStream> external_;
    int64_t position_ = 0;
    bool closed_ = false;
};
}  // namespace paimon
