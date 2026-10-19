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

#include "paimon/core/io/spillable_index_output_stream.h"

#include <limits>

#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {
SpillableIndexOutputStream::SpillableIndexOutputStream(
    const std::shared_ptr<FileSystem>& file_system, const std::string& path, int64_t threshold,
    const std::shared_ptr<MemoryPool>& pool)
    : threshold_(threshold),
      pool_(pool),
      file_system_(file_system),
      path_(path),
      memory_(std::make_shared<ByteArrayOutputStream>(std::make_unique<MemorySegmentOutputStream>(
          MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool))) {}

Result<int64_t> SpillableIndexOutputStream::Write(const char* buffer, int64_t size) {
    if (size < 0 || size > std::numeric_limits<int64_t>::max() - position_) {
        return Status::Invalid("File index size overflow");
    }
    if (!external_ && position_ + size > threshold_) {
        PAIMON_ASSIGN_OR_RAISE(external_, file_system_->Create(path_, /*overwrite=*/false));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> prefix, memory_->Finish(pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(int64_t written, external_->Write(prefix->data(), prefix->size()));
        if (written != static_cast<int64_t>(prefix->size())) {
            return Status::IOError("Short write for file index");
        }
        memory_.reset();
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t written,
                           (external_ ? external_.get() : memory_.get())->Write(buffer, size));
    if (written != size) {
        return Status::IOError("Short write for file index");
    }
    position_ += written;
    return written;
}

Result<int64_t> SpillableIndexOutputStream::GetPos() const {
    return position_;
}

Result<std::string> SpillableIndexOutputStream::GetUri() const {
    return path_;
}

Status SpillableIndexOutputStream::Flush() {
    return external_ ? external_->Flush() : memory_->Flush();
}

Status SpillableIndexOutputStream::Close() {
    if (closed_) {
        return Status::OK();
    }
    closed_ = true;
    return external_ ? external_->Close() : memory_->Close();
}

Result<std::shared_ptr<Bytes>> SpillableIndexOutputStream::EmbeddedBytes() {
    if (external_) {
        return std::shared_ptr<Bytes>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, memory_->Finish(pool_.get()));
    return bytes;
}

Status SpillableIndexOutputStream::Abort() {
    Status close_status = Close();
    if (Spilled()) {
        PAIMON_RETURN_NOT_OK(file_system_->Delete(path_, /*recursive=*/false));
    }
    return close_status;
}
}  // namespace paimon
