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
#include <string>
#include <utility>

#include "paimon/fs/file_system.h"

namespace paimon {

/// An independent view over a stream shared by several concurrent readers.
///
/// Opening a remote file is expensive, so the readers of one data file open it
/// once and read through a view each. Sharing the stream itself would not work:
/// `Seek()`, `GetPos()` and the position-dependent `Read()` all act on the
/// stream position, so concurrent readers would move each other's position, and
/// the first reader to close the stream would break the others.
///
/// This view solves both. It keeps its own position and turns the
/// position-dependent operations into `pread()`-like reads of the shared
/// stream, which carry the offset explicitly and leave no shared state to race
/// over. Closing a view is a no-op: the shared stream is owned by the readers
/// together and released once the last of them is gone.
///
/// The shared stream must therefore support concurrent position-independent
/// reads, which is what its `Read()` with an offset and `ReadAsync()` are
/// specified to do.
class SharedInputStreamView : public InputStream {
 public:
    explicit SharedInputStreamView(std::shared_ptr<InputStream> stream)
        : stream_(std::move(stream)) {}

    Status Seek(int64_t offset, SeekOrigin origin) override {
        int64_t base = 0;
        switch (origin) {
            case SeekOrigin::FS_SEEK_SET:
                base = 0;
                break;
            case SeekOrigin::FS_SEEK_CUR:
                base = pos_;
                break;
            case SeekOrigin::FS_SEEK_END: {
                PAIMON_ASSIGN_OR_RAISE(base, stream_->Length());
                break;
            }
            default:
                return Status::Invalid("unknown seek origin");
        }
        const int64_t target = base + offset;
        if (target < 0) {
            return Status::Invalid("seek before the beginning of the stream");
        }
        pos_ = target;
        return Status::OK();
    }

    Result<int64_t> GetPos() const override {
        return pos_;
    }

    Result<int64_t> Read(char* buffer, int64_t size) override {
        PAIMON_ASSIGN_OR_RAISE(int64_t read_bytes, stream_->Read(buffer, size, pos_));
        pos_ += read_bytes;
        return read_bytes;
    }

    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        return stream_->Read(buffer, size, offset);
    }

    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        stream_->ReadAsync(buffer, size, offset, std::move(callback));
    }

    /// Closing a view leaves the shared stream open for the other views.
    Status Close() override {
        return Status::OK();
    }

    Result<std::string> GetUri() const override {
        return stream_->GetUri();
    }

    Result<int64_t> Length() const override {
        return stream_->Length();
    }

 private:
    std::shared_ptr<InputStream> stream_;
    // This view's own position, so that it does not share one with the other
    // views of the same stream.
    int64_t pos_ = 0;
};

}  // namespace paimon
