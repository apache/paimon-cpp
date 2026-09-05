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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "paimon/fs/file_system.h"

namespace paimon::test {

/// An InputStream wrapper that holds the ReadAsync callbacks until ReleaseAll()
/// is called, letting tests observe a cache while its fetches are in flight.
class GatedAsyncInputStream : public InputStream {
 public:
    explicit GatedAsyncInputStream(std::shared_ptr<InputStream> inner) : inner_(std::move(inner)) {}

    Status Close() override {
        return inner_->Close();
    }
    Status Seek(int64_t offset, SeekOrigin origin) override {
        return inner_->Seek(offset, origin);
    }
    Result<int64_t> GetPos() const override {
        return inner_->GetPos();
    }
    Result<int64_t> Read(char* buffer, int64_t size) override {
        return inner_->Read(buffer, size);
    }
    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        return inner_->Read(buffer, size, offset);
    }
    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        std::lock_guard<std::mutex> lock(mutex_);
        async_read_count_++;
        pending_.push_back({buffer, size, offset, std::move(callback)});
    }
    Result<std::string> GetUri() const override {
        return inner_->GetUri();
    }
    Result<int64_t> Length() const override {
        return inner_->Length();
    }

    int32_t AsyncReadCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return async_read_count_;
    }

    /// Complete all held fetches against the underlying stream.
    void ReleaseAll() {
        for (auto& read : TakePending()) {
            Result<int64_t> res = inner_->Read(read.buffer, read.size, read.offset);
            read.callback(res.ok() ? Status::OK() : res.status());
        }
    }

    /// Complete all held fetches against the underlying stream but keep their
    /// callbacks alive, the way an object store stream destroys the callback of a
    /// read later than it resolves it: whatever the callback captured stays alive
    /// until DropCompletedCallbacks() is called.
    void ReleaseAllKeepingCallbacks() {
        std::vector<PendingRead> taken = TakePending();
        for (auto& read : taken) {
            Result<int64_t> res = inner_->Read(read.buffer, read.size, read.offset);
            read.callback(res.ok() ? Status::OK() : res.status());
        }
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& read : taken) {
            completed_.push_back(std::move(read));
        }
    }

    /// Destroy the callbacks kept alive by ReleaseAllKeepingCallbacks().
    void DropCompletedCallbacks() {
        std::vector<PendingRead> taken;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            taken = std::move(completed_);
            completed_.clear();
        }
    }

    /// Fail all held fetches without touching the underlying stream, so that a
    /// test can observe how a cache reports a failed fetch.
    void FailAll(const Status& status) {
        for (auto& read : TakePending()) {
            read.callback(status);
        }
    }

 private:
    struct PendingRead {
        char* buffer;
        int64_t size;
        int64_t offset;
        std::function<void(Status)> callback;
    };

    std::vector<PendingRead> TakePending() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<PendingRead> taken = std::move(pending_);
        pending_.clear();
        return taken;
    }

    std::shared_ptr<InputStream> inner_;
    mutable std::mutex mutex_;
    std::vector<PendingRead> pending_;
    // The fetches completed by ReleaseAllKeepingCallbacks(), held to keep their
    // callbacks alive.
    std::vector<PendingRead> completed_;
    int32_t async_read_count_ = 0;
};

}  // namespace paimon::test
