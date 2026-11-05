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

#include <functional>
#include <memory>
#include <string>

#include "paimon/common/utils/math.h"
#include "paimon/common/utils/read_ahead_cache.h"
#include "paimon/fs/file_system.h"

namespace paimon {

class FileReadAhead;

class CacheInputStream : public InputStream {
 public:
    using CacheFactory = std::function<Result<std::shared_ptr<ReadAheadCache>>()>;

    CacheInputStream(std::unique_ptr<InputStream> input_stream,
                     const std::shared_ptr<ReadAheadCache>& cache)
        : CacheInputStream(std::move(input_stream), cache, CacheFactory(), nullptr) {}

    CacheInputStream(std::unique_ptr<InputStream> input_stream,
                     const std::shared_ptr<ReadAheadCache>& cache, CacheFactory cache_factory)
        : CacheInputStream(std::move(input_stream), cache, std::move(cache_factory), nullptr) {}

    CacheInputStream(std::unique_ptr<InputStream> input_stream,
                     const std::shared_ptr<ReadAheadCache>& cache, CacheFactory cache_factory,
                     const std::shared_ptr<FileReadAhead>& read_ahead)
        : cache_(cache),
          input_stream_(std::move(input_stream)),
          cache_factory_(std::move(cache_factory)),
          read_ahead_(read_ahead) {}

    const std::shared_ptr<FileReadAhead>& GetReadAhead() const {
        return read_ahead_;
    }

    // Configure before the reader starts consuming data. Capable builders may enable the cache
    // early to coordinate their own buffering; otherwise the factory enables it after Build().
    Result<bool> EnableCache() {
        if (cache_) {
            return true;
        }
        if (!cache_factory_) {
            return false;
        }
        PAIMON_ASSIGN_OR_RAISE(cache_, cache_factory_());
        cache_factory_ = nullptr;
        return cache_ != nullptr;
    }

    void ClearCacheFactory() {
        cache_factory_ = nullptr;
    }

    Status Seek(int64_t offset, SeekOrigin origin) override {
        return input_stream_->Seek(offset, origin);
    }
    Result<int64_t> GetPos() const override {
        return input_stream_->GetPos();
    }
    Result<int64_t> Read(char* buffer, int64_t size) override {
        return input_stream_->Read(buffer, size);
    }
    Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
        if (cache_) {
            PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint64_t>(offset, "read offset"));
            PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint64_t>(size, "read size"));
            ByteRange range{static_cast<uint64_t>(offset), static_cast<uint64_t>(size)};
            PAIMON_ASSIGN_OR_RAISE(bool hit, cache_->Read(range, buffer));
            if (hit) {
                return size;
            }
        }
        return input_stream_->Read(buffer, size, offset);
    }
    void ReadAsync(char* buffer, int64_t size, int64_t offset,
                   std::function<void(Status)>&& callback) override {
        if (cache_) {
            Status status = ValidateValueInRange<uint64_t>(offset, "read offset");
            if (!status.ok()) {
                callback(status);
                return;
            }
            status = ValidateValueInRange<uint64_t>(size, "read size");
            if (!status.ok()) {
                callback(status);
                return;
            }
            ByteRange range{static_cast<uint64_t>(offset), static_cast<uint64_t>(size)};
            Result<bool> hit = cache_->Read(range, buffer);
            if (!hit.ok()) {
                callback(hit.status());
                return;
            }
            if (hit.value()) {
                callback(Status::OK());
                return;
            }
        }
        return input_stream_->ReadAsync(buffer, size, offset, std::move(callback));
    }

    Status Close() override {
        return input_stream_->Close();
    }

    Result<std::string> GetUri() const override {
        return input_stream_->GetUri();
    }

    Result<int64_t> Length() const override {
        return input_stream_->Length();
    }

 private:
    std::shared_ptr<ReadAheadCache> cache_;
    std::unique_ptr<InputStream> input_stream_;
    CacheFactory cache_factory_;
    std::shared_ptr<FileReadAhead> read_ahead_;
};

}  // namespace paimon
