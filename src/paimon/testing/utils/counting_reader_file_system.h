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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/local/local_file_system.h"

namespace paimon::test {

class CountingReaderFileSystem : public LocalFileSystem {
 public:
    struct Counts {
        std::atomic<uint64_t> opens = 0;
        std::atomic<uint64_t> closes = 0;
        std::atomic<uint64_t> live = 0;
        std::atomic<uint64_t> peak = 0;
        std::atomic<uint64_t> reads = 0;
        std::atomic<uint64_t> bytes = 0;
    };

    explicit CountingReaderFileSystem(std::string suffix) : suffix_(std::move(suffix)) {}

    using LocalFileSystem::Open;

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        if (!StringUtils::EndsWith(path, suffix_)) {
            return LocalFileSystem::Open(path);
        }
        counts->opens++;
        if (before_open) {
            PAIMON_RETURN_NOT_OK(before_open(path));
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> input, LocalFileSystem::Open(path));
        return std::make_unique<CountedInput>(std::move(input), counts, length_override);
    }

    std::shared_ptr<Counts> counts = std::make_shared<Counts>();
    std::function<Status(const std::string&)> before_open;
    std::optional<int64_t> length_override;

 private:
    class CountedInput : public InputStream {
     public:
        CountedInput(std::unique_ptr<InputStream> input, std::shared_ptr<Counts> counts,
                     std::optional<int64_t> length_override)
            : input_(std::move(input)),
              counts_(std::move(counts)),
              length_override_(length_override) {
            const uint64_t live = ++counts_->live;
            uint64_t peak = counts_->peak.load();
            while (peak < live && !counts_->peak.compare_exchange_weak(peak, live)) {
            }
        }
        ~CountedInput() override {
            counts_->live--;
        }
        Status Close() override {
            counts_->closes++;
            return input_->Close();
        }
        Status Seek(int64_t offset, SeekOrigin origin) override {
            return input_->Seek(offset, origin);
        }
        Result<int64_t> GetPos() const override {
            return input_->GetPos();
        }
        Result<int64_t> Read(char* buffer, int64_t size) override {
            counts_->reads++;
            PAIMON_ASSIGN_OR_RAISE(int64_t bytes, input_->Read(buffer, size));
            counts_->bytes += bytes;
            return bytes;
        }
        Result<int64_t> Read(char* buffer, int64_t size, int64_t offset) override {
            counts_->reads++;
            PAIMON_ASSIGN_OR_RAISE(int64_t bytes, input_->Read(buffer, size, offset));
            counts_->bytes += bytes;
            return bytes;
        }
        void ReadAsync(char* buffer, int64_t size, int64_t offset,
                       std::function<void(Status)>&& callback) override {
            counts_->reads++;
            input_->ReadAsync(
                buffer, size, offset,
                [counts = counts_, size, callback = std::move(callback)](Status status) {
                    if (status.ok()) {
                        counts->bytes += size;
                    }
                    callback(status);
                });
        }
        Result<std::string> GetUri() const override {
            return input_->GetUri();
        }
        Result<int64_t> Length() const override {
            if (length_override_) {
                return *length_override_;
            }
            return input_->Length();
        }

     private:
        std::unique_ptr<InputStream> input_;
        std::shared_ptr<Counts> counts_;
        std::optional<int64_t> length_override_;
    };

    std::string suffix_;
};

}  // namespace paimon::test
