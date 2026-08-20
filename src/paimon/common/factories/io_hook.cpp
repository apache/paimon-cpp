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

#include "paimon/common/factories/io_hook.h"

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>

#include "fmt/format.h"
#include "paimon/macros.h"
#include "paimon/status.h"

namespace paimon {

class IOHook::Impl {
 public:
    Status Try(const std::string& path) {
        // Fast path: the hook is disabled, which is always the case in production;
        // writers (Reset()/Clear()) only exist in tests. This keeps Try() a single
        // atomic load on the IO path instead of a shared_mutex acquisition per IO.
        if (PAIMON_UNLIKELY(armed_.load(std::memory_order_acquire))) {
            return TryArmed(path);
        }
        return Status::OK();
    }

    inline void Reset(int64_t pos, IOHook::Mode mode) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        mode_ = mode;
        pos_ = pos;
        io_count_ = 0;
        // Arm only after the configuration is complete: TryArmed() reads mode_/pos_
        // under mutex_, which synchronizes with this store, so an observed armed state
        // always implies a complete configuration.
        armed_.store(true, std::memory_order_release);
    }

    int64_t IOCount() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return io_count_.load();
    }

    void Clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        // Disarm first so IO threads stop taking the lock as soon as possible.
        armed_.store(false, std::memory_order_release);
        mode_ = IOHook::Mode::SILENT;
        pos_ = -1;
        io_count_ = 0;
    }

 private:
    Status TryArmed(const std::string& path) {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (io_count_.fetch_add(1) < pos_) {
            return Status::OK();
        } else {
            switch (mode_) {
                case IOHook::Mode::SILENT:
                    return Status::OK();
                case IOHook::Mode::RETURN_ERROR:
                    return Status::IOError(fmt::format(
                        "io hook triggered io error at position {}, path {}", pos_, path));
                case IOHook::Mode::THROW_EXCEPTION:
                    throw std::runtime_error(fmt::format(
                        "io hook throw io exception at position {}, path {}", pos_, path));
                    return Status::OK();
                default:
                    return Status::OK();
            }
        }
    }

    mutable std::shared_mutex mutex_;
    std::atomic<bool> armed_ = {false};
    std::atomic<int64_t> io_count_ = {0};
    int64_t pos_ = -1;
    IOHook::Mode mode_ = IOHook::Mode::SILENT;
};

IOHook::IOHook() : impl_(std::make_unique<IOHook::Impl>()) {}
IOHook::~IOHook() = default;

Status IOHook::Try(const std::string& path) {
    return impl_->Try(path);
}

int64_t IOHook::IOCount() const {
    return impl_->IOCount();
}

void IOHook::Clear() {
    return impl_->Clear();
}

void IOHook::Reset(int64_t pos, IOHook::Mode mode) {
    return impl_->Reset(pos, mode);
}

}  // namespace paimon
