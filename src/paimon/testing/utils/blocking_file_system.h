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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "paimon/fs/file_system.h"
#include "paimon/macros.h"

namespace paimon::test {

/// Blocks selected file operations on a delegated file system.
class BlockingFileSystem final : public FileSystem {
 public:
    enum class Operation { OPEN, CREATE, CLOSE };

    class Gate {
     public:
        Gate(std::string path, uint32_t arrivals, bool auto_release)
            : path_(std::move(path)), arrivals_(arrivals), auto_release_(auto_release) {}

        bool WaitForArrival() const {
            std::unique_lock<std::mutex> lock(mutex_);
            return cv_.wait_for(lock, std::chrono::seconds(5), [&]() { return entered_ > 0; });
        }

        void Release() {
            Release(Status::OK());
        }

        /// Releases the operation with a success or injected error status.
        void Release(const Status& status) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!released_) {
                result_ = status;
                released_ = true;
                cv_.notify_all();
            }
        }

        std::string MatchedPath() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return matched_path_;
        }

     private:
        friend class BlockingFileSystem;

        Status Arrive(const std::string& path) {
            std::unique_lock<std::mutex> lock(mutex_);
            if ((!path_.empty() && path_ != path) || entered_ >= arrivals_) {
                return Status::OK();
            }
            matched_path_ = path;
            ++entered_;
            if (auto_release_ && entered_ == arrivals_) {
                released_ = true;
            }
            cv_.notify_all();
            if (!cv_.wait_for(lock, std::chrono::seconds(60), [&]() { return released_; })) {
                result_ = Status::IOError("timed out waiting for blocked file operation");
                released_ = true;
                cv_.notify_all();
            }
            return result_;
        }

        std::string path_;
        uint32_t arrivals_;
        bool auto_release_;
        mutable std::mutex mutex_;
        mutable std::condition_variable cv_;
        uint32_t entered_ = 0;
        bool released_ = false;
        std::string matched_path_;
        Status result_ = Status::OK();
    };

    explicit BlockingFileSystem(std::shared_ptr<FileSystem> delegate)
        : delegate_(std::move(delegate)), controls_(std::make_shared<Controls>()) {}

    /// Blocks the next matching operation. An empty path matches any file.
    std::shared_ptr<Gate> BlockNext(Operation operation, const std::string& path) {
        return SetGate(operation, path, 1, false);
    }

    /// Releases concurrent matching operations once all required arrivals have entered.
    Result<std::shared_ptr<Gate>> Barrier(Operation operation, const std::string& path,
                                          uint32_t arrivals) {
        if (arrivals == 0) {
            return Status::Invalid("file operation barrier requires positive arrivals");
        }
        return SetGate(operation, path, arrivals, true);
    }

    Result<std::unique_ptr<InputStream>> Open(const std::string& path) const override {
        PAIMON_RETURN_NOT_OK(Wait(controls_, Operation::OPEN, path));
        return delegate_->Open(path);
    }

    Result<std::unique_ptr<InputStream>> Open(const FileStatus& file_status) const override {
        PAIMON_RETURN_NOT_OK(Wait(controls_, Operation::OPEN, file_status.GetPath()));
        return delegate_->Open(file_status);
    }

    Result<std::unique_ptr<OutputStream>> Create(const std::string& path,
                                                 bool overwrite) const override {
        PAIMON_RETURN_NOT_OK(Wait(controls_, Operation::CREATE, path));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> stream,
                               delegate_->Create(path, overwrite));
        return std::make_unique<ControlledOutputStream>(std::move(stream), path, controls_);
    }

    Status Mkdirs(const std::string& path) const override {
        return delegate_->Mkdirs(path);
    }

    Status Rename(const std::string& src, const std::string& dst) const override {
        return delegate_->Rename(src, dst);
    }

    Status Delete(const std::string& path, bool recursive) const override {
        return delegate_->Delete(path, recursive);
    }

    Result<FileStatus> GetFileStatus(const std::string& path) const override {
        return delegate_->GetFileStatus(path);
    }

    Status ListDir(const std::string& directory,
                   std::vector<BasicFileStatus>* file_status_list) const override {
        return delegate_->ListDir(directory, file_status_list);
    }

    Status ListFileStatus(const std::string& path,
                          std::vector<FileStatus>* file_status_list) const override {
        return delegate_->ListFileStatus(path, file_status_list);
    }

    Result<bool> Exists(const std::string& path) const override {
        return delegate_->Exists(path);
    }

 private:
    struct Controls {
        std::mutex mutex;
        std::map<Operation, std::shared_ptr<Gate>> gates;
    };

    class ControlledOutputStream final : public OutputStream {
     public:
        ControlledOutputStream(std::unique_ptr<OutputStream> inner, std::string path,
                               std::shared_ptr<Controls> controls)
            : inner_(std::move(inner)), path_(std::move(path)), controls_(std::move(controls)) {}

        Result<int64_t> GetPos() const override {
            return inner_->GetPos();
        }
        Result<int64_t> Write(const char* buffer, int64_t size) override {
            return inner_->Write(buffer, size);
        }
        Status Flush() override {
            return inner_->Flush();
        }
        Status Close() override {
            PAIMON_RETURN_NOT_OK(inner_->Close());
            return Wait(controls_, Operation::CLOSE, path_);
        }
        Result<std::string> GetUri() const override {
            return inner_->GetUri();
        }

     private:
        std::unique_ptr<OutputStream> inner_;
        std::string path_;
        std::shared_ptr<Controls> controls_;
    };

    std::shared_ptr<Gate> SetGate(Operation operation, const std::string& path, uint32_t arrivals,
                                  bool auto_release) {
        auto gate = std::make_shared<Gate>(path, arrivals, auto_release);
        std::lock_guard<std::mutex> lock(controls_->mutex);
        controls_->gates[operation] = gate;
        return gate;
    }

    static Status Wait(const std::shared_ptr<Controls>& controls, Operation operation,
                       const std::string& path) {
        std::shared_ptr<Gate> gate;
        {
            std::lock_guard<std::mutex> lock(controls->mutex);
            auto iter = controls->gates.find(operation);
            if (iter != controls->gates.end()) {
                gate = iter->second;
            }
        }
        return gate ? gate->Arrive(path) : Status::OK();
    }

    std::shared_ptr<FileSystem> delegate_;
    std::shared_ptr<Controls> controls_;
};

}  // namespace paimon::test
