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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "paimon/status.h"

namespace paimon {
class InputStream;
class OutputStream;
}  // namespace paimon

namespace paimon::mosaic {

class MosaicInputContext {
 public:
    MosaicInputContext(const std::shared_ptr<InputStream>& input, uint64_t length)
        : input_(input), length_(length) {}

    static int32_t ReadAt(void* context, uint64_t offset, uint8_t* buffer, size_t length) noexcept;
    static uint64_t Length(void* context) noexcept;

    Status GetCallbackStatus() const;

 private:
    void SetCallbackStatus(const Status& status);

    std::shared_ptr<InputStream> input_;
    uint64_t length_;
    mutable std::mutex mutex_;
    Status callback_status_;
};

class MosaicOutputContext {
 public:
    explicit MosaicOutputContext(const std::shared_ptr<OutputStream>& output) : output_(output) {}

    static int32_t Write(void* context, const uint8_t* data, size_t length) noexcept;
    static int32_t Flush(void* context) noexcept;
    static int64_t GetPos(void* context) noexcept;

    Status GetCallbackStatus() const;

 private:
    void SetCallbackStatus(const Status& status);

    std::shared_ptr<OutputStream> output_;
    mutable std::mutex mutex_;
    Status callback_status_;
};

Status MosaicFfiError(const std::string& operation, const Status& callback_status);

}  // namespace paimon::mosaic
