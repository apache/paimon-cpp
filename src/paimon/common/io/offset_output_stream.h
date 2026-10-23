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
#include "paimon/visibility.h"

namespace paimon {
/// A borrowed output view reporting positions relative to the wrapped stream's position at
/// creation. Flush forwards to the wrapped stream; Close leaves it open. The wrapped stream must
/// outlive this view.
class PAIMON_EXPORT OffsetOutputStream : public OutputStream {
 public:
    static Result<std::unique_ptr<OffsetOutputStream>> Create(OutputStream* output);
    ~OffsetOutputStream() override = default;

    Result<int64_t> Write(const char* buffer, int64_t size) override;
    Result<int64_t> GetPos() const override;
    Result<std::string> GetUri() const override;
    Status Flush() override;
    Status Close() override;

 private:
    OffsetOutputStream(OutputStream* output, int64_t start);

    OutputStream* output_;
    const int64_t start_;
};
}  // namespace paimon
