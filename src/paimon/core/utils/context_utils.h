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

#include "fmt/format.h"
#include "paimon/status.h"

namespace paimon {

/// Shared operator-context checks.
class ContextUtils {
 public:
    ContextUtils() = delete;
    ~ContextUtils() = delete;

    /// Requires a context, memory pool and executor; kind labels errors.
    template <typename ContextPointer>
    static Status CheckOperatorContext(const ContextPointer& context, const char* kind) {
        if (context == nullptr) {
            return Status::Invalid(fmt::format("{} context is null pointer", kind));
        }
        if (context->GetMemoryPool() == nullptr) {
            return Status::Invalid("memory pool is null pointer");
        }
        if (context->GetExecutor() == nullptr) {
            return Status::Invalid("executor is null pointer");
        }
        return Status::OK();
    }
};

}  // namespace paimon
