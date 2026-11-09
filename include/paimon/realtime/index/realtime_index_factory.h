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

#include <map>
#include <memory>
#include <string>

#include "paimon/factories/factory.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowSchema;

namespace paimon {
class MemoryPool;
class RealtimeIndex;

/// Factory for field indexes configured by realtime.index.<identifier>.columns.
class PAIMON_EXPORT RealtimeIndexFactory : public Factory {
 public:
    ~RealtimeIndexFactory() override = default;

    /// Registry suffix for real-time index plugins.
    static const char REALTIME_INDEX_IDENTIFIER_SUFFIX[];

    /// Resolves identifier + "-realtime" and consumes the [_REALTIME_OFFSET, field] schema.
    static Result<std::shared_ptr<RealtimeIndex>> Get(
        const std::string& identifier, ::ArrowSchema* write_schema,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    /// Creates a new field index; consumes the [_REALTIME_OFFSET, field] schema.
    virtual Result<std::shared_ptr<RealtimeIndex>> Create(
        ::ArrowSchema* write_schema, const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& pool) const = 0;
};

}  // namespace paimon
