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

#include "paimon/realtime/index/realtime_index_factory.h"

namespace paimon::lumina {

/// Creates column indexes configured by realtime.index.lumina.<column>.<option>.
class LuminaRealtimeIndexFactory final : public RealtimeIndexFactory {
 public:
    static const char IDENTIFIER[];

    const char* Identifier() const override {
        return IDENTIFIER;
    }

    Result<std::shared_ptr<RealtimeIndex>> Create(
        ::ArrowSchema* write_schema, const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& pool) const override;
};

}  // namespace paimon::lumina
