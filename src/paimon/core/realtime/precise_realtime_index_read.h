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

#include <memory>

#include "paimon/reader/batch_reader.h"
#include "paimon/realtime/realtime_store.h"

namespace paimon {
class InternalReadContext;

/// Executes the same precise predicate/search plan as file reads, using pinned real-time offsets.
/// TopK is source-local, matching the existing file-local search contract.
class PreciseRealtimeIndexRead {
 public:
    PreciseRealtimeIndexRead() = delete;
    ~PreciseRealtimeIndexRead() = delete;

    static Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<InternalReadContext>& context, RealtimeStoreMode mode,
        const std::shared_ptr<RealtimeStore>& store, const std::shared_ptr<RealtimeReadView>& view,
        const OffsetRange& offset_range);
};

}  // namespace paimon
