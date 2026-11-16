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

#include <functional>
#include <memory>
#include <vector>

#include "paimon/core/realtime/realtime_context_impl.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace paimon {
class InternalReadContext;
class RealtimeSplit;
class Split;

class TableReadUtils {
 public:
    TableReadUtils() = delete;
    ~TableReadUtils() = delete;

    using DiskReaderFactory = std::function<Result<std::unique_ptr<BatchReader>>(
        const std::shared_ptr<Split>&)>;

    using RealtimeReaderFactory = std::function<Result<std::unique_ptr<BatchReader>>(
        const std::shared_ptr<RealtimeSplit>&, const RealtimePartitionBucketView&)>;

    /// Validates a real-time split and resolves its view without consuming its ticket.
    static Result<RealtimePartitionBucketView> ResolveRealtimeReadView(
        const RealtimeSplit& split, const InternalReadContext& context);

    /// Both factories must own their read state. The real-time factory It receives the view pinned for its split
    /// during validation, even if the caller has since destroyed the TableRead.
    static Result<std::unique_ptr<BatchReader>> CreateLazyReader(
        const std::vector<std::shared_ptr<Split>>& splits, const DiskReaderFactory& open_disk,
        const RealtimeReaderFactory& open_realtime, const InternalReadContext& context);
};
}  // namespace paimon
