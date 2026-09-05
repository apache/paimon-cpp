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

#include <memory>

#include "paimon/commit_message.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/realtime/realtime_context.h"
#include "paimon/visibility.h"

namespace paimon {

/// A real-time commit message and its partition-bucket offset progress.
///
/// Offsets are scoped to one partition and bucket. `offset_range` is left-closed and right-open
/// and bounds all offsets represented by `commit_message`. Offsets may have gaps, so the range
/// count is not the represented row count. The progress fields are not embedded in `CommitMessage`
/// serialization.
struct PAIMON_EXPORT RealtimeCommitProgress {
    /// Paimon commit message generated from one or more sealed segments.
    std::shared_ptr<CommitMessage> commit_message;
    /// Partition-bucket containing the sealed segments.
    RealtimePartitionBucket partition_bucket;
    /// Left-closed, right-open offset range represented by the commit message.
    OffsetRange offset_range;
};

}  // namespace paimon
