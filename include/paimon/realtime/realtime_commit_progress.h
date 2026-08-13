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
#include "paimon/realtime/realtime_context.h"
#include "paimon/utils/range.h"
#include "paimon/visibility.h"

namespace paimon {

/// A real-time commit message and its partition-bucket offset progress.
///
/// Offsets are scoped to one partition and bucket. `offset_range` is inclusive and covers all
/// rows represented by `commit_message`. The progress fields are not embedded in
/// `CommitMessage` serialization.
struct PAIMON_EXPORT RealtimeCommitProgress {
    /// Paimon commit message generated from one sealed segment.
    std::shared_ptr<CommitMessage> commit_message;
    /// Partition-bucket containing the sealed segment.
    RealtimePartitionBucket partition_bucket;
    /// Inclusive offset range represented by the commit message.
    Range offset_range;
};

}  // namespace paimon
