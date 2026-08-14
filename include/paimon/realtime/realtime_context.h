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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

class MemIndexerFactory;

/// Identifies one partition-bucket by its logical partition values.
struct PAIMON_EXPORT RealtimePartitionBucket {
    /// Creates a key from logical partition values and a fixed bucket id.
    RealtimePartitionBucket(std::map<std::string, std::string> partition, int32_t bucket)
        : partition(std::move(partition)), bucket(bucket) {}

    /// Orders keys by partition values and then bucket id.
    bool operator<(const RealtimePartitionBucket& other) const {
        if (partition != other.partition) {
            return partition < other.partition;
        }
        return bucket < other.bucket;
    }

    /// Returns whether both partition values and bucket id are equal.
    bool operator==(const RealtimePartitionBucket& other) const {
        return partition == other.partition && bucket == other.bucket;
    }

    /// Returns whether the partition values or bucket id differ.
    bool operator!=(const RealtimePartitionBucket& other) const {
        return !(*this == other);
    }

    /// Logical partition values, before partition-path escaping.
    std::map<std::string, std::string> partition;
    /// Fixed bucket id.
    int32_t bucket = -1;
};

/// Largest committed offset for each partition-bucket.
using RealtimeOffsetMap = std::map<RealtimePartitionBucket, int64_t>;

/// Shared context that owns the `MemIndexer` instances used by a real-time writer.
///
/// Applications share one context between `WriteContext`, `ScanContext`, and `ReadContext`. The
/// context uses either the default Arrow implementation or an application-provided factory and
/// keeps each created indexer available across writes, prepare-commit operations, and
/// process-local reads.
class PAIMON_EXPORT RealtimeContext {
 public:
    /// Creates a context backed by Paimon's default Arrow `MemIndexer`.
    static Result<std::shared_ptr<RealtimeContext>> Create();

    /// Creates a context backed by an application-provided indexer factory.
    ///
    /// @param factory Non-null factory used to create indexers on demand.
    static Result<std::shared_ptr<RealtimeContext>> Create(
        const std::shared_ptr<MemIndexerFactory>& factory);

    virtual ~RealtimeContext();

 protected:
    RealtimeContext() = default;
};

}  // namespace paimon
