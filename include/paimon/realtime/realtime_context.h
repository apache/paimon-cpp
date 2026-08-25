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

class RealtimeStoreFactory;

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

/// Exclusive committed end offset for each partition-bucket.
using RealtimeOffsetMap = std::map<RealtimePartitionBucket, int64_t>;

/// Framework-managed context that owns the `RealtimeStore` instances used by real-time operations.
///
/// Applications share one context between `WriteContext`, `ScanContext`, and `ReadContext`. The
/// context uses either the default Arrow implementation or an application-provided factory and
/// keeps each created store available across writes, prepare-commit operations, and process-local
/// reads. `RealtimeContext` itself is not a customization interface and must not be implemented by
/// applications. Customize real-time storage and retrieval through `RealtimeStoreFactory` and
/// `RealtimeStore` instead.
///
/// A context is valid only for one uninterrupted committed-progress history. Overwrite, truncate,
/// partition drop, and rollback operations do not automatically clear process-local real-time
/// state. Applications must coordinate these operations with active real-time writers and recreate
/// the `RealtimeContext` and writers before continuing.
class PAIMON_EXPORT RealtimeContext {
 public:
    /// Creates a context backed by Paimon's default in-memory Arrow `RealtimeStore`.
    static Result<std::shared_ptr<RealtimeContext>> Create();

    /// Creates a context backed by an application-provided store factory.
    ///
    /// @param factory Non-null factory used to create stores on demand.
    static Result<std::shared_ptr<RealtimeContext>> Create(
        const std::shared_ptr<RealtimeStoreFactory>& factory);

    virtual ~RealtimeContext();

 protected:
    RealtimeContext() = default;
};

}  // namespace paimon
