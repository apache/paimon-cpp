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
#include <optional>
#include <string>

#include "paimon/commit_message.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"

namespace paimon {
class CommitMessageImpl : public CommitMessage {
 public:
    CommitMessageImpl(const BinaryRow& partition, int32_t bucket,
                      const std::optional<int32_t>& total_buckets,
                      const DataIncrement& data_increment,
                      const CompactIncrement& compact_increment);

    ~CommitMessageImpl() override = default;

    const BinaryRow& Partition() const {
        return partition_;
    }

    /// Bucket of this commit message.
    int32_t Bucket() const {
        return bucket_;
    }

    /// Total number of buckets in this partition.
    const std::optional<int32_t>& TotalBuckets() const {
        return total_buckets_;
    }

    const DataIncrement& GetNewFilesIncrement() const {
        return data_increment_;
    }

    const CompactIncrement& GetCompactIncrement() const {
        return compact_increment_;
    }

    bool IsEmpty() const {
        return data_increment_.IsEmpty() && compact_increment_.IsEmpty();
    }

    std::string ToString() const;

    bool operator==(const CommitMessageImpl& other) const;

    bool TEST_Equal(const CommitMessageImpl& other) const;

 private:
    BinaryRow partition_;
    int32_t bucket_;
    std::optional<int32_t> total_buckets_;
    DataIncrement data_increment_;
    CompactIncrement compact_increment_;
};

}  // namespace paimon
