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

#include "paimon/core/table/sink/commit_message_impl.h"

#include "fmt/format.h"

namespace paimon {

CommitMessageImpl::CommitMessageImpl(const BinaryRow& partition, int32_t bucket,
                                     const std::optional<int32_t>& total_buckets,
                                     const DataIncrement& data_increment,
                                     const CompactIncrement& compact_increment)
    : partition_(partition),
      bucket_(bucket),
      total_buckets_(total_buckets),
      data_increment_(data_increment),
      compact_increment_(compact_increment) {}

std::string CommitMessageImpl::ToString() const {
    std::string total_buckets_str =
        (total_buckets_ == std::nullopt ? "null" : std::to_string(total_buckets_.value()));
    return fmt::format(
        "FileCommittable {{partition = {}, bucket = {}, totalBuckets = {}, newFilesIncrement = "
        "{}, compactIncrement = {}}}",
        partition_.ToString(), bucket_, total_buckets_str, data_increment_.ToString(),
        compact_increment_.ToString());
}

bool CommitMessageImpl::operator==(const CommitMessageImpl& other) const {
    if (this == &other) {
        return true;
    }
    return partition_ == other.partition_ && bucket_ == other.bucket_ &&
           total_buckets_ == other.total_buckets_ && data_increment_ == other.data_increment_ &&
           compact_increment_ == other.compact_increment_;
}

bool CommitMessageImpl::TEST_Equal(const CommitMessageImpl& other) const {
    if (this == &other) {
        return true;
    }
    return partition_ == other.partition_ && bucket_ == other.bucket_ &&
           total_buckets_ == other.total_buckets_ &&
           data_increment_.TEST_Equal(other.data_increment_) &&
           compact_increment_.TEST_Equal(other.compact_increment_);
}

}  // namespace paimon
