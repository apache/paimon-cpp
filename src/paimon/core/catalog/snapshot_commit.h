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

#include <string>
#include <vector>

#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/snapshot.h"

namespace paimon {

/// Interface to commit snapshot atomically.
class SnapshotCommit {
 public:
    virtual ~SnapshotCommit() = default;

    virtual Result<bool> Commit(const Snapshot& snapshot,
                                const std::vector<PartitionStatistics>& statistics) = 0;

    virtual Result<std::string> GetLastCommitTableRequest() = 0;
};

}  // namespace paimon
