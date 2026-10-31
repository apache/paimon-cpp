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

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// One acquisition of a `CommitLock`, held for a single snapshot publication attempt. Destroying
/// the scope ends it.
class CommitLockScope {
 public:
    virtual ~CommitLockScope() = default;

    /// Returns an error once the scope can no longer protect a publication, for example after its
    /// lease expired. Called right before the snapshot is published.
    virtual Status EnsureValid() = 0;
};

/// A lock taken around each snapshot publication attempt, mirroring Java
/// `org.apache.paimon.operation.Lock`.
class CommitLock {
 public:
    virtual ~CommitLock() = default;

    /// Acquires a scope for one publication attempt, waiting while another writer holds the lock.
    virtual Result<std::unique_ptr<CommitLockScope>> Acquire() = 0;
};

}  // namespace paimon
