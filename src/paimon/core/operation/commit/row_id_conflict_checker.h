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

#include <memory>

#include "paimon/result.h"

namespace paimon {

struct DataFileMeta;

/// Decides whether a data file committed after the checked snapshot conflicts with the files
/// this commit is writing.
///
/// The commit that carries the check picks the rule: a partial-column update only conflicts
/// with another update of the same columns over the same rows, while a commit that reassigns
/// row ids conflicts with anything written over the rows it moves.
class RowIdConflictChecker {
 public:
    virtual ~RowIdConflictChecker() = default;

    /// Whether this checker holds nothing to conflict with, in which case the whole replay of
    /// the snapshots since the checked one can be skipped.
    virtual bool IsEmpty() const = 0;

    virtual Result<bool> ConflictsWith(const std::shared_ptr<DataFileMeta>& file) const = 0;
};

}  // namespace paimon
