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

#include "paimon/visibility.h"

namespace paimon {

class SnapshotReadViewImpl;

/// An immutable, process-local view of the snapshot and table-schema metadata used to create a
/// scan plan.
///
/// A view is bound to one base table path and branch. Pass a view returned by `Plan` to
/// `ScanContextBuilder::WithSnapshotReadView()` to plan another batch scan against the same
/// parsed planning metadata without resolving the latest snapshot or schema again. Views are
/// published and accepted only for non-streaming, non-real-time latest-snapshot scans. A missing
/// snapshot id represents an empty table at the time the view was created. The base path identity
/// is also used by supported system-table scans such as `$ro`, because they consume the same
/// snapshot and schema metadata.
class PAIMON_EXPORT SnapshotReadView {
 public:
    virtual ~SnapshotReadView() = default;

    /// Normalized base table path this view belongs to, without a system-table suffix.
    virtual const std::string& TablePath() const = 0;

    /// Normalized branch this view belongs to.
    virtual const std::string& Branch() const = 0;

    /// Snapshot id captured by this view, or `std::nullopt` if the table was empty.
    virtual std::optional<int64_t> SnapshotId() const = 0;

 private:
    SnapshotReadView() = default;

    friend class SnapshotReadViewImpl;
};

}  // namespace paimon
