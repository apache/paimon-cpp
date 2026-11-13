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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/table/source/file_row_selection.h"
#include "paimon/visibility.h"

namespace paimon {
class Predicate;
class FileSystem;
class MemoryPool;
class Split;

/// Queries live physical positions in a managed append-table batch split; bitmap32 only.
/// Uses indexes and data filtering to return exact positions.
/// Caches index readers and historical DV. Not thread-safe.
class PAIMON_EXPORT FileIndexRead {
 public:
    /// Binds a split; options override table options.
    static Result<std::unique_ptr<FileIndexRead>> Create(
        const std::string& table_path, const std::string& branch,
        const std::map<std::string, std::string>& options, const std::shared_ptr<Split>& split,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);
    virtual ~FileIndexRead() = default;

    /// Returns matching files and positions; empty for an empty split or no matching rows.
    /// Predicate must be non-null.
    virtual Result<std::vector<FileRowSelection>> Query(
        const std::shared_ptr<Predicate>& predicate) = 0;
};

}  // namespace paimon
