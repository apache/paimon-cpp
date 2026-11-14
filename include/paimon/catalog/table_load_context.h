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
#include <optional>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/visibility.h"

namespace paimon {
class Catalog;
class FileSystem;

/// Context used only while loading a table for a static operator factory.
/// A catalog requires an identifier; a path is required for file-system loading.
struct PAIMON_EXPORT TableLoadContext {
    /// Optional for catalog loads; otherwise the native table or system-table path.
    std::string path = {};
    /// Explicit storage binding, taking precedence over the catalog's per-table file system.
    std::shared_ptr<FileSystem> file_system = nullptr;
    /// URI scheme routing; cannot be specified together with file_system.
    std::map<std::string, std::string> fs_scheme_to_identifier_map = {};
    /// Serialized Paimon table schema, independent of the reader's Arrow projection.
    /// Used for the main branch, including a fallback to main from another branch.
    std::optional<std::string> table_schema_json = std::nullopt;
    std::shared_ptr<Catalog> catalog = nullptr;
    /// Requires catalog. A named branch must agree with the dynamic branch option.
    std::optional<Identifier> identifier = std::nullopt;
    /// Expected catalog UUID, or the UUID for an external REST commit request.
    std::optional<std::string> table_id = std::nullopt;
};
}  // namespace paimon
