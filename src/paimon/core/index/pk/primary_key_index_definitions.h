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

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/index/pk/primary_key_index_definition.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/result.h"

namespace paimon {
/// Resolves all configured source-backed primary-key indexes of a table schema.
class PrimaryKeyIndexDefinitions {
 public:
    /// Resolves index definitions from `pk-btree.index.columns`, `pk-bitmap.index.columns`,
    /// `pk-vector.index.columns` and `pk-full-text.index.columns` together with their
    /// field-scoped option JSON, rejecting duplicate columns and columns owned by more than
    /// one index family.
    static Result<PrimaryKeyIndexDefinitions> Create(const TableSchema& schema);

    const std::vector<PrimaryKeyIndexDefinition>& Definitions() const {
        return definitions_;
    }

    /// @return The scalar (BTree / Bitmap) definitions usable by batch scans.
    std::vector<PrimaryKeyIndexDefinition> ScalarDefinitions() const;

    /// Filters an arbitrary definition list to the scalar families usable by batch scans.
    static std::vector<PrimaryKeyIndexDefinition> ScalarDefinitions(
        const std::vector<PrimaryKeyIndexDefinition>& definitions);

 private:
    explicit PrimaryKeyIndexDefinitions(std::vector<PrimaryKeyIndexDefinition> definitions)
        : definitions_(std::move(definitions)) {}

    std::vector<PrimaryKeyIndexDefinition> definitions_;
};

}  // namespace paimon
