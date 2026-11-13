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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "paimon/core/catalog/table_query_auth_result.h"
#include "paimon/table/source/split.h"

namespace paimon {

/// The table, branch and columns a `QueryAuthSplit` was authorized for.
struct QueryAuthScope {
    /// The table location and branch the scan authorized. Matching field ids alone must not let a
    /// read apply the rules to a different table or branch with the same schema and file names.
    std::string table_path;
    std::string branch;
    /// The field id, in the planned schema, of each column the scan asked the catalog to
    /// authorize; every column of that schema when the scan asked for all of them. A read may
    /// only output or filter on these columns, and only while they keep these field ids.
    std::map<std::string, int32_t> authorized_field_ids;
    /// The field id, in the planned schema, of each column the rules reference: the row filter
    /// operands and the targets and inputs of the masks. A read refuses rules referencing a column
    /// this does not bind, or one the schema it reads exposes under another field id.
    std::map<std::string, int32_t> rule_field_ids;

    bool operator==(const QueryAuthScope& other) const {
        return table_path == other.table_path && branch == other.branch &&
               authorized_field_ids == other.authorized_field_ids &&
               rule_field_ids == other.rule_field_ids;
    }
};

/// A split of a table with `query-auth.enabled=true`, carrying the rules the scan that planned it
/// got from the catalog, so the read applies them. Mirrors Java `QueryAuthSplit`.
///
/// A split planned here carries its `QueryAuthScope` and serializes in a paimon-cpp envelope,
/// which Java rejects as a corrupted split. A split Java serialized has no scope, so the read
/// refuses it; written back, it takes the Java `QueryAuthSplit` format.
class QueryAuthSplit : public Split {
 public:
    /// The magic (ASCII "SPLIT_V1") and version Java `SplitSerializer` starts a split with, and
    /// its type tag.
    static constexpr int64_t kSplitSerializerMagic = 0x53504C49545F5631LL;
    static constexpr int32_t kSplitSerializerVersion = 1;
    static constexpr int32_t kSplitSerializerDataSplit = 1;
    static constexpr int32_t kSplitSerializerIndexedSplit = 3;
    static constexpr int32_t kSplitSerializerQueryAuthSplit = 5;

    /// The magic and version of the envelope a split carrying its scope is written in: the Java
    /// `SplitSerializer` bytes of the split, then the scope. Both parts are required. The magic is
    /// ASCII "PCQAUTH1".
    static constexpr int64_t kScopedMagic = 0x5043514155544831LL;
    static constexpr int32_t kScopedVersion = 1;

    /// @param scope What the rules were authorized for, or null when the split does not record it.
    QueryAuthSplit(std::shared_ptr<Split> split, std::shared_ptr<TableQueryAuthResult> auth_result,
                   std::optional<QueryAuthScope> scope)
        : split_(std::move(split)),
          auth_result_(std::move(auth_result)),
          scope_(std::move(scope)) {}

    const std::shared_ptr<Split>& GetSplit() const {
        return split_;
    }

    const std::shared_ptr<TableQueryAuthResult>& GetAuthResult() const {
        return auth_result_;
    }

    const std::optional<QueryAuthScope>& GetScope() const {
        return scope_;
    }

 private:
    std::shared_ptr<Split> split_;
    std::shared_ptr<TableQueryAuthResult> auth_result_;
    std::optional<QueryAuthScope> scope_;
};

}  // namespace paimon
