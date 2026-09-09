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

#include "paimon/core/index/pk/primary_key_index_definitions.h"

#include <functional>
#include <set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {
namespace {
using IndexOptions = std::map<std::string, std::string>;

constexpr char kBTreeIndexType[] = "btree";
constexpr char kBitmapIndexType[] = "bitmap";
constexpr char kFullTextIndexType[] = "full-text";
constexpr char kBTreeOptionFamily[] = "pk-btree";
constexpr char kBitmapOptionFamily[] = "pk-bitmap";
constexpr char kBTreeAlgorithmPrefix[] = "btree-index.";
constexpr char kBitmapAlgorithmPrefix[] = "bitmap-index.";
constexpr char kFieldScopedPrefix[] = "fields.";
constexpr char kRecordsPerRangeKey[] = "sorted-index.records-per-range";

std::vector<std::string> IndexColumns(const std::map<std::string, std::string>& options,
                                      const char* option_key) {
    auto iter = options.find(option_key);
    if (iter == options.end()) {
        return {};
    }
    std::vector<std::string> columns = StringUtils::Split(iter->second, ",", false);
    for (std::string& column : columns) {
        StringUtils::Trim(&column);
    }
    return columns;
}

Status AddUniqueColumns(const std::vector<std::string>& columns,
                        const std::function<Status(const std::string&)>& on_duplicate,
                        std::set<std::string>* unique_columns) {
    for (const std::string& column : columns) {
        if (!unique_columns->insert(column).second) {
            return on_duplicate(column);
        }
    }
    return Status::OK();
}

Status ValidateNoDuplicates(const std::vector<std::string>& columns, const char* option_key) {
    std::set<std::string> unique_columns;
    return AddUniqueColumns(
        columns,
        [option_key](const std::string& column) {
            return Status::Invalid(
                fmt::format("{} contains duplicate column '{}'.", option_key, column));
        },
        &unique_columns);
}

Status ValidateUniqueColumns(const std::vector<std::string>& columns,
                             std::set<std::string>* indexed_columns) {
    return AddUniqueColumns(
        columns,
        [](const std::string& column) {
            return Status::Invalid(
                fmt::format("Column '{}' can own at most one primary-key index.", column));
        },
        indexed_columns);
}

/// Resolves the effective option map of one sorted-index definition: table options first,
/// then the field-scoped JSON options with unqualified keys prefixed by the algorithm
/// prefix, mirroring Java `CoreOptions#primaryKeySortedIndexOptions`.
Result<std::map<std::string, std::string>> SortedIndexOptions(
    const std::map<std::string, std::string>& table_options, const std::string& column,
    const char* option_family, const char* algorithm_prefix) {
    std::map<std::string, std::string> resolved = table_options;
    resolved.erase(kRecordsPerRangeKey);
    std::string option_key =
        fmt::format("{}{}.{}.index.options", kFieldScopedPrefix, column, option_family);
    auto iter = table_options.find(option_key);
    if (iter == table_options.end()) {
        return resolved;
    }
    if (StringUtils::IsEmptyAfterTrim(iter->second)) {
        return resolved;
    }

    rapidjson::Document document;
    document.Parse(iter->second.c_str());
    if (document.HasParseError() || !document.IsObject()) {
        return Status::Invalid(
            fmt::format("{} must be a JSON object of option key-value pairs.", option_key));
    }
    for (auto member = document.MemberBegin(); member != document.MemberEnd(); ++member) {
        if (!member->name.IsString()) {
            return Status::Invalid(fmt::format("{} contains an empty option key.", option_key));
        }
        std::string key = member->name.GetString();
        if (StringUtils::IsEmptyAfterTrim(key)) {
            return Status::Invalid(fmt::format("{} contains an empty option key.", option_key));
        }
        if (member->value.IsNull()) {
            return Status::Invalid(
                fmt::format("{} value for key {} must not be null.", option_key, key));
        }
        if (member->value.IsObject() || member->value.IsArray()) {
            return Status::Invalid(
                fmt::format("{} must be a JSON object of option key-value pairs.", option_key));
        }
        std::string value;
        if (member->value.IsString()) {
            value = member->value.GetString();
        } else {
            // Java's parseJsonMap(..., String.class) coerces scalar JSON values (numbers,
            // booleans) to their text form, so `{"compression-level":3}` is valid there.
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            member->value.Accept(writer);
            value = buffer.GetString();
        }
        std::string qualified_key = StringUtils::StartsWith(key, algorithm_prefix) ||
                                            StringUtils::StartsWith(key, kFieldScopedPrefix)
                                        ? key
                                        : algorithm_prefix + key;
        auto previous = resolved.find(qualified_key);
        if (previous != resolved.end() && previous->second != value) {
            return Status::Invalid(
                fmt::format("{} defines conflicting values for {}.", option_key, qualified_key));
        }
        resolved[qualified_key] = value;
    }
    return resolved;
}

}  // namespace

Result<PrimaryKeyIndexDefinitions> PrimaryKeyIndexDefinitions::Create(const TableSchema& schema) {
    const std::map<std::string, std::string>& options = schema.Options();
    std::vector<std::string> vector_columns =
        IndexColumns(options, Options::PK_VECTOR_INDEX_COLUMNS);
    std::vector<std::string> btree_columns = IndexColumns(options, Options::PK_BTREE_INDEX_COLUMNS);
    std::vector<std::string> bitmap_columns =
        IndexColumns(options, Options::PK_BITMAP_INDEX_COLUMNS);
    std::vector<std::string> full_text_columns =
        IndexColumns(options, Options::PK_FULL_TEXT_INDEX_COLUMNS);
    PAIMON_RETURN_NOT_OK(ValidateNoDuplicates(vector_columns, Options::PK_VECTOR_INDEX_COLUMNS));
    PAIMON_RETURN_NOT_OK(ValidateNoDuplicates(btree_columns, Options::PK_BTREE_INDEX_COLUMNS));
    PAIMON_RETURN_NOT_OK(ValidateNoDuplicates(bitmap_columns, Options::PK_BITMAP_INDEX_COLUMNS));
    PAIMON_RETURN_NOT_OK(
        ValidateNoDuplicates(full_text_columns, Options::PK_FULL_TEXT_INDEX_COLUMNS));
    std::set<std::string> indexed_columns;
    PAIMON_RETURN_NOT_OK(ValidateUniqueColumns(vector_columns, &indexed_columns));
    PAIMON_RETURN_NOT_OK(ValidateUniqueColumns(btree_columns, &indexed_columns));
    PAIMON_RETURN_NOT_OK(ValidateUniqueColumns(bitmap_columns, &indexed_columns));
    PAIMON_RETURN_NOT_OK(ValidateUniqueColumns(full_text_columns, &indexed_columns));

    std::vector<PrimaryKeyIndexDefinition> definitions;
    for (const DataField& field : schema.Fields()) {
        const std::string& column = field.Name();
        if (ObjectUtils::Contains(btree_columns, column)) {
            PAIMON_ASSIGN_OR_RAISE(
                IndexOptions definition_options,
                SortedIndexOptions(options, column, kBTreeOptionFamily, kBTreeAlgorithmPrefix));
            definitions.emplace_back(column, field.Id(), kBTreeIndexType,
                                     PrimaryKeyIndexDefinition::Family::BTREE,
                                     std::move(definition_options));
        } else if (ObjectUtils::Contains(bitmap_columns, column)) {
            PAIMON_ASSIGN_OR_RAISE(
                IndexOptions definition_options,
                SortedIndexOptions(options, column, kBitmapOptionFamily, kBitmapAlgorithmPrefix));
            definitions.emplace_back(column, field.Id(), kBitmapIndexType,
                                     PrimaryKeyIndexDefinition::Family::BITMAP,
                                     std::move(definition_options));
        } else if (ObjectUtils::Contains(vector_columns, column)) {
            std::string index_type;
            auto type_iter =
                options.find(fmt::format("{}{}.pk-vector.index.type", kFieldScopedPrefix, column));
            if (type_iter != options.end()) {
                index_type = type_iter->second;
            }
            definitions.emplace_back(column, field.Id(), index_type,
                                     PrimaryKeyIndexDefinition::Family::VECTOR,
                                     std::map<std::string, std::string>());
        } else if (ObjectUtils::Contains(full_text_columns, column)) {
            definitions.emplace_back(column, field.Id(), kFullTextIndexType,
                                     PrimaryKeyIndexDefinition::Family::FULL_TEXT,
                                     std::map<std::string, std::string>());
        }
    }
    return PrimaryKeyIndexDefinitions(std::move(definitions));
}

std::vector<PrimaryKeyIndexDefinition> PrimaryKeyIndexDefinitions::ScalarDefinitions() const {
    return ScalarDefinitions(definitions_);
}

std::vector<PrimaryKeyIndexDefinition> PrimaryKeyIndexDefinitions::ScalarDefinitions(
    const std::vector<PrimaryKeyIndexDefinition>& definitions) {
    std::vector<PrimaryKeyIndexDefinition> scalar_definitions;
    for (const PrimaryKeyIndexDefinition& definition : definitions) {
        if (definition.GetFamily() == PrimaryKeyIndexDefinition::Family::BTREE ||
            definition.GetFamily() == PrimaryKeyIndexDefinition::Family::BITMAP) {
            scalar_definitions.push_back(definition);
        }
    }
    return scalar_definitions;
}

}  // namespace paimon
