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
#include <map>
#include <string>
#include <utility>

namespace paimon {
/// Resolved definition of one source-backed primary-key index.
class PrimaryKeyIndexDefinition {
 public:
    /// Built-in primary-key index families.
    enum class Family {
        VECTOR,
        BTREE,
        BITMAP,
        FULL_TEXT,
    };

    PrimaryKeyIndexDefinition(std::string column, int32_t field_id, std::string index_type,
                              Family family, std::map<std::string, std::string> options)
        : column_(std::move(column)),
          field_id_(field_id),
          index_type_(std::move(index_type)),
          family_(family),
          options_(std::move(options)) {}

    const std::string& Column() const {
        return column_;
    }

    int32_t FieldId() const {
        return field_id_;
    }

    const std::string& IndexType() const {
        return index_type_;
    }

    Family GetFamily() const {
        return family_;
    }

    const std::map<std::string, std::string>& Options() const {
        return options_;
    }

 private:
    std::string column_;
    int32_t field_id_;
    std::string index_type_;
    Family family_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon
