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

#include "paimon/catalog/table_load_options.h"
#include "paimon/result.h"

namespace paimon {
class Table;

/// Loads the table used by a static operator factory.
class TableFactory {
 public:
    TableFactory() = delete;
    ~TableFactory() = delete;

    static Result<std::shared_ptr<Table>> Create(
        const TableLoadOptions& load_options,
        const std::map<std::string, std::string>& dynamic_options, const char* operation);
};
}  // namespace paimon
