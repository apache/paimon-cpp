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

#include "paimon/core/core_options.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class FileSystem;
class FileStorePathFactory;
class MemoryPool;
class SchemaManager;
class TableSchema;

// TODO(xinyu.lxy): Switch to Table APIs once Table is implemented.
struct AppendDvContext {
    static Result<AppendDvContext> Create(const std::string& table_path, const std::string& branch,
                                          const std::map<std::string, std::string>& options,
                                          const std::shared_ptr<FileSystem>& file_system,
                                          const std::shared_ptr<MemoryPool>& pool);

    CoreOptions options;
    std::shared_ptr<TableSchema> schema;
    std::shared_ptr<arrow::Schema> arrow_schema;
    std::shared_ptr<SchemaManager> schema_manager;
    std::shared_ptr<FileStorePathFactory> path_factory;

 private:
    AppendDvContext() = default;
};

}  // namespace paimon
