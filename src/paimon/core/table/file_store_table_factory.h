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

#include <memory>

#include "paimon/result.h"

namespace paimon {

class CommitContext;
class ReadContext;
class ScanContext;
class Table;
class WriteContext;

/// Loads the table a context names, as Java Paimon's `FileStoreTableFactory` does: the format
/// table the builder was handed, the system table behind a `T$system` path, or the managed table
/// the schema under the context's path describes. The static `Create` of all four operators asks
/// the same question, so one place answers it and they cannot disagree.
class FileStoreTableFactory {
 public:
    FileStoreTableFactory() = delete;
    ~FileStoreTableFactory() = delete;

    static Result<std::shared_ptr<Table>> Create(const ScanContext& context);

    static Result<std::shared_ptr<Table>> Create(const ReadContext& context);

    static Result<std::shared_ptr<Table>> Create(const WriteContext& context);

    static Result<std::shared_ptr<Table>> Create(const CommitContext& context);
};

}  // namespace paimon
