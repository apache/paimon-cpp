/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <memory>
#include <string>

#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

namespace paimon {

class Schema;

/// The managed table a catalog hands out: one with snapshots and manifests, which the schema under
/// its root path describes. A format table - a directory of plain data files - is a `FormatTable`,
/// the other kind of `Table`.
///
/// Its `NewScan`/`NewRead`/`NewWrite`/`NewCommit` build the operators here, from the context plus
/// the schema the table was constructed with, so they serve the table as it was loaded. The
/// context-only entry points - `TableScan::Create()`, `TableRead::Create()`,
/// `FileStoreWrite::Create()` and `FileStoreCommit::Create()` - load the table the context names
/// and hand the context to that table's `NewXxx`, which is how a context built without a table
/// reaches the same operators.
class FileStoreTable : public Table {
 public:
    /// The constructor a catalog builds a managed table through, so a `shared_ptr` can hold one;
    /// the base constructor is protected, which an inherited constructor would stay.
    /// @param file_system File system the table's data is accessed through.
    /// @param root_path Root path of the table in the file system.
    /// @param uuid Metastore UUID, or empty if unavailable.
    FileStoreTable(const std::shared_ptr<FileSystem>& file_system, const std::string& root_path,
                   const std::shared_ptr<Schema>& schema, const Identifier& identifier,
                   const std::string& uuid = "")
        : Table(file_system, root_path, schema, identifier, uuid) {}

    std::string Name() const override {
        return identifier_.GetObjectName();
    }

    std::string FullName() const override {
        return identifier_.GetFullName();
    }

    Result<std::unique_ptr<TableScan>> NewScan(std::unique_ptr<ScanContext> context) const override;

    Result<std::unique_ptr<TableRead>> NewRead(std::unique_ptr<ReadContext> context) const override;

    Result<std::unique_ptr<FileStoreWrite>> NewWrite(
        std::unique_ptr<WriteContext> context) const override;

    Result<std::unique_ptr<FileStoreCommit>> NewCommit(
        std::unique_ptr<CommitContext> context) const override;
};

}  // namespace paimon
