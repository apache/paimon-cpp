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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/catalog/table.h"
#include "paimon/core/catalog/version_managed_catalog.h"
#include "paimon/core/partition/partition_statistics.h"
#include "paimon/core/snapshot.h"
#include "paimon/result.h"
#include "paimon/schema/schema.h"
#include "paimon/status.h"

namespace paimon::test {

class MockVersionManagedCatalog : public Catalog, public VersionManagedCatalog {
 public:
    struct CommitCall {
        Identifier identifier{"", ""};
        std::optional<std::string> table_uuid;
        std::optional<std::string> base_snapshot_uuid;
        Snapshot snapshot;
        std::vector<PartitionStatistics> statistics;
    };

    explicit MockVersionManagedCatalog(Result<bool> answer = true,
                                       const std::string& table_uuid = "mock-table-uuid")
        : answer_(std::move(answer)), table_uuid_(table_uuid) {}

    bool SupportsVersionManagement() const override {
        return supports_version_management_;
    }

    void SetSupportsVersionManagement(bool supports_version_management) {
        supports_version_management_ = supports_version_management;
    }

    Result<bool> CommitSnapshot(const Identifier& identifier,
                                const std::optional<std::string>& table_uuid,
                                const std::optional<std::string>& base_snapshot_uuid,
                                const Snapshot& snapshot,
                                const std::vector<PartitionStatistics>& statistics) override {
        commit_calls_.push_back(
            CommitCall{identifier, table_uuid, base_snapshot_uuid, snapshot, statistics});
        if (check_table_uuid_ && table_uuid && table_uuid.value() != table_uuid_) {
            return Status::NotExist("no table with id " + table_uuid.value());
        }
        Result<bool> answer = answer_;
        if (check_base_snapshot_uuid_) {
            answer = base_snapshot_uuid == current_snapshot_uuid_;
            if (answer.value()) {
                current_snapshot_uuid_ = snapshot.Uuid();
                accepted_.push_back(snapshot);
            }
        }
        if (on_commit_) {
            on_commit_();
        }
        if (!lost_answer_.ok() && answer.ok() && answer.value()) {
            return lost_answer_;
        }
        return answer;
    }

    void SetLostAnswer(const Status& status) {
        lost_answer_ = status;
    }

    Result<std::shared_ptr<Table>> GetTable(const Identifier& identifier) const override {
        ++get_table_calls_;
        return std::make_shared<Table>(table_schema_, identifier.GetDatabaseName(),
                                       identifier.GetTableName(), table_uuid_);
    }

    void SetTableSchema(const std::shared_ptr<Schema>& table_schema) {
        table_schema_ = table_schema;
    }

    Result<std::shared_ptr<Schema>> LoadTableSchema(const Identifier& identifier) const override {
        load_table_schema_identifiers_.push_back(identifier);
        if (table_schema_ == nullptr) {
            return Status::NotExist(identifier.ToString() + " not exist");
        }
        return table_schema_;
    }

    const std::vector<Identifier>& LoadTableSchemaIdentifiers() const {
        return load_table_schema_identifiers_;
    }

    size_t LoadTableSchemaCalls() const {
        return load_table_schema_identifiers_.size();
    }

    Result<std::optional<Snapshot>> LoadSnapshot(const Identifier& identifier) const override {
        load_snapshot_identifiers_.push_back(identifier);
        if (!serve_snapshots_) {
            return Status::NotImplemented("this mock does not serve snapshots");
        }
        if (!load_snapshot_status_.ok()) {
            return load_snapshot_status_;
        }
        if (accepted_.empty()) {
            return std::optional<Snapshot>();
        }
        return std::optional<Snapshot>(accepted_.back());
    }

    void SetLoadSnapshotStatus(const Status& status) {
        serve_snapshots_ = true;
        load_snapshot_status_ = status;
    }

    const std::vector<Identifier>& LoadSnapshotIdentifiers() const {
        return load_snapshot_identifiers_;
    }

    size_t LoadSnapshotCalls() const {
        return load_snapshot_identifiers_.size();
    }

    void CheckTableUuid() {
        check_table_uuid_ = true;
    }

    void CheckBaseSnapshotUuid() {
        check_base_snapshot_uuid_ = true;
        serve_snapshots_ = true;
    }

    const std::vector<Snapshot>& AcceptedSnapshots() const {
        return accepted_;
    }

    void SetHeldSnapshot(const Snapshot& snapshot) {
        serve_snapshots_ = true;
        current_snapshot_uuid_ = snapshot.Uuid();
        accepted_.push_back(snapshot);
    }

    void SetAnswer(Result<bool> answer) {
        answer_ = std::move(answer);
    }

    void SetOnCommit(std::function<void()> on_commit) {
        on_commit_ = std::move(on_commit);
    }

    const std::vector<CommitCall>& CommitCalls() const {
        return commit_calls_;
    }

    size_t GetTableCalls() const {
        return get_table_calls_;
    }

    Status CreateDatabase(const std::string&, const std::map<std::string, std::string>&,
                          bool) override {
        return Unsupported();
    }
    Status CreateTable(const Identifier&, ArrowSchema*, const std::vector<std::string>&,
                       const std::vector<std::string>&, const std::map<std::string, std::string>&,
                       bool) override {
        return Unsupported();
    }
    Result<std::vector<std::string>> ListDatabases() const override {
        return Unsupported();
    }
    Result<std::vector<std::string>> ListTables(const std::string&) const override {
        return Unsupported();
    }
    Status DropDatabase(const std::string&, bool, bool) override {
        return Unsupported();
    }
    Status DropTable(const Identifier&, bool) override {
        return Unsupported();
    }
    Status RenameTable(const Identifier&, const Identifier&, bool) override {
        return Unsupported();
    }
    Result<bool> DatabaseExists(const std::string&) const override {
        return Unsupported();
    }
    Result<bool> TableExists(const Identifier&) const override {
        return Unsupported();
    }
    Result<std::string> GetDatabaseLocation(const std::string&) const override {
        return Unsupported();
    }
    Result<std::string> GetTableLocation(const Identifier&) const override {
        return Unsupported();
    }
    std::string GetRootPath() const override {
        return "";
    }
    void SetFileSystem(const std::shared_ptr<FileSystem>& file_system) {
        file_system_ = file_system;
    }

    std::shared_ptr<FileSystem> GetFileSystem() const override {
        return file_system_;
    }

    /// Records what was asked and serves the per-table file system when one was set, so a
    /// test can tell it apart from the catalog-wide one; falls back to `GetFileSystem()`,
    /// matching the base default, when none was set.
    Result<std::shared_ptr<FileSystem>> GetTableFileSystem(
        const Identifier& identifier) const override {
        table_file_system_requests_.push_back(identifier);
        if (table_file_system_ != nullptr) {
            return table_file_system_;
        }
        return GetFileSystem();
    }

    void SetTableFileSystem(const std::shared_ptr<FileSystem>& file_system) {
        table_file_system_ = file_system;
    }

    const std::vector<Identifier>& TableFileSystemRequests() const {
        return table_file_system_requests_;
    }

    const std::map<std::string, std::string>& GetOptions() const override {
        return options_;
    }
    Result<std::vector<SnapshotInfo>> ListSnapshots(const Identifier&,
                                                    const std::string&) const override {
        return Unsupported();
    }

 private:
    static Status Unsupported() {
        return Status::NotImplemented("not used by this mock");
    }

    Result<bool> answer_;
    std::string table_uuid_;
    std::shared_ptr<Schema> table_schema_;
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<FileSystem> table_file_system_;
    mutable std::vector<Identifier> table_file_system_requests_;
    bool supports_version_management_ = true;
    std::function<void()> on_commit_;
    bool check_table_uuid_ = false;
    bool check_base_snapshot_uuid_ = false;
    Status lost_answer_;
    std::optional<std::string> current_snapshot_uuid_;
    std::vector<Snapshot> accepted_;
    mutable size_t get_table_calls_ = 0;
    mutable std::vector<Identifier> load_table_schema_identifiers_;
    mutable std::vector<Identifier> load_snapshot_identifiers_;
    bool serve_snapshots_ = false;
    Status load_snapshot_status_;
    std::vector<CommitCall> commit_calls_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon::test
