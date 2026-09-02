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

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/snapshot.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/snapshot_read_view.h"

namespace paimon {

class SnapshotReadViewImpl final : public SnapshotReadView {
 public:
    static std::shared_ptr<const SnapshotReadView> Create(
        const std::string& table_path, const std::string& branch, std::optional<Snapshot> snapshot,
        std::shared_ptr<TableSchema> table_schema) {
        std::shared_ptr<const Snapshot> immutable_snapshot;
        if (snapshot) {
            immutable_snapshot = std::make_shared<const Snapshot>(std::move(snapshot).value());
        }
        return std::shared_ptr<const SnapshotReadView>(new SnapshotReadViewImpl(
            table_path, branch, std::move(immutable_snapshot), std::move(table_schema)));
    }

    static Status ValidateBinding(const std::shared_ptr<const SnapshotReadView>& read_view,
                                  const std::string& table_path, const std::string& branch) {
        if (!read_view) {
            return Status::Invalid("snapshot read view is null");
        }
        if (read_view->TablePath() != table_path) {
            return Status::Invalid(
                fmt::format("snapshot read view is bound to table '{}', not '{}'",
                            read_view->TablePath(), table_path));
        }
        if (read_view->Branch() != branch) {
            return Status::Invalid(
                fmt::format("snapshot read view is bound to branch '{}', not '{}'",
                            read_view->Branch(), branch));
        }
        return Status::OK();
    }

    static Result<std::shared_ptr<const Snapshot>> GetSnapshot(
        const std::shared_ptr<const SnapshotReadView>& read_view) {
        auto impl = std::dynamic_pointer_cast<const SnapshotReadViewImpl>(read_view);
        if (!impl) {
            return Status::Invalid("unsupported snapshot read view implementation");
        }
        return impl->snapshot_;
    }

    static Result<std::shared_ptr<TableSchema>> GetTableSchema(
        const std::shared_ptr<const SnapshotReadView>& read_view) {
        auto impl = std::dynamic_pointer_cast<const SnapshotReadViewImpl>(read_view);
        if (!impl) {
            return Status::Invalid("unsupported snapshot read view implementation");
        }
        if (!impl->table_schema_) {
            return Status::Invalid("snapshot read view has no table schema");
        }
        return impl->table_schema_;
    }

    const std::string& TablePath() const override {
        return table_path_;
    }

    const std::string& Branch() const override {
        return branch_;
    }

    std::optional<int64_t> SnapshotId() const override {
        return snapshot_ ? std::optional<int64_t>(snapshot_->Id()) : std::nullopt;
    }

 private:
    SnapshotReadViewImpl(const std::string& table_path, const std::string& branch,
                         std::shared_ptr<const Snapshot> snapshot,
                         std::shared_ptr<TableSchema> table_schema)
        : table_path_(table_path),
          branch_(branch),
          snapshot_(std::move(snapshot)),
          table_schema_(std::move(table_schema)) {}

    std::string table_path_;
    std::string branch_;
    std::shared_ptr<const Snapshot> snapshot_;
    // TableSchema exposes no mutators. Keeping the parsed instance alongside the snapshot lets a
    // reused view rebuild scan objects without listing and rereading the schema directory.
    std::shared_ptr<TableSchema> table_schema_;
};

}  // namespace paimon
