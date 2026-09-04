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

#include "arrow/type_fwd.h"
#include "paimon/realtime/realtime_store.h"
#include "paimon/result.h"

namespace paimon {

/// Defines the Arrow schemas at each real-time read and write boundary.
class RealtimeSchemaLayout {
 public:
    /// Creates all real-time boundary schemas for the requested store mode.
    /// User fields retain their original order after any internal field prefix.
    static Result<std::unique_ptr<RealtimeSchemaLayout>> Create(
        RealtimeStoreMode mode, const std::shared_ptr<arrow::Schema>& user_schema);

    RealtimeSchemaLayout(const RealtimeSchemaLayout&) = delete;
    RealtimeSchemaLayout& operator=(const RealtimeSchemaLayout&) = delete;

    /// Append-only fields: [user fields].
    /// Primary-key fields: [user fields].
    const std::shared_ptr<arrow::Schema>& UserSchema() const {
        return user_schema_;
    }

    /// Append-only fields: [_REALTIME_OFFSET, user fields].
    /// Primary-key fields: [_REALTIME_OFFSET, user fields].
    const std::shared_ptr<arrow::Schema>& InputSchema() const {
        return input_schema_;
    }

    /// Append-only fields: [_REALTIME_OFFSET, user fields].
    /// Primary-key fields: [_SEQUENCE_NUMBER, _VALUE_KIND, _REALTIME_OFFSET, user fields].
    const std::shared_ptr<arrow::Schema>& StoreWriteSchema() const {
        return store_write_schema_;
    }

    /// Append-only fields: [_VALUE_KIND, _REALTIME_OFFSET, user fields].
    /// Primary-key fields: [_SEQUENCE_NUMBER, _VALUE_KIND, _REALTIME_OFFSET, user fields].
    const std::shared_ptr<arrow::Schema>& StoreCommitSchema() const {
        return store_commit_schema_;
    }

    /// Append-only fields: [user fields].
    /// Primary-key fields: [_SEQUENCE_NUMBER, _VALUE_KIND, user fields].
    const std::shared_ptr<arrow::Schema>& CommitSchema() const {
        return commit_schema_;
    }

    /// Append-only fields: [_VALUE_KIND, user fields].
    /// Primary-key fields: [_SEQUENCE_NUMBER, _VALUE_KIND, user fields].
    const std::shared_ptr<arrow::Schema>& QuerySchema() const {
        return query_schema_;
    }

 private:
    RealtimeSchemaLayout(RealtimeStoreMode mode, const std::shared_ptr<arrow::Schema>& user_schema);

    std::shared_ptr<arrow::Schema> user_schema_;
    std::shared_ptr<arrow::Schema> input_schema_;
    std::shared_ptr<arrow::Schema> store_write_schema_;
    std::shared_ptr<arrow::Schema> store_commit_schema_;
    std::shared_ptr<arrow::Schema> commit_schema_;
    std::shared_ptr<arrow::Schema> query_schema_;
};

}  // namespace paimon
