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

#include "paimon/realtime/realtime_store.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class MemoryPool;

/// Internal in-memory implementation of the default primary-key `RealtimeStore`.
class PrimaryKeyRealtimeStore final : public RealtimeStore {
 public:
    static Result<std::shared_ptr<PrimaryKeyRealtimeStore>> Create(
        const std::shared_ptr<arrow::Schema>& transport_schema,
        const std::shared_ptr<MemoryPool>& memory_pool);

    ~PrimaryKeyRealtimeStore() override;

    Status Write(RealtimeWriteBatch&& batch) override;
    Result<std::optional<std::shared_ptr<RealtimeSegmentHandle>>> SealForCommit() override;
    Result<std::vector<std::unique_ptr<BatchReader>>> CreateCommitReaders(
        const std::shared_ptr<RealtimeSegmentHandle>& segment) override;
    Result<std::shared_ptr<RealtimeReadView>> AcquireReadView() override;
    Result<std::vector<std::unique_ptr<BatchReader>>> CreateQueryReaders(
        const std::shared_ptr<RealtimeReadView>& view, int64_t offset_begin,
        const RealtimeQueryContext& context) override;
    Status AdvanceCommittedOffset(int64_t committed_end_offset) override;
    uint64_t GetMemoryUsage() const override;

 private:
    class Impl;
    explicit PrimaryKeyRealtimeStore(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
