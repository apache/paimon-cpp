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

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "paimon/realtime/index/realtime_index.h"
#include "paimon/realtime/index/realtime_index_factory.h"
#include "paimon/testing/mock/mock_search_realtime_index_reader.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::test {

/// Test index that records written offsets and serves configured search results.
class MockRealtimeIndex final : public RealtimeIndex {
 public:
    MockRealtimeIndex(std::shared_ptr<arrow::Schema> schema,
                      IndexReaderCapabilities::SearchType search_type,
                      const std::shared_ptr<MockSearchState>& state);

    Status AddBatch(::ArrowArray* batch) override;

    Result<std::shared_ptr<RealtimeIndexReader>> CreateReader(
        const OffsetRange& offset_range) const override;

    uint64_t GetMemoryUsage() const override;

 private:
    std::shared_ptr<arrow::Schema> schema_;
    IndexReaderCapabilities::SearchType search_type_;
    std::shared_ptr<MockSearchState> state_;
    mutable std::mutex mutex_;
    RoaringBitmap64 offsets_;
};

/// Register with FactoryCreator under Identifier(), then configure
/// realtime.index.<identifier>.columns to use mock search with real Arrow segments.
class MockRealtimeIndexFactory final : public RealtimeIndexFactory {
 public:
    MockRealtimeIndexFactory(std::string identifier,
                             IndexReaderCapabilities::SearchType search_type,
                             const std::shared_ptr<MockSearchState>& state);

    const char* Identifier() const override;

    Result<std::shared_ptr<RealtimeIndex>> Create(
        ::ArrowSchema* write_schema, const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    std::string identifier_;
    IndexReaderCapabilities::SearchType search_type_;
    std::shared_ptr<MockSearchState> state_;
};

}  // namespace paimon::test
