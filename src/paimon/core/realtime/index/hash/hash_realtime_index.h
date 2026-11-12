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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/utils/concurrent_hash_map.h"
#include "paimon/realtime/index/realtime_index.h"
#include "paimon/realtime/index/realtime_index_factory.h"
#include "paimon/realtime/index/realtime_index_reader.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;
enum class FieldType;

/// Single-writer, multiple-reader hash index: TBB map -> concurrent_vector postings.
/// Atomic lengths publish initialized prefixes without copying.
// TODO(xinyu.lxy): Replace the temporary layout with more lock-friendly data structures.
class HashRealtimeIndex final : public RealtimeIndex,
                                public std::enable_shared_from_this<HashRealtimeIndex> {
 public:
    static Result<std::shared_ptr<RealtimeIndex>> Create(
        const std::shared_ptr<arrow::Schema>& schema, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(::ArrowArray* batch) override;

    Result<std::shared_ptr<RealtimeIndexReader>> CreateReader(
        const OffsetRange& offset_range) const override;

    uint64_t GetMemoryUsage() const override;

 private:
    struct Posting;
    class Reader final : public RealtimeIndexReader {
     public:
        Reader(std::shared_ptr<const HashRealtimeIndex> index, const OffsetRange& offset_range);

        Result<std::optional<RoaringBitmap64>> VisitEqual(const Literal& literal) override;
        Result<std::optional<RoaringBitmap64>> VisitIn(
            const std::vector<Literal>& literals) override;

     private:
        std::shared_ptr<const HashRealtimeIndex> index_;
        OffsetRange offset_range_;
    };
    using PostingMap = ConcurrentHashMap<uint64_t, std::shared_ptr<Posting>>;

    HashRealtimeIndex(std::shared_ptr<arrow::Schema> schema, FieldType field_type);

    std::shared_ptr<arrow::Schema> schema_;
    FieldType field_type_;
    PostingMap postings_;
    std::atomic<uint64_t> posting_memory_usage_{0};
};

class HashRealtimeIndexFactory final : public RealtimeIndexFactory {
 public:
    const char* Identifier() const override;

    Result<std::shared_ptr<RealtimeIndex>> Create(
        ::ArrowSchema* write_schema, const std::map<std::string, std::string>& options,
        const std::shared_ptr<MemoryPool>& pool) const override;
};

}  // namespace paimon
