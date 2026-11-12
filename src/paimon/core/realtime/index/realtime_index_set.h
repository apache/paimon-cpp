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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "paimon/defs.h"
#include "paimon/realtime/offset_range.h"
#include "paimon/result.h"

struct ArrowArray;

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class MemoryPool;
class RealtimeIndex;

/// Shared per-segment indexes with fixed field bindings.
class RealtimeIndexSet {
 public:
    /// Configure realtime.index.<plugin>.columns=field1,field2.
    /// Example: realtime.index.hash.columns=id,value.
    static const char kIndexPrefix[];
    static const char kColumnsSuffix[];

    static bool IsEnabled(const std::map<std::string, std::string>& options);

    static Result<std::shared_ptr<RealtimeIndexSet>> Create(
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    /// Consumes a store batch and dispatches offset/field pairs to indexes.
    Status AddBatch(::ArrowArray* batch, const OffsetRange& offset_range);

    std::vector<std::shared_ptr<RealtimeIndex>> FindIndexes(const std::string& field_name,
                                                            FieldType field_type) const;

    std::vector<std::shared_ptr<RealtimeIndex>> FindIndexes(const std::string& field_name) const;

    uint64_t GetMemoryUsage() const;

 private:
    struct Entry {
        int32_t position;
        FieldType type;
        std::shared_ptr<RealtimeIndex> index;
    };

    RealtimeIndexSet(std::shared_ptr<arrow::Schema> schema,
                     std::multimap<std::string, Entry>&& indexes)
        : schema_(std::move(schema)), indexes_(std::move(indexes)) {}

    std::shared_ptr<arrow::Schema> schema_;
    std::multimap<std::string, Entry> indexes_;
    int64_t end_offset_ = 0;
};

}  // namespace paimon
