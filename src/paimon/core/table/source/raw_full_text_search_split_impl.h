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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/table/source/full_text_search_split.h"
#include "paimon/utils/range.h"

namespace paimon {
/// Carries the index type so a raw split can be read independently of its plan's index splits.
class RawFullTextSearchSplitImpl : public RawFullTextSearchSplit {
 public:
    /// "PFTRAWS1" in ASCII.
    static constexpr int64_t MAGIC = 0x5046545241575331L;
    /// Version 2 adds the text field ID. Version 3 adds the planning schema ID.
    static constexpr int32_t VERSION = 3;

    static Result<std::shared_ptr<RawFullTextSearchSplitImpl>> Create(
        int64_t snapshot_id, const std::string& column_name, const std::string& index_type,
        std::vector<Range> row_ranges, std::optional<int32_t> field_id,
        std::optional<int64_t> schema_id = std::nullopt);

    int64_t SnapshotId() const override {
        return snapshot_id_;
    }

    const std::string& ColumnName() const override {
        return column_name_;
    }

    const std::vector<Range>& RowRanges() const override {
        return row_ranges_;
    }

    const std::string& IndexType() const {
        return index_type_;
    }

    std::optional<int32_t> FieldId() const {
        return field_id_;
    }

    std::optional<int64_t> SchemaId() const {
        return schema_id_;
    }

    bool operator==(const RawFullTextSearchSplitImpl& other) const;

    std::string ToString() const;

 private:
    RawFullTextSearchSplitImpl(int64_t snapshot_id, const std::string& column_name,
                               const std::string& index_type, std::vector<Range> row_ranges,
                               std::optional<int32_t> field_id, std::optional<int64_t> schema_id)
        : snapshot_id_(snapshot_id),
          column_name_(column_name),
          index_type_(index_type),
          row_ranges_(std::move(row_ranges)),
          field_id_(field_id),
          schema_id_(schema_id) {}

    int64_t snapshot_id_;
    std::string column_name_;
    std::string index_type_;
    std::vector<Range> row_ranges_;
    std::optional<int32_t> field_id_;
    std::optional<int64_t> schema_id_;
};

}  // namespace paimon
