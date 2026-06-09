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
#include <string>
#include <string_view>

#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"

namespace paimon {
class Bytes;
class InternalArray;
class InternalMap;
class RowKind;

/// A `InternalRow` to wrap row with offset.
class OffsetRow : public InternalRow {
 public:
    OffsetRow(const InternalRow& row, int32_t arity, int32_t offset)
        : row_(row), arity_(arity), offset_(offset) {}

    int32_t GetFieldCount() const override {
        return arity_;
    }

    Result<const RowKind*> GetRowKind() const override {
        return row_.GetRowKind();
    }

    void SetRowKind(const RowKind* kind) override {}

    bool IsNullAt(int32_t pos) const override {
        return row_.IsNullAt(offset_ + pos);
    }

    bool GetBoolean(int32_t pos) const override {
        return row_.GetBoolean(offset_ + pos);
    }

    char GetByte(int32_t pos) const override {
        return row_.GetByte(offset_ + pos);
    }

    int16_t GetShort(int32_t pos) const override {
        return row_.GetShort(offset_ + pos);
    }

    int32_t GetInt(int32_t pos) const override {
        return row_.GetInt(offset_ + pos);
    }

    int32_t GetDate(int32_t pos) const override {
        return row_.GetDate(offset_ + pos);
    }

    int64_t GetLong(int32_t pos) const override {
        return row_.GetLong(offset_ + pos);
    }

    float GetFloat(int32_t pos) const override {
        return row_.GetFloat(offset_ + pos);
    }

    double GetDouble(int32_t pos) const override {
        return row_.GetDouble(offset_ + pos);
    }

    BinaryString GetString(int32_t pos) const override {
        return row_.GetString(offset_ + pos);
    }

    std::string_view GetStringView(int32_t pos) const override {
        return row_.GetStringView(offset_ + pos);
    }

    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override {
        return row_.GetDecimal(offset_ + pos, precision, scale);
    }

    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override {
        return row_.GetTimestamp(offset_ + pos, precision);
    }

    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override {
        return row_.GetBinary(offset_ + pos);
    }

    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override {
        return row_.GetArray(offset_ + pos);
    }

    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        return row_.GetMap(offset_ + pos);
    }

    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override {
        return row_.GetRow(offset_ + pos, num_fields);
    }

    std::string ToString() const override {
        return fmt::format("OffsetRow, arity {}, offset {}", arity_, offset_);
    }

 private:
    const InternalRow& row_;
    int32_t arity_;
    int32_t offset_;
};
}  // namespace paimon
