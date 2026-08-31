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

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Array;
class DictionaryArray;
}  // namespace arrow

namespace paimon {
/// An immutable, type specialized lookup structure built once from the literals of an `IN` /
/// `NOT IN` predicate.
///
/// `MultiLiteralsLeafFunction` materializes the whole column into `Literal` objects (one heap
/// allocation per row) and then linearly scans the literals for every row, which is
/// `O(rows * literals)` with a heap allocation per row. `LiteralSet` is built once when the
/// predicate is constructed and probes an arrow array in `O(rows)` without allocating per row.
///
/// The class only covers the types where a bitwise / byte-wise equality is exactly equivalent to
/// `Literal::CompareTo(...) == 0`. Everything else makes `CreateOrNull` return `nullptr` so that
/// the caller falls back to `In` / `NotIn`.
class LiteralSet {
 public:
    /// Builds a lookup structure for `literals` of a column typed `field_type`.
    ///
    /// @return `nullptr` when the combination is not supported, in which case the caller must fall
    ///         back to the generic `LeafFunction` implementation. This never fails.
    static std::shared_ptr<const LiteralSet> CreateOrNull(FieldType field_type,
                                                          const std::vector<Literal>& literals);

    /// Checks whether `array` maps to the exact `FieldType` this set was built for. The mapping is
    /// identical to `LiteralConverter::ConvertLiteralsFromArray`, so a `false` result means the
    /// fallback path would have produced a type mismatch error (or would not support the array at
    /// all) and must be taken to keep the error behavior unchanged.
    bool MatchesArrowType(const arrow::Array& array) const;

    /// Probes every non-null row of `array` against this set.
    ///
    /// @param negate `false` for `IN` semantics, `true` for `NOT IN` semantics.
    /// @param out Must be sized `array.length()` with all elements pre-set to 0. Only non-null rows
    ///            are written, so null rows stay 0 (`IN` and `NOT IN` are both false on null).
    Status TestArray(const arrow::Array& array, bool negate, std::vector<char>* out) const;

    /// Probes a single value against this set. `value` must not be of a different type than the one
    /// this set was built for.
    Result<bool> TestValue(const Literal& value, bool negate) const;

 private:
    explicit LiteralSet(FieldType field_type) : field_type_(field_type) {}
    // `binary_set_` views into `binary_storage_`, so copying would leave dangling views.
    LiteralSet(const LiteralSet&) = delete;
    LiteralSet& operator=(const LiteralSet&) = delete;

    // Each builder returns false when the literals cannot be represented, which turns into a
    // `nullptr` from `CreateOrNull`.
    bool BuildIntegers(const std::vector<Literal>& literals);
    bool BuildBooleans(const std::vector<Literal>& literals);
    bool BuildBinaries(const std::vector<Literal>& literals);

    bool ContainsInteger(int64_t value) const {
        if (value < min_ || value > max_) {
            return false;
        }
        if (!dense_.empty()) {
            return dense_[static_cast<size_t>(value - min_)];
        }
        return sparse_.find(value) != sparse_.end();
    }

    bool ContainsBinary(std::string_view value) const {
        if (value.size() < min_length_ || value.size() > max_length_) {
            return false;
        }
        if (value.empty()) {
            return has_empty_binary_;
        }
        const auto first_byte = static_cast<uint8_t>(value[0]);
        if ((first_byte_bitmap_[first_byte >> 6] & (1ULL << (first_byte & 63))) == 0) {
            return false;
        }
        return binary_set_.find(value) != binary_set_.end();
    }

    void TestBooleanArray(const arrow::Array& array, bool negate, std::vector<char>* out) const;

    template <typename ArrayType>
    void TestIntegerArray(const arrow::Array& array, bool negate, std::vector<char>* out) const;

    template <typename ArrayType>
    void TestBinaryArray(const arrow::Array& array, bool negate, std::vector<char>* out) const;

    template <typename DictArrayType, typename IndicesArrayType>
    void TestDictionaryArray(const arrow::DictionaryArray& dict_array, bool negate,
                             std::vector<char>* out) const;

    FieldType field_type_;
    // Whether the literals contain a null, which makes `NOT IN` false for every row.
    bool has_null_literal_ = false;

    // Integer family (TINYINT, SMALLINT, INT, BIGINT, DATE), all widened to int64_t. `min_` /
    // `max_` reject out of range values without touching the set. Exactly one of `dense_` /
    // `sparse_` is populated: `dense_` indexes by `value - min_` and needs no hashing at all.
    int64_t min_ = 0;
    int64_t max_ = 0;
    std::vector<bool> dense_;
    std::unordered_set<int64_t> sparse_;

    // BOOLEAN.
    bool has_true_ = false;
    bool has_false_ = false;

    // STRING / BINARY. `binary_storage_` owns the bytes (a deque never invalidates references on
    // growth) and `binary_set_` views into it. Lengths and the first byte bitmap reject
    // non-candidates before hashing.
    std::deque<std::string> binary_storage_;
    std::unordered_set<std::string_view> binary_set_;
    size_t min_length_ = 0;
    size_t max_length_ = 0;
    std::array<uint64_t, 4> first_byte_bitmap_ = {0, 0, 0, 0};
    bool has_empty_binary_ = false;
};
}  // namespace paimon
