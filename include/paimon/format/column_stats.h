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

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/visibility.h"

namespace paimon {
/// ColumnStats is an abstract base class that represents statistical information for data columns
/// in Paimon tables. It provides min/max values and null count statistics
///
/// Only primitive data types support min/max statistics. Nested types (arrays, vectors, maps,
/// structs) only track null counts through `NestedColumnStats`.
///
/// @note This is an abstract base class. Use the static factory methods `CreateXXXColumnStats()` to
/// create concrete instances for specific data types.
class PAIMON_EXPORT ColumnStats {
 public:
    virtual ~ColumnStats() = default;

    /// Gets the number of null values in this column.
    virtual std::optional<int64_t> NullCount() const = 0;
    virtual std::string ToString() const = 0;

    /// Gets the field type that this column statistics instance represents.
    virtual FieldType GetFieldType() const = 0;

    /// @name CreateXXXColumnStats()
    /// %Factory methods `CreateXXXColumnStats()` to create column statistics.
    /// - min/max/null_count for primitive data types
    /// - null_count for nested data types (arrays, vectors, maps, structs)
    ///
    /// @{
    static std::unique_ptr<ColumnStats> CreateBooleanColumnStats(std::optional<bool> min,
                                                                 std::optional<bool> max,
                                                                 std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateTinyIntColumnStats(std::optional<int8_t> min,
                                                                 std::optional<int8_t> max,
                                                                 std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateSmallIntColumnStats(
        std::optional<int16_t> min, std::optional<int16_t> max, std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateIntColumnStats(std::optional<int32_t> min,
                                                             std::optional<int32_t> max,
                                                             std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateBigIntColumnStats(std::optional<int64_t> min,
                                                                std::optional<int64_t> max,
                                                                std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateFloatColumnStats(std::optional<float> min,
                                                               std::optional<float> max,
                                                               std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateDoubleColumnStats(std::optional<double> min,
                                                                std::optional<double> max,
                                                                std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateStringColumnStats(
        const std::optional<std::string>& min, const std::optional<std::string>& max,
        std::optional<int64_t> null_count);
    static std::unique_ptr<ColumnStats> CreateTimestampColumnStats(
        const std::optional<Timestamp>& min, const std::optional<Timestamp>& max,
        std::optional<int64_t> null_count, int32_t precision);
    static std::unique_ptr<ColumnStats> CreateDecimalColumnStats(const std::optional<Decimal>& min,
                                                                 const std::optional<Decimal>& max,
                                                                 std::optional<int64_t> null_count,
                                                                 int32_t precision, int32_t scale);
    static std::unique_ptr<ColumnStats> CreateDateColumnStats(std::optional<int32_t> min,
                                                              std::optional<int32_t> max,
                                                              std::optional<int64_t> null_count);
    /// Creates column statistics for nested data types (arrays, vectors, maps, structs), which only
    /// track null counts.
    static std::unique_ptr<ColumnStats> CreateNestedColumnStats(const FieldType& nested_type,
                                                                std::optional<int64_t> null_count);
    /// @}
};

class PAIMON_EXPORT BooleanColumnStats : public ColumnStats {
 public:
    virtual std::optional<bool> Min() const = 0;
    virtual std::optional<bool> Max() const = 0;
    virtual void Collect(std::optional<bool> value) = 0;
};

class PAIMON_EXPORT TinyIntColumnStats : public ColumnStats {
 public:
    virtual std::optional<int8_t> Min() const = 0;
    virtual std::optional<int8_t> Max() const = 0;
    virtual void Collect(std::optional<int8_t> value) = 0;
};

class PAIMON_EXPORT SmallIntColumnStats : public ColumnStats {
 public:
    virtual std::optional<int16_t> Min() const = 0;
    virtual std::optional<int16_t> Max() const = 0;
    virtual void Collect(std::optional<int16_t> value) = 0;
};

class PAIMON_EXPORT IntColumnStats : public ColumnStats {
 public:
    virtual std::optional<int32_t> Min() const = 0;
    virtual std::optional<int32_t> Max() const = 0;
    virtual void Collect(std::optional<int32_t> value) = 0;
};

class PAIMON_EXPORT BigIntColumnStats : public ColumnStats {
 public:
    virtual std::optional<int64_t> Min() const = 0;
    virtual std::optional<int64_t> Max() const = 0;
    virtual void Collect(std::optional<int64_t> value) = 0;
};

class PAIMON_EXPORT FloatColumnStats : public ColumnStats {
 public:
    virtual std::optional<float> Min() const = 0;
    virtual std::optional<float> Max() const = 0;
    virtual void Collect(std::optional<float> value) = 0;
};

class PAIMON_EXPORT DoubleColumnStats : public ColumnStats {
 public:
    virtual std::optional<double> Min() const = 0;
    virtual std::optional<double> Max() const = 0;
    virtual void Collect(std::optional<double> value) = 0;
};

class PAIMON_EXPORT StringColumnStats : public ColumnStats {
 public:
    virtual const std::optional<std::string>& Min() const = 0;
    virtual const std::optional<std::string>& Max() const = 0;
    virtual void Collect(const std::optional<std::string>& value) = 0;
};

class PAIMON_EXPORT TimestampColumnStats : public ColumnStats {
 public:
    virtual std::optional<Timestamp> Min() const = 0;
    virtual std::optional<Timestamp> Max() const = 0;
    virtual void Collect(const std::optional<Timestamp>& value) = 0;
    virtual int32_t GetPrecision() const = 0;
};

class PAIMON_EXPORT DecimalColumnStats : public ColumnStats {
 public:
    virtual std::optional<Decimal> Min() const = 0;
    virtual std::optional<Decimal> Max() const = 0;
    virtual void Collect(const std::optional<Decimal>& value) = 0;
    virtual int32_t GetPrecision() const = 0;
    virtual int32_t GetScale() const = 0;
};

class PAIMON_EXPORT DateColumnStats : public ColumnStats {
 public:
    virtual std::optional<int32_t> Min() const = 0;
    virtual std::optional<int32_t> Max() const = 0;
    virtual void Collect(std::optional<int32_t> value) = 0;
};

class PAIMON_EXPORT NestedColumnStats : public ColumnStats {
 public:
    NestedColumnStats(const FieldType& nested_type, std::optional<int64_t> null_count)
        : nested_type_(nested_type), null_count_(null_count) {
        assert(nested_type == FieldType::ARRAY || nested_type == FieldType::MAP ||
               nested_type == FieldType::STRUCT || nested_type == FieldType::VECTOR);
    }

    std::optional<int64_t> NullCount() const override {
        return null_count_;
    }

    std::string ToString() const override;

    FieldType GetFieldType() const override;

 private:
    FieldType nested_type_;
    std::optional<int64_t> null_count_;
};

}  // namespace paimon
