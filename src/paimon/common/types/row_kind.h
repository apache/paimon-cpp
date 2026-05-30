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
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// Lists all kinds of changes that a row can describe in a changelog.
class RowKind {
 public:
    /// Returns a short string representation of this `RowKind`.
    ///
    /// <ul>
    /// <li>"+I" represents `Insert()`.
    /// <li>"-U" represents `UpdateBefore()`.
    /// <li>"+U" represents `UpdateAfter()`.
    /// <li>"-D" represents `Delete()`.
    /// </ul>
    const std::string& ShortString() const {
        return short_string_;
    }

    const std::string& Name() const {
        return name_;
    }

    /// Returns the byte value representation of this `RowKind`. The byte value is
    /// used for serialization and deserialization.
    ///
    /// <ul>
    /// <li>"0" represents `Insert()`.
    /// <li>"1" represents `UpdateBefore()`.
    /// <li>"2" represents `UpdateAfter()`.
    /// <li>"3" represents `Delete()`.
    /// </ul>
    int8_t ToByteValue() const {
        return value_;
    }

    bool operator==(const RowKind& other) const {
        if (this == &other) {
            return true;
        }
        return value_ == other.value_ && short_string_ == other.short_string_ &&
               name_ == other.name_;
    }

    /// Insertion operation.
    static const RowKind* Insert();

    /// Update operation with the previous content of the updated row.
    ///
    /// This kind SHOULD occur together with `UpdateAfter()` for modelling an update
    /// that needs to retract the previous row first. It is useful in cases of a non-idempotent
    /// update, i.e., an update of a row that is not uniquely identifiable by a key.
    static const RowKind* UpdateBefore();

    /// Update operation with new content of the updated row.
    ///
    /// This kind CAN occur together with `UpdateBefore()` for modelling an update
    /// that needs to retract the previous row first. OR it describes an idempotent update,
    /// i.e., an update of a row that is uniquely identifiable by a key.
    static const RowKind* UpdateAfter();

    /// Deletion operation.
    static const RowKind* Delete();

    /// Is `UpdateBefore()` or `Delete()`.
    bool IsRetract() const {
        return this == UpdateBefore() || this == Delete();
    }

    /// Is `Insert()` or `UpdateAfter()`.
    bool IsAdd() const {
        return this == Insert() || this == UpdateAfter();
    }

    /// Creates a `RowKind` from the given byte value. Each `RowKind` has a
    /// byte value representation.
    ///
    /// @see #ToByteValue() for mapping of byte value and `RowKind`.
    static Result<const RowKind*> FromByteValue(int8_t value) {
        switch (value) {
            case 0:
                return Insert();
            case 1:
                return UpdateBefore();
            case 2:
                return UpdateAfter();
            case 3:
                return Delete();
            default:
                return Status::Invalid(fmt::format("Unsupported byte value {} for row kind.",
                                                   static_cast<int32_t>(value)));
        }
    }

    /// Creates a `RowKind` from the given short string.
    ///
    /// @see #shortString() for mapping of string and `RowKind`.
    static Result<const RowKind*> FromShortString(const std::string& value);

 private:
    /// Creates a `RowKind` with the given short string and byte value representation
    /// of the `RowKind`.
    RowKind(const std::string& short_string, const std::string& name, int8_t value)
        : short_string_(short_string), name_(name), value_(value) {}

 private:
    std::string short_string_;
    std::string name_;
    int8_t value_;
};
}  // namespace paimon
