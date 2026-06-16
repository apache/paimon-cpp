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
#include <string>

#include "fmt/format.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// The Source of a file.
class FileSource {
 public:
    /// The file from new input.
    static const FileSource& Append();
    /// The file from compaction.
    static const FileSource& Compact();

    int8_t ToByteValue() const {
        return value_;
    }

    static Result<FileSource> FromByteValue(int8_t value) {
        switch (value) {
            case 0:
                return Append();
            case 1:
                return Compact();
            default:
                return Status::Invalid(
                    fmt::format("Unsupported byte value {} for value kind.", value));
        }
    }

    bool operator==(const FileSource& other) const {
        if (this == &other) {
            return true;
        }
        return value_ == other.value_;
    }

    bool operator!=(const FileSource& other) const {
        return !(*this == other);
    }

    std::string ToString() const {
        switch (value_) {
            case 0:
                return "APPEND";
            case 1:
                return "COMPACT";
            default:
                return "UNKNOWN";
        }
    }

 private:
    explicit FileSource(int8_t value) : value_(value) {}

 private:
    int8_t value_;
};
}  // namespace paimon
