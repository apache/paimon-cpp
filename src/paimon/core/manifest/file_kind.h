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

#include "paimon/result.h"

namespace paimon {
/// Kind of a file.
class FileKind {
 public:
    FileKind() = default;
    explicit FileKind(int8_t value) : value_(value) {}
    int8_t ToByteValue() const {
        return value_;
    }

    static const FileKind& Add();
    static const FileKind& Delete();

    static Result<FileKind> FromByteValue(int8_t value);

    bool operator==(const FileKind& other) const {
        return value_ == other.value_;
    }

 private:
    int8_t value_{-1};
};
}  // namespace paimon
