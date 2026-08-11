/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
#include <utility>

namespace paimon {
/// One ordered source data file covered by a source-backed primary-key index payload.
///
/// Two source files are interchangeable only when both the file name and the row count
/// match; coverage validation relies on this strict identity.
struct PrimaryKeyIndexSourceFile {
    PrimaryKeyIndexSourceFile(std::string file_name, int64_t row_count)
        : file_name(std::move(file_name)), row_count(row_count) {}

    bool operator==(const PrimaryKeyIndexSourceFile& other) const {
        return file_name == other.file_name && row_count == other.row_count;
    }

    bool operator!=(const PrimaryKeyIndexSourceFile& other) const {
        return !(*this == other);
    }

    std::string file_name;
    int64_t row_count;
};

}  // namespace paimon
