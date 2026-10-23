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
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "paimon/result.h"

namespace paimon {
class DataInputStream;
class InputStream;
class MemoryPool;

class FileIndexFormatUtils {
 public:
    /// Column name -> index type -> (payload offset, payload length), ordered by name.
    /// Empty indexes use (-1, 0).
    using FileIndexEntries =
        std::map<std::string, std::map<std::string, std::pair<int64_t, int64_t>>>;

    FileIndexFormatUtils() = delete;
    ~FileIndexFormatUtils() = delete;

    /// Creates a bounded, buffered input stream for the container's index metadata.
    static Result<std::shared_ptr<InputStream>> CreateMetadataInputStream(
        const std::shared_ptr<InputStream>& input, int64_t start, int64_t length,
        int64_t file_length, const std::shared_ptr<MemoryPool>& pool);

    /// Reads shared column/type entries using the offsets and lengths of the given version.
    static Result<FileIndexEntries> ReadFileIndexEntries(const DataInputStream& in,
                                                         int32_t version);
};
}  // namespace paimon
