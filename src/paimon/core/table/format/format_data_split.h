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
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "paimon/table/source/split.h"

namespace paimon {

/// A split of a format table: the data files of one partition directory, or of the table
/// directory itself when the table is not partitioned.
///
/// A file is never divided between splits: parquet and orc record their own row group and stripe
/// boundaries, which a reader handed a byte range would have to rediscover. In-memory only, so
/// plan and read within one process.
struct FormatDataSplit : public Split {
    struct FileMeta {
        FileMeta(const std::string& _file_path, int64_t _file_size)
            : file_path(_file_path), file_size(_file_size) {}

        bool operator==(const FileMeta& other) const {
            return file_path == other.file_path && file_size == other.file_size;
        }

        std::string file_path;
        /// In bytes, as reported by the listing that found it.
        int64_t file_size;
    };

    FormatDataSplit(const std::vector<FileMeta>& files,
                    const std::map<std::string, std::string>& partition)
        : files(files), partition(partition) {}

    ~FormatDataSplit() override = default;

    /// Sum of every file's size. Saturates rather than wrapping, since the sizes are whatever
    /// the split was given.
    int64_t TotalSize() const {
        int64_t total = 0;
        for (const FileMeta& file : files) {
            if (file.file_size > 0 &&
                total > std::numeric_limits<int64_t>::max() - file.file_size) {
                return std::numeric_limits<int64_t>::max();
            }
            total += file.file_size;
        }
        return total;
    }

    /// Data files of this split, read in this order.
    std::vector<FileMeta> files;

    /// Keyed by partition field name, empty when the table is not partitioned.
    /// `partition.default-name` stands for a null value.
    std::map<std::string, std::string> partition;
};

}  // namespace paimon
