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

#include <memory>
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {
struct DataFileMeta;

/// Util class for data evolution.
class DataEvolutionUtils {
 public:
    DataEvolutionUtils() = delete;
    ~DataEvolutionUtils() = delete;

    /// Retrieves the anchor file of a row range group: always the oldest normal file (neither a
    /// blob file nor a vector-store file), comparing files by (max_sequence_number, file_name)
    /// pairs. A group's deletion vector is maintained against its anchor, so the vector's
    /// positions are relative to the anchor's row id range.
    ///
    /// The rule has to stay identical to the engine that writes the vectors, which keys them by
    /// the same file (Paimon Java's DataEvolutionUtils#retrieveAnchorFile). A vector keyed by
    /// any other file of the group, or one whose anchor a pruning dropped, is never found here
    /// and its deleted rows silently come back.
    ///
    /// @param files The files of one row range group, in any order.
    /// @return The anchor file, or an error when the group holds no normal file.
    static Result<std::shared_ptr<DataFileMeta>> RetrieveAnchorFile(
        const std::vector<std::shared_ptr<DataFileMeta>>& files);

    /// Whether the file can anchor a row range group, that is neither a blob file nor a
    /// vector-store file.
    static bool IsNormalFile(const std::string& file_name);
};

}  // namespace paimon
