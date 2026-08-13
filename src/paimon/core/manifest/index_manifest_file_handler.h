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

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/manifest/index_manifest_file.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// IndexManifestFile Handler.
class IndexManifestFileHandler {
 public:
    IndexManifestFileHandler() = delete;
    ~IndexManifestFileHandler() = delete;

    static Result<std::string> Write(const std::optional<std::string>& previous_index_manifest,
                                     const std::vector<IndexManifestEntry>& new_index_entries,
                                     int32_t bucket_mode, IndexManifestFile* index_manifest_file);

 private:
    class IndexManifestFileCombiner {
     public:
        virtual ~IndexManifestFileCombiner() = default;
        virtual Result<std::vector<IndexManifestEntry>> Combine(
            const std::vector<IndexManifestEntry>& prev_index_files,
            const std::vector<IndexManifestEntry>& new_index_files) const = 0;
    };

    /// Combine previous and new index files by partition, bucket and index type.
    class BucketedCombiner : public IndexManifestFileCombiner {
     public:
        Result<std::vector<IndexManifestEntry>> Combine(
            const std::vector<IndexManifestEntry>& prev_index_files,
            const std::vector<IndexManifestEntry>& new_index_files) const override;
    };

    /// Combine previous and new index files by file name.
    class GlobalFileNameCombiner : public IndexManifestFileCombiner {
     public:
        Result<std::vector<IndexManifestEntry>> Combine(
            const std::vector<IndexManifestEntry>& prev_index_files,
            const std::vector<IndexManifestEntry>& new_index_files) const override;
    };

    /// Combine previous and new deletion vector index files by file name, for a table without
    /// buckets, where the bucket cannot tell two index files apart.
    ///
    /// A data file is covered by at most one deletion vector, so a delta that adds a second one
    /// for a data file, or drops one that is not there, was built against a different base and
    /// is rejected rather than silently changing which rows the read returns.
    class GlobalDeletionVectorCombiner : public IndexManifestFileCombiner {
     public:
        Result<std::vector<IndexManifestEntry>> Combine(
            const std::vector<IndexManifestEntry>& prev_index_files,
            const std::vector<IndexManifestEntry>& new_index_files) const override;
    };

    static std::map<std::string, std::vector<IndexManifestEntry>> SeparateIndexEntries(
        const std::vector<IndexManifestEntry>& index_entries);

    static Result<std::unique_ptr<IndexManifestFileCombiner>> GetIndexManifestFileCombine(
        const std::string& index_type, int32_t bucket_mode);
};
}  // namespace paimon
