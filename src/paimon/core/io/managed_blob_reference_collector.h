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
#include <set>
#include <string>
#include <vector>

#include "paimon/core/key_value.h"
#include "paimon/logging.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {

class FileSystem;

/// Collects, for one primary-key data file, the managed blob pack files its rows reference,
/// and writes them to the file's `.blobref` sidecar on close.
///
/// The collector scans every written key-value batch: managed blob values of non-retract rows
/// hold serialized blob descriptors after externalization, and each descriptor URI ending in
/// `.managed.blob` contributes one reference. Values that are not descriptors (for example
/// inline blob bytes) are ignored. Because compaction rewrites descriptors verbatim, the
/// sidecar of a compacted file lists exactly the packs its surviving rows still reference.
class ManagedBlobReferenceCollector {
 public:
    /// Creates a collector for the data file at `data_file_path`. `write_schema` is the
    /// key-value batch schema (special fields followed by value fields); `managed_field_names`
    /// selects the managed blob columns in it.
    static Result<std::unique_ptr<ManagedBlobReferenceCollector>> Create(
        const std::shared_ptr<FileSystem>& fs, const std::string& data_file_path,
        const std::shared_ptr<arrow::Schema>& write_schema,
        const std::vector<std::string>& managed_field_names);

    /// Records the managed blob references of `batch`. The batch's Arrow array is imported and
    /// re-exported in place, its content is not changed.
    Status Collect(KeyValueBatch* batch);

    /// Writes the sidecar next to the data file. A failed write aborts the collector.
    Status Close();

    /// Deletes the sidecar, logging a warning on failure, and marks the collector closed.
    void Abort();

    /// The sidecar file name to record in the data file's extra files. Only valid after a
    /// successful Close().
    Result<std::string> ResultFileName() const;

    /// The sidecar path next to the data file.
    const std::string& SidecarPath() const {
        return sidecar_path_;
    }

 private:
    ManagedBlobReferenceCollector(const std::shared_ptr<FileSystem>& fs,
                                  const std::string& data_file_path,
                                  const std::shared_ptr<arrow::DataType>& batch_type,
                                  std::vector<int32_t> managed_field_indices,
                                  int32_t value_kind_index);

 private:
    std::shared_ptr<FileSystem> fs_;
    std::string sidecar_path_;
    std::shared_ptr<arrow::DataType> batch_type_;
    std::vector<int32_t> managed_field_indices_;
    int32_t value_kind_index_;

    std::unique_ptr<Logger> logger_;

    std::set<std::string> descriptor_uris_;
    bool closed_ = false;
};

}  // namespace paimon
