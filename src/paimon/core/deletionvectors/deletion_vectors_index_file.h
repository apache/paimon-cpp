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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/index/index_file.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/table/source/deletion_file.h"

namespace paimon {

class DeletionVectorIndexFileWriter;

/// DeletionVectors index file.
class DeletionVectorsIndexFile : public IndexFile {
 public:
    static constexpr char DELETION_VECTORS_INDEX[] = "DELETION_VECTORS";
    static constexpr int8_t VERSION_ID_V1 = 1;

    DeletionVectorsIndexFile(const std::shared_ptr<FileSystem>& fs,
                             const std::shared_ptr<IndexPathFactory>& path_factory, bool bitmap64,
                             const std::shared_ptr<MemoryPool>& pool)
        : IndexFile(fs, path_factory), bitmap64_(bitmap64), pool_(pool) {}

    ~DeletionVectorsIndexFile() override = default;

    bool Bitmap64() const {
        return bitmap64_;
    }

    Result<std::shared_ptr<IndexFileMeta>> WriteSingleFile(
        const std::map<std::string, std::shared_ptr<DeletionVector>>& input);

    /// Writes `input` into as many index files as `target_size` allows, rolling to a new one
    /// once the current file reaches it.
    Result<std::vector<std::shared_ptr<IndexFileMeta>>> WriteWithRolling(
        const std::map<std::string, std::shared_ptr<DeletionVector>>& input, int64_t target_size);

    /// Reads one deletion vector by its recorded position, leaving the other vectors of the
    /// same index file untouched.
    Result<std::shared_ptr<DeletionVector>> ReadDeletionVector(
        const DeletionFile& deletion_file) const;

    /// Reads several deletion vectors that all live in one index file, keyed by the data file
    /// each covers, through a single opened stream.
    ///
    /// Reading n vectors of the same index file one by one costs n opens, which on an object
    /// store dominates the read itself. Every `DeletionFile` has to name the same index file;
    /// one that does not is a caller bug and is rejected.
    Result<std::map<std::string, std::shared_ptr<DeletionVector>>> ReadDeletionVectors(
        const std::map<std::string, DeletionFile>& deletion_file_by_data_file) const;

    Result<std::map<std::string, std::shared_ptr<DeletionVector>>> ReadAllDeletionVectors(
        const std::shared_ptr<IndexFileMeta>& file_meta) const;

    Result<std::map<std::string, std::shared_ptr<DeletionVector>>> ReadAllDeletionVectors(
        const std::vector<std::shared_ptr<IndexFileMeta>>& index_files) const;

 private:
    static Status CheckVersion(const std::shared_ptr<DataInputStream>& in);

    std::shared_ptr<DeletionVectorIndexFileWriter> CreateWriter() const;

    const bool bitmap64_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon
