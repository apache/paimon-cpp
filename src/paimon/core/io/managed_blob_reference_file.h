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
#include <optional>
#include <string>
#include <vector>

#include "paimon/result.h"

namespace paimon {

class FileSystem;

/// The `.blobref` sidecar of a primary-key data file, listing the managed blob pack files
/// (`.managed.blob`) whose payloads the data file's blob descriptors reference. The sidecar is
/// carried in the data file's extra files, so it is removed together with the data file
/// wherever the data file's companion files are collected for deletion, while a
/// pack file may be shared by several data files and is never deleted by table maintenance.
///
/// On-disk layout, byte-compatible with Java's ManagedBlobReferenceFile (big-endian):
///
///   magic    int32  = 0x50424C52 ("PBLR"), not covered by the checksum
///   version  byte   = 1                     -- checksum covers from here ...
///   count    int32
///   count x { storage_root_id : Java writeUTF (uint16 length + modified UTF-8 bytes)
///             relative_path   : Java writeUTF }                      -- ... to here
///   checksum int32  = CRC32 over the covered region
///
/// An empty reference list is a valid file and means "this data file references no pack",
/// which is different from a missing sidecar.
class ManagedBlobReferenceFile {
 public:
    static constexpr char kManagedBlobSuffix[] = ".managed.blob";
    static constexpr char kReferenceFileSuffix[] = ".blobref";

    /// One referenced pack file: the directory it lives in and its bare file name.
    struct Reference {
        /// Creates a reference after validating that `relative_path` is a bare file name.
        static Result<Reference> Create(const std::string& storage_root_id,
                                        const std::string& relative_path);

        bool operator==(const Reference& other) const {
            return storage_root_id == other.storage_root_id && relative_path == other.relative_path;
        }
        bool operator<(const Reference& other) const {
            if (storage_root_id != other.storage_root_id) {
                return storage_root_id < other.storage_root_id;
            }
            return relative_path < other.relative_path;
        }

        std::string ToString() const;

        std::string storage_root_id;
        std::string relative_path;
    };

    ManagedBlobReferenceFile() = delete;
    ~ManagedBlobReferenceFile() = delete;

    /// Resolves a blob descriptor URI to a pack reference. Returns nullopt when the URI does
    /// not point at a managed pack file (its name does not end with `.managed.blob`).
    static Result<std::optional<Reference>> FromDescriptorUri(const std::string& uri);

    /// The sidecar path of a data file: the data file path plus `.blobref`.
    static std::string SidecarPath(const std::string& data_file_path);

    /// Writes `references` to `path`, normalized by sorting and deduplication so the content
    /// is deterministic. A failed write deletes the partial file.
    static Status Write(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                        std::vector<Reference> references);

    /// Reads and validates a sidecar: magic, version, reference validity, checksum, and that
    /// no trailing bytes follow the checksum.
    static Result<std::vector<Reference>> Read(const std::shared_ptr<FileSystem>& fs,
                                               const std::string& path);
};

}  // namespace paimon
