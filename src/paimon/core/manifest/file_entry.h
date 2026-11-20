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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/murmurhash_utils.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {

/// Entry representing a file.
class FileEntry {
 public:
    /// The same `Identifier` indicates that the `ManifestEntry` refers to the same data
    /// file.
    struct Identifier {
        Identifier(const BinaryRow& _partition, int32_t _bucket, int32_t _level,
                   const std::string& _file_name,
                   const std::vector<std::optional<std::string>>& _extra_files,
                   const std::shared_ptr<Bytes>& _embedded_index,
                   const std::optional<std::string>& _external_path)
            : partition(_partition),
              bucket(_bucket),
              level(_level),
              file_name(_file_name),
              extra_files(_extra_files),
              embedded_index(_embedded_index),
              external_path(_external_path) {}

        bool operator==(const Identifier& other) const {
            return partition == other.partition && bucket == other.bucket && level == other.level &&
                   file_name == other.file_name && extra_files == other.extra_files &&
                   (embedded_index == other.embedded_index ||
                    (embedded_index && other.embedded_index &&
                     *embedded_index == *other.embedded_index)) &&
                   external_path == other.external_path;
        }

        size_t HashCode() const {
            if (hash_ == static_cast<size_t>(-1)) {
                hash_ = partition.HashCode();
                hash_ =
                    MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(&bucket),
                                                     /*offset=*/0, sizeof(bucket), /*seed=*/hash_);
                hash_ = MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(&level),
                                                         /*offset=*/0, sizeof(level),
                                                         /*seed=*/hash_);
                auto hash_bytes = [this](const char* data, size_t length) {
                    while (length > 0) {
                        int32_t chunk_size = static_cast<int32_t>(
                            std::min<size_t>(length, std::numeric_limits<int32_t>::max()));
                        hash_ = MurmurHashUtils::HashUnsafeBytes(data, /*offset=*/0, chunk_size,
                                                                 /*seed=*/hash_);
                        data += chunk_size;
                        length -= chunk_size;
                    }
                };
                hash_bytes(file_name.data(), file_name.size());
                for (const auto& extra_file : extra_files) {
                    bool present = extra_file.has_value();
                    hash_ = MurmurHashUtils::HashUnsafeBytes(&present, /*offset=*/0,
                                                             sizeof(present), /*seed=*/hash_);
                    if (extra_file) {
                        hash_bytes(extra_file->data(), extra_file->size());
                    }
                }
                bool has_embedded_index = embedded_index != nullptr;
                hash_ = MurmurHashUtils::HashUnsafeBytes(
                    &has_embedded_index, /*offset=*/0, sizeof(has_embedded_index), /*seed=*/hash_);
                if (embedded_index) {
                    hash_bytes(embedded_index->data(), embedded_index->size());
                }
                bool has_external_path = external_path.has_value();
                hash_ = MurmurHashUtils::HashUnsafeBytes(&has_external_path, /*offset=*/0,
                                                         sizeof(has_external_path), /*seed=*/hash_);
                if (external_path) {
                    hash_bytes(external_path->data(), external_path->size());
                }
            }
            return hash_;
        }

        std::string ToString() const {
            std::vector<std::string> extra_file_names;
            for (const auto& extra_file : extra_files) {
                extra_file_names.push_back(extra_file.value_or("null"));
            }
            std::string embedded_index_str =
                embedded_index ? fmt::format("{} bytes", embedded_index->size()) : "null";
            std::string external_path_str =
                external_path == std::nullopt ? "null" : external_path.value();
            return fmt::format("{{{}, {}, {}, {}, [{}], {}, {}}}", partition.ToString(), bucket,
                               level, file_name, fmt::join(extra_file_names, ", "),
                               embedded_index_str, external_path_str);
        }

        BinaryRow partition;
        int32_t bucket;
        int32_t level;
        std::string file_name;
        std::vector<std::optional<std::string>> extra_files;
        std::shared_ptr<Bytes> embedded_index;
        std::optional<std::string> external_path;

     private:
        mutable size_t hash_ = -1;
    };

 public:
    virtual ~FileEntry() = default;
    virtual const FileKind& Kind() const = 0;
    virtual const BinaryRow& Partition() const = 0;
    virtual int32_t Bucket() const = 0;
    virtual int32_t Level() const = 0;
    virtual const std::string& FileName() const = 0;
    virtual const std::optional<std::string>& ExternalPath() const = 0;
    virtual Identifier CreateIdentifier() const = 0;
    virtual const BinaryRow& MinKey() const = 0;
    virtual const BinaryRow& MaxKey() const = 0;

    template <typename T>
    static Status MergeEntries(const std::vector<T>& unmerged_entries,
                               std::vector<T>* merged_entries) {
        LinkedHashMap<Identifier, T> merged_map;
        PAIMON_RETURN_NOT_OK(MergeEntries(unmerged_entries, &merged_map));
        for (const auto& [identifier, entry] : merged_map) {
            merged_entries->emplace_back(entry);
        }
        return Status::OK();
    }

    template <typename T>
    static Status MergeEntries(const std::vector<T>& unmerged_entries,
                               LinkedHashMap<Identifier, T>* merged_map_ptr) {
        auto& merged_map = *merged_map_ptr;
        for (const auto& entry : unmerged_entries) {
            Identifier identifier = entry.CreateIdentifier();
            const auto& kind = entry.Kind();
            auto iter = merged_map.find(identifier);
            if (kind == FileKind::Add()) {
                if (iter != merged_map.end()) {
                    return Status::Invalid(fmt::format(
                        "Trying to add file {} which is already added.", identifier.ToString()));
                }
                merged_map.insert(identifier, entry);
            } else if (kind == FileKind::Delete()) {
                // each dataFile will only be added once and deleted once,
                // if we know that it is added before then both add and delete entry can be
                // removed because there won't be further operations on this file,
                // otherwise we have to keep the delete entry because the add entry must be
                // in the previous manifest files
                if (iter != merged_map.end()) {
                    merged_map.erase(identifier);
                } else {
                    merged_map.insert(identifier, entry);
                }
            } else {
                return Status::Invalid("Unknown value kind ",
                                       std::to_string(static_cast<int32_t>(kind.ToByteValue())));
            }
        }
        return Status::OK();
    }
};
}  // namespace paimon

namespace std {
template <>
struct hash<paimon::FileEntry::Identifier> {
    size_t operator()(const paimon::FileEntry::Identifier& identifier) const {
        return identifier.HashCode();
    }
};
}  // namespace std
