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
#include <memory>
#include <optional>
#include <string>

#include "paimon/cache/cache.h"

namespace paimon {

class SnapshotLiveManifestEntriesCacheKey : public CacheKey {
 public:
    static std::shared_ptr<CacheKey> ForExplicit(const std::string& table_path,
                                                 const std::string& branch, int32_t bucket);
    static std::shared_ptr<CacheKey> ForInferred(const std::string& table_path,
                                                 const std::string& branch, int32_t bucket,
                                                 int32_t total_buckets, int64_t schema_id);

    bool IsIndex() const override {
        return false;
    }

    bool Equals(const CacheKey& other) const override {
        const auto* rhs = dynamic_cast<const SnapshotLiveManifestEntriesCacheKey*>(&other);
        if (!rhs) {
            return false;
        }
        return table_path_ == rhs->table_path_ && branch_ == rhs->branch_ &&
               bucket_ == rhs->bucket_ && mode_ == rhs->mode_ &&
               total_buckets_ == rhs->total_buckets_ && schema_id_ == rhs->schema_id_ &&
               GetKind() == rhs->GetKind();
    }

    size_t HashCode() const override {
        size_t seed = 0;
        seed ^= std::hash<std::string>{}(table_path_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
        seed ^= std::hash<std::string>{}(branch_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
        seed ^= std::hash<int32_t>{}(bucket_) + HASH_CONSTANT + (seed << 6) + (seed >> 2);
        seed ^= std::hash<int32_t>{}(static_cast<int32_t>(GetKind())) + HASH_CONSTANT +
                (seed << 6) + (seed >> 2);
        seed ^= std::hash<std::optional<int32_t>>{}(total_buckets_) + HASH_CONSTANT + (seed << 6) +
                (seed >> 2);
        seed ^= std::hash<std::optional<int64_t>>{}(schema_id_) + HASH_CONSTANT + (seed << 6) +
                (seed >> 2);
        seed ^= std::hash<int32_t>{}(static_cast<int32_t>(mode_)) + HASH_CONSTANT + (seed << 6) +
                (seed >> 2);
        return seed;
    }

 private:
    enum class Mode { kExplicit, kInferred };

    SnapshotLiveManifestEntriesCacheKey(const std::string& table_path, const std::string& branch,
                                        int32_t bucket, Mode mode,
                                        std::optional<int32_t> total_buckets,
                                        std::optional<int64_t> schema_id)
        : CacheKey(CacheKind::SNAPSHOT_LIVE_MANIFEST),
          table_path_(table_path),
          branch_(branch),
          bucket_(bucket),
          mode_(mode),
          total_buckets_(total_buckets),
          schema_id_(schema_id) {}

    static constexpr uint64_t HASH_CONSTANT = 0x9e3779b97f4a7c15ULL;

    const std::string table_path_;
    const std::string branch_;
    const int32_t bucket_;
    const Mode mode_;
    const std::optional<int32_t> total_buckets_;
    const std::optional<int64_t> schema_id_;
};

class PositionCacheKey : public CacheKey {
 public:
    PositionCacheKey(const std::string& file_path, int64_t position, int32_t length, bool is_index,
                     CacheKind kind)
        : CacheKey(kind),
          file_path_(file_path),
          position_(position),
          length_(length),
          is_index_(is_index) {}

    bool IsIndex() const override;
    size_t HashCode() const override;
    bool Equals(const CacheKey& other) const override;
    int64_t Position() const;
    int32_t Length() const;

 private:
    static constexpr uint64_t HASH_CONSTANT = 0x9e3779b97f4a7c15ULL;

    const std::string file_path_;
    const int64_t position_;
    const int32_t length_;
    const bool is_index_;
};

struct CacheKeyHash {
    size_t operator()(const std::shared_ptr<CacheKey>& key) const {
        return key ? key->HashCode() : 0;
    }
};

struct CacheKeyEqual {
    bool operator()(const std::shared_ptr<CacheKey>& lhs,
                    const std::shared_ptr<CacheKey>& rhs) const {
        if (lhs == rhs) {
            return true;
        }
        if (!lhs || !rhs) {
            return false;
        }
        return lhs->Equals(*rhs);
    }
};

}  // namespace paimon
