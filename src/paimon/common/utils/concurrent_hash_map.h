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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/utils/murmurhash_utils.h"
#include "tbb/concurrent_hash_map.h"

namespace paimon {
template <typename Key, typename T, typename HashCompare = tbb::tbb_hash_compare<Key>>
class ConcurrentHashMap {
 private:
    using HashMap = tbb::concurrent_hash_map<Key, T, HashCompare>;

 public:
    ConcurrentHashMap() = default;
    ~ConcurrentHashMap() = default;

    // No copying allowed
    ConcurrentHashMap(const ConcurrentHashMap&) = delete;
    void operator=(const ConcurrentHashMap&) = delete;
    ConcurrentHashMap(ConcurrentHashMap&&) = delete;
    ConcurrentHashMap& operator=(ConcurrentHashMap&&) = delete;

    std::optional<T> Find(const Key& key) const {
        typename HashMap::const_accessor accessor;
        if (hash_map_.find(accessor, key)) {
            return accessor->second;
        }
        return std::nullopt;
    }

    void Insert(const Key& key, const T& value) {
        typename HashMap::accessor accessor;
        hash_map_.insert(accessor, key);
        accessor->second = value;
    }

    void Erase(const Key& key) {
        typename HashMap::accessor accessor;
        if (hash_map_.find(accessor, key)) {
            hash_map_.erase(accessor);
        }
    }

    size_t Size() const {
        return hash_map_.size();
    }

 private:
    HashMap hash_map_;
};

class VectorStringHashCompare {
 public:
    size_t hash(const std::vector<std::string>& key) const {
        int32_t ret = MurmurHashUtils::DEFAULT_SEED;
        for (const auto& s : key) {
            ret = MurmurHashUtils::HashUnsafeBytes(reinterpret_cast<const void*>(s.data()),
                                                   /*offset=*/0, s.size(), ret);
        }
        return ret;
    }

    bool equal(const std::vector<std::string>& a, const std::vector<std::string>& b) const {
        return a == b;
    }
};
}  // namespace paimon
