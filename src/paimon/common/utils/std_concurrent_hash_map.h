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
#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/utils/murmurhash_utils.h"

namespace paimon {

template <typename Key>
class DefaultHashCompare {
 public:
    size_t hash(const Key& key) const {
        return std::hash<Key>{}(key);
    }

    bool equal(const Key& lhs, const Key& rhs) const {
        return lhs == rhs;
    }
};

namespace detail {

template <typename Key, typename HashCompare>
class HashCompareHasher {
 public:
    size_t operator()(const Key& key) const {
        return HashCompare{}.hash(key);
    }
};

template <typename Key, typename HashCompare>
class HashCompareEqual {
 public:
    bool operator()(const Key& lhs, const Key& rhs) const {
        return HashCompare{}.equal(lhs, rhs);
    }
};

}  // namespace detail

template <typename Key, typename T, typename HashCompare = DefaultHashCompare<Key>>
class ConcurrentHashMap {
 private:
    using HashMap = std::unordered_map<Key, T, detail::HashCompareHasher<Key, HashCompare>,
                                       detail::HashCompareEqual<Key, HashCompare>>;

 public:
    ConcurrentHashMap() = default;
    ~ConcurrentHashMap() = default;

    ConcurrentHashMap(const ConcurrentHashMap&) = delete;
    void operator=(const ConcurrentHashMap&) = delete;
    ConcurrentHashMap(ConcurrentHashMap&&) = delete;
    ConcurrentHashMap& operator=(ConcurrentHashMap&&) = delete;

    std::optional<T> Find(const Key& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        typename HashMap::const_iterator iter = hash_map_.find(key);
        if (iter != hash_map_.end()) {
            return iter->second;
        }
        return std::nullopt;
    }

    void Insert(const Key& key, const T& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        hash_map_.insert_or_assign(key, value);
    }

    void Erase(const Key& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        hash_map_.erase(key);
    }

    size_t Size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return hash_map_.size();
    }

 private:
    HashMap hash_map_;
    mutable std::shared_mutex mutex_;
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
