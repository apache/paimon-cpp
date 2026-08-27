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
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/utils/concurrent_backend_factory.h"
#include "paimon/common/utils/murmurhash_utils.h"
#ifdef PAIMON_USE_TBB
#include "tbb/concurrent_hash_map.h"
#endif

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

template <typename Key, typename T, typename HashCompare = DefaultHashCompare<Key>>
class ConcurrentHashMapBackend {
 public:
    virtual ~ConcurrentHashMapBackend() = default;

    virtual std::optional<T> Find(const Key& key) const = 0;
    virtual void Insert(const Key& key, const T& value) = 0;
    virtual void Erase(const Key& key) = 0;
    virtual size_t Size() const = 0;
};

#ifndef PAIMON_USE_TBB
namespace detail {

template <typename Key, typename T, typename HashCompare>
class StdConcurrentHashMapBackend : public ConcurrentHashMapBackend<Key, T, HashCompare> {
 private:
    using HashMap = std::unordered_map<Key, T, HashCompareHasher<Key, HashCompare>,
                                       HashCompareEqual<Key, HashCompare>>;

 public:
    std::optional<T> Find(const Key& key) const override {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        typename HashMap::const_iterator iter = hash_map_.find(key);
        if (iter != hash_map_.end()) {
            return iter->second;
        }
        return std::nullopt;
    }

    void Insert(const Key& key, const T& value) override {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        hash_map_.insert_or_assign(key, value);
    }

    void Erase(const Key& key) override {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        hash_map_.erase(key);
    }

    size_t Size() const override {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return hash_map_.size();
    }

 private:
    HashMap hash_map_;
    mutable std::shared_mutex mutex_;
};

}  // namespace detail
#endif

template <typename Key, typename T, typename HashCompare = DefaultHashCompare<Key>>
class ConcurrentHashMap {
 private:
    using Backend = ConcurrentHashMapBackend<Key, T, HashCompare>;

 public:
#ifdef PAIMON_USE_TBB
    ConcurrentHashMap() = default;
#else
    ConcurrentHashMap() : backend_(ConcurrentBackendFactory<Backend>::Create()) {
        if (backend_ == nullptr) {
            backend_ = std::make_unique<detail::StdConcurrentHashMapBackend<Key, T, HashCompare>>();
        }
    }
#endif
    ~ConcurrentHashMap() = default;

    ConcurrentHashMap(const ConcurrentHashMap&) = delete;
    void operator=(const ConcurrentHashMap&) = delete;
    ConcurrentHashMap(ConcurrentHashMap&&) = delete;
    ConcurrentHashMap& operator=(ConcurrentHashMap&&) = delete;

    std::optional<T> Find(const Key& key) const {
#ifdef PAIMON_USE_TBB
        typename tbb::concurrent_hash_map<Key, T, HashCompare>::const_accessor accessor;
        if (hash_map_.find(accessor, key)) {
            return accessor->second;
        }
        return std::nullopt;
#else
        return backend_->Find(key);
#endif
    }

    void Insert(const Key& key, const T& value) {
#ifdef PAIMON_USE_TBB
        typename tbb::concurrent_hash_map<Key, T, HashCompare>::accessor accessor;
        hash_map_.insert(accessor, key);
        accessor->second = value;
#else
        backend_->Insert(key, value);
#endif
    }

    void Erase(const Key& key) {
#ifdef PAIMON_USE_TBB
        typename tbb::concurrent_hash_map<Key, T, HashCompare>::accessor accessor;
        if (hash_map_.find(accessor, key)) {
            hash_map_.erase(accessor);
        }
#else
        backend_->Erase(key);
#endif
    }

    size_t Size() const {
#ifdef PAIMON_USE_TBB
        return hash_map_.size();
#else
        return backend_->Size();
#endif
    }

 private:
#ifdef PAIMON_USE_TBB
    tbb::concurrent_hash_map<Key, T, HashCompare> hash_map_;
#else
    std::unique_ptr<Backend> backend_;
#endif
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
