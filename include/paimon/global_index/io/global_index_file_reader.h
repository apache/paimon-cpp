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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/result.h"
#include "paimon/visibility.h"
namespace paimon {
class InputStream;
/// Abstract interface for reading global index files from storage.
class PAIMON_EXPORT GlobalIndexFileReader {
 public:
    GlobalIndexFileReader() : cache_namespace_(NewCacheNamespace()) {}
    virtual ~GlobalIndexFileReader() = default;

    /// Process-local backend identity used to isolate shared immutable-page caches.
    const std::string& CacheNamespace() const {
        return cache_namespace_;
    }

    /// Opens an input stream for reading the specified global index file.
    virtual Result<std::unique_ptr<InputStream>> GetInputStream(
        const std::string& file_path) const = 0;

 protected:
    explicit GlobalIndexFileReader(std::string cache_namespace)
        : cache_namespace_(std::move(cache_namespace)) {}

    /// Returns a stable namespace while the same backend object is shared by reader wrappers. Dead
    /// backends are removed from the registry and namespace ids are never reused.
    static std::string CacheNamespaceFor(const std::shared_ptr<void>& backend) {
        using WeakBackend = std::weak_ptr<void>;
        static std::mutex mutex;
        static std::map<WeakBackend, std::string, std::owner_less<WeakBackend>> namespaces;

        std::lock_guard<std::mutex> lock(mutex);
        for (auto iter = namespaces.begin(); iter != namespaces.end();) {
            if (iter->first.expired()) {
                iter = namespaces.erase(iter);
            } else {
                ++iter;
            }
        }
        WeakBackend key(backend);
        auto iter = namespaces.find(key);
        if (iter != namespaces.end()) {
            return iter->second;
        }
        std::string cache_namespace = NewCacheNamespace();
        namespaces.emplace(std::move(key), cache_namespace);
        return cache_namespace;
    }

 private:
    static std::string NewCacheNamespace() {
        static std::atomic<uint64_t> next_namespace{0};
        return fmt::format("index-backend:{}", next_namespace.fetch_add(1));
    }

    std::string cache_namespace_;
};

}  // namespace paimon
