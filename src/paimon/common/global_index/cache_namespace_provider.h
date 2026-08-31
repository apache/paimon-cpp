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

#include <memory>
#include <string>

#include "fmt/format.h"
#include "paimon/global_index/io/global_index_file_reader.h"

namespace paimon {

/// Internal extension for reader wrappers that can identify a shared storage backend.
class CacheNamespaceProvider {
 public:
    virtual ~CacheNamespaceProvider() = default;
    virtual std::string CacheNamespace() const = 0;
};

inline std::string GetGlobalIndexCacheNamespace(
    const std::shared_ptr<GlobalIndexFileReader>& file_reader) {
    const auto* provider = dynamic_cast<const CacheNamespaceProvider*>(file_reader.get());
    if (provider) {
        return provider->CacheNamespace();
    }
    return fmt::format("index-reader:{}", fmt::ptr(file_reader.get()));
}

}  // namespace paimon
