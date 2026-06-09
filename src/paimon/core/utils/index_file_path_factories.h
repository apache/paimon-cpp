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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/concurrent_hash_map.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/result.h"

namespace paimon {
class IndexPathFactory;

/// Cache for index `PathFactory`s.
class IndexFilePathFactories {
 public:
    explicit IndexFilePathFactories(const std::shared_ptr<FileStorePathFactory>& path_factory)
        : path_factory_(path_factory) {}

    Result<std::shared_ptr<IndexPathFactory>> Get(const BinaryRow& partition, int32_t bucket) {
        auto key = std::make_pair(partition, bucket);
        auto cached_factory = cache_.Find(key);
        if (cached_factory) {
            return cached_factory.value();
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<IndexPathFactory> index_path_factory,
                               path_factory_->CreateIndexFileFactory(partition, bucket));
        cache_.Insert(key, index_path_factory);
        return index_path_factory;
    }

 private:
    class PartitionBucketCmp {
     public:
        size_t hash(const std::pair<BinaryRow, int32_t>& key) const {
            return std::hash<std::pair<BinaryRow, int32_t>>{}(key);
        }

        bool equal(const std::pair<BinaryRow, int32_t>& a,
                   const std::pair<BinaryRow, int32_t>& b) const {
            return a.first == b.first && a.second == b.second;
        }
    };

 private:
    std::shared_ptr<FileStorePathFactory> path_factory_;
    ConcurrentHashMap<std::pair<BinaryRow, int32_t>, std::shared_ptr<IndexPathFactory>,
                      PartitionBucketCmp>
        cache_;
};
}  // namespace paimon
