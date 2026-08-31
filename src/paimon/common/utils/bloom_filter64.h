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
#include <functional>
#include <memory>

#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class Bytes;
class MemoryPool;

/// Bloom filter 64 handle 64 bits hash.
class PAIMON_EXPORT BloomFilter64 {
 public:
    static Result<BloomFilter64> Create(int64_t items, double fpp,
                                        const std::shared_ptr<MemoryPool>& pool);

    class BitSet;

    BloomFilter64(int32_t num_hash_functions, std::unique_ptr<BitSet>&& bit_set);

    void AddHash(int64_t hash64);

    bool TestHash(int64_t hash64) const;

    int32_t GetNumHashFunctions() const {
        return num_hash_functions_;
    }

    const BitSet& GetBitSet() const {
        return *bit_set_;
    }

    class BitSet {
     public:
        BitSet(const std::shared_ptr<Bytes>& bytes, int32_t offset);
        void Set(int32_t index);
        bool Get(int32_t index) const;
        int32_t BitSize() const;
        int32_t ByteLength() const;
        void ToByteArray(int32_t offset, int32_t length, char* bytes) const;

     private:
        static constexpr int8_t MASK = 0x07;

     private:
        int32_t offset_;
        std::shared_ptr<Bytes> bytes_;
    };

 private:
    BloomFilter64(int32_t num_hash_functions, std::unique_ptr<BitSet>&& bit_set,
                  const std::shared_ptr<MemoryPool>& pool);

    static constexpr int32_t BYTE_SIZE = 8;

    int32_t num_bits_ = -1;
    int32_t num_hash_functions_ = -1;
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<BitSet> bit_set_;
};
}  // namespace paimon
