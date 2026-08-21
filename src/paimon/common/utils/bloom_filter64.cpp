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

#include "paimon/common/utils/bloom_filter64.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <limits>
#include <utility>

#include "paimon/memory/bytes.h"

namespace paimon {
class MemoryPool;

BloomFilter64::BitSet::BitSet(const std::shared_ptr<Bytes>& bytes, int32_t offset)
    : offset_(offset), bytes_(bytes) {
    assert(bytes_->size() > 0);
    assert(offset_ >= 0);
}

void BloomFilter64::BitSet::Set(int32_t index) {
    char* data = bytes_->data();
    data[(static_cast<uint32_t>(index) >> 3) + offset_] |=
        static_cast<char>(1u << (index & BloomFilter64::BitSet::MASK));
}

bool BloomFilter64::BitSet::Get(int32_t index) const {
    const char* data = bytes_->data();
    return (data[(static_cast<uint32_t>(index) >> 3) + offset_] &
            static_cast<char>(1u << (index & BloomFilter64::BitSet::MASK))) != 0;
}

int32_t BloomFilter64::BitSet::BitSize() const {
    return (bytes_->size() - offset_) * BloomFilter64::BYTE_SIZE;
}

void BloomFilter64::BitSet::ToByteArray(int32_t offset, int32_t length, char* bytes) const {
    assert(bytes);
    assert(offset >= 0);
    assert(length >= 0);
    assert(static_cast<size_t>(offset_ + length) <= bytes_->size());
    std::memcpy(bytes + offset, bytes_->data() + offset_, length);
}

Result<BloomFilter64> BloomFilter64::Create(int64_t items, double fpp,
                                            const std::shared_ptr<MemoryPool>& pool) {
    if (items <= 0) {
        return Status::Invalid("items must be greater than 0 for bloom filter");
    }
    if (!std::isfinite(fpp) || fpp <= 0.0 || fpp >= 1.0) {
        return Status::Invalid("fpp must be greater than 0 and less than 1 for bloom filter");
    }
    const double log_two = std::log(2);
    const double estimated_bits = -static_cast<double>(items) * std::log(fpp) / (log_two * log_two);
    if (estimated_bits > std::numeric_limits<int32_t>::max() - BYTE_SIZE) {
        return Status::Invalid("bloom filter size exceeds the supported range");
    }
    const auto num_bits_without_padding = static_cast<int32_t>(estimated_bits);
    const int32_t num_bits =
        num_bits_without_padding + (BYTE_SIZE - (num_bits_without_padding % BYTE_SIZE));
    const int32_t num_hash_functions = std::max(
        1, static_cast<int32_t>(std::round(static_cast<double>(num_bits) / items * log_two)));
    auto bytes = std::make_shared<Bytes>(num_bits / BYTE_SIZE, pool.get());
    auto bit_set = std::make_unique<BitSet>(bytes, /*offset=*/0);
    return BloomFilter64(num_hash_functions, std::move(bit_set));
}

BloomFilter64::BloomFilter64(int32_t num_hash_functions, std::unique_ptr<BitSet>&& bit_set)
    : num_bits_(bit_set->BitSize()),
      num_hash_functions_(num_hash_functions),
      bit_set_(std::move(bit_set)) {}

void BloomFilter64::AddHash(int64_t hash64) {
    auto hash1 = static_cast<int32_t>(hash64);
    auto hash2 = static_cast<int32_t>(static_cast<uint64_t>(hash64) >> 32);

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        // Use uint32_t arithmetic to avoid signed overflow UB (matches Java int wrap semantics)
        auto combined_hash =
            static_cast<int32_t>(static_cast<uint32_t>(hash1) +
                                 (static_cast<uint32_t>(i) * static_cast<uint32_t>(hash2)));
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % num_bits_;
        bit_set_->Set(pos);
    }
}

bool BloomFilter64::TestHash(int64_t hash64) const {
    auto hash1 = static_cast<int32_t>(hash64);
    auto hash2 = static_cast<int32_t>(static_cast<uint64_t>(hash64) >> 32);

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        // Use uint32_t arithmetic to avoid signed overflow UB (matches Java int wrap semantics)
        auto combined_hash =
            static_cast<int32_t>(static_cast<uint32_t>(hash1) +
                                 (static_cast<uint32_t>(i) * static_cast<uint32_t>(hash2)));
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % num_bits_;
        if (!bit_set_->Get(pos)) {
            return false;
        }
    }
    return true;
}

}  // namespace paimon
