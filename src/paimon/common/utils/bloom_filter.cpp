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

#include "paimon/common/utils/bloom_filter.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <utility>
#include <vector>

#include "paimon/memory/memory_pool.h"
#include "paimon/memory/memory_segment.h"

namespace paimon {
namespace {

Status ValidateFpp(double fpp) {
    if (!std::isfinite(fpp) || fpp <= 0.0 || fpp >= 1.0) {
        return Status::Invalid("fpp must be greater than 0 and less than 1 for bloom filter");
    }
    return Status::OK();
}

class FixedBloomFilterBuilder : public BloomFilter::Builder {
 public:
    explicit FixedBloomFilterBuilder(const std::shared_ptr<BloomFilter>& bloom_filter)
        : bloom_filter_(bloom_filter) {}

    Status AddHash(int32_t hash) override {
        return bloom_filter_->AddHash(hash);
    }

    Result<std::shared_ptr<BloomFilter>> Build() override {
        return bloom_filter_;
    }

 private:
    std::shared_ptr<BloomFilter> bloom_filter_;
};

class DynamicBloomFilterBuilder : public BloomFilter::Builder {
 public:
    DynamicBloomFilterBuilder(double fpp, const std::shared_ptr<MemoryPool>& pool)
        : fpp_(fpp), pool_(pool) {
        hashes_.reserve(kInitialHashCapacity);
    }

    Status AddHash(int32_t hash) override {
        if (!built_) {
            hashes_.push_back(hash);
        }
        return Status::OK();
    }

    Result<std::shared_ptr<BloomFilter>> Build() override {
        if (built_) {
            return bloom_filter_;
        }
        if (hashes_.empty()) {
            built_ = true;
            return std::shared_ptr<BloomFilter>();
        }
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<BloomFilter::Builder> builder,
            BloomFilter::FixedBuilder(static_cast<int64_t>(hashes_.size()), fpp_, pool_.get()));
        for (int32_t hash : hashes_) {
            PAIMON_RETURN_NOT_OK(builder->AddHash(hash));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BloomFilter> bloom_filter, builder->Build());
        bloom_filter_ = std::move(bloom_filter);
        built_ = true;
        std::vector<int32_t>().swap(hashes_);
        return bloom_filter_;
    }

 private:
    static constexpr int32_t kInitialHashCapacity = 1024;

    double fpp_;
    std::shared_ptr<MemoryPool> pool_;
    std::vector<int32_t> hashes_;
    std::shared_ptr<BloomFilter> bloom_filter_;
    bool built_ = false;
};

}  // namespace

int32_t BloomFilter::OptimalNumOfBits(int64_t expected_entries, double fpp) {
    if (expected_entries <= 0 || fpp <= 0.0 || fpp >= 1.0) {
        return 0;
    }
    double result = -static_cast<double>(expected_entries) * log(fpp) / (log(2) * log(2));
    if (result > INT32_MAX) return INT32_MAX;
    if (result < 0) return 0;
    return static_cast<int32_t>(result);
}

int32_t BloomFilter::OptimalNumOfHashFunctions(int64_t expected_entries, int64_t bit_size) {
    if (expected_entries <= 0) {
        return 1;
    }
    double ratio = static_cast<double>(bit_size) / static_cast<double>(expected_entries);
    double result = ratio * std::log(2.0);
    return std::max(1, static_cast<int32_t>(std::round(result)));
}

Result<std::shared_ptr<BloomFilter>> BloomFilter::Create(int64_t expected_entries, double fpp) {
    if (expected_entries <= 0) {
        return Status::Invalid("expected entries must be greater than 0 for bloom filter");
    }
    PAIMON_RETURN_NOT_OK(ValidateFpp(fpp));
    auto bytes =
        static_cast<int32_t>(ceil(BloomFilter::OptimalNumOfBits(expected_entries, fpp) / 8.0));
    return std::make_shared<BloomFilter>(expected_entries, bytes);
}

Result<std::shared_ptr<BloomFilter::Builder>> BloomFilter::FixedBuilder(int64_t expected_entries,
                                                                        double fpp,
                                                                        MemoryPool* pool) {
    if (pool == nullptr) {
        return Status::Invalid("Memory pool must not be null for bloom filter builder");
    }
    PAIMON_RETURN_NOT_OK(ValidateFpp(fpp));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BloomFilter> bloom_filter,
                           BloomFilter::Create(expected_entries, fpp));
    MemorySegment memory = MemorySegment::AllocateHeapMemory(bloom_filter->ByteLength(), pool);
    PAIMON_RETURN_NOT_OK(bloom_filter->SetMemorySegment(std::move(memory)));
    bloom_filter->Reset();
    return std::shared_ptr<Builder>(new FixedBloomFilterBuilder(bloom_filter));
}

Result<std::shared_ptr<BloomFilter::Builder>> BloomFilter::DynamicBuilder(
    double fpp, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(ValidateFpp(fpp));
    if (pool == nullptr) {
        return Status::Invalid("Memory pool must not be null for bloom filter builder");
    }
    return std::shared_ptr<Builder>(new DynamicBloomFilterBuilder(fpp, pool));
}

BloomFilter::BloomFilter(int64_t expected_entries, int32_t byte_length)
    : expected_entries_(expected_entries) {
    num_hash_functions_ = OptimalNumOfHashFunctions(
        expected_entries, static_cast<int64_t>(static_cast<uint64_t>(byte_length) << 3));
    bit_set_ = std::make_shared<BitSet>(byte_length);
}

Status BloomFilter::AddHash(int32_t hash1) {
    auto hash2 = static_cast<int32_t>(static_cast<uint32_t>(hash1) >> 16);

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        // Use uint32_t arithmetic to avoid signed overflow UB (matches Java int wrap semantics)
        auto combined_hash =
            static_cast<int32_t>(static_cast<uint32_t>(hash1) +
                                 (static_cast<uint32_t>(i) * static_cast<uint32_t>(hash2)));
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % bit_set_->BitSize();
        PAIMON_RETURN_NOT_OK(bit_set_->Set(pos));
    }
    return Status::OK();
}

bool BloomFilter::TestHash(int32_t hash1) const {
    auto hash2 = static_cast<int32_t>(static_cast<uint32_t>(hash1) >> 16);

    for (int32_t i = 1; i <= num_hash_functions_; i++) {
        // Use uint32_t arithmetic to avoid signed overflow UB (matches Java int wrap semantics)
        auto combined_hash =
            static_cast<int32_t>(static_cast<uint32_t>(hash1) +
                                 (static_cast<uint32_t>(i) * static_cast<uint32_t>(hash2)));
        // hashcode should be positive, flip all the bits if it's negative
        if (combined_hash < 0) {
            combined_hash = ~combined_hash;
        }
        int32_t pos = combined_hash % bit_set_->BitSize();
        if (!bit_set_->Get(pos)) {
            return false;
        }
    }
    return true;
}

Status BloomFilter::SetMemorySegment(MemorySegment segment, int32_t offset) {
    return bit_set_->SetMemorySegment(segment, offset);
}

void BloomFilter::Reset() {
    bit_set_->Clear();
}

}  // namespace paimon
