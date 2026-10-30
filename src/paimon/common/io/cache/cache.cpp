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

#include "paimon/cache/cache.h"

#include <algorithm>
#include <utility>

namespace paimon {

CacheValue::CacheValue(CacheCallback callback, int64_t memory_usage)
    : callback_(std::move(callback)), memory_usage_(memory_usage) {}

CacheValue::~CacheValue() = default;

int64_t CacheValue::GetMemoryUsage() const {
    return memory_usage_;
}

void CacheValue::OnEvict(const std::shared_ptr<CacheKey>& key) const {
    if (callback_) {
        callback_(key);
    }
}

MemorySegmentCacheValue::MemorySegmentCacheValue(const MemorySegment& segment,
                                                 CacheCallback callback)
    : MemorySegmentCacheValue(segment, std::move(callback), segment.Size()) {}

MemorySegmentCacheValue::MemorySegmentCacheValue(const MemorySegment& segment,
                                                 CacheCallback callback, int64_t memory_usage)
    : TypedCacheValue<const MemorySegment>(std::move(callback),
                                           std::max<int64_t>(segment.Size(), memory_usage)),
      segment_(segment) {}

bool MemorySegmentCacheValue::operator==(const CacheValue& other) const {
    const auto* rhs = dynamic_cast<const MemorySegmentCacheValue*>(&other);
    return rhs && segment_ == rhs->segment_;
}

}  // namespace paimon
