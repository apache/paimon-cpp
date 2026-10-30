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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "paimon/memory/memory_segment.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

class CacheValue;
enum class CacheKind {
    DEFAULT,
    MANIFEST,
    DATA_FILE_FOOTER,
    SNAPSHOT_LIVE_MANIFEST,
    INDEX_READER,
};

class PAIMON_EXPORT CacheKey {
 public:
    static std::shared_ptr<CacheKey> ForPosition(const std::string& file_path, int64_t position,
                                                 int32_t length, bool is_index);
    static std::shared_ptr<CacheKey> ForKind(const std::string& file_path, int64_t position,
                                             int32_t length, CacheKind kind);
    static std::shared_ptr<CacheKey> ForIndexReader(const std::string& uri,
                                                    const std::string& column_name);

 public:
    virtual ~CacheKey() = default;

    virtual bool IsIndex() const = 0;

    CacheKind GetKind() const {
        return kind_;
    }

    virtual bool Equals(const CacheKey& other) const = 0;

    virtual size_t HashCode() const = 0;

 protected:
    explicit CacheKey(CacheKind kind) : kind_(kind) {}

 private:
    CacheKind kind_ = CacheKind::DEFAULT;
};

using CacheCallback = std::function<void(const std::shared_ptr<CacheKey>&)>;

class PAIMON_EXPORT Cache {
 public:
    virtual ~Cache() = default;

    virtual Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) = 0;

    virtual Status Put(const std::shared_ptr<CacheKey>& key,
                       const std::shared_ptr<CacheValue>& value) = 0;

    virtual void Invalidate(const std::shared_ptr<CacheKey>& key) = 0;

    virtual void InvalidateAll() = 0;

    virtual size_t Size() const = 0;
};

class PAIMON_EXPORT CacheValue {
 public:
    virtual ~CacheValue();

    /// Returns the typed payload, retaining its owner; null for a null value or type mismatch.
    template <typename T>
    static std::shared_ptr<T> Get(const std::shared_ptr<CacheValue>& value);

    /// Bytes charged against the cache budget.
    int64_t GetMemoryUsage() const;

    void OnEvict(const std::shared_ptr<CacheKey>& key) const;

    virtual bool operator==(const CacheValue& other) const = 0;

 protected:
    CacheValue(CacheCallback callback, int64_t memory_usage);

 private:
    CacheCallback callback_;
    int64_t memory_usage_;
};

template <typename T>
class PAIMON_EXPORT TypedCacheValue : public CacheValue {
 protected:
    using CacheValue::CacheValue;

 private:
    friend class CacheValue;
    virtual T* GetValuePointer() = 0;
};

template <typename T>
std::shared_ptr<T> CacheValue::Get(const std::shared_ptr<CacheValue>& value) {
    auto* typed_value = dynamic_cast<TypedCacheValue<T>*>(value.get());
    if (typed_value) {
        if (T* payload = typed_value->GetValuePointer()) {
            return std::shared_ptr<T>(value, payload);
        }
    }
    return nullptr;
}

class PAIMON_EXPORT MemorySegmentCacheValue final : public TypedCacheValue<const MemorySegment> {
 public:
    MemorySegmentCacheValue(const MemorySegment& segment, CacheCallback callback);

    /// Charges retained allocation size, at least the segment's logical size.
    MemorySegmentCacheValue(const MemorySegment& segment, CacheCallback callback,
                            int64_t memory_usage);

    bool operator==(const CacheValue& other) const override;

 private:
    const MemorySegment* GetValuePointer() override {
        return &segment_;
    }

    MemorySegment segment_;
};

template <typename T>
class PAIMON_EXPORT ObjectCacheValue final : public TypedCacheValue<T> {
 public:
    ObjectCacheValue(std::shared_ptr<T> value, CacheCallback callback, int64_t memory_usage)
        : TypedCacheValue<T>(std::move(callback), std::max<int64_t>(1, memory_usage)),
          value_(std::move(value)) {}

    bool operator==(const CacheValue& other) const override {
        const auto* rhs = dynamic_cast<const ObjectCacheValue<T>*>(&other);
        return rhs && value_ == rhs->value_;
    }

 private:
    T* GetValuePointer() override {
        return value_.get();
    }

    std::shared_ptr<T> value_;
};

}  // namespace paimon
