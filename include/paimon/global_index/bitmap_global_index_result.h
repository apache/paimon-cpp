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

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "paimon/global_index/global_index_result.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {
/// Represents a global index query result backed by a Roaring bitmap.
class PAIMON_EXPORT BitmapGlobalIndexResult : public GlobalIndexResult {
 public:
    using BitmapSupplier = std::function<Result<RoaringBitmap64>()>;
    explicit BitmapGlobalIndexResult(RoaringBitmap64 bitmap) : bitmap_(std::move(bitmap)) {}

    /// Executes the supplier immediately and returns its bitmap, or propagates its error.
    /// The returned result does not retain the supplier or its captures.
    static Result<std::shared_ptr<BitmapGlobalIndexResult>> Create(BitmapSupplier bitmap_supplier);

    class Iterator : public GlobalIndexResult::Iterator {
     public:
        Iterator(const RoaringBitmap64* bitmap, RoaringBitmap64::Iterator&& iter)
            : bitmap_(bitmap), iter_(std::move(iter)) {}

        bool HasNext() const override {
            return iter_ != bitmap_->End();
        }

        int64_t Next() override {
            uint64_t value = *iter_;
            ++iter_;
            return value;
        }

     private:
        const RoaringBitmap64* bitmap_;
        RoaringBitmap64::Iterator iter_;
    };

    Result<std::unique_ptr<GlobalIndexResult::Iterator>> CreateIterator() const override;

    Result<std::shared_ptr<GlobalIndexResult>> And(
        const std::shared_ptr<GlobalIndexResult>& other) override;

    Result<std::shared_ptr<GlobalIndexResult>> Or(
        const std::shared_ptr<GlobalIndexResult>& other) override;

    Result<bool> IsEmpty() const override;

    Result<std::shared_ptr<GlobalIndexResult>> AddOffset(int64_t offset) override;

    std::string ToString() const override;

    /// @return A non-owning, const pointer to the bitmap. The returned pointer is valid as long as
    ///         this BitmapGlobalIndexResult object is alive. The caller must not modify the bitmap.
    /// @note Returns the stored bitmap without performing computation or I/O.
    Result<const RoaringBitmap64*> GetBitmap() const;

    /// Creates `BitmapGlobalIndexResult` for all row ids in the given ranges.
    /// @note Overlapping or unsorted ranges are accepted.
    static std::shared_ptr<BitmapGlobalIndexResult> FromRanges(const std::vector<Range>& ranges);

 private:
    RoaringBitmap64 bitmap_;
};
}  // namespace paimon
