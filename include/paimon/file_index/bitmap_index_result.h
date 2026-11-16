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

#include <functional>
#include <memory>
#include <string>

#include "paimon/file_index/file_index_result.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/visibility.h"

namespace paimon {
/// The implementation of bitmap file index result, represents row granularity.
/// @note The inner bitmap in BitmapIndexResult is lazily initialized only when
/// the result is about to be used.
class PAIMON_EXPORT BitmapIndexResult : public FileIndexResult {
 public:
    using BitmapSupplier = std::function<Result<RoaringBitmap32>()>;

    explicit BitmapIndexResult(BitmapSupplier bitmap_supplier, bool is_exact = false);
    ~BitmapIndexResult() override;

    /// @return Whether the file is remained.
    Result<bool> IsRemain() const override;

    /// Compute the intersection of the current result with the provided result.
    Result<std::shared_ptr<FileIndexResult>> And(
        const std::shared_ptr<FileIndexResult>& other) override;

    /// Compute the union of the current result with the provided result.
    Result<std::shared_ptr<FileIndexResult>> Or(
        const std::shared_ptr<FileIndexResult>& other) override;

    /// @return Inner `RoaringBitmap32`.
    Result<const RoaringBitmap32*> GetBitmap() const;

    std::string ToString() const override;

 private:
    mutable bool initialized_ = false;
    BitmapSupplier bitmap_supplier_;
    mutable RoaringBitmap32 bitmap_;
};

}  // namespace paimon
