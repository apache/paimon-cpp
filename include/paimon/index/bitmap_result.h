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

#include "paimon/index/index_result.h"

namespace paimon {

/// Lazily loaded row positions, shared by file and real-time index results.
template <typename Bitmap>
class PAIMON_EXPORT BitmapResult : public IndexResult<Bitmap> {
 public:
    using BitmapSupplier = std::function<Result<Bitmap>()>;

    /// Construct a materialized result, rejecting negative row positions.
    static Result<std::shared_ptr<BitmapResult<Bitmap>>> Create(Bitmap rows, bool is_exact);

    explicit BitmapResult(BitmapSupplier bitmap_supplier, bool is_exact = false);

    Result<bool> IsRemain() const override;

    /// Load the bitmap if needed. The result owns the returned bitmap.
    Result<const Bitmap*> GetBitmap() const;

    Result<std::shared_ptr<IndexResult<Bitmap>>> And(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    Result<std::shared_ptr<IndexResult<Bitmap>>> Or(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    std::string ToString() const override;

 private:
    BitmapResult(Bitmap rows, bool is_exact);

    mutable bool initialized_ = false;
    BitmapSupplier bitmap_supplier_;
    mutable Bitmap bitmap_;
};

}  // namespace paimon
