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

#include "paimon/index/bitmap_result.h"

#include <utility>

#include "paimon/common/utils/checked_cast.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

template <typename Bitmap>
Result<std::shared_ptr<BitmapResult<Bitmap>>> BitmapResult<Bitmap>::Create(Bitmap rows,
                                                                           bool is_exact) {
    for (auto iter = rows.Begin(); iter != rows.End(); ++iter) {
        if (*iter < 0) {
            return Status::Invalid("Index result returned a negative row position");
        }
    }
    return std::shared_ptr<BitmapResult<Bitmap>>(
        new BitmapResult<Bitmap>(std::move(rows), is_exact));
}

template <typename Bitmap>
Result<bool> BitmapResult<Bitmap>::IsRemain() const {
    PAIMON_ASSIGN_OR_RAISE(const Bitmap* rows, GetBitmap());
    return !rows->IsEmpty();
}

template <typename Bitmap>
BitmapResult<Bitmap>::BitmapResult(Bitmap rows, bool is_exact)
    : IndexResult<Bitmap>(is_exact), initialized_(true), bitmap_(std::move(rows)) {}

template <typename Bitmap>
BitmapResult<Bitmap>::BitmapResult(BitmapSupplier bitmap_supplier, bool is_exact)
    : IndexResult<Bitmap>(is_exact), bitmap_supplier_(std::move(bitmap_supplier)) {}

template <typename Bitmap>
Result<const Bitmap*> BitmapResult<Bitmap>::GetBitmap() const {
    if (!initialized_) {
        PAIMON_ASSIGN_OR_RAISE(bitmap_, bitmap_supplier_());
        initialized_ = true;
    }
    return &bitmap_;
}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> BitmapResult<Bitmap>::And(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    if (auto typed_other = std::dynamic_pointer_cast<BitmapResult<Bitmap>>(other)) {
        return std::make_shared<BitmapResult<Bitmap>>(
            [result = std::dynamic_pointer_cast<BitmapResult<Bitmap>>(this->shared_from_this()),
             typed_other]() -> Result<Bitmap> {
                PAIMON_ASSIGN_OR_RAISE(const Bitmap* bitmap, result->GetBitmap());
                PAIMON_ASSIGN_OR_RAISE(const Bitmap* other_bitmap, typed_other->GetBitmap());
                return Bitmap::And(*bitmap, *other_bitmap);
            },
            this->IsExact() && other->IsExact());
    } else if (std::dynamic_pointer_cast<RemainIndexResult<Bitmap>>(other)) {
        if (!this->IsExact()) {
            return this->shared_from_this();
        }
        return std::make_shared<BitmapResult<Bitmap>>(
            [result = checked_pointer_cast<BitmapResult<Bitmap>>(
                 this->shared_from_this())]() -> Result<Bitmap> {
                PAIMON_ASSIGN_OR_RAISE(const Bitmap* bitmap, result->GetBitmap());
                return *bitmap;
            },
            /*is_exact=*/false);
    } else if (std::dynamic_pointer_cast<SkipIndexResult<Bitmap>>(other)) {
        return IndexResult<Bitmap>::Skip();
    } else {
        return Status::Invalid("Cannot AND BitmapResult with unsupported result type");
    }
}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> BitmapResult<Bitmap>::Or(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    if (auto typed_other = std::dynamic_pointer_cast<BitmapResult<Bitmap>>(other)) {
        return std::make_shared<BitmapResult<Bitmap>>(
            [result = std::dynamic_pointer_cast<BitmapResult<Bitmap>>(this->shared_from_this()),
             typed_other]() -> Result<Bitmap> {
                PAIMON_ASSIGN_OR_RAISE(const Bitmap* bitmap, result->GetBitmap());
                PAIMON_ASSIGN_OR_RAISE(const Bitmap* other_bitmap, typed_other->GetBitmap());
                return Bitmap::Or(*bitmap, *other_bitmap);
            },
            this->IsExact() && other->IsExact());
    } else if (std::dynamic_pointer_cast<RemainIndexResult<Bitmap>>(other)) {
        return IndexResult<Bitmap>::Remain();
    } else if (std::dynamic_pointer_cast<SkipIndexResult<Bitmap>>(other)) {
        return this->shared_from_this();
    } else {
        return Status::Invalid("Cannot OR BitmapResult with unsupported result type");
    }
}

template <typename Bitmap>
std::string BitmapResult<Bitmap>::ToString() const {
    auto bitmap = GetBitmap();
    if (!bitmap.ok()) {
        return bitmap.status().ToString();
    }
    return bitmap.value()->ToString();
}

template class BitmapResult<RoaringBitmap32>;
template class BitmapResult<RoaringBitmap64>;

}  // namespace paimon
