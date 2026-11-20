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

#include "paimon/file_index/bitmap_index_result.h"

#include <utility>

#include "paimon/common/utils/checked_cast.h"
#include "paimon/status.h"

namespace paimon {
BitmapIndexResult::BitmapIndexResult(BitmapSupplier bitmap_supplier, bool is_exact)
    : FileIndexResult(is_exact), bitmap_supplier_(std::move(bitmap_supplier)) {}

BitmapIndexResult::~BitmapIndexResult() = default;

Result<const RoaringBitmap32*> BitmapIndexResult::GetBitmap() const {
    if (!initialized_) {
        PAIMON_ASSIGN_OR_RAISE(bitmap_, bitmap_supplier_());
        initialized_ = true;
    }
    return &bitmap_;
}

Result<bool> BitmapIndexResult::IsRemain() const {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, GetBitmap());
    return !bitmap->IsEmpty();
}

Result<std::shared_ptr<FileIndexResult>> BitmapIndexResult::And(
    const std::shared_ptr<FileIndexResult>& other) {
    if (auto typed_other = std::dynamic_pointer_cast<BitmapIndexResult>(other)) {
        return std::make_shared<BitmapIndexResult>(
            [result = std::dynamic_pointer_cast<BitmapIndexResult>(shared_from_this()),
             typed_other]() -> Result<RoaringBitmap32> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, result->GetBitmap());
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* other_bitmap,
                                       typed_other->GetBitmap());
                return RoaringBitmap32::And(*bitmap, *other_bitmap);
            },
            IsExact() && other->IsExact());
    } else if (std::dynamic_pointer_cast<paimon::Remain>(other)) {
        if (!IsExact()) {
            return shared_from_this();
        }
        return std::make_shared<BitmapIndexResult>(
            [result = checked_pointer_cast<BitmapIndexResult>(
                 shared_from_this())]() -> Result<RoaringBitmap32> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, result->GetBitmap());
                return *bitmap;
            },
            /*is_exact=*/false);
    } else if (std::dynamic_pointer_cast<paimon::Skip>(other)) {
        return FileIndexResult::Skip();
    } else {
        return Status::Invalid("Cannot AND BitmapIndexResult with unsupported result type");
    }
}

Result<std::shared_ptr<FileIndexResult>> BitmapIndexResult::Or(
    const std::shared_ptr<FileIndexResult>& other) {
    if (auto typed_other = std::dynamic_pointer_cast<BitmapIndexResult>(other)) {
        return std::make_shared<BitmapIndexResult>(
            [result = std::dynamic_pointer_cast<BitmapIndexResult>(shared_from_this()),
             typed_other]() -> Result<RoaringBitmap32> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, result->GetBitmap());
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* other_bitmap,
                                       typed_other->GetBitmap());
                return RoaringBitmap32::Or(*bitmap, *other_bitmap);
            },
            IsExact() && other->IsExact());
    } else if (std::dynamic_pointer_cast<paimon::Remain>(other)) {
        return FileIndexResult::Remain();
    } else if (std::dynamic_pointer_cast<paimon::Skip>(other)) {
        return shared_from_this();
    } else {
        return Status::Invalid("Cannot OR BitmapIndexResult with unsupported result type");
    }
}

std::string BitmapIndexResult::ToString() const {
    auto bitmap = GetBitmap();
    if (!bitmap.ok()) {
        return bitmap.status().ToString();
    }
    return bitmap.value()->ToString();
}
}  // namespace paimon
