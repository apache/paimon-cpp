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
#include "paimon/global_index/bitmap_global_index_result.h"

namespace paimon {
Result<std::shared_ptr<BitmapGlobalIndexResult>> BitmapGlobalIndexResult::Create(
    BitmapSupplier bitmap_supplier) {
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 bitmap, bitmap_supplier());
    return std::make_shared<BitmapGlobalIndexResult>(std::move(bitmap));
}

Result<std::unique_ptr<GlobalIndexResult::Iterator>> BitmapGlobalIndexResult::CreateIterator()
    const {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, GetBitmap());
    auto iter = bitmap->Begin();
    return std::make_unique<BitmapGlobalIndexResult::Iterator>(bitmap, std::move(iter));
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapGlobalIndexResult::And(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(other);
    if (typed_result) {
        return std::make_shared<BitmapGlobalIndexResult>(
            RoaringBitmap64::And(bitmap_, typed_result->bitmap_));
    }
    return GlobalIndexResult::And(other);
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapGlobalIndexResult::Or(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(other);
    if (typed_result) {
        return std::make_shared<BitmapGlobalIndexResult>(
            RoaringBitmap64::Or(bitmap_, typed_result->bitmap_));
    }
    return GlobalIndexResult::Or(other);
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapGlobalIndexResult::AddOffset(int64_t offset) {
    if (offset == 0) {
        return shared_from_this();
    }
    RoaringBitmap64 bitmap64;
    for (auto iter = bitmap_.Begin(); iter != bitmap_.End(); ++iter) {
        bitmap64.Add(offset + (*iter));
    }
    bitmap64.RunOptimize();
    return std::make_shared<BitmapGlobalIndexResult>(std::move(bitmap64));
}

Result<const RoaringBitmap64*> BitmapGlobalIndexResult::GetBitmap() const {
    return &bitmap_;
}

Result<bool> BitmapGlobalIndexResult::IsEmpty() const {
    return bitmap_.IsEmpty();
}

std::string BitmapGlobalIndexResult::ToString() const {
    return bitmap_.ToString();
}

std::shared_ptr<BitmapGlobalIndexResult> BitmapGlobalIndexResult::FromRanges(
    const std::vector<Range>& ranges) {
    RoaringBitmap64 bitmap;
    for (const auto& range : ranges) {
        bitmap.AddRange(range.from, range.to + 1);
    }
    return std::make_shared<BitmapGlobalIndexResult>(std::move(bitmap));
}
}  // namespace paimon
