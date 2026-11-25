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

#include "paimon/index/index_result.h"

#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

template <typename Bitmap>
std::shared_ptr<IndexResult<Bitmap>> IndexResult<Bitmap>::Remain() {
    static std::shared_ptr<IndexResult<Bitmap>> remain =
        std::make_shared<RemainIndexResult<Bitmap>>();
    return remain;
}

template <typename Bitmap>
std::shared_ptr<IndexResult<Bitmap>> IndexResult<Bitmap>::Skip() {
    static std::shared_ptr<IndexResult<Bitmap>> skip = std::make_shared<SkipIndexResult<Bitmap>>();
    return skip;
}

template <typename Bitmap>
Result<bool> RemainIndexResult<Bitmap>::IsRemain() const {
    return true;
}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> RemainIndexResult<Bitmap>::And(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    if (!other->IsExact()) {
        return other;
    }
    return other->And(this->shared_from_this());
}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> RemainIndexResult<Bitmap>::Or(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    return this->shared_from_this();
}

template <typename Bitmap>
std::string RemainIndexResult<Bitmap>::ToString() const {
    return "REMAIN";
}

template <typename Bitmap>
Result<bool> SkipIndexResult<Bitmap>::IsRemain() const {
    return false;
}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> SkipIndexResult<Bitmap>::And(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    return this->shared_from_this();
}

template <typename Bitmap>
Result<std::shared_ptr<IndexResult<Bitmap>>> SkipIndexResult<Bitmap>::Or(
    const std::shared_ptr<IndexResult<Bitmap>>& other) {
    return other;
}

template <typename Bitmap>
std::string SkipIndexResult<Bitmap>::ToString() const {
    return "SKIP";
}

template class IndexResult<RoaringBitmap32>;
template class IndexResult<RoaringBitmap64>;
template class RemainIndexResult<RoaringBitmap32>;
template class RemainIndexResult<RoaringBitmap64>;
template class SkipIndexResult<RoaringBitmap32>;
template class SkipIndexResult<RoaringBitmap64>;

}  // namespace paimon
