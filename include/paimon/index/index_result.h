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

#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

/// An index result within one source, read view and candidate scope.
/// Implementations are provided for RoaringBitmap32 and RoaringBitmap64.
/// Bitmap constrains And/Or to compatible result types; this base class stores no bitmap.
/// Exact results contain all and only matching rows. Inexact results may omit matches.
template <typename Bitmap>
class PAIMON_EXPORT IndexResult : public std::enable_shared_from_this<IndexResult<Bitmap>> {
 public:
    virtual ~IndexResult() = default;

    static std::shared_ptr<IndexResult<Bitmap>> Remain();
    static std::shared_ptr<IndexResult<Bitmap>> Skip();

    bool IsExact() const {
        return is_exact_;
    }

    /// Whether the result retains candidates. This alone does not establish exactness.
    virtual Result<bool> IsRemain() const = 0;

    /// Combine different predicates over the same source, view and candidate scope.
    virtual Result<std::shared_ptr<IndexResult<Bitmap>>> And(
        const std::shared_ptr<IndexResult<Bitmap>>& other) = 0;
    virtual Result<std::shared_ptr<IndexResult<Bitmap>>> Or(
        const std::shared_ptr<IndexResult<Bitmap>>& other) = 0;

    virtual std::string ToString() const = 0;

 protected:
    explicit IndexResult(bool is_exact) : is_exact_(is_exact) {}

 private:
    bool is_exact_;
};

template <typename Bitmap>
class PAIMON_EXPORT RemainIndexResult : public IndexResult<Bitmap> {
 public:
    RemainIndexResult() : IndexResult<Bitmap>(false) {}

    Result<bool> IsRemain() const override;
    Result<std::shared_ptr<IndexResult<Bitmap>>> And(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    Result<std::shared_ptr<IndexResult<Bitmap>>> Or(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    std::string ToString() const override;
};

template <typename Bitmap>
class PAIMON_EXPORT SkipIndexResult : public IndexResult<Bitmap> {
 public:
    SkipIndexResult() : IndexResult<Bitmap>(true) {}

    Result<bool> IsRemain() const override;
    Result<std::shared_ptr<IndexResult<Bitmap>>> And(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    Result<std::shared_ptr<IndexResult<Bitmap>>> Or(
        const std::shared_ptr<IndexResult<Bitmap>>& other) override;
    std::string ToString() const override;
};

}  // namespace paimon
