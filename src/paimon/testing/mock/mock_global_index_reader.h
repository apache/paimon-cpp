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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {

class MockGlobalIndexReader : public GlobalIndexReader {
 public:
    explicit MockGlobalIndexReader(bool supports_search = true)
        : supports_search_(supports_search) {}

    /// Sets the result returned by all Visit* methods (default behavior).
    /// Pass an empty vector for an empty bitmap.
    void SetDefaultResult(const std::vector<int64_t>& row_ids) {
        default_result_ = row_ids;
        return_nullptr_ = false;
        return_error_ = false;
    }

    /// Configures this reader to return nullptr for all Visit* methods.
    void SetReturnNullptr() {
        return_nullptr_ = true;
        return_error_ = false;
    }

    /// Configures this reader to return an error Status for all Visit* methods.
    void SetReturnError(const std::string& message) {
        return_error_ = true;
        return_nullptr_ = false;
        error_message_ = message;
    }

    void SetThrowException(const std::string& message) {
        throw_exception_ = true;
        exception_message_ = message;
    }

    /// Sets a scored result returned by VisitVectorSearch.
    void SetScoredResult(const std::vector<int64_t>& row_ids, const std::vector<float>& scores) {
        scored_row_ids_ = row_ids;
        scored_scores_ = scores;
        has_scored_result_ = true;
    }

    void SetThreadSafe(bool thread_safe) {
        thread_safe_ = thread_safe;
    }

    /// Counts how many times any Visit* method was invoked. Useful to assert all readers
    /// are exercised by UnionGlobalIndexReader.
    int InvocationCount() const {
        return invocation_count_.load();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(
        const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override {
        return MakeResult();
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        invocation_count_++;
        if (!supports_search_) {
            return Status::Invalid("not supported");
        }
        if (return_error_) {
            return Status::Invalid(error_message_);
        }
        if (!has_scored_result_) {
            return std::shared_ptr<ScoredGlobalIndexResult>(nullptr);
        }
        auto bitmap = RoaringBitmap64::From(scored_row_ids_);
        auto scores = scored_scores_;
        return std::make_shared<BitmapScoredGlobalIndexResult>(std::move(bitmap),
                                                               std::move(scores));
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        if (!supports_search_) {
            return Status::Invalid("not supported");
        }
        {
            std::lock_guard<std::mutex> lock(captured_search_mutex_);
            captured_full_text_search_ = full_text_search;
        }
        return MakeResult();
    }

    // Captures the (possibly pre_filter-rewritten) FullTextSearch the offset
    // reader forwarded, so tests can assert field propagation.
    std::shared_ptr<FullTextSearch> CapturedFullTextSearch() const {
        std::lock_guard<std::mutex> lock(captured_search_mutex_);
        return captured_full_text_search_;
    }

    bool IsThreadSafe() const override {
        return thread_safe_;
    }

    std::string GetIndexType() const override {
        return "fake";
    }

 private:
    Result<std::shared_ptr<GlobalIndexResult>> MakeResult() {
        invocation_count_++;
        if (throw_exception_) {
            throw std::runtime_error(exception_message_);
        }
        if (return_error_) {
            return Status::Invalid(error_message_);
        }
        if (return_nullptr_) {
            return std::shared_ptr<GlobalIndexResult>(nullptr);
        }
        auto ids = default_result_;
        return std::make_shared<BitmapGlobalIndexResult>(
            [ids]() { return RoaringBitmap64::From(ids); });
    }

 private:
    bool supports_search_;
    mutable std::mutex captured_search_mutex_;
    std::shared_ptr<FullTextSearch> captured_full_text_search_;
    std::vector<int64_t> default_result_;
    bool return_nullptr_ = false;
    bool return_error_ = false;
    bool throw_exception_ = false;
    std::string error_message_;
    std::string exception_message_;
    std::vector<int64_t> scored_row_ids_;
    std::vector<float> scored_scores_;
    bool has_scored_result_ = false;
    bool thread_safe_ = true;
    std::atomic<int32_t> invocation_count_{0};
};

}  // namespace paimon::test
