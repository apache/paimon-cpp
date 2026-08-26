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

#include <cassert>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

#include "paimon/common/data/serializer/row_compacted_serializer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/changelog_result.h"
#include "paimon/core/mergetree/compact/first_row_merge_function.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// Wrapper for `MergeFunction`s to produce changelog by lookup for first row.
class FirstRowMergeFunctionWrapper : public MergeFunctionWrapper<ChangelogResult> {
 public:
    FirstRowMergeFunctionWrapper(
        std::unique_ptr<FirstRowMergeFunction>&& merge_function,
        std::function<Result<bool>(const std::shared_ptr<InternalRow>&)> contains,
        std::unique_ptr<RowCompactedSerializer>&& value_serializer)
        : merge_function_(std::move(merge_function)),
          contains_(std::move(contains)),
          value_serializer_(std::move(value_serializer)) {}

    void Reset() override {
        merge_function_->Reset();
    }

    Status Add(KeyValue&& kv) override {
        return merge_function_->Add(std::move(kv));
    }

    Result<std::optional<ChangelogResult>> GetResult() override {
        PAIMON_ASSIGN_OR_RAISE(std::optional<KeyValue> result, merge_function_->GetResult());
        ChangelogResult changelog_result;
        if (merge_function_->ContainsHighLevel()) {
            changelog_result.result = std::move(result);
            Reset();
            return std::optional<ChangelogResult>(std::move(changelog_result));
        }
        if (!result) {
            Reset();
            return Status::Invalid(
                "In FirstRowMergeFunctionWrapper when call GetResult, there must have a result");
        }
        PAIMON_ASSIGN_OR_RAISE(bool contains, contains_(result.value().key));
        if (contains) {
            // empty
            Reset();
            return std::optional<ChangelogResult>(std::move(changelog_result));
        }
        if (value_serializer_) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes,
                                   value_serializer_->SerializeToBytes(*result->value));
            PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InternalRow> changelog_value,
                                   value_serializer_->Deserialize(bytes));
            changelog_result.changelogs.emplace_back(result->value_kind, result->sequence_number,
                                                     result->level, result->key,
                                                     std::move(changelog_value));
        }
        changelog_result.result = std::move(result);
        Reset();
        return std::optional<ChangelogResult>(std::move(changelog_result));
    }

 private:
    std::unique_ptr<FirstRowMergeFunction> merge_function_;
    std::function<Result<bool>(const std::shared_ptr<InternalRow>&)> contains_;
    std::unique_ptr<RowCompactedSerializer> value_serializer_;
};

}  // namespace paimon
