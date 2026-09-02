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

#include <memory>
#include <optional>
#include <utility>

#include "paimon/common/data/serializer/row_compacted_serializer.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/changelog_result.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Wrapper for `MergeFunction`s which produces changelog during a full compaction.
class FullChangelogMergeFunctionWrapper : public MergeFunctionWrapper<ChangelogResult> {
 public:
    FullChangelogMergeFunctionWrapper(std::unique_ptr<MergeFunction>&& merge_function,
                                      int32_t max_level,
                                      std::unique_ptr<RowCompactedSerializer>&& value_serializer,
                                      FieldsComparator::FieldComparatorFunc value_equalizer)
        : merge_function_(std::move(merge_function)),
          max_level_(max_level),
          value_serializer_(std::move(value_serializer)),
          value_equalizer_(std::move(value_equalizer)) {}

    void Reset() override {
        merge_function_->Reset();
        top_level_kv_ = std::nullopt;
        initial_kv_ = std::nullopt;
        is_initialized_ = false;
    }

    Status Add(KeyValue&& kv) override {
        if (!initial_kv_) {
            initial_kv_ = std::move(kv);
            return Status::OK();
        }

        if (!is_initialized_) {
            if (initial_kv_->level == max_level_) {
                PAIMON_RETURN_NOT_OK(RememberTopLevel(*initial_kv_));
            }
            PAIMON_RETURN_NOT_OK(merge_function_->Add(std::move(initial_kv_).value()));
            is_initialized_ = true;
        }

        if (kv.level == max_level_) {
            PAIMON_RETURN_NOT_OK(RememberTopLevel(kv));
        }
        return merge_function_->Add(std::move(kv));
    }

    Result<std::optional<ChangelogResult>> GetResult() override {
        std::optional<KeyValue> merged;
        if (is_initialized_) {
            PAIMON_ASSIGN_OR_RAISE(merged, merge_function_->GetResult());
        } else {
            merged = std::move(initial_kv_);
        }

        ChangelogResult result;
        if (is_initialized_) {
            if (!top_level_kv_) {
                if (merged && merged->value_kind->IsAdd()) {
                    PAIMON_ASSIGN_OR_RAISE(KeyValue insert,
                                           CloneKeyValue(*merged, RowKind::Insert()));
                    result.changelogs.emplace_back(std::move(insert));
                }
            } else if (!merged || !merged->value_kind->IsAdd()) {
                top_level_kv_->value_kind = RowKind::Delete();
                result.changelogs.emplace_back(std::move(top_level_kv_).value());
            } else if (!value_equalizer_ ||
                       value_equalizer_(*top_level_kv_->value, *merged->value) != 0) {
                top_level_kv_->value_kind = RowKind::UpdateBefore();
                result.changelogs.emplace_back(std::move(top_level_kv_).value());
                PAIMON_ASSIGN_OR_RAISE(KeyValue update_after,
                                       CloneKeyValue(*merged, RowKind::UpdateAfter()));
                result.changelogs.emplace_back(std::move(update_after));
            }
        } else if (merged && merged->level != max_level_ && merged->value_kind->IsAdd()) {
            PAIMON_ASSIGN_OR_RAISE(KeyValue insert, CloneKeyValue(*merged, RowKind::Insert()));
            result.changelogs.emplace_back(std::move(insert));
        }

        if (merged && merged->value_kind->IsAdd()) {
            result.result = std::move(merged);
        }
        Reset();
        return std::optional<ChangelogResult>(std::move(result));
    }

 private:
    Status RememberTopLevel(const KeyValue& kv) {
        if (top_level_kv_) {
            return Status::Invalid("Top level key-value already exists. This is unexpected.");
        }
        PAIMON_ASSIGN_OR_RAISE(top_level_kv_, CloneKeyValue(kv, kv.value_kind));
        return Status::OK();
    }

    Result<KeyValue> CloneKeyValue(const KeyValue& from, const RowKind* value_kind) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes,
                               value_serializer_->SerializeToBytes(*from.value));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InternalRow> value,
                               value_serializer_->Deserialize(bytes));
        return KeyValue(value_kind, from.sequence_number, KeyValue::UNKNOWN_LEVEL, from.key,
                        std::move(value));
    }

    std::unique_ptr<MergeFunction> merge_function_;
    int32_t max_level_;
    std::unique_ptr<RowCompactedSerializer> value_serializer_;
    FieldsComparator::FieldComparatorFunc value_equalizer_;
    std::optional<KeyValue> top_level_kv_;
    std::optional<KeyValue> initial_kv_;
    bool is_initialized_ = false;
};

}  // namespace paimon
