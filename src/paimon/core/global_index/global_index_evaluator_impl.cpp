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

#include "paimon/core/global_index/global_index_evaluator_impl.h"

#include <set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/predicate/predicate_utils.h"

namespace paimon {
namespace {
void FlattenChildren(const std::shared_ptr<CompoundPredicate>& compound_predicate,
                     std::vector<std::shared_ptr<Predicate>>* flattened) {
    for (const std::shared_ptr<Predicate>& child : compound_predicate->Children()) {
        auto compound_child = std::dynamic_pointer_cast<CompoundPredicate>(child);
        if (compound_child != nullptr && compound_child->GetFunction().GetType() ==
                                             compound_predicate->GetFunction().GetType()) {
            FlattenChildren(compound_child, flattened);
        } else {
            flattened->push_back(child);
        }
    }
}

/// A predicate is null-rejecting when it cannot match a row whose tested field is null.
/// Under SQL three-valued logic every comparison and match predicate rejects null; only
/// IS NULL accepts it, and IS NOT NULL is the predicate being pruned.
bool IsNullRejecting(const std::shared_ptr<Predicate>& predicate) {
    auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    if (leaf_predicate == nullptr) {
        return false;
    }
    switch (leaf_predicate->GetFunction().GetType()) {
        case Function::Type::EQUAL:
        case Function::Type::NOT_EQUAL:
        case Function::Type::GREATER_THAN:
        case Function::Type::GREATER_OR_EQUAL:
        case Function::Type::LESS_THAN:
        case Function::Type::LESS_OR_EQUAL:
        case Function::Type::IN:
        case Function::Type::NOT_IN:
        case Function::Type::STARTS_WITH:
        case Function::Type::ENDS_WITH:
        case Function::Type::CONTAINS:
        case Function::Type::LIKE:
            return true;
        default:
            return false;
    }
}

bool IsIsNotNull(const std::shared_ptr<Predicate>& predicate) {
    auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate);
    return leaf_predicate != nullptr &&
           leaf_predicate->GetFunction().GetType() == Function::Type::IS_NOT_NULL;
}
}  // namespace

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexEvaluatorImpl::Evaluate(
    const std::shared_ptr<Predicate>& predicate) {
    PAIMON_ASSIGN_OR_RAISE(std::optional<Evaluation> evaluation,
                           EvaluateWithContributingFields(predicate));
    if (!evaluation) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return evaluation->result;
}

Result<std::optional<GlobalIndexEvaluatorImpl::Evaluation>>
GlobalIndexEvaluatorImpl::EvaluateWithContributingFields(
    const std::shared_ptr<Predicate>& predicate) {
    if (!predicate) {
        return std::optional<Evaluation>();
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> normalized_predicate,
                           NormalizePredicate(predicate));
    return EvaluatePredicate(normalized_predicate);
}

Result<std::set<int32_t>> GlobalIndexEvaluatorImpl::CollectFieldIds(
    const TableSchema& table_schema, const std::shared_ptr<Predicate>& predicate) {
    std::set<int32_t> field_ids;
    if (!predicate) {
        return field_ids;
    }
    std::set<std::string> field_names;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(predicate, &field_names));
    for (const DataField& field : table_schema.Fields()) {
        if (field_names.count(field.Name()) > 0) {
            field_ids.insert(field.Id());
        }
    }
    return field_ids;
}

Result<std::shared_ptr<Predicate>> GlobalIndexEvaluatorImpl::NormalizePredicate(
    const std::shared_ptr<Predicate>& predicate) {
    auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate);
    if (compound_predicate == nullptr) {
        return predicate;
    }
    std::vector<std::shared_ptr<Predicate>> children;
    FlattenChildren(compound_predicate, &children);

    bool is_and = compound_predicate->GetFunction().GetType() == Function::Type::AND;
    if (is_and) {
        std::set<std::string> constrained_fields;
        for (const std::shared_ptr<Predicate>& child : children) {
            auto leaf = std::dynamic_pointer_cast<LeafPredicate>(child);
            if (leaf != nullptr && IsNullRejecting(child)) {
                constrained_fields.insert(leaf->FieldName());
            }
        }
        if (!constrained_fields.empty()) {
            std::vector<std::shared_ptr<Predicate>> pruned;
            pruned.reserve(children.size());
            for (const std::shared_ptr<Predicate>& child : children) {
                auto leaf = std::dynamic_pointer_cast<LeafPredicate>(child);
                if (leaf != nullptr && IsIsNotNull(child) &&
                    constrained_fields.count(leaf->FieldName()) > 0) {
                    continue;
                }
                pruned.push_back(child);
            }
            children = std::move(pruned);
        }
    }

    std::vector<std::shared_ptr<Predicate>> normalized_children;
    normalized_children.reserve(children.size());
    for (const std::shared_ptr<Predicate>& child : children) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Predicate> normalized_child,
                               NormalizePredicate(child));
        normalized_children.push_back(std::move(normalized_child));
    }
    if (normalized_children.size() == 1) {
        return normalized_children[0];
    }
    if (is_and) {
        return PredicateBuilder::And(normalized_children);
    }
    return PredicateBuilder::Or(normalized_children);
}

Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GlobalIndexEvaluatorImpl::GetIndexReaders(
    int32_t field_id) {
    // get or create global index readers for current field
    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    auto iter = index_readers_cache_.find(field_id);
    if (iter != index_readers_cache_.end()) {
        readers = iter->second;
    } else {
        PAIMON_ASSIGN_OR_RAISE(readers, create_index_readers_(field_id));
        index_readers_cache_.insert({field_id, readers});
    }
    return readers;
}

Result<std::optional<GlobalIndexEvaluatorImpl::Evaluation>>
GlobalIndexEvaluatorImpl::EvaluatePredicate(const std::shared_ptr<Predicate>& predicate) {
    if (predicate == nullptr) {
        return std::optional<Evaluation>();
    }

    if (auto compound_predicate = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return EvaluateCompoundPredicate(compound_predicate);
    } else if (auto leaf_predicate = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        PAIMON_ASSIGN_OR_RAISE(DataField field,
                               table_schema_->GetField(leaf_predicate->FieldName()));
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<GlobalIndexReader>> readers,
                               GetIndexReaders(field.Id()));
        if (readers.empty()) {
            // No usable index for this field within the requested range. Treat as "no
            // pushdown available" so the upstream falls back to a full scan instead of
            // wrongly producing an empty result.
            return std::optional<Evaluation>();
        }
        // calculate compound result as field may has multiple indexes
        std::shared_ptr<GlobalIndexResult> compound_result;
        for (const auto& index_reader : readers) {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<GlobalIndexResult> sub_result,
                PredicateUtils::VisitPredicate<std::shared_ptr<GlobalIndexResult>>(leaf_predicate,
                                                                                   index_reader));
            if (sub_result) {
                if (!compound_result) {
                    compound_result = sub_result;
                } else {
                    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> and_result,
                                           compound_result->And(sub_result));
                    compound_result = and_result;
                }
            }
            if (compound_result) {
                PAIMON_ASSIGN_OR_RAISE(bool is_empty, compound_result->IsEmpty());
                if (is_empty) {
                    break;
                }
            }
        }
        if (!compound_result) {
            return std::optional<Evaluation>();
        }
        return std::optional<Evaluation>(
            Evaluation{compound_result, {field.Id()}, /*declined_field_ids=*/{}});
    }
    return Status::Invalid(fmt::format(
        "cannot cast predicate {} to CompoundPredicate or LeafPredicate", predicate->ToString()));
}

Result<std::optional<GlobalIndexEvaluatorImpl::Evaluation>>
GlobalIndexEvaluatorImpl::EvaluateCompoundPredicate(
    const std::shared_ptr<CompoundPredicate>& compound_predicate) {
    std::shared_ptr<GlobalIndexResult> compound_result;
    std::set<int32_t> contributing_field_ids;
    std::set<int32_t> declined_field_ids;
    auto merge_fields = [&](const Evaluation& evaluation) {
        contributing_field_ids.insert(evaluation.contributing_field_ids.begin(),
                                      evaluation.contributing_field_ids.end());
        declined_field_ids.insert(evaluation.declined_field_ids.begin(),
                                  evaluation.declined_field_ids.end());
    };
    if (compound_predicate->GetFunction().GetType() == Function::Type::OR) {
        std::vector<std::shared_ptr<GlobalIndexResult>> results;
        results.reserve(compound_predicate->Children().size());
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<Evaluation> sub_evaluation,
                                   EvaluatePredicate(child));
            if (!sub_evaluation) {
                return std::optional<Evaluation>();
            }
            results.push_back(std::move(sub_evaluation->result));
            merge_fields(*sub_evaluation);
        }
        PAIMON_ASSIGN_OR_RAISE(compound_result, GlobalIndexUtils::UnionResults(results));
    } else if (compound_predicate->GetFunction().GetType() == Function::Type::AND) {
        for (const auto& child : compound_predicate->Children()) {
            PAIMON_ASSIGN_OR_RAISE(std::optional<Evaluation> sub_evaluation,
                                   EvaluatePredicate(child));
            if (sub_evaluation) {
                if (!compound_result) {
                    compound_result = sub_evaluation->result;
                } else {
                    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> and_result,
                                           compound_result->And(sub_evaluation->result));
                    compound_result = and_result;
                }
                merge_fields(*sub_evaluation);
            } else {
                PAIMON_ASSIGN_OR_RAISE(std::set<int32_t> child_field_ids,
                                       CollectFieldIds(*table_schema_, child));
                declined_field_ids.insert(child_field_ids.begin(), child_field_ids.end());
            }

            if (compound_result) {
                PAIMON_ASSIGN_OR_RAISE(bool is_empty, compound_result->IsEmpty());
                if (is_empty) {
                    break;
                }
            }
        }
    } else {
        return Status::Invalid("CompoundPredicate only support And/Or function");
    }
    if (!compound_result) {
        return std::optional<Evaluation>();
    }
    for (int32_t declined_field_id : declined_field_ids) {
        contributing_field_ids.erase(declined_field_id);
    }
    return std::optional<Evaluation>(Evaluation{std::move(compound_result),
                                                std::move(contributing_field_ids),
                                                std::move(declined_field_ids)});
}

}  // namespace paimon
