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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "paimon/core/global_index/global_index_evaluator.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/predicate/compound_predicate.h"

namespace paimon {
class GlobalIndexEvaluatorImpl : public GlobalIndexEvaluator {
 public:
    /// Creates the underlying readers for the given field. Returns an empty vector when the field
    /// has no usable index.
    using IndexReadersCreator =
        std::function<Result<std::vector<std::shared_ptr<GlobalIndexReader>>>(int32_t)>;

    /// The rows that the indexes matched for a predicate, aligned with Java
    /// `GlobalIndexEvaluator.Evaluation`.
    struct Evaluation {
        std::shared_ptr<GlobalIndexResult> result;
        /// The ids of the fields whose indexes decided `result`. Unlike Java, a field of
        /// `declined_field_ids` does not count even if another of its predicates was evaluated, as
        /// `result` is then only a superset of the rows matching that field.
        std::set<int32_t> contributing_field_ids;
        /// The ids of the fields of the conjuncts that were dropped, at any depth of the predicate,
        /// because no index could evaluate them.
        std::set<int32_t> declined_field_ids;
    };

    GlobalIndexEvaluatorImpl(const std::shared_ptr<TableSchema>& table_schema,
                             IndexReadersCreator create_index_readers)
        : table_schema_(table_schema), create_index_readers_(std::move(create_index_readers)) {}

    Result<std::shared_ptr<GlobalIndexResult>> Evaluate(
        const std::shared_ptr<Predicate>& predicate) override;

    /// Evaluates `predicate` as `Evaluate()` does, and reports the fields that decided the result.
    /// @return The evaluation, or nullopt if the indexes cannot evaluate the predicate.
    Result<std::optional<Evaluation>> EvaluateWithContributingFields(
        const std::shared_ptr<Predicate>& predicate);

    /// Returns the ids of the fields of `table_schema` that `predicate` refers to, as Java
    /// `PredicateVisitor#collectFieldIds` does; empty for a null predicate.
    static Result<std::set<int32_t>> CollectFieldIds(const TableSchema& table_schema,
                                                     const std::shared_ptr<Predicate>& predicate);

    /// Applies Java-compatible compound flattening and removes redundant IS NOT NULL
    /// predicates from AND expressions.
    static Result<std::shared_ptr<Predicate>> NormalizePredicate(
        const std::shared_ptr<Predicate>& predicate);

 private:
    Result<std::optional<Evaluation>> EvaluatePredicate(
        const std::shared_ptr<Predicate>& predicate);

    Result<std::optional<Evaluation>> EvaluateCompoundPredicate(
        const std::shared_ptr<CompoundPredicate>& compound_predicate);

    Result<std::vector<std::shared_ptr<GlobalIndexReader>>> GetIndexReaders(int32_t field_id);

 private:
    std::shared_ptr<TableSchema> table_schema_;
    // create_index_readers_(field_id)
    IndexReadersCreator create_index_readers_;
    // [field_id, vector<reader>]
    std::map<int32_t, std::vector<std::shared_ptr<GlobalIndexReader>>> index_readers_cache_;
};

}  // namespace paimon
