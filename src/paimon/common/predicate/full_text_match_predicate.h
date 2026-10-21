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

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/predicate/leaf_predicate.h"

namespace paimon {
struct FullTextMatch;

/// A full-text leaf evaluated by an index, rather than scalar PredicateFilter::Test.
/// Use PredicateBuilder::FullTextMatch to construct it.
class FullTextMatchPredicate : public LeafPredicate {
 public:
    FullTextMatchPredicate(int32_t field_index, const std::shared_ptr<FullTextMatch>& match);

    const std::shared_ptr<FullTextMatch>& GetFullTextMatch() const {
        return full_text_match_;
    }

    bool operator==(const Predicate& other) const override;
    std::string ToString() const override;

    /// Rebind the field position while sharing the original request.
    std::shared_ptr<FullTextMatchPredicate> NewLeafPredicate(int32_t new_field_index) const;
    /// Rebind the field name using a request copy, preserving the original request.
    std::shared_ptr<FullTextMatchPredicate> NewLeafPredicate(
        const std::string& new_field_name) const;

 private:
    std::shared_ptr<FullTextMatch> full_text_match_;
};

}  // namespace paimon
