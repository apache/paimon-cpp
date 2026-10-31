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

#include "paimon/common/predicate/full_text_match_predicate.h"

#include "fmt/format.h"
#include "paimon/common/predicate/full_text_match_function.h"
#include "paimon/defs.h"
#include "paimon/predicate/full_text_match.h"

namespace paimon {

FullTextMatchPredicate::FullTextMatchPredicate(int32_t field_index,
                                               const std::shared_ptr<FullTextMatch>& match)
    : LeafPredicate(FullTextMatchFunction::Instance(), field_index, match->field_name,
                    FieldType::STRING, {}),
      full_text_match_(match) {}

bool FullTextMatchPredicate::operator==(const Predicate& other) const {
    if (this == &other) {
        return true;
    }
    auto text_predicate = dynamic_cast<FullTextMatchPredicate*>(const_cast<Predicate*>(&other));
    if (!text_predicate) {
        return false;
    }
    return LeafPredicate::operator==(other) &&
           *full_text_match_ == *text_predicate->full_text_match_;
}

std::string FullTextMatchPredicate::ToString() const {
    return fmt::format(
        "FullTextMatch({}, {}, include_row_ids={})", FieldName(), full_text_match_->query,
        full_text_match_->include_row_ids ? full_text_match_->include_row_ids->ToString() : "null");
}

std::shared_ptr<FullTextMatchPredicate> FullTextMatchPredicate::NewLeafPredicate(
    int32_t new_field_index) const {
    return std::make_shared<FullTextMatchPredicate>(new_field_index, full_text_match_);
}

std::shared_ptr<FullTextMatchPredicate> FullTextMatchPredicate::NewLeafPredicate(
    const std::string& new_field_name) const {
    auto request = std::make_shared<FullTextMatch>(*full_text_match_);
    request->field_name = new_field_name;
    return std::make_shared<FullTextMatchPredicate>(FieldIndex(), request);
}

}  // namespace paimon
