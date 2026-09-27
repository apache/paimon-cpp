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

#include "paimon/common/predicate/like_optimization.h"

#include <string>

#include "paimon/common/predicate/contains.h"
#include "paimon/common/predicate/ends_with.h"
#include "paimon/common/predicate/equal.h"
#include "paimon/common/predicate/starts_with.h"

namespace paimon {

std::optional<OptimizedLike> LikeOptimization::TryOptimize(const Literal& literal) {
    if (literal.IsNull() || literal.GetType() != FieldType::STRING) {
        return std::nullopt;
    }

    const auto pattern = literal.GetValue<std::string>();
    if (pattern.find('_') != std::string::npos || pattern.find('\\') != std::string::npos) {
        return std::nullopt;
    }

    size_t first_percent = pattern.find('%');
    if (first_percent == std::string::npos) {
        if (pattern.empty()) {
            return std::nullopt;
        }
        return OptimizedLike{&Equal::Instance(),
                             Literal(FieldType::STRING, pattern.data(), pattern.size())};
    }

    size_t last_percent = pattern.rfind('%');
    if (first_percent != last_percent) {
        if (first_percent == 0 && last_percent == pattern.size() - 1 && pattern.size() > 2 &&
            pattern.find('%', 1) == last_percent) {
            std::string infix = pattern.substr(1, pattern.size() - 2);
            return OptimizedLike{&Contains::Instance(),
                                 Literal(FieldType::STRING, infix.data(), infix.size())};
        }
        return std::nullopt;
    }

    if (first_percent == pattern.size() - 1 && first_percent > 0) {
        std::string prefix = pattern.substr(0, pattern.size() - 1);
        return OptimizedLike{&StartsWith::Instance(),
                             Literal(FieldType::STRING, prefix.data(), prefix.size())};
    }
    if (first_percent == 0 && pattern.size() > 1) {
        std::string suffix = pattern.substr(1);
        return OptimizedLike{&EndsWith::Instance(),
                             Literal(FieldType::STRING, suffix.data(), suffix.size())};
    }
    return std::nullopt;
}

}  // namespace paimon
