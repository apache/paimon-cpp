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

#include "paimon/common/types/row_kind.h"

#include <cstdint>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/status.h"

namespace paimon {

const RowKind* RowKind::Insert() {
    static const RowKind kInsert{"+I", "INSERT", static_cast<std::int8_t>(0)};
    return &kInsert;
}

const RowKind* RowKind::UpdateBefore() {
    static const RowKind kUpdateBefore{"-U", "UPDATE_BEFORE", static_cast<std::int8_t>(1)};
    return &kUpdateBefore;
}

const RowKind* RowKind::UpdateAfter() {
    static const RowKind kUpdateAfter{"+U", "UPDATE_AFTER", static_cast<std::int8_t>(2)};
    return &kUpdateAfter;
}

const RowKind* RowKind::Delete() {
    static const RowKind kDelete{"-D", "DELETE", static_cast<std::int8_t>(3)};
    return &kDelete;
}

Result<const RowKind*> RowKind::FromShortString(const std::string& value) {
    std::string upper_value = StringUtils::ToUpperCase(value);
    if (upper_value == "+I") {
        return Insert();
    } else if (upper_value == "-U") {
        return UpdateBefore();
    } else if (upper_value == "+U") {
        return UpdateAfter();
    } else if (upper_value == "-D") {
        return Delete();
    } else {
        return Status::Invalid(fmt::format("Unsupported short string {} for row kind.", value));
    }
}

}  // namespace paimon
