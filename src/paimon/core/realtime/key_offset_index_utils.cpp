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

#include "paimon/core/realtime/key_offset_index_utils.h"

#include "arrow/api.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/macros.h"
#include "xxhash.h"  // NOLINT(build/include_subdir)

namespace paimon {
Result<std::vector<uint64_t>> KeyOffsetIndexUtils::HashKeys(
    const std::shared_ptr<arrow::StructArray>& keys, const std::shared_ptr<arrow::Field>& key_field,
    const std::shared_ptr<KeySerializer>& key_serializer) {
    if (!keys || keys->num_fields() != 1 || !keys->field(0)->type()->Equals(key_field->type())) {
        return Status::Invalid(
            "deduplicate key lookup must contain exactly the configured key field");
    }
    if (keys->null_count() != 0 || keys->field(0)->null_count() != 0) {
        return Status::Invalid("deduplicate key must not contain null");
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<Literal> literals,
                           LiteralConverter::ConvertLiteralsFromArray(*keys->field(0),
                                                                      /*own_data=*/true));
    std::vector<uint64_t> hashes;
    hashes.reserve(literals.size());
    for (const Literal& literal : literals) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, key_serializer->Serialize(literal));
        hashes.push_back(XXH64(bytes->data(), bytes->size(), /*seed=*/0));
    }
    return hashes;
}

}  // namespace paimon
