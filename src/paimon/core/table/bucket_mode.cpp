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

#include "paimon/core/table/bucket_mode.h"

#include "paimon/core/schema/table_schema.h"

namespace paimon {

BucketMode ResolveBucketMode(int32_t bucket, const std::shared_ptr<TableSchema>& table_schema) {
    bool has_primary_keys = !table_schema->PrimaryKeys().empty();
    // Postpone bucket is only valid for primary key tables.
    if (has_primary_keys && bucket == BucketModeDefine::POSTPONE_BUCKET) {
        return BucketMode::POSTPONE_MODE;
    }
    if (bucket == -1) {
        return has_primary_keys ? BucketMode::HASH_DYNAMIC : BucketMode::BUCKET_UNAWARE;
    }
    return BucketMode::HASH_FIXED;
}

}  // namespace paimon
