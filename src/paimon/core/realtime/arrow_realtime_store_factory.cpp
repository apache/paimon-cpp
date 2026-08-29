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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/realtime/arrow_realtime_store_factory.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/realtime/arrow_realtime_store.h"
#include "paimon/core/realtime/primary_key_realtime_store.h"
#include "paimon/macros.h"

namespace paimon {

Result<std::shared_ptr<RealtimeStore>> ArrowRealtimeStoreFactory::Create(
    RealtimeStoreCreateRequest&& request) {
    if (!request.write_schema || !request.write_schema->release) {
        return Status::Invalid("real-time store write schema is null");
    }
    ScopeGuard schema_guard(
        [schema = request.write_schema.get()]() { ArrowSchemaRelease(schema); });
    if (!request.memory_pool) {
        return Status::Invalid("real-time store memory pool is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_schema,
                                      arrow::ImportSchema(request.write_schema.get()));
    switch (request.mode) {
        case RealtimeStoreMode::APPEND_ONLY: {
            std::shared_ptr<arrow::MemoryPool> arrow_pool =
                GetSharedArrowPool(request.memory_pool);
            return std::make_shared<ArrowRealtimeStore>(imported_schema, request.statistics_mode,
                                                        request.memory_pool, arrow_pool);
        }
        case RealtimeStoreMode::PRIMARY_KEY: {
            PAIMON_ASSIGN_OR_RAISE(
                std::shared_ptr<PrimaryKeyRealtimeStore> store,
                PrimaryKeyRealtimeStore::Create(imported_schema, request.memory_pool));
            return std::shared_ptr<RealtimeStore>(std::move(store));
        }
    }
    return Status::Invalid("invalid real-time store mode: ", static_cast<int32_t>(request.mode));
}

}  // namespace paimon
