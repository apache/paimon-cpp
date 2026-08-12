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

#include "paimon/realtime/arrow_mem_indexer_factory.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/realtime/arrow_mem_indexer.h"
#include "paimon/macros.h"

namespace paimon {

Result<std::shared_ptr<MemIndexer>> ArrowMemIndexerFactory::Create(
    std::unique_ptr<ArrowSchema> write_schema, const std::map<std::string, std::string>&,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (!write_schema || !write_schema->release) {
        return Status::Invalid("mem indexer write schema is null");
    }
    ScopeGuard schema_guard([schema = write_schema.get()]() { ArrowSchemaRelease(schema); });
    if (!memory_pool) {
        return Status::Invalid("mem indexer memory pool is null");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> imported_schema,
                                      arrow::ImportSchema(write_schema.get()));
    std::shared_ptr<arrow::MemoryPool> arrow_pool = GetArrowPool(memory_pool);
    return std::make_shared<ArrowMemIndexer>(imported_schema, memory_pool, arrow_pool);
}

}  // namespace paimon
