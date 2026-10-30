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

#include "paimon/table/source/table_read.h"

#include "paimon/catalog/table.h"
#include "paimon/core/table/file_store_table_factory.h"
#include "paimon/core/utils/context_utils.h"

namespace paimon {

Result<std::unique_ptr<TableRead>> TableRead::Create(std::unique_ptr<ReadContext> ctx) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "read"));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, FileStoreTableFactory::Create(*ctx));
    return table->NewRead(std::move(ctx));
}

Result<std::unique_ptr<CountReader>> TableRead::CreateCountReader(
    const std::vector<std::shared_ptr<Split>>& splits) {
    (void)splits;
    return Status::NotImplemented("CreateCountReader is not implemented for this table type");
}

}  // namespace paimon
