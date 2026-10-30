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

#include "paimon/file_store_write.h"

#include "paimon/catalog/table.h"
#include "paimon/core/table/file_store_table_factory.h"
#include "paimon/core/utils/context_utils.h"
#include "paimon/write_context.h"

namespace paimon {

Result<std::unique_ptr<FileStoreWrite>> FileStoreWrite::Create(std::unique_ptr<WriteContext> ctx) {
    PAIMON_RETURN_NOT_OK(ContextUtils::CheckOperatorContext(ctx, "write"));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, FileStoreTableFactory::Create(*ctx));
    return table->NewWrite(std::move(ctx));
}

Result<std::vector<RealtimeCommitProgress>> FileStoreWrite::PrepareCommitWithProgress(int64_t) {
    return Status::Invalid("prepare commit with progress requires a real-time writer");
}

Status FileStoreWrite::Seal() {
    return Status::Invalid("seal requires a real-time writer");
}

Status FileStoreWrite::RefreshCommittedSnapshot(int64_t) {
    return Status::Invalid("refresh committed snapshot requires a real-time writer");
}

}  // namespace paimon
