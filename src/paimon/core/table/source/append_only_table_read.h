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

#pragma once

#include <memory>
#include <vector>

#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/operation/split_read.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/table/source/table_read.h"

namespace paimon {

class SplitRead;
class Executor;
class FileStorePathFactory;
class InternalReadContext;
class MemoryPool;
class RealtimeSplit;

class AppendOnlyTableRead : public TableRead {
 public:
    AppendOnlyTableRead(const std::shared_ptr<FileStorePathFactory>& path_factory,
                        const std::shared_ptr<InternalReadContext>& context,
                        const std::shared_ptr<MemoryPool>& memory_pool,
                        const std::shared_ptr<Executor>& executor);

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& data_split) override;

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override;

    Result<std::unique_ptr<CountReader>> CreateCountReader(
        const std::vector<std::shared_ptr<Split>>& splits) override;

 private:
    Result<std::unique_ptr<BatchReader>> CreateRealtimeReader(
        const std::shared_ptr<RealtimeSplit>& realtime_split, bool release_ticket);

    Result<std::unique_ptr<BatchReader>> CreateDiskReader(const std::shared_ptr<Split>& split);

    std::vector<std::unique_ptr<SplitRead>> split_reads_;
    std::shared_ptr<InternalReadContext> context_;
};

}  // namespace paimon
