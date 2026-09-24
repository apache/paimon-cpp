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

#pragma once

#include <memory>

#include "lumina/extensions/experimental/CkptManager.h"
#include "paimon/global_index/io/global_index_checkpoint_file_manager.h"

namespace paimon::lumina {

/// Adapts Paimon's task-scoped global index checkpoint storage to Lumina.
class LuminaCheckpointManager final : public ::lumina::extensions::experimental::CkptManager {
 public:
    explicit LuminaCheckpointManager(
        const std::shared_ptr<GlobalIndexCheckpointFileManager>& file_manager)
        : file_manager_(file_manager) {}

    std::unique_ptr<::lumina::io::FileWriter> CreateCkptFileWriter() override;

    ::lumina::core::Result<bool> HasCkptFile() override;

    std::unique_ptr<::lumina::io::FileReader> GetCkptFileReader() override;

 private:
    std::shared_ptr<GlobalIndexCheckpointFileManager> file_manager_;
};

}  // namespace paimon::lumina
