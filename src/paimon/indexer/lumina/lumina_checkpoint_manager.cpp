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

#include "paimon/indexer/lumina/lumina_checkpoint_manager.h"

#include <utility>

#include "paimon/indexer/lumina/lumina_file_reader.h"
#include "paimon/indexer/lumina/lumina_file_writer.h"
#include "paimon/indexer/lumina/lumina_utils.h"

namespace paimon::lumina {

std::unique_ptr<::lumina::io::FileWriter> LuminaCheckpointManager::CreateCkptFileWriter() {
    Result<std::unique_ptr<OutputStream>> output = file_manager_->CreateCheckpointOutputStream();
    if (!output.ok()) {
        return nullptr;
    }
    std::shared_ptr<OutputStream> shared_output = std::move(output).value();
    return std::make_unique<LuminaFileWriter>(shared_output);
}

::lumina::core::Result<bool> LuminaCheckpointManager::HasCkptFile() {
    Result<bool> exists = file_manager_->CheckpointExists();
    if (!exists.ok()) {
        return ::lumina::core::Result<bool>::Err(PaimonToLuminaStatus(exists.status()));
    }
    return ::lumina::core::Result<bool>::Ok(exists.value());
}

std::unique_ptr<::lumina::io::FileReader> LuminaCheckpointManager::GetCkptFileReader() {
    Result<std::unique_ptr<InputStream>> input = file_manager_->OpenCheckpointInputStream();
    if (!input.ok()) {
        return nullptr;
    }
    std::shared_ptr<InputStream> shared_input = std::move(input).value();
    return std::make_unique<LuminaFileReader>(shared_input);
}

}  // namespace paimon::lumina
