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

#include "paimon/core/mergetree/spill_writer.h"

#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/arrow_ipc_file.h"
#include "paimon/core/mergetree/spill_channel_manager.h"
#include "paimon/macros.h"

namespace paimon {

SpillWriter::~SpillWriter() = default;

Result<std::unique_ptr<SpillWriter>> SpillWriter::Create(
    const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<FileIOChannel::Enumerator>& channel_enumerator,
    const std::shared_ptr<SpillChannelManager>& spill_channel_manager,
    const std::string& compression, int32_t compression_level, bool use_threads,
    const std::shared_ptr<MemoryPool>& pool) {
    std::unique_ptr<SpillWriter> writer(new SpillWriter());
    PAIMON_RETURN_NOT_OK(writer->Open(fs, schema, channel_enumerator, spill_channel_manager,
                                      compression, compression_level, use_threads, pool));
    return writer;
}

Status SpillWriter::Open(const std::shared_ptr<FileSystem>& fs,
                         const std::shared_ptr<arrow::Schema>& schema,
                         const std::shared_ptr<FileIOChannel::Enumerator>& channel_enumerator,
                         const std::shared_ptr<SpillChannelManager>& spill_channel_manager,
                         const std::string& compression, int32_t compression_level,
                         bool use_threads, const std::shared_ptr<MemoryPool>& pool) {
    channel_id_ = channel_enumerator->Next();
    auto cleanup_guard = ScopeGuard([&]() {
        ipc_writer_.reset();
        if (!channel_id_.GetPath().empty()) {
            [[maybe_unused]] auto status = fs->Delete(channel_id_.GetPath());
        }
    });
    PAIMON_ASSIGN_OR_RAISE(ipc_writer_, ArrowIpcFileWriter::Create(
                                            fs, channel_id_.GetPath(), schema, compression,
                                            compression_level, use_threads, GetArrowPool(pool)));
    spill_channel_manager->AddChannel(channel_id_);
    cleanup_guard.Release();
    return Status::OK();
}

Status SpillWriter::WriteBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    return ipc_writer_->WriteBatch(batch);
}

Status SpillWriter::Close() {
    return ipc_writer_->Close();
}

Result<int64_t> SpillWriter::GetFileSize() const {
    if (channel_id_.GetPath().empty()) {
        return Status::Invalid("spill writer has no channel id");
    }
    return ipc_writer_->GetFileSize();
}

const FileIOChannel::ID& SpillWriter::GetChannelId() const {
    return channel_id_;
}

}  // namespace paimon
