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

#include <cstdint>
#include <memory>
#include <vector>

#include "paimon/commit_message.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/type_fwd.h"

namespace paimon {

class DataFileMetaSerializer;
class IndexFileMetaSerializer;
class MemorySegmentOutputStream;
class DataInputStream;
struct DataFileMeta;
class IndexFileMeta;
template <typename T>
class ObjectSerializer;
class MemoryPool;

class CommitMessageSerializer {
 public:
    static const int32_t CURRENT_VERSION;

    explicit CommitMessageSerializer(const std::shared_ptr<MemoryPool>& pool);
    ~CommitMessageSerializer();
    Status Serialize(const std::shared_ptr<CommitMessage>& obj, MemorySegmentOutputStream* out);
    Status SerializeList(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages,
                         MemorySegmentOutputStream* out);

    Result<std::vector<std::shared_ptr<CommitMessage>>> DeserializeList(int32_t version,
                                                                        DataInputStream* in);
    Result<std::shared_ptr<CommitMessage>> Deserialize(int32_t version, DataInputStream* in);

 private:
    Result<std::shared_ptr<CommitMessage>> Deserialize(
        int32_t version,
        const ObjectSerializer<std::shared_ptr<DataFileMeta>>* data_file_meta_serializer,
        const ObjectSerializer<std::shared_ptr<IndexFileMeta>>* index_entry_serializer,
        DataInputStream* in);

 private:
    std::shared_ptr<MemoryPool> memory_pool_;
    std::unique_ptr<DataFileMetaSerializer> data_file_serializer_;
    std::unique_ptr<IndexFileMetaSerializer> index_entry_serializer_;
};

}  // namespace paimon
