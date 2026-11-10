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
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "paimon/common/utils/path_util.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/io/data_file_path_factory.h"

namespace paimon {
/// Path factory to create an index path.
class IndexInDataFileDirPathFactory : public IndexPathFactory {
 public:
    IndexInDataFileDirPathFactory(
        const std::string& uuid, const std::shared_ptr<std::atomic<int32_t>>& index_file_count,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory)
        : uuid_(uuid),
          index_file_count_(index_file_count),
          data_file_path_factory_(data_file_path_factory) {}

    std::string NewPath() const override {
        std::string name = IndexPathFactory::INDEX_PREFIX + uuid_ + "-" +
                           std::to_string(index_file_count_->fetch_add(1));
        return NewPathFromName(name);
    }

    std::string NewPathFromName(const std::string& file_name) const override {
        return data_file_path_factory_->NewPathFromName(file_name);
    }

    std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
        if (file->ExternalPath() != std::nullopt) {
            return file->ExternalPath().value();
        }
        return PathUtil::JoinPath(data_file_path_factory_->Parent(), file->FileName());
    }

    std::string ToPath(const std::string& file_name) const override {
        return data_file_path_factory_->ToPath(file_name);
    }

    bool IsExternalPath() const override {
        return data_file_path_factory_->IsExternalPath();
    }

 private:
    std::string uuid_;
    std::shared_ptr<std::atomic<int32_t>> index_file_count_;
    std::shared_ptr<DataFilePathFactory> data_file_path_factory_;
};

}  // namespace paimon
