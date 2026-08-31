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

#pragma once

#include <memory>

#include "paimon/format/mosaic/mosaic_file_batch_reader.h"
#include "paimon/format/reader_builder.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::mosaic {

class MosaicReaderBuilder : public ReaderBuilder {
 public:
    explicit MosaicReaderBuilder(int32_t batch_size)
        : batch_size_(batch_size), pool_(GetDefaultPool()) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& input) const override {
        if (pool_ == nullptr) {
            return Status::Invalid("Mosaic reader memory pool is nullptr");
        }
        return MosaicFileBatchReader::Create(input, batch_size_, pool_);
    }

 private:
    int32_t batch_size_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::mosaic
