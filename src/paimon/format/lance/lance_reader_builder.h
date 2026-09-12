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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <map>
#include <memory>
#include <string>

#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/format/lance/lance_file_batch_reader.h"
#include "paimon/format/lance/lance_utils.h"
#include "paimon/format/reader_builder.h"

namespace paimon::lance {

class LanceReaderBuilder : public ReaderBuilder {
 public:
    LanceReaderBuilder(const std::map<std::string, std::string>& options, int32_t batch_size)
        : options_(options),
          batch_size_(batch_size),
          pool_(GetDefaultPool()),
          arrow_pool_(GetArrowPool(pool_)) {}

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        arrow_pool_ = pool == nullptr ? nullptr : GetArrowPool(pool);
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& input) const override {
        if (pool_ == nullptr) {
            return Status::Invalid("Lance reader memory pool is nullptr");
        }
        PAIMON_ASSIGN_OR_RAISE(uint32_t batch_readahead,
                               OptionsUtils::GetValueFromMap<uint32_t>(
                                   options_, kLanceBatchReadahead, kDefaultLanceBatchReadahead));
        return LanceFileBatchReader::Create(input, batch_size_, batch_readahead, options_,
                                            arrow_pool_);
    }

 private:
    std::map<std::string, std::string> options_;
    int32_t batch_size_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

}  // namespace paimon::lance
