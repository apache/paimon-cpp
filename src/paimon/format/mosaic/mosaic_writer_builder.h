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

#include <map>
#include <memory>
#include <string>

#include "paimon/format/mosaic/mosaic_format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/memory/memory_pool.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::mosaic {

class MosaicWriterBuilder : public WriterBuilder {
 public:
    MosaicWriterBuilder(const std::shared_ptr<arrow::Schema>& schema,
                        const std::map<std::string, std::string>& options)
        : schema_(schema), options_(options), pool_(GetDefaultPool()) {}

    WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        pool_ = pool;
        return this;
    }

    Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& output,
                                                const std::string& compression) override;

 private:
    std::shared_ptr<arrow::Schema> schema_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::mosaic
