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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/global_index/global_indexer.h"

namespace paimon {

/// Indexer for the Java-compatible bitmap global index format.
class BitmapGlobalIndexer : public GlobalIndexer {
 public:
    static Result<std::unique_ptr<BitmapGlobalIndexer>> Create(
        const std::map<std::string, std::string>& options);

    Result<std::optional<std::vector<std::string>>> GetExtraFieldNames() const override;

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    BitmapGlobalIndexer(int64_t fallback_scan_max_size,
                        const std::map<std::string, std::string>& options)
        : fallback_scan_max_size_(fallback_scan_max_size), options_(options) {}

    int64_t fallback_scan_max_size_;
    std::map<std::string, std::string> options_;
};

}  // namespace paimon
