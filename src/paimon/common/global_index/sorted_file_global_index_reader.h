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
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/common/global_index/sorted_file_meta_selector.h"
#include "paimon/executor.h"
#include "paimon/global_index/global_index_reader.h"

namespace paimon {

/// Base reader for sorted global index files with manifest-level min/max pruning.
class SortedFileGlobalIndexReader : public GlobalIndexReader {
 public:
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override;

    bool IsThreadSafe() const override {
        return false;
    }

 protected:
    SortedFileGlobalIndexReader(std::unique_ptr<SortedFileMetaSelector> file_selector,
                                int64_t fallback_scan_max_size, std::shared_ptr<Executor> executor);

    virtual Result<std::shared_ptr<GlobalIndexReader>> OpenReader(
        const GlobalIndexIOMeta& meta) = 0;

 private:
    using SelectAction = std::function<Result<std::vector<GlobalIndexIOMeta>>()>;
    using ReaderAction = std::function<Result<std::shared_ptr<GlobalIndexResult>>(
        const std::shared_ptr<GlobalIndexReader>&)>;

    Result<std::shared_ptr<GlobalIndexResult>> VisitParallel(SelectAction select_files,
                                                             ReaderAction visitor);
    Result<std::shared_ptr<GlobalIndexResult>> VisitFallbackParallel(SelectAction select_files,
                                                                     ReaderAction visitor);
    Result<std::shared_ptr<GlobalIndexResult>> VisitSelectedFiles(
        const std::vector<GlobalIndexIOMeta>& files, ReaderAction visitor);
    Result<std::shared_ptr<GlobalIndexReader>> CreateUnionReader(
        const std::vector<GlobalIndexIOMeta>& files);
    Result<std::shared_ptr<GlobalIndexReader>> GetOrCreateReader(const GlobalIndexIOMeta& meta);

    bool FallbackScanEnabled(const std::vector<GlobalIndexIOMeta>& files) const;

    std::unique_ptr<SortedFileMetaSelector> file_selector_;
    int64_t fallback_scan_max_size_;
    std::map<std::string, std::shared_ptr<GlobalIndexReader>> reader_cache_;
    std::shared_ptr<Executor> executor_;
};

}  // namespace paimon
