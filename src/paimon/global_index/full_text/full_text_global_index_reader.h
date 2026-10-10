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

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "paimon/global_index/full_text/full_text_defs.h"
#include "paimon/global_index/full_text/full_text_ffi_utils.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/full_text_search.h"

namespace paimon::full_text {

/// Context of the native positional-read callback.
struct FullTextInputContext;

/// Reads one full-text index archive through the native `paimon-full-text-index` engine,
/// aligned with Java `NativeFullTextGlobalIndexReader`. The archive is opened lazily on the first
/// search and carries its own analyzer configuration.
///
/// `FullTextSearch::query` is passed to the engine unchanged, so it must be a JSON DSL query such
/// as `{"match":{"query":"paimon lake"}}`, and a positive `limit` is required. Other predicates are
/// not supported and return nullptr.
class FullTextGlobalIndexReader : public GlobalIndexReader {
 public:
    static Result<std::shared_ptr<FullTextGlobalIndexReader>> Create(
        const GlobalIndexIOMeta& io_meta, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::shared_ptr<MemoryPool>& pool);

    ~FullTextGlobalIndexReader() override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(const std::vector<Literal>&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(const std::vector<Literal>&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal&) override {
        return std::shared_ptr<GlobalIndexResult>();
    }

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>&) override {
        return Status::Invalid(
            "FullTextGlobalIndexReader is not supposed to handle vector search query");
    }

    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    /// The native reader supports concurrent searches, and opening it is guarded by a mutex.
    bool IsThreadSafe() const override {
        return true;
    }

    std::string GetIndexType() const override {
        return kIdentifier;
    }

 private:
    FullTextGlobalIndexReader(const GlobalIndexIOMeta& io_meta,
                              const std::shared_ptr<GlobalIndexFileReader>& file_reader,
                              const std::shared_ptr<MemoryPool>& pool);

    /// Returns the native reader, opening the archive on the first call.
    Result<PaimonFtindexReaderHandle*> GetOrOpenReader();

    void CloseInputStream(InputStream* stream) const;

    GlobalIndexIOMeta io_meta_;
    std::shared_ptr<GlobalIndexFileReader> file_reader_;
    std::shared_ptr<MemoryPool> pool_;
    std::mutex open_mutex_;
    /// Must outlive `reader_`, which reads through it.
    std::unique_ptr<FullTextInputContext> input_;
    FtindexReaderPtr reader_;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon::full_text
