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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/reader/data_file_reader_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_read.h"

namespace paimon {

class Executor;
class Predicate;

/// Reads the splits a `FormatTableScan` produced.
///
/// Partition columns are filled from the split rather than read: the file is asked only for the
/// columns that are not partition keys, so the directory decides a file's partition values even
/// when the file stores them too. Batches carry the leading `_VALUE_KIND` field every
/// `BatchReader` promises, and borrow memory from the reader that produced them.
///
/// A read may be shared between threads; the `BatchReader`s it hands out may not be.
class FormatTableRead : public TableRead {
 public:
    /// Reads what a `ReadContext` asks for, which is how `TableRead::Create()` reaches a format
    /// table. Everything - columns, predicate, pool, executor and how a file is opened - comes
    /// from the context. A setting a format table cannot honour is refused by name rather than
    /// dropped: a projected read schema, and a real-time context.
    static Result<std::unique_ptr<FormatTableRead>> Create(
        const std::shared_ptr<FormatTable>& table,
        const std::shared_ptr<ReadContext>& read_context);

    /// Names the columns, the predicate and the pool directly, without a `ReadContext`. Files are
    /// opened plainly: no prefetch, no cache. For tests; production goes through `Create()`.
    ///
    /// @param projection Columns to read, in the order they should appear; absent reads them all.
    /// @param predicate Pushed into the file readers, so on its own it is best effort. Validated
    ///        as on the managed table path: every field it names must be one the projection keeps.
    /// @param enable_predicate_filter Whether the returned reader also applies `predicate`
    ///        exactly. Off by default, as everywhere else in paimon-cpp.
    static Result<std::unique_ptr<FormatTableRead>> TEST_Create(
        const std::shared_ptr<FormatTable>& table,
        const std::optional<std::vector<std::string>>& projection,
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
        bool enable_predicate_filter);

    ~FormatTableRead() override;

    /// Reads one split's files, in the split's order.
    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;

    /// Reads several splits, in the given order.
    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override;

    class Impl;

 private:
    explicit FormatTableRead(std::unique_ptr<Impl> impl);

    /// Shared body of `Create()` and `TEST_Create()`. Every setting arrives resolved, so each has
    /// exactly one source and no `ReadContext` is read here as well.
    ///
    /// @param read_options What a data file is opened with, as far as the caller decides it; a
    ///        default-constructed value opens files plainly. The fields describing the table
    ///        rather than the read are filled in from its options afterwards.
    /// @param executor Runs a prefetching reader's read-ahead. Null when nothing asked for it.
    static Result<std::unique_ptr<FormatTableRead>> CreateInternal(
        const std::shared_ptr<FormatTable>& table,
        const std::optional<std::vector<std::string>>& projection,
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
        bool enable_predicate_filter, const DataFileReadOptions& read_options,
        const std::shared_ptr<Executor>& executor);

    /// Builds the reader over one split's files, without the predicate filter or the row kinds.
    Result<std::unique_ptr<BatchReader>> CreateSplitReader(const std::shared_ptr<Split>& split);

    /// Wraps a reader in the exact predicate filter when one was asked for, and then in the
    /// `_VALUE_KIND` field. The filter runs underneath it, on the table's own columns.
    Result<std::unique_ptr<BatchReader>> ApplyFilterAndRowKind(
        std::unique_ptr<BatchReader>&& reader);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
