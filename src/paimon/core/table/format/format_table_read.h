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

#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_read.h"

namespace paimon {

class Predicate;

/// Reads the splits a `FormatTableScan` produced.
///
/// The batches carry the leading `_VALUE_KIND` field every `BatchReader` promises, filled with
/// inserts: a directory of plain data files records no row kind and every row in it is an insert.
/// Partition columns are rebuilt from the split's partition values, since the data files do not
/// carry them.
///
/// A batch borrows memory from the reader that produced it, so every batch must be released before
/// that reader is destroyed.
///
/// Building a reader leaves the read as it was, so one may be shared between threads; the
/// `BatchReader`s it hands out may not be, as `TableRead` says.
///
/// `CreateCountReader()` is not implemented and falls through to `TableRead`'s default, which
/// refuses: counting a format table's rows means reading them.
class FormatTableRead : public TableRead {
 public:
    /// @param table Table the splits belong to.
    /// @param projection Names of the columns to read, in the order they should appear. When
    ///        absent, every column of the table is read. A column named twice is rejected.
    /// @param pool Memory pool the batches are allocated from.
    /// @param predicate Rows the caller is interested in, as a filter over the columns being read.
    ///        It is pushed into the file readers so the format skips what its own statistics let
    ///        it skip; on its own that is a best effort and rows the predicate rejects can still
    ///        come back. As on the managed table path, every field it names must be one the
    ///        projection keeps, of the type the conjunct declares, with literals of that same type
    ///        and none of them null. Field indexes are ignored: a field is resolved by name.
    /// @param enable_predicate_filter Whether the returned reader applies `predicate` exactly to
    ///        the rows it returns. Off by default, as everywhere else in paimon-cpp.
    static Result<std::unique_ptr<FormatTableRead>> Create(
        const std::shared_ptr<FormatTable>& table,
        const std::optional<std::vector<std::string>>& projection,
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
        bool enable_predicate_filter);

    /// Reads what a `ReadContext` asks for, which is how `TableRead::Create()` reaches a format
    /// table. The columns to read, the predicate and whether to apply it exactly, the memory pool
    /// and executor, and what a file is opened with (prefetch, the read-ahead cache and the block
    /// cache) all come from the context. The last three go through the same component the managed
    /// table path opens its files with, so a format table's file is read the way any other data
    /// file is.
    ///
    /// A setting a format table cannot honour is refused by name rather than dropped: a projected
    /// read schema, and a real-time context.
    static Result<std::unique_ptr<FormatTableRead>> Create(
        const std::shared_ptr<FormatTable>& table,
        const std::shared_ptr<ReadContext>& read_context);

    ~FormatTableRead() override;

    /// Creates a reader over one split's files, read in the split's order.
    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;

    /// Creates a reader over several splits, read in the given order.
    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override;

    class Impl;

 private:
    explicit FormatTableRead(std::unique_ptr<Impl> impl, const std::shared_ptr<MemoryPool>& pool);

    /// Shared body of both `Create()` overloads. A null `read_context` reads with neither prefetch
    /// nor a cache.
    static Result<std::unique_ptr<FormatTableRead>> CreateInternal(
        const std::shared_ptr<FormatTable>& table,
        const std::optional<std::vector<std::string>>& projection,
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<Predicate>& predicate,
        bool enable_predicate_filter, const std::shared_ptr<ReadContext>& read_context);

    /// Builds the reader over one split's files, without the predicate filter or the row kinds.
    Result<std::unique_ptr<BatchReader>> CreateSplitReader(const std::shared_ptr<Split>& split);

    /// Wraps a reader in the exact predicate filter when one was asked for, and then in the
    /// `_VALUE_KIND` field every `BatchReader` promises. The filter runs underneath, on the
    /// table's own columns, as it does on the managed table path.
    Result<std::unique_ptr<BatchReader>> ApplyFilterAndRowKind(
        std::unique_ptr<BatchReader>&& reader);

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
