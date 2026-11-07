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

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "paimon/core/catalog/table_query_auth_result.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/table/source/table_read.h"

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace paimon {
class Metrics;
class Predicate;
class QueryAuthSplit;

/// Applies the column masks to each batch, each computed from the raw batch, and drops the columns
/// only the rules needed, mirroring Java `TableQueryAuthResult.doAuth`. The masked columns, and
/// the columns a predicate evaluated next reads, are output decoded.
class QueryAuthBatchReader : public BatchReader {
 public:
    /// @param masks The masks to apply, keyed by the column they replace; a mask whose column is
    ///        not in a batch is not applied.
    /// @param dropped_fields The columns removed from each batch after masking.
    /// @param decoded_fields The unmasked columns output no longer dictionary-encoded or large
    ///        strings; the other unmasked columns are output as they are read.
    QueryAuthBatchReader(std::unique_ptr<BatchReader> reader,
                         std::map<std::string, std::shared_ptr<MaskTransform>> masks,
                         std::set<std::string> dropped_fields, std::set<std::string> decoded_fields,
                         std::shared_ptr<arrow::MemoryPool> arrow_pool);

    Result<BatchReader::ReadBatch> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

 private:
    /// Returns `batch` with `decode_inputs_` no longer dictionary-encoded or large strings; the
    /// other columns are left as they are.
    Result<std::shared_ptr<arrow::StructArray>> DecodeInputs(
        const std::shared_ptr<arrow::StructArray>& batch) const;

    std::unique_ptr<BatchReader> reader_;
    std::map<std::string, std::shared_ptr<MaskTransform>> masks_;
    std::set<std::string> dropped_fields_;
    std::set<std::string> decoded_fields_;
    /// `decoded_fields_` and the columns `masks_` read.
    std::set<std::string> decode_inputs_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
};

/// Reads a table with `query-auth.enabled=true`, mirroring Java `AbstractDataTableRead` and
/// `ReadTransform`: the rows of each `QueryAuthSplit` go through the row filter, the column masks,
/// then the read's own predicate, which is not pushed into the files.
class QueryAuthTableRead : public TableRead {
 public:
    /// Builds the read the rules are applied around from the context it is given.
    using InnerReadFactory =
        std::function<Result<std::unique_ptr<TableRead>>(std::unique_ptr<ReadContext>)>;

    /// @param context The read as the caller asked for it.
    /// @param table_schema The schema the read reads with.
    /// @param create_inner Builds the read the rules are applied around.
    static Result<std::unique_ptr<TableRead>> Create(
        const std::shared_ptr<ReadContext>& context,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::shared_ptr<arrow::MemoryPool>& arrow_pool, InnerReadFactory create_inner);

    Result<std::unique_ptr<BatchReader>> CreateReader(const std::shared_ptr<Split>& split) override;
    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override;

 private:
    QueryAuthTableRead(const std::shared_ptr<ReadContext>& context,
                       const std::shared_ptr<TableSchema>& table_schema,
                       const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                       InnerReadFactory create_inner,
                       std::optional<std::vector<std::string>> output_fields);

    /// The predicate evaluated on the masked rows of a split read under `rules`: the whole
    /// predicate with `EnablePredicateFilter(true)`, else its conjuncts on masked columns.
    Result<std::shared_ptr<Predicate>> PostMaskFilter(const TableQueryAuthResult& rules) const;

    /// Checks that `split` was planned against the columns this read sees, and for the columns it
    /// outputs or filters on, `filter_fields` being the operands of `PostMaskFilter()`.
    Status ValidateSplit(const QueryAuthSplit& split,
                         const std::set<std::string>& filter_fields) const;

    /// The read of the requested columns plus `extra_fields`, built on first use.
    Result<TableRead*> GetOrCreateInnerRead(const std::vector<std::string>& extra_fields);

    std::shared_ptr<ReadContext> context_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    InnerReadFactory create_inner_;
    /// The table columns the caller reads, or null when it reads them all.
    std::optional<std::vector<std::string>> output_fields_;
    std::map<std::vector<std::string>, std::unique_ptr<TableRead>> inner_reads_;
};

}  // namespace paimon
