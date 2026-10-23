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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/read_context.h"
#include "paimon/result.h"

namespace arrow {
class MemoryPool;
class Schema;
}  // namespace arrow

namespace paimon {
struct CatalogContext;
class Executor;
class FileSystem;
class MemoryPool;
class Predicate;
class RealtimeContext;

// internal read context, contains ReadContext, CoreOptions and TableSchema
class InternalReadContext {
 public:
    struct IndexScoreSource {
        enum class Type { VECTOR_SEARCH, FULL_TEXT_SEARCH, FULL_TEXT_MATCH };

        bool operator==(const IndexScoreSource& other) const {
            return type == other.type && field_name == other.field_name;
        }

        Type type;
        /// Original table field name, before per-file field mapping.
        std::string field_name;
    };

    /// Builds the context of a read a table serves: the options are the table's own, already
    /// parsed - the store's `options()` - and the cache the context carries is armed on them
    /// here. `path` names the root path the table was opened with, which the read reads from in
    /// place of the one named by the context.
    static Result<std::unique_ptr<InternalReadContext>> Create(
        const std::shared_ptr<ReadContext>& read_context,
        const std::shared_ptr<TableSchema>& table_schema, CoreOptions core_options,
        const std::string* path = nullptr);

    /// Builds the context of a read whose options are composed on the spot rather than the
    /// table's own - the fallback branch of a read, whose schema belongs to another branch, and
    /// the reads a write or a lookup serve inside themselves. The options are parsed from
    /// `options` here: the read's own file system is `file_system`, or the context's when null -
    /// the scheme map the context carries does not take part. A cache the context carries is
    /// armed on the options this parses.
    static Result<std::unique_ptr<InternalReadContext>> Create(
        const std::shared_ptr<ReadContext>& read_context,
        const std::shared_ptr<TableSchema>& table_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<FileSystem>& file_system = nullptr,
        const std::string* path = nullptr);

    /// Maps score output positions in the read schema to their logical search sources.
    const std::map<int32_t, IndexScoreSource>& GetIndexScoreBindings() const {
        return index_score_bindings_;
    }

    /// Root TopK needs scores even without a score projection.
    bool NeedsScores(const IndexScoreSource& source) const;

    const CoreOptions& GetCoreOptions() const {
        return options_;
    }
    std::shared_ptr<arrow::Schema> GetReadSchema() const {
        return read_schema_;
    }
    const std::shared_ptr<TableSchema>& GetTableSchema() const {
        return table_schema_;
    }
    const std::string& GetPath() const {
        return path_ ? *path_ : read_context_->GetPath();
    }

    std::shared_ptr<CatalogContext> CreateDependencyCatalogContext() const;

    const std::vector<std::string>& GetPartitionKeys() const {
        return table_schema_->PartitionKeys();
    }
    const std::vector<std::string>& GetPrimaryKeys() const {
        return table_schema_->PrimaryKeys();
    }
    const std::shared_ptr<Predicate>& GetPredicate() const {
        return read_context_->GetPredicate();
    }
    const std::shared_ptr<VectorSearch>& GetVectorSearch() const {
        return read_context_->GetVectorSearch();
    }
    const std::shared_ptr<FullTextSearch>& GetFullTextSearch() const {
        return read_context_->GetFullTextSearch();
    }
    bool HasVectorOrFullTextSearch() const {
        return read_context_->HasVectorOrFullTextSearch();
    }
    bool EnablePredicateFilter() const {
        return read_context_->EnablePredicateFilter();
    }
    bool EnablePrefetch() const {
        return read_context_->EnablePrefetch();
    }
    bool EnableLateMaterializing() const {
        return read_context_->EnableLateMaterializing();
    }
    uint32_t GetPrefetchBatchCount() const {
        return read_context_->GetPrefetchBatchCount();
    }
    uint32_t GetPrefetchMaxParallelNum() const {
        return read_context_->GetPrefetchMaxParallelNum();
    }
    bool EnableMultiThreadRowToBatch() const {
        return read_context_->EnableMultiThreadRowToBatch();
    }
    uint32_t GetRowToBatchThreadNumber() const {
        return read_context_->GetRowToBatchThreadNumber();
    }
    std::shared_ptr<MemoryPool> GetMemoryPool() const {
        return read_context_->GetMemoryPool();
    }
    const std::shared_ptr<arrow::MemoryPool>& GetArrowMemoryPool() const {
        return arrow_pool_;
    }
    std::shared_ptr<Executor> GetExecutor() const {
        return read_context_->GetExecutor();
    }
    std::shared_ptr<RealtimeContext> GetRealtimeContext() const {
        return read_context_->GetRealtimeContext();
    }
    /// The scheme map the file system of this read was resolved with, which a catalog reading a
    /// dependency table on its behalf needs to resolve that table's location the same way.
    const std::map<std::string, std::string>& GetFileSystemSchemeToIdentifierMap() const {
        return read_context_->GetFileSystemSchemeToIdentifierMap();
    }

    bool ReadAheadCacheEnabled() const {
        return read_context_->ReadAheadCacheEnabled();
    }

    const CacheConfig& GetCacheConfig() const {
        return read_context_->GetCacheConfig();
    }

    WarmupLevel GetWarmupLevel() const {
        return read_context_->GetWarmupLevel();
    }

    /// Create the minimal read schema for PK COUNT(*). Search requests and score
    /// projections are unsupported; their bindings cannot be inherited across schemas.
    static Result<std::shared_ptr<InternalReadContext>> CreateWithSchema(
        const std::shared_ptr<InternalReadContext>& original,
        const std::shared_ptr<arrow::Schema>& new_read_schema);

 private:
    InternalReadContext(const std::shared_ptr<ReadContext>& read_context,
                        const std::shared_ptr<TableSchema>& table_schema,
                        const std::shared_ptr<arrow::Schema>& read_schema,
                        const CoreOptions& options,
                        const std::shared_ptr<arrow::MemoryPool>& arrow_pool,
                        std::optional<std::string> path = std::nullopt);

    /// The tail both `Create` entries share: the options arrive parsed and cached, and the
    /// validations and the read schema below are the same whichever way they were resolved.
    static Result<std::unique_ptr<InternalReadContext>> CreateFromCoreOptions(
        const std::shared_ptr<ReadContext>& read_context,
        const std::shared_ptr<TableSchema>& table_schema, CoreOptions core_options,
        const std::string* path);

    static std::optional<DataField> TryResolveSpecialFieldById(int32_t field_id,
                                                               const CoreOptions& core_options,
                                                               bool has_vector_or_full_text_search);
    static std::optional<DataField> TryResolveSpecialFieldByName(
        const std::string& name, const CoreOptions& core_options,
        bool has_vector_or_full_text_search, int32_t output_field_idx);
    static Result<std::shared_ptr<arrow::Field>> AlignReadFieldWithTableFieldIds(
        const std::shared_ptr<arrow::Field>& read_field,
        const std::shared_ptr<arrow::Field>& table_field);

    Status BindIndexScores();

    std::shared_ptr<ReadContext> read_context_;
    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    CoreOptions options_;
    std::map<int32_t, IndexScoreSource> index_score_bindings_;
    /// The root path a table reads through, or null for the read context's own, which callers
    /// without a table hand over.
    std::optional<std::string> path_;
};

}  // namespace paimon
