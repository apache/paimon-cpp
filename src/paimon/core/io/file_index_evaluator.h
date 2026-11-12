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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class Cache;
class LeafPredicate;
class CompoundPredicate;
class DataFilePathFactory;
class FileIndexReader;
class MemoryPool;
class Predicate;
class ScoredFileIndexResult;
struct DataFileMeta;
struct FullTextSearch;
struct VectorSearch;

/// Evaluates indexes for one data file, sharing its container and field readers across operations.
/// Predicates, search requests and backend options use file field names.
class FileIndexEvaluator {
 public:
    /// Open one index container and reuse its field readers throughout a single file read.
    /// Uses the embedded index when present; otherwise, loads the external index if available.
    /// Files whose row counts cannot be represented by bitmap32 skip index loading.
    static Result<std::unique_ptr<FileIndexEvaluator>> Create(
        const std::shared_ptr<arrow::Schema>& data_schema,
        const std::map<std::string, std::string>& options,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<Cache>& cache,
        const std::shared_ptr<MemoryPool>& pool);

    /// Evaluates a predicate tree for index pruning, reusing the cached field readers.
    /// This does not perform exact scalar filtering or reorder children by selectivity.
    Result<std::shared_ptr<FileIndexResult>> Evaluate(const std::shared_ptr<Predicate>& predicate);

    Result<std::shared_ptr<FileIndexReader>> GetSearchReader(
        const std::string& field_name, FileIndexReader::SearchType search_type);

    Result<std::shared_ptr<FileIndexResult>> EvaluateLeaf(
        const std::shared_ptr<LeafPredicate>& predicate, bool needs_scores);

    Result<std::optional<double>> EstimateSelectivity(
        const std::shared_ptr<LeafPredicate>& predicate);

    Result<std::shared_ptr<ScoredFileIndexResult>> SearchVector(
        const std::shared_ptr<VectorSearch>& search);

    Result<std::shared_ptr<ScoredFileIndexResult>> SearchFullText(
        const std::shared_ptr<FullTextSearch>& search);

 private:
    FileIndexEvaluator(std::shared_ptr<arrow::Schema> data_schema,
                       std::shared_ptr<DataFileMeta> file_meta,
                       std::unique_ptr<FileIndexFormat::Reader> format_reader,
                       std::shared_ptr<Cache> cache, std::string uri);

    Result<std::vector<std::shared_ptr<FileIndexReader>>> GetReaders(const std::string& field_name);

    static Result<std::shared_ptr<InputStream>> ExtractIndexInputStream(
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<FileSystem>& file_system);

    Result<std::shared_ptr<FileIndexResult>> EvaluatePredicate(
        const std::shared_ptr<Predicate>& predicate);

    Result<std::shared_ptr<FileIndexResult>> EvaluateCompoundPredicate(
        const std::shared_ptr<CompoundPredicate>& compound_predicate);

 private:
    std::shared_ptr<arrow::Schema> data_schema_;
    std::shared_ptr<DataFileMeta> file_meta_;
    std::unique_ptr<FileIndexFormat::Reader> format_reader_;
    std::shared_ptr<Cache> cache_;
    std::string uri_;
    std::map<std::string, std::vector<std::shared_ptr<FileIndexReader>>> field_readers_;
};

}  // namespace paimon
