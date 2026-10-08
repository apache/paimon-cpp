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
#include <set>
#include <string>
#include <vector>

#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/predicate.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class LeafPredicate;
class CompoundPredicate;
class CoreOptions;
class DataFilePathFactory;
class FileIndexReader;
class MemoryPool;
class Predicate;
class ScoredFileIndexResult;
struct DataFileMeta;
struct FullTextSearch;
struct VectorSearch;

class FileIndexEvaluator {
 public:
    FileIndexEvaluator() = delete;
    ~FileIndexEvaluator() = delete;

    // for scan process, will only use embedding file to evaluate
    static Result<std::shared_ptr<FileIndexResult>> Evaluate(
        const std::shared_ptr<arrow::Schema>& data_schema, const CoreOptions& core_options,
        const std::shared_ptr<Predicate>& predicate, const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<MemoryPool>& pool);

    // for read process, will use embedding file or extra index file to evaluate
    static Result<std::shared_ptr<FileIndexResult>> Evaluate(
        const std::shared_ptr<arrow::Schema>& data_schema, const CoreOptions& core_options,
        const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);

    static Result<std::shared_ptr<ScoredFileIndexResult>> EvaluateVectorSearch(
        const std::shared_ptr<arrow::Schema>& data_schema, const CoreOptions& core_options,
        const std::shared_ptr<VectorSearch>& vector_search,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);

    static Result<std::shared_ptr<FileIndexResult>> EvaluateFullTextSearch(
        const std::shared_ptr<arrow::Schema>& data_schema, const CoreOptions& core_options,
        const std::shared_ptr<FullTextSearch>& full_text_search,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);

 private:
    static Result<std::shared_ptr<FileIndexResult>> Evaluate(
        bool only_use_embedding_index, const std::shared_ptr<arrow::Schema>& data_schema,
        const CoreOptions& core_options, const std::shared_ptr<Predicate>& predicate,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<FileSystem>& file_system, const std::shared_ptr<MemoryPool>& pool);

    static Result<std::shared_ptr<InputStream>> ExtractIndexInputStream(
        bool only_use_embedding_index,
        const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
        const std::shared_ptr<DataFileMeta>& file_meta,
        const std::shared_ptr<FileSystem>& file_system);

    static Result<std::shared_ptr<FileIndexResult>> Evaluate(
        const std::shared_ptr<Predicate>& predicate,
        const std::map<std::string, std::vector<std::shared_ptr<FileIndexReader>>>&
            field_name_to_index_readers);

    static Result<std::shared_ptr<FileIndexResult>> EvaluateCompoundPredicate(
        const std::shared_ptr<CompoundPredicate>& compound_predicate,
        const std::map<std::string, std::vector<std::shared_ptr<FileIndexReader>>>&
            field_name_to_index_readers);
};

}  // namespace paimon
