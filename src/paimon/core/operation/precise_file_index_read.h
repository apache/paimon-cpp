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
#include <utility>
#include <vector>

#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/operation/precise_index_evaluator.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class BinaryRow;
class CompoundPredicate;
class DataFilePathFactory;
class FieldMappingBuilder;
class FileIndexEvaluator;
class LeafPredicate;
class Predicate;
class RawFileSplitRead;
class ScoredFileIndexResult;
struct DataFileMeta;
struct FullTextSearch;
struct VectorSearch;

/// Evaluates a file predicate and root search before opening the selected output rows.
class PreciseFileIndexRead {
 public:
    /// Uses the file-specific predicate and schema supplied by the caller, including any fields
    /// added for visibility filtering. The predicate is evaluated before root search.
    static Result<std::unique_ptr<PreciseFileIndexRead>> Create(
        const RawFileSplitRead& split_read, const BinaryRow& partition,
        const std::shared_ptr<DataFileMeta>& file,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<Predicate>& predicate,
        const FieldMappingBuilder* field_mapping_builder,
        const std::shared_ptr<DataFilePathFactory>& path_factory,
        const std::map<std::string, std::string>& extra_format_options);

    /// Finishes evaluation synchronously. The returned reader does not retain this instance.
    Result<std::unique_ptr<FileBatchReader>> CreateReader(
        const DeletionVector::Factory& dv_factory,
        const std::optional<std::vector<Range>>& row_ranges) const;

 private:
    using Evaluator = PreciseIndexEvaluator<RoaringBitmap32, ScoredFileIndexResult>;
    using Evaluation = Evaluator::Evaluation;

    PreciseFileIndexRead(const RawFileSplitRead& split_read, const BinaryRow& partition,
                         const std::shared_ptr<DataFileMeta>& file,
                         const std::shared_ptr<arrow::Schema>& read_schema,
                         const std::shared_ptr<Predicate>& predicate,
                         const FieldMappingBuilder* field_mapping_builder,
                         const std::shared_ptr<DataFilePathFactory>& path_factory,
                         const std::map<std::string, std::string>& extra_format_options,
                         std::unique_ptr<FileIndexEvaluator> indexes,
                         const std::map<std::shared_ptr<LeafPredicate>,
                                        std::shared_ptr<LeafPredicate>>& mapped_leaves,
                         const std::shared_ptr<VectorSearch>& vector_search,
                         const std::shared_ptr<FullTextSearch>& full_text_search);

    Result<RoaringBitmap32> CreateCandidates(
        const DeletionVector::Factory& dv_factory,
        const std::optional<std::vector<Range>>& row_ranges) const;
    Result<Evaluation> EvaluateLeaf(const std::shared_ptr<LeafPredicate>& predicate,
                                    const RoaringBitmap32& candidates) const;
    Result<std::optional<double>> EstimateLeafSelectivity(
        const std::shared_ptr<LeafPredicate>& predicate) const;
    /// Opens a fresh mapped reader for selected physical rows without re-evaluating indexes,
    /// predicates or deletion vectors.
    Result<std::unique_ptr<FileBatchReader>> CreateSelectedFileReader(
        const RoaringBitmap32& selection, const FieldMappingBuilder* field_mapping_builder) const;
    Result<RoaringBitmap32> ScanScalar(const std::shared_ptr<LeafPredicate>& predicate,
                                       const RoaringBitmap32& candidates) const;
    Status ApplyRootSearch(Evaluation* evaluated) const;

 private:
    const RawFileSplitRead& split_read_;
    const BinaryRow& partition_;
    std::shared_ptr<DataFileMeta> file_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<Predicate> predicate_;
    const FieldMappingBuilder* field_mapping_builder_;
    std::shared_ptr<DataFilePathFactory> path_factory_;
    std::map<std::string, std::string> extra_format_options_;
    std::unique_ptr<FileIndexEvaluator> indexes_;
    std::map<std::shared_ptr<LeafPredicate>, std::shared_ptr<LeafPredicate>> mapped_leaves_;
    std::shared_ptr<VectorSearch> vector_search_;
    std::shared_ptr<FullTextSearch> full_text_search_;
};

}  // namespace paimon
