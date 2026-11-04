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

#include "paimon/core/io/file_index_evaluator.h"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/predicate/full_text_match_predicate.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/file_index/file_index_format.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/full_text_search.h"
#include "paimon/predicate/function.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate.h"
#include "paimon/predicate/predicate_utils.h"
#include "paimon/predicate/vector_search.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class MemoryPool;
enum class FieldType;

FileIndexEvaluator::FileIndexEvaluator(std::shared_ptr<arrow::Schema> data_schema,
                                       std::shared_ptr<DataFileMeta> file_meta,
                                       std::unique_ptr<FileIndexFormat::Reader> format_reader)
    : data_schema_(std::move(data_schema)),
      file_meta_(std::move(file_meta)),
      format_reader_(std::move(format_reader)) {}

Result<std::unique_ptr<FileIndexEvaluator>> FileIndexEvaluator::Create(
    const std::shared_ptr<arrow::Schema>& data_schema,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<DataFilePathFactory>& path_factory,
    const std::shared_ptr<DataFileMeta>& file_meta, const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& pool) {
    std::unique_ptr<FileIndexFormat::Reader> format_reader;
    if (file_meta->row_count >= 0 && file_meta->row_count <= RoaringBitmap32::MAX_VALUE) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               ExtractIndexInputStream(path_factory, file_meta, file_system));
        if (input_stream) {
            PAIMON_ASSIGN_OR_RAISE(format_reader,
                                   FileIndexFormat::CreateReader(input_stream, pool, options));
        }
    }
    return std::unique_ptr<FileIndexEvaluator>(
        new FileIndexEvaluator(data_schema, file_meta, std::move(format_reader)));
}

Result<std::vector<std::shared_ptr<FileIndexReader>>> FileIndexEvaluator::GetReaders(
    const std::string& field_name) {
    auto iter = field_readers_.find(field_name);
    if (iter != field_readers_.end()) {
        return iter->second;
    }
    std::shared_ptr<arrow::Field> field = data_schema_->GetFieldByName(field_name);
    if (!field) {
        return Status::Invalid(fmt::format("field '{}' does not exist in data schema", field_name));
    }
    std::vector<std::shared_ptr<FileIndexReader>> readers;
    if (format_reader_) {
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportSchema(*arrow::schema({std::move(field)}), &c_schema));
        PAIMON_ASSIGN_OR_RAISE(readers, format_reader_->ReadColumnIndex(field_name, &c_schema));
    }
    field_readers_.emplace(field_name, readers);
    return readers;
}

Result<std::shared_ptr<FileIndexReader>> FileIndexEvaluator::GetSearchReader(
    const std::string& field_name, FileIndexReader::SearchType search_type) {
    if (!data_schema_->GetFieldByName(field_name)) {
        return Status::Invalid(
            fmt::format("Search field '{}' does not exist in data schema", field_name));
    }
    if (!format_reader_) {
        return Status::Invalid(fmt::format("Data file '{}' has no File Index for search field '{}'",
                                           file_meta_->file_name, field_name));
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<FileIndexReader>> readers,
                           GetReaders(field_name));
    std::shared_ptr<FileIndexReader> selected;
    for (const auto& reader : readers) {
        if (!reader->SupportsSearch(search_type)) {
            continue;
        }
        if (selected) {
            return Status::Invalid(fmt::format(
                "Multiple File Index readers support requested search for field '{}'", field_name));
        }
        selected = reader;
    }
    if (!selected) {
        return Status::NotImplemented(fmt::format(
            "No File Index reader supports requested search for field '{}'", field_name));
    }
    return selected;
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::EvaluateLeaf(
    const std::shared_ptr<LeafPredicate>& predicate, bool needs_scores) {
    if (predicate->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileIndexReader> reader,
            GetSearchReader(predicate->FieldName(), FileIndexReader::SearchType::FULL_TEXT_MATCH));
        auto full_text = std::dynamic_pointer_cast<FullTextMatchPredicate>(predicate);
        if (!full_text) {
            return Status::Invalid("FullTextMatch requires a full-text match predicate");
        }
        return reader->VisitFullTextMatch(full_text->GetFullTextMatch(), needs_scores);
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<FileIndexReader>> readers,
                           GetReaders(predicate->FieldName()));
    std::shared_ptr<FileIndexResult> result = FileIndexResult::Remain();
    for (const auto& reader : readers) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileIndexResult> child,
            PredicateUtils::VisitPredicate<std::shared_ptr<FileIndexResult>>(predicate, reader));
        PAIMON_ASSIGN_OR_RAISE(result, result->And(child));
        PAIMON_ASSIGN_OR_RAISE(bool remain, result->IsRemain());
        if (!remain) {
            break;
        }
    }
    return result;
}

Result<std::optional<double>> FileIndexEvaluator::EstimateSelectivity(
    const std::shared_ptr<LeafPredicate>& predicate) {
    if (predicate->GetFunction().GetType() == Function::Type::FULL_TEXT_MATCH) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<FileIndexReader> reader,
            GetSearchReader(predicate->FieldName(), FileIndexReader::SearchType::FULL_TEXT_MATCH));
        return reader->EstimateSelectivity(predicate);
    }
    PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<FileIndexReader>> readers,
                           GetReaders(predicate->FieldName()));
    std::optional<double> estimate;
    for (const auto& reader : readers) {
        auto value = reader->EstimateSelectivity(predicate);
        if (value && std::isfinite(*value) && *value >= 0.0 && *value <= 1.0 &&
            (!estimate || *value < *estimate)) {
            estimate = value;
        }
    }
    return estimate;
}

Result<std::shared_ptr<ScoredFileIndexResult>> FileIndexEvaluator::SearchVector(
    const std::shared_ptr<VectorSearch>& search) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileIndexReader> reader,
        GetSearchReader(search->field_name, FileIndexReader::SearchType::VECTOR_SEARCH));
    return reader->VisitVectorSearch(search);
}

Result<std::shared_ptr<ScoredFileIndexResult>> FileIndexEvaluator::SearchFullText(
    const std::shared_ptr<FullTextSearch>& search) {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileIndexReader> reader,
        GetSearchReader(search->field_name, FileIndexReader::SearchType::FULL_TEXT_SEARCH));
    return reader->VisitFullTextSearch(search);
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::Evaluate(
    const std::shared_ptr<Predicate>& predicate) {
    if (!predicate || file_meta_->row_count < 0 ||
        file_meta_->row_count > RoaringBitmap32::MAX_VALUE || !format_reader_) {
        return FileIndexResult::Remain();
    }
    // Load every referenced field before short-circuiting, so missing fields and invalid indexes
    // are reported even when an earlier branch already excludes the file.
    std::set<std::string> required_field_names;
    PAIMON_RETURN_NOT_OK(PredicateUtils::GetAllNames(predicate, &required_field_names));
    for (const auto& field_name : required_field_names) {
        PAIMON_RETURN_NOT_OK(GetReaders(field_name));
    }
    return EvaluatePredicate(predicate);
}

Result<std::shared_ptr<InputStream>> FileIndexEvaluator::ExtractIndexInputStream(
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    const std::shared_ptr<DataFileMeta>& file_meta,
    const std::shared_ptr<FileSystem>& file_system) {
    const auto& embedded_index = file_meta->embedded_index;
    if (embedded_index != nullptr) {
        // inline index
        return std::make_shared<ByteArrayInputStream>(embedded_index->data(),
                                                      embedded_index->size());
    }
    if (!data_file_path_factory || !file_system) {
        return Status::Invalid(
            "read process for FileIndexEvaluator must have data_file_path_factory and file_system");
    }
    // get input stream from file
    std::vector<std::string> index_files;
    for (const auto& extra_file : file_meta->extra_files) {
        if (extra_file) {
            if (StringUtils::EndsWith(extra_file.value(), DataFilePathFactory::INDEX_PATH_SUFFIX)) {
                index_files.emplace_back(extra_file.value());
            }
        }
    }
    if (!index_files.empty()) {
        if (index_files.size() > 1) {
            return Status::Invalid(
                fmt::format("Found more than one index file for one data file: {}",
                            fmt::join(index_files, ", ")));
        }
        std::string file_index_path =
            data_file_path_factory->ToAlignedPath(index_files[0], file_meta);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input_stream,
                               file_system->Open(file_index_path));
        return input_stream;
    }
    // no index
    return std::shared_ptr<InputStream>();
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::EvaluatePredicate(
    const std::shared_ptr<Predicate>& predicate) {
    if (auto compound = std::dynamic_pointer_cast<CompoundPredicate>(predicate)) {
        return EvaluateCompoundPredicate(compound);
    }
    if (auto leaf = std::dynamic_pointer_cast<LeafPredicate>(predicate)) {
        return EvaluateLeaf(leaf, /*needs_scores=*/false);
    }
    return Status::Invalid(fmt::format(
        "cannot cast predicate {} to CompoundPredicate or LeafPredicate", predicate->ToString()));
}

Result<std::shared_ptr<FileIndexResult>> FileIndexEvaluator::EvaluateCompoundPredicate(
    const std::shared_ptr<CompoundPredicate>& compound_predicate) {
    const Function::Type type = compound_predicate->GetFunction().GetType();
    if (type != Function::Type::AND && type != Function::Type::OR) {
        return Status::Invalid("CompoundPredicate only support And/Or function");
    }
    std::shared_ptr<FileIndexResult> result;
    for (const auto& child : compound_predicate->Children()) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexResult> child_result,
                               EvaluatePredicate(child));
        if (!result) {
            result = std::move(child_result);
        } else {
            PAIMON_ASSIGN_OR_RAISE(result, type == Function::Type::AND ? result->And(child_result)
                                                                       : result->Or(child_result));
        }
        if (type == Function::Type::AND) {
            PAIMON_ASSIGN_OR_RAISE(bool is_remain, result->IsRemain());
            if (!is_remain) {
                return result;
            }
        }
    }
    assert(result);
    return result;
}

}  // namespace paimon
