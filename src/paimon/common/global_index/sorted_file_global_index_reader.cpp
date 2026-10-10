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

#include "paimon/common/global_index/sorted_file_global_index_reader.h"

#include <future>
#include <limits>
#include <optional>
#include <utility>

#include "paimon/common/executor/future.h"
#include "paimon/common/predicate/like_optimization.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
namespace {

std::shared_ptr<GlobalIndexResult> EmptyResult() {
    return std::make_shared<BitmapGlobalIndexResult>([]() { return RoaringBitmap64(); });
}

}  // namespace

SortedFileGlobalIndexReader::SortedFileGlobalIndexReader(
    std::unique_ptr<SortedFileMetaSelector> file_selector, int64_t fallback_scan_max_size,
    std::shared_ptr<Executor> executor)
    : file_selector_(std::move(file_selector)),
      fallback_scan_max_size_(fallback_scan_max_size),
      executor_(std::move(executor)) {}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitIsNotNull() {
    return VisitParallel(
        [this]() { return file_selector_->VisitIsNotNull(); },
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNotNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitIsNull() {
    return VisitParallel(
        [this]() { return file_selector_->VisitIsNull(); },
        [](const std::shared_ptr<GlobalIndexReader>& reader) { return reader->VisitIsNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitEqual(
    const Literal& literal) {
    return VisitParallel([this, &literal]() { return file_selector_->VisitEqual(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitEqual(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitNotEqual(
    const Literal& literal) {
    return VisitParallel([this, &literal]() { return file_selector_->VisitNotEqual(literal); },
                         [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitNotEqual(literal);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitLessThan(
    const Literal& literal) {
    if (literal.IsNull() || fallback_scan_max_size_ <= 0) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitFallbackParallel(
        [this, &literal]() { return file_selector_->VisitLessThan(literal); },
        [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
            return reader->VisitLessThan(literal);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    if (literal.IsNull() || fallback_scan_max_size_ <= 0) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitFallbackParallel(
        [this, &literal]() { return file_selector_->VisitLessOrEqual(literal); },
        [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
            return reader->VisitLessOrEqual(literal);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitGreaterThan(
    const Literal& literal) {
    if (literal.IsNull() || fallback_scan_max_size_ <= 0) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitFallbackParallel(
        [this, &literal]() { return file_selector_->VisitGreaterThan(literal); },
        [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
            return reader->VisitGreaterThan(literal);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    if (literal.IsNull() || fallback_scan_max_size_ <= 0) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitFallbackParallel(
        [this, &literal]() { return file_selector_->VisitGreaterOrEqual(literal); },
        [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
            return reader->VisitGreaterOrEqual(literal);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return VisitParallel([this, &literals]() { return file_selector_->VisitIn(literals); },
                         [&literals](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitIn(literals);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return VisitParallel([this, &literals]() { return file_selector_->VisitNotIn(literals); },
                         [&literals](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitNotIn(literals);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitStartsWith(
    const Literal& prefix) {
    if (prefix.IsNull() || prefix.GetType() != FieldType::STRING) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitParallel([this, &prefix]() { return file_selector_->VisitStartsWith(prefix); },
                         [&prefix](const std::shared_ptr<GlobalIndexReader>& reader) {
                             return reader->VisitStartsWith(prefix);
                         });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitEndsWith(
    const Literal& suffix) {
    if (suffix.IsNull() || suffix.GetType() != FieldType::STRING || fallback_scan_max_size_ <= 0) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitFallbackParallel(
        [this, &suffix]() { return file_selector_->VisitEndsWith(suffix); },
        [&suffix](const std::shared_ptr<GlobalIndexReader>& reader) {
            return reader->VisitEndsWith(suffix);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitContains(
    const Literal& literal) {
    if (literal.IsNull() || literal.GetType() != FieldType::STRING ||
        fallback_scan_max_size_ <= 0) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitFallbackParallel(
        [this, &literal]() { return file_selector_->VisitContains(literal); },
        [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
            return reader->VisitContains(literal);
        });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitLike(
    const Literal& literal) {
    if (literal.IsNull() || literal.GetType() != FieldType::STRING) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    std::optional<OptimizedLike> optimized = LikeOptimization::TryOptimize(literal);
    if (optimized.has_value()) {
        switch (optimized->function->GetType()) {
            case Function::Type::EQUAL:
                return VisitEqual(optimized->literal);
            case Function::Type::STARTS_WITH:
                return VisitStartsWith(optimized->literal);
            case Function::Type::ENDS_WITH:
                return VisitEndsWith(optimized->literal);
            case Function::Type::CONTAINS:
                return VisitContains(optimized->literal);
            default:
                return std::shared_ptr<GlobalIndexResult>(nullptr);
        }
    }
    if (fallback_scan_max_size_ <= 0) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitFallbackParallel([this, &literal]() { return file_selector_->VisitLike(literal); },
                                 [&literal](const std::shared_ptr<GlobalIndexReader>& reader) {
                                     return reader->VisitLike(literal);
                                 });
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitParallel(
    SelectAction select_files, ReaderAction visitor) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> selected_files, select_files());
    return VisitSelectedFiles(selected_files, std::move(visitor));
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitFallbackParallel(
    SelectAction select_files, ReaderAction visitor) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<GlobalIndexIOMeta> selected_files, select_files());
    if (selected_files.empty()) {
        return EmptyResult();
    }
    if (!FallbackScanEnabled(selected_files)) {
        return std::shared_ptr<GlobalIndexResult>(nullptr);
    }
    return VisitSelectedFiles(selected_files, std::move(visitor));
}

Result<std::shared_ptr<GlobalIndexResult>> SortedFileGlobalIndexReader::VisitSelectedFiles(
    const std::vector<GlobalIndexIOMeta>& files, ReaderAction visitor) {
    if (files.empty()) {
        return EmptyResult();
    }

    std::vector<std::shared_ptr<GlobalIndexReader>> readers;
    readers.reserve(files.size());
    for (const GlobalIndexIOMeta& meta : files) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexReader> reader, GetOrCreateReader(meta));
        readers.push_back(std::move(reader));
    }

    using ReaderResult = Result<std::shared_ptr<GlobalIndexResult>>;
    std::vector<ReaderResult> results;
    results.reserve(readers.size());
    if (executor_ == nullptr || readers.size() == 1) {
        for (const std::shared_ptr<GlobalIndexReader>& reader : readers) {
            results.push_back(visitor(reader));
        }
    } else {
        std::vector<std::future<ReaderResult>> futures;
        futures.reserve(readers.size());
        for (const std::shared_ptr<GlobalIndexReader>& reader : readers) {
            futures.push_back(
                Via(executor_.get(), [visitor, reader]() { return visitor(reader); }));
        }
        results = CollectAll(futures);
    }

    std::shared_ptr<GlobalIndexResult> merged_result = nullptr;
    for (ReaderResult& result_or_status : results) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexResult> result,
                               std::move(result_or_status));
        if (result == nullptr) {
            continue;
        }
        if (merged_result == nullptr) {
            merged_result = std::move(result);
        } else {
            PAIMON_ASSIGN_OR_RAISE(merged_result, merged_result->Or(result));
        }
    }
    return merged_result;
}

Result<std::shared_ptr<GlobalIndexReader>> SortedFileGlobalIndexReader::GetOrCreateReader(
    const GlobalIndexIOMeta& meta) {
    auto iterator = reader_cache_.find(meta.file_path);
    if (iterator != reader_cache_.end()) {
        return iterator->second;
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexReader> reader, OpenReader(meta));
    reader_cache_[meta.file_path] = reader;
    return reader;
}

bool SortedFileGlobalIndexReader::FallbackScanEnabled(
    const std::vector<GlobalIndexIOMeta>& files) const {
    if (fallback_scan_max_size_ <= 0) {
        return false;
    }
    int64_t total_size = 0;
    for (const GlobalIndexIOMeta& file : files) {
        if (file.file_size < 0 ||
            file.file_size > std::numeric_limits<int64_t>::max() - total_size) {
            return false;
        }
        total_size += file.file_size;
        if (total_size > fallback_scan_max_size_) {
            return false;
        }
    }
    return true;
}

}  // namespace paimon
