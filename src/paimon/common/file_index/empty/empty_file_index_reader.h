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
#include <utility>
#include <vector>

#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/file_index/scored_file_index_result.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {
class Literal;

/// Reader for a file index entry with no serialized index data.
/// No data in the file index, which mean this file has no related records.
/// Vector and full-text searches return no matches.
class EmptyFileIndexReader : public FileIndexReader {
 public:
    Result<std::shared_ptr<ScoredFileIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override {
        return ScoredFileIndexResult::Create(RoaringBitmap32(), {});
    }

    Result<std::shared_ptr<FileIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override {
        return std::make_shared<BitmapIndexResult>(
            []() -> Result<RoaringBitmap32> { return RoaringBitmap32(); });
    }

    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitIsNotNull() override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitStartsWith(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitEndsWith(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitContains(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitLike(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitLessThan(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitGreaterOrEqual(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitLessOrEqual(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitGreaterThan(const Literal& literal) override {
        return FileIndexResult::Skip();
    }
    Result<std::shared_ptr<FileIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override {
        return FileIndexResult::Skip();
    }
};

}  // namespace paimon
