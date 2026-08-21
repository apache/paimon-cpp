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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class BitSliceIndexRoaringBitmap;
class InputStream;
class MemoryPool;
enum class FieldType;

/// implementation of BSI file index.
class BitSliceIndexBitmapFileIndex : public FileIndexer {
 public:
    explicit BitSliceIndexBitmapFileIndex(const std::map<std::string, std::string>& options);
    ~BitSliceIndexBitmapFileIndex() override = default;

    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input_stream,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const override;

    using ValueMapperType = std::function<Result<int64_t>(const Literal& literal)>;

    static constexpr int8_t VERSION_1 = 1;

 private:
    static Result<ValueMapperType> GetValueMapper(
        const std::shared_ptr<arrow::DataType>& arrow_type);

    template <typename T>
    static Result<int64_t> GetValueFromLiteral(const Literal& literal) {
        if (literal.IsNull()) {
            return Status::Invalid(
                "literal cannot be null when GetValue in BitSliceIndexBitmapFileIndex");
        }
        return static_cast<int64_t>(literal.GetValue<T>());
    }
};

class BitSliceIndexBitmapFileIndexWriter : public FileIndexWriter {
 public:
    BitSliceIndexBitmapFileIndexWriter(
        const std::shared_ptr<arrow::Field>& field,
        const BitSliceIndexBitmapFileIndex::ValueMapperType& value_mapper,
        const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(::ArrowArray* batch) override;

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override;

 private:
    std::shared_ptr<arrow::DataType> struct_type_;
    BitSliceIndexBitmapFileIndex::ValueMapperType value_mapper_;
    std::vector<std::optional<int64_t>> values_;
    int64_t positive_min_ = 0;
    int64_t positive_max_ = 0;
    int64_t negative_min_ = 0;
    int64_t negative_max_ = 0;
    std::shared_ptr<MemoryPool> pool_;
};

class BitSliceIndexBitmapFileIndexReader
    : public FileIndexReader,
      public std::enable_shared_from_this<BitSliceIndexBitmapFileIndexReader> {
 public:
    BitSliceIndexBitmapFileIndexReader(
        int32_t row_number, const BitSliceIndexBitmapFileIndex::ValueMapperType& value_mapper,
        const std::shared_ptr<BitSliceIndexRoaringBitmap>& positive,
        const std::shared_ptr<BitSliceIndexRoaringBitmap>& negative);

    Result<std::shared_ptr<FileIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitLessOrEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitNotEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitIn(const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<FileIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;

    Result<std::shared_ptr<FileIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<FileIndexResult>> VisitIsNotNull() override;

 private:
    int32_t row_number_;
    BitSliceIndexBitmapFileIndex::ValueMapperType value_mapper_;
    std::shared_ptr<BitSliceIndexRoaringBitmap> positive_;
    std::shared_ptr<BitSliceIndexRoaringBitmap> negative_;
};
}  // namespace paimon
