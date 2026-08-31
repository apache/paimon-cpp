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
#include <string>
#include <unordered_map>
#include <vector>

#include "paimon/common/file_index/bitmap/bitmap_file_index_meta.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/visibility.h"

namespace paimon {
class BitmapFileIndexMeta;
class InputStream;
class MemoryPool;

class PAIMON_EXPORT BitmapFileIndex : public FileIndexer {
 public:
    explicit BitmapFileIndex(const std::map<std::string, std::string>& options);
    ~BitmapFileIndex() override = default;

    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input_stream,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const override;

    /// Currently, it is mainly used to convert timestamps to long
    static Result<Literal> ConvertLiteral(const Literal& literal,
                                          const std::shared_ptr<arrow::DataType>& arrow_type);
    static FieldType ConvertType(const FieldType& data_type);

 public:
    static constexpr int8_t VERSION_1 = 1;
    static constexpr int8_t VERSION_2 = 2;
    static constexpr char VERSION[] = "version";
    static constexpr char INDEX_BLOCK_SIZE[] = "index-block-size";

 private:
    std::map<std::string, std::string> options_;
};

class BitmapFileIndexWriter : public FileIndexWriter {
 public:
    static Result<std::shared_ptr<BitmapFileIndexWriter>> Create(
        const std::shared_ptr<arrow::Field>& field,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(::ArrowArray* batch) override;

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override;

 private:
    BitmapFileIndexWriter(int8_t version, const std::shared_ptr<arrow::DataType>& struct_type,
                          const std::shared_ptr<arrow::DataType>& arrow_type,
                          const std::map<std::string, std::string>& options,
                          const std::shared_ptr<MemoryPool>& pool);

 private:
    int8_t version_;
    /// @note struct_type_ contains only one field with arrow_type_, used for import from C
    /// ArrowArray
    std::shared_ptr<arrow::DataType> struct_type_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    std::unordered_map<Literal, RoaringBitmap32> id_to_bitmap_;
    RoaringBitmap32 null_bitmap_;
    int32_t row_number_ = 0;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> pool_;
};

class BitmapFileIndexReader : public FileIndexReader,
                              public std::enable_shared_from_this<BitmapFileIndexReader> {
 public:
    BitmapFileIndexReader(const std::shared_ptr<arrow::DataType>& arrow_type,
                          const FieldType& data_type, int32_t start, int32_t length,
                          const std::shared_ptr<InputStream>& input_stream,
                          const std::shared_ptr<MemoryPool>& pool);

    // TODO(xinyu.lxy): may overwrite VisitGreaterThan... like VisitIsNotNull

    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitNotEqual(const Literal& literal) override;

    Result<std::shared_ptr<FileIndexResult>> VisitIn(const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<FileIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;

    Result<std::shared_ptr<FileIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<FileIndexResult>> VisitIsNotNull() override;

 private:
    Result<RoaringBitmap32> GetInListResultBitmap(const std::vector<Literal>& literals);
    Result<RoaringBitmap32> ReadBitmap(const Literal& literal);
    Status ReadInternalMeta();

 private:
    int32_t head_start_;
    int32_t length_;
    FieldType data_type_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<InputStream> input_stream_;
    std::unordered_map<Literal, RoaringBitmap32> bitmaps_;
    std::shared_ptr<BitmapFileIndexMeta> bitmap_file_index_meta_;
};
}  // namespace paimon
