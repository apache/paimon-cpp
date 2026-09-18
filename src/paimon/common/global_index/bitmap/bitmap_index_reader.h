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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "paimon/common/global_index/bitmap/bitmap_global_index_format.h"
#include "paimon/global_index/global_index_io_meta.h"
#include "paimon/global_index/global_index_reader.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

class KeySerializer;

/// Reader for one Java-compatible bitmap global index file.
class BitmapIndexReader : public GlobalIndexReader,
                          public BitmapGlobalIndexFormat::SeekableReader,
                          public std::enable_shared_from_this<BitmapIndexReader> {
 public:
    static Result<std::shared_ptr<BitmapIndexReader>> Create(
        const std::shared_ptr<KeySerializer>& key_serializer,
        const std::shared_ptr<GlobalIndexFileReader>& file_reader, const GlobalIndexIOMeta& meta,
        const std::shared_ptr<MemoryPool>& pool);

    ~BitmapIndexReader() override;

    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNotNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLessOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitStartsWith(const Literal& prefix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitEndsWith(const Literal& suffix) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitContains(const Literal& literal) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitLike(const Literal& literal) override;

    Result<std::shared_ptr<ScoredGlobalIndexResult>> VisitVectorSearch(
        const std::shared_ptr<VectorSearch>& vector_search) override;
    Result<std::shared_ptr<GlobalIndexResult>> VisitFullTextSearch(
        const std::shared_ptr<FullTextSearch>& full_text_search) override;

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return "bitmap";
    }

    using BitmapGlobalIndexFormat::SeekableReader::Read;
    Result<std::shared_ptr<Bytes>> Read(int64_t offset, int32_t length) override;

    Status Close();

 private:
    using DictionaryPredicate =
        std::function<Result<bool>(const BitmapGlobalIndexFormat::SerializedKey&)>;
    using LiteralPredicate = std::function<Result<bool>(const Literal&)>;

    BitmapIndexReader(std::shared_ptr<KeySerializer> key_serializer,
                      std::shared_ptr<InputStream> input, int64_t file_size,
                      std::shared_ptr<MemoryPool> pool)
        : key_serializer_(std::move(key_serializer)),
          input_(std::move(input)),
          file_size_(file_size),
          pool_(std::move(pool)) {}

    std::shared_ptr<GlobalIndexResult> CreateResult(
        std::function<Result<RoaringBitmap64>()> supplier);

    Result<RoaringBitmap64> IsNull();
    Result<RoaringBitmap64> IsNotNull();
    Result<RoaringBitmap64> Equal(const Literal& literal);
    Result<RoaringBitmap64> In(const std::vector<Literal>& literals);
    Result<RoaringBitmap64> StartsWith(const Literal& literal);
    Result<RoaringBitmap64> EndsWith(const Literal& literal);
    Result<RoaringBitmap64> Contains(const Literal& literal);
    Result<RoaringBitmap64> LessThan(const Literal& literal);
    Result<RoaringBitmap64> LessOrEqual(const Literal& literal);
    Result<RoaringBitmap64> GreaterThan(const Literal& literal);
    Result<RoaringBitmap64> GreaterOrEqual(const Literal& literal);
    Result<RoaringBitmap64> NotEqual(const Literal& literal);
    Result<RoaringBitmap64> NotIn(const std::vector<Literal>& literals);
    Result<RoaringBitmap64> Like(const Literal& literal);

    Result<RoaringBitmap64> ScanDictionary(const LiteralPredicate& predicate);
    Result<RoaringBitmap64> ScanSerializedDictionary(const DictionaryPredicate& predicate);
    Result<std::optional<BitmapGlobalIndexFormat::BlockInfo>> FindBitmapBlock(
        const Literal& literal);

    Result<const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>*> GetDictionaryBlocks();
    Result<const BitmapGlobalIndexFormat::DictionaryBlock*> GetDictionaryBlock(
        const BitmapGlobalIndexFormat::DictionaryBlockMeta& block_meta);
    Result<const RoaringBitmap64*> GetNullRows();
    Result<const RoaringBitmap64*> GetNonNullRows();

    Result<int32_t> FindLogicalDictionaryBlockIndex(
        const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>& blocks,
        const Literal& literal);
    static int32_t FindSerializedDictionaryBlockIndex(
        const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>& blocks,
        const BitmapGlobalIndexFormat::SerializedKey& key);

    static bool StartsWith(const BitmapGlobalIndexFormat::SerializedKey& key,
                           const BitmapGlobalIndexFormat::SerializedKey& prefix);
    static bool EndsWith(const BitmapGlobalIndexFormat::SerializedKey& key,
                         const BitmapGlobalIndexFormat::SerializedKey& suffix);
    static bool Contains(const BitmapGlobalIndexFormat::SerializedKey& key,
                         const BitmapGlobalIndexFormat::SerializedKey& infix);
    static std::optional<BitmapGlobalIndexFormat::SerializedKey> PrefixUpperBound(
        const BitmapGlobalIndexFormat::SerializedKey& prefix, MemoryPool* pool);

    std::shared_ptr<KeySerializer> key_serializer_;
    std::shared_ptr<InputStream> input_;
    int64_t file_size_;
    BitmapGlobalIndexFormat::Footer footer_{{0, 0}, {0, 0}, {0, 0}};
    std::shared_ptr<MemoryPool> pool_;

    std::optional<RoaringBitmap64> null_rows_;
    std::optional<RoaringBitmap64> non_null_rows_;
    std::optional<std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>> dictionary_blocks_;
    std::unordered_map<int64_t, std::shared_ptr<BitmapGlobalIndexFormat::DictionaryBlock>>
        dictionary_block_cache_;
    bool closed_ = false;
};

}  // namespace paimon
