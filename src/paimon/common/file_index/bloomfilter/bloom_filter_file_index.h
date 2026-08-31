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

#include "arrow/c/bridge.h"
#include "paimon/common/file_index/bloomfilter/fast_hash.h"
#include "paimon/common/utils/bloom_filter64.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/result.h"
namespace paimon {
class Bytes;
class InputStream;
class Literal;
class MemoryPool;
enum class FieldType;

/// Bloom filter for file index.
///
/// @note This class use `BloomFilter64` as a base filter. Store the num hash function (one
/// integer) and bit set bytes only. Use `HashFunction` to hash the objects, which hash bytes
/// type(like varchar, binary, etc.) using xx hash, hash numeric type by specified number hash(see
/// http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm).
class BloomFilterFileIndex : public FileIndexer {
 public:
    explicit BloomFilterFileIndex(const std::map<std::string, std::string>& options);
    ~BloomFilterFileIndex() override = default;

    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input_stream,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const override;

    static constexpr int32_t kDefaultItems = 1000000;
    static constexpr double kDefaultFpp = 0.1;
    static constexpr char kItems[] = "items";
    static constexpr char kFpp[] = "fpp";

 private:
    std::map<std::string, std::string> options_;
};

class BloomFilterFileIndexWriter : public FileIndexWriter {
 public:
    static Result<std::shared_ptr<BloomFilterFileIndexWriter>> Create(
        const std::shared_ptr<arrow::Field>& field,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(::ArrowArray* batch) override;

    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override;

 private:
    BloomFilterFileIndexWriter(const std::shared_ptr<arrow::DataType>& struct_type,
                               const FastHash::HashFunction& hash_function, BloomFilter64&& filter,
                               const std::shared_ptr<MemoryPool>& pool);

 private:
    std::shared_ptr<arrow::DataType> struct_type_;
    FastHash::HashFunction hash_function_;
    BloomFilter64 filter_;
    std::shared_ptr<MemoryPool> pool_;
};

class BloomFilterFileIndexReader : public FileIndexReader {
 public:
    static Result<std::shared_ptr<BloomFilterFileIndexReader>> Create(
        const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<Bytes>& bytes);

    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override;

 private:
    BloomFilterFileIndexReader(const FastHash::HashFunction& hash_function, BloomFilter64&& filter);

 private:
    FastHash::HashFunction hash_function_;
    BloomFilter64 filter_;
};
}  // namespace paimon
