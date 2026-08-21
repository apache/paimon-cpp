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

#include "paimon/common/file_index/bloomfilter/bloom_filter_file_index.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/predicate/literal_converter.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

BloomFilterFileIndex::BloomFilterFileIndex(const std::map<std::string, std::string>& options)
    : options_(options) {}
Result<std::shared_ptr<FileIndexReader>> BloomFilterFileIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, int32_t start, int32_t length,
    const std::shared_ptr<InputStream>& input_stream,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BloomFilterFileIndexReader, supposed to have single "
            "field.");
    }
    auto arrow_type = arrow_schema->field(0)->type();

    PAIMON_RETURN_NOT_OK(input_stream->Seek(start, SeekOrigin::FS_SEEK_SET));
    auto bytes = std::make_shared<Bytes>(length, pool.get());
    PAIMON_ASSIGN_OR_RAISE(int64_t actual_read_len,
                           input_stream->Read(bytes->data(), bytes->size()));
    if (static_cast<size_t>(actual_read_len) != bytes->size()) {
        return Status::Invalid(
            fmt::format("create reader for BloomFilterFileIndex failed, expected read len "
                        "{}, actual read len {}",
                        bytes->size(), actual_read_len));
    }
    return BloomFilterFileIndexReader::Create(arrow_type, bytes);
}

Result<std::shared_ptr<FileIndexWriter>> BloomFilterFileIndex::CreateWriter(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BloomFilterFileIndexWriter, supposed to have single field.");
    }
    return BloomFilterFileIndexWriter::Create(arrow_schema->field(0), options_, pool);
}

Result<std::shared_ptr<BloomFilterFileIndexWriter>> BloomFilterFileIndexWriter::Create(
    const std::shared_ptr<arrow::Field>& field, const std::map<std::string, std::string>& options,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(FastHash::HashFunction hash_function,
                           FastHash::GetHashFunction(field->type()));
    PAIMON_ASSIGN_OR_RAISE(
        int32_t items, OptionsUtils::GetValueFromMap<int32_t>(options, BloomFilterFileIndex::kItems,
                                                              BloomFilterFileIndex::kDefaultItems));
    PAIMON_ASSIGN_OR_RAISE(
        double fpp, OptionsUtils::GetValueFromMap<double>(options, BloomFilterFileIndex::kFpp,
                                                          BloomFilterFileIndex::kDefaultFpp));
    std::shared_ptr<arrow::DataType> struct_type = arrow::struct_({field});
    PAIMON_ASSIGN_OR_RAISE(BloomFilter64 filter, BloomFilter64::Create(items, fpp, pool));
    return std::shared_ptr<BloomFilterFileIndexWriter>(
        new BloomFilterFileIndexWriter(struct_type, hash_function, std::move(filter), pool));
}

BloomFilterFileIndexWriter::BloomFilterFileIndexWriter(
    const std::shared_ptr<arrow::DataType>& struct_type,
    const FastHash::HashFunction& hash_function, BloomFilter64&& filter,
    const std::shared_ptr<MemoryPool>& pool)
    : struct_type_(struct_type),
      hash_function_(hash_function),
      filter_(std::move(filter)),
      pool_(pool) {}

Status BloomFilterFileIndexWriter::AddBatch(::ArrowArray* batch) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(batch, struct_type_));
    if (!array || array->type_id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            "invalid batch for BloomFilterFileIndexWriter, expected a struct array");
    }
    std::shared_ptr<arrow::StructArray> struct_array =
        checked_pointer_cast<arrow::StructArray>(array);
    if (struct_array->num_fields() != 1) {
        return Status::Invalid(
            "invalid batch for BloomFilterFileIndexWriter, expected a struct array with exactly "
            "one field");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<Literal> values,
        LiteralConverter::ConvertLiteralsFromArray(*struct_array->field(0), /*own_data=*/true));
    for (const Literal& value : values) {
        if (!value.IsNull()) {
            filter_.AddHash(hash_function_(value));
        }
    }
    return Status::OK();
}

Result<PAIMON_UNIQUE_PTR<Bytes>> BloomFilterFileIndexWriter::SerializedBytes() const {
    constexpr int32_t kHeaderLength = sizeof(int32_t);
    const int32_t bit_set_length = filter_.GetBitSet().BitSize() / BloomFilter64::BYTE_SIZE;
    PAIMON_UNIQUE_PTR<Bytes> bytes =
        Bytes::AllocateBytes(kHeaderLength + bit_set_length, pool_.get());
    const auto num_hash_functions = static_cast<uint32_t>(filter_.GetNumHashFunctions());
    bytes->data()[0] = static_cast<char>((num_hash_functions >> 24) & 0xff);
    bytes->data()[1] = static_cast<char>((num_hash_functions >> 16) & 0xff);
    bytes->data()[2] = static_cast<char>((num_hash_functions >> 8) & 0xff);
    bytes->data()[3] = static_cast<char>(num_hash_functions & 0xff);
    filter_.GetBitSet().ToByteArray(kHeaderLength, bit_set_length, bytes->data());
    return bytes;
}

Result<std::shared_ptr<BloomFilterFileIndexReader>> BloomFilterFileIndexReader::Create(
    const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<Bytes>& bytes) {
    // Compatible with Java's big-endian numHashFunctions header.
    const char* data = bytes->data();
    auto num_hash_functions =
        static_cast<int32_t>((static_cast<uint32_t>(static_cast<uint8_t>(data[0])) << 24) |
                             (static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 16) |
                             (static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 8) |
                             static_cast<uint32_t>(static_cast<uint8_t>(data[3])));
    PAIMON_ASSIGN_OR_RAISE(FastHash::HashFunction hash_function,
                           FastHash::GetHashFunction(arrow_type));
    auto bit_set = std::make_unique<BloomFilter64::BitSet>(bytes, /*offset=*/sizeof(int32_t));
    return std::shared_ptr<BloomFilterFileIndexReader>(new BloomFilterFileIndexReader(
        hash_function, BloomFilter64(num_hash_functions, std::move(bit_set))));
}

BloomFilterFileIndexReader::BloomFilterFileIndexReader(const FastHash::HashFunction& hash_function,
                                                       BloomFilter64&& filter)
    : hash_function_(hash_function), filter_(std::move(filter)) {}

Result<std::shared_ptr<FileIndexResult>> BloomFilterFileIndexReader::VisitEqual(
    const Literal& literal) {
    // This returns `Remain` to align with the current Java implementation in BF index, even though
    // its predicate semantics are inconsistent here. In practice, equality tests in predicate
    // evaluation always return false when the literal is null. See
    // `null_false_leaf_binary_function.h`.
    if (literal.IsNull()) {
        return FileIndexResult::Remain();
    }
    int64_t hash = hash_function_(literal);
    return filter_.TestHash(hash) ? FileIndexResult::Remain() : FileIndexResult::Skip();
}

}  // namespace paimon
