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

#include "paimon/common/global_index/bitmap/bitmap_index_reader.h"

#include <algorithm>
#include <cstring>
#include <set>
#include <string_view>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/predicate/like.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/predicate/literal.h"

namespace paimon {

Result<std::shared_ptr<BitmapIndexReader>> BitmapIndexReader::Create(
    const std::shared_ptr<KeySerializer>& key_serializer,
    const std::shared_ptr<GlobalIndexFileReader>& file_reader, const GlobalIndexIOMeta& meta,
    const std::shared_ptr<MemoryPool>& pool) {
    if (key_serializer == nullptr || file_reader == nullptr || pool == nullptr) {
        return Status::Invalid(
            "Cannot create BitmapIndexReader without serializer, file reader, and memory pool.");
    }
    if (meta.file_size < 0) {
        return Status::Invalid("Cannot create BitmapIndexReader with a negative file size.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input,
                           file_reader->GetInputStream(meta.file_path));
    std::shared_ptr<BitmapIndexReader> reader(
        new BitmapIndexReader(key_serializer, std::move(input), meta.file_size, pool));
    Result<BitmapGlobalIndexFormat::Footer> footer =
        BitmapGlobalIndexFormat::ReadFooter(meta.file_size, reader.get());
    if (!footer.ok()) {
        [[maybe_unused]] Status close_status = reader->Close();
        return footer.status();
    }
    reader->footer_ = std::move(footer).value();
    return reader;
}

BitmapIndexReader::~BitmapIndexReader() {
    [[maybe_unused]] Status close_status = Close();
}

std::shared_ptr<GlobalIndexResult> BitmapIndexReader::CreateResult(
    std::function<Result<RoaringBitmap64>()> supplier) {
    return std::make_shared<BitmapGlobalIndexResult>(std::move(supplier));
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitIsNotNull() {
    return CreateResult([reader = shared_from_this()]() { return reader->IsNotNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitIsNull() {
    return CreateResult([reader = shared_from_this()]() { return reader->IsNull(); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitEqual(const Literal& literal) {
    return CreateResult(
        [reader = shared_from_this(), literal]() { return reader->Equal(literal); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitNotEqual(
    const Literal& literal) {
    return CreateResult(
        [reader = shared_from_this(), literal]() { return reader->NotEqual(literal); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitLessThan(
    const Literal& literal) {
    return CreateResult(
        [reader = shared_from_this(), literal]() { return reader->LessThan(literal); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitLessOrEqual(
    const Literal& literal) {
    return CreateResult(
        [reader = shared_from_this(), literal]() { return reader->LessOrEqual(literal); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitGreaterThan(
    const Literal& literal) {
    return CreateResult(
        [reader = shared_from_this(), literal]() { return reader->GreaterThan(literal); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitGreaterOrEqual(
    const Literal& literal) {
    return CreateResult(
        [reader = shared_from_this(), literal]() { return reader->GreaterOrEqual(literal); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitIn(
    const std::vector<Literal>& literals) {
    return CreateResult([reader = shared_from_this(), literals]() { return reader->In(literals); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitNotIn(
    const std::vector<Literal>& literals) {
    return CreateResult(
        [reader = shared_from_this(), literals]() { return reader->NotIn(literals); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitStartsWith(
    const Literal& prefix) {
    return CreateResult(
        [reader = shared_from_this(), prefix]() { return reader->StartsWith(prefix); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitEndsWith(const Literal& suffix) {
    return CreateResult(
        [reader = shared_from_this(), suffix]() { return reader->EndsWith(suffix); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitContains(
    const Literal& literal) {
    return CreateResult(
        [reader = shared_from_this(), literal]() { return reader->Contains(literal); });
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitLike(const Literal& literal) {
    return CreateResult([reader = shared_from_this(), literal]() { return reader->Like(literal); });
}

Result<std::shared_ptr<ScoredGlobalIndexResult>> BitmapIndexReader::VisitVectorSearch(
    const std::shared_ptr<VectorSearch>& vector_search) {
    return Status::Invalid("Vector search is not supported in BitmapIndexReader.");
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapIndexReader::VisitFullTextSearch(
    const std::shared_ptr<FullTextSearch>& full_text_search) {
    return Status::Invalid("Full text search is not supported in BitmapIndexReader.");
}

Result<std::shared_ptr<Bytes>> BitmapIndexReader::Read(int64_t offset, int32_t length) {
    if (closed_ || input_ == nullptr) {
        return Status::Invalid("Cannot read from a closed BitmapIndexReader.");
    }
    if (offset < 0 || length < 0 || offset > file_size_ || length > file_size_ - offset) {
        return Status::Invalid(fmt::format("Bitmap index read range [{}, {}) exceeds file size {}.",
                                           offset, offset + length, file_size_));
    }
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(length, pool_.get());
    if (length == 0) {
        return bytes;
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t actual_length, input_->Read(bytes->data(), length, offset));
    if (actual_length != length) {
        return Status::IOError(
            fmt::format("Truncated bitmap index read: expected {} bytes at {}, but read {}.",
                        length, offset, actual_length));
    }
    return bytes;
}

Status BitmapIndexReader::Close() {
    if (closed_) {
        return Status::OK();
    }
    closed_ = true;
    if (input_ == nullptr) {
        return Status::OK();
    }
    return input_->Close();
}

Result<RoaringBitmap64> BitmapIndexReader::IsNull() {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, GetNullRows());
    return *bitmap;
}

Result<RoaringBitmap64> BitmapIndexReader::IsNotNull() {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, GetNonNullRows());
    return *bitmap;
}

Result<RoaringBitmap64> BitmapIndexReader::Equal(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<BitmapGlobalIndexFormat::BlockInfo> bitmap_block,
                           FindBitmapBlock(literal));
    if (!bitmap_block.has_value()) {
        return RoaringBitmap64();
    }
    return BitmapGlobalIndexFormat::ReadBitmap(bitmap_block.value(), this);
}

Result<RoaringBitmap64> BitmapIndexReader::In(const std::vector<Literal>& literals) {
    RoaringBitmap64 result;
    std::set<std::string> serialized_keys;
    for (const Literal& literal : literals) {
        if (literal.IsNull()) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(
            BitmapGlobalIndexFormat::SerializedKey serialized_key,
            BitmapGlobalIndexFormat::SerializedKey::FromLiteral(key_serializer_, literal));
        const std::shared_ptr<Bytes>& key_bytes = serialized_key.GetBytes();
        if (!serialized_keys.emplace(key_bytes->data(), key_bytes->size()).second) {
            continue;
        }
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 bitmap, Equal(literal));
        result |= bitmap;
    }
    return result;
}

Result<RoaringBitmap64> BitmapIndexReader::StartsWith(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    if (literal.GetType() != FieldType::STRING) {
        return Status::Invalid("StartsWith requires a string literal in BitmapIndexReader.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        BitmapGlobalIndexFormat::SerializedKey prefix,
        BitmapGlobalIndexFormat::SerializedKey::FromLiteral(key_serializer_, literal));
    if (prefix.GetBytes()->size() == 0) {
        return IsNotNull();
    }
    std::optional<BitmapGlobalIndexFormat::SerializedKey> upper_bound =
        PrefixUpperBound(prefix, pool_.get());
    PAIMON_ASSIGN_OR_RAISE(const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>* blocks,
                           GetDictionaryBlocks());
    int32_t index = std::max(FindSerializedDictionaryBlockIndex(*blocks, prefix), 0);
    RoaringBitmap64 result;
    while (index < static_cast<int32_t>(blocks->size())) {
        const BitmapGlobalIndexFormat::DictionaryBlockMeta& block_meta = (*blocks)[index];
        if (upper_bound.has_value() && block_meta.FirstKey().CompareTo(upper_bound.value()) >= 0) {
            return result;
        }
        PAIMON_ASSIGN_OR_RAISE(const BitmapGlobalIndexFormat::DictionaryBlock* block,
                               GetDictionaryBlock(block_meta));
        for (const BitmapGlobalIndexFormat::DictionaryEntry& entry : block->Entries()) {
            if (entry.Key().CompareTo(prefix) < 0) {
                continue;
            }
            if (!StartsWith(entry.Key(), prefix)) {
                return result;
            }
            PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 bitmap,
                                   BitmapGlobalIndexFormat::ReadBitmap(entry.BitmapBlock(), this));
            result |= bitmap;
        }
        ++index;
    }
    return result;
}

Result<RoaringBitmap64> BitmapIndexReader::EndsWith(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    if (literal.GetType() != FieldType::STRING) {
        return Status::Invalid("EndsWith requires a string literal in BitmapIndexReader.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        BitmapGlobalIndexFormat::SerializedKey suffix,
        BitmapGlobalIndexFormat::SerializedKey::FromLiteral(key_serializer_, literal));
    if (suffix.GetBytes()->size() == 0) {
        return IsNotNull();
    }
    return ScanSerializedDictionary(
        [&suffix](const BitmapGlobalIndexFormat::SerializedKey& key) -> Result<bool> {
            return EndsWith(key, suffix);
        });
}

Result<RoaringBitmap64> BitmapIndexReader::Contains(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    if (literal.GetType() != FieldType::STRING) {
        return Status::Invalid("Contains requires a string literal in BitmapIndexReader.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        BitmapGlobalIndexFormat::SerializedKey infix,
        BitmapGlobalIndexFormat::SerializedKey::FromLiteral(key_serializer_, literal));
    if (infix.GetBytes()->size() == 0) {
        return IsNotNull();
    }
    return ScanSerializedDictionary(
        [&infix](const BitmapGlobalIndexFormat::SerializedKey& key) -> Result<bool> {
            return Contains(key, infix);
        });
}

// TODO(xinyu.lxy): Optimize range predicates by locating the dictionary lower/upper bound and
// scanning only the matching side instead of traversing the entire dictionary.
Result<RoaringBitmap64> BitmapIndexReader::LessThan(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    return ScanDictionary([&literal](const Literal& key) -> Result<bool> {
        PAIMON_ASSIGN_OR_RAISE(int32_t comparison, key.CompareTo(literal));
        return comparison < 0;
    });
}

Result<RoaringBitmap64> BitmapIndexReader::LessOrEqual(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    return ScanDictionary([&literal](const Literal& key) -> Result<bool> {
        PAIMON_ASSIGN_OR_RAISE(int32_t comparison, key.CompareTo(literal));
        return comparison <= 0;
    });
}

Result<RoaringBitmap64> BitmapIndexReader::GreaterThan(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    return ScanDictionary([&literal](const Literal& key) -> Result<bool> {
        PAIMON_ASSIGN_OR_RAISE(int32_t comparison, key.CompareTo(literal));
        return comparison > 0;
    });
}

Result<RoaringBitmap64> BitmapIndexReader::GreaterOrEqual(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    return ScanDictionary([&literal](const Literal& key) -> Result<bool> {
        PAIMON_ASSIGN_OR_RAISE(int32_t comparison, key.CompareTo(literal));
        return comparison >= 0;
    });
}

Result<RoaringBitmap64> BitmapIndexReader::NotEqual(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, IsNotNull());
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 equal, Equal(literal));
    result -= equal;
    return result;
}

Result<RoaringBitmap64> BitmapIndexReader::NotIn(const std::vector<Literal>& literals) {
    for (const Literal& literal : literals) {
        if (literal.IsNull()) {
            return RoaringBitmap64();
        }
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 result, IsNotNull());
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 in, In(literals));
    result -= in;
    return result;
}

Result<RoaringBitmap64> BitmapIndexReader::Like(const Literal& literal) {
    if (literal.IsNull()) {
        return RoaringBitmap64();
    }
    if (literal.GetType() != FieldType::STRING) {
        return Status::Invalid("LIKE requires a string literal in BitmapIndexReader.");
    }
    std::string pattern = literal.GetValue<std::string>();
    return ScanDictionary([pattern = std::move(pattern)](const Literal& key) -> Result<bool> {
        if (key.GetType() != FieldType::STRING) {
            return false;
        }
        return paimon::Like::Instance().TestString(key.GetValue<std::string>(), pattern);
    });
}

Result<RoaringBitmap64> BitmapIndexReader::ScanDictionary(const LiteralPredicate& predicate) {
    PAIMON_ASSIGN_OR_RAISE(
        const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>* block_metas,
        GetDictionaryBlocks());
    RoaringBitmap64 result;
    for (const BitmapGlobalIndexFormat::DictionaryBlockMeta& block_meta : *block_metas) {
        PAIMON_ASSIGN_OR_RAISE(const BitmapGlobalIndexFormat::DictionaryBlock* block,
                               GetDictionaryBlock(block_meta));
        for (const BitmapGlobalIndexFormat::DictionaryEntry& entry : block->Entries()) {
            PAIMON_ASSIGN_OR_RAISE(Literal key, key_serializer_->Deserialize(
                                                    MemorySlice::Wrap(entry.Key().GetBytes())));
            PAIMON_ASSIGN_OR_RAISE(bool matches, predicate(key));
            if (matches) {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 bitmap, BitmapGlobalIndexFormat::ReadBitmap(
                                                                   entry.BitmapBlock(), this));
                result |= bitmap;
            }
        }
    }
    return result;
}

Result<RoaringBitmap64> BitmapIndexReader::ScanSerializedDictionary(
    const DictionaryPredicate& predicate) {
    PAIMON_ASSIGN_OR_RAISE(
        const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>* block_metas,
        GetDictionaryBlocks());
    RoaringBitmap64 result;
    for (const BitmapGlobalIndexFormat::DictionaryBlockMeta& block_meta : *block_metas) {
        PAIMON_ASSIGN_OR_RAISE(const BitmapGlobalIndexFormat::DictionaryBlock* block,
                               GetDictionaryBlock(block_meta));
        for (const BitmapGlobalIndexFormat::DictionaryEntry& entry : block->Entries()) {
            PAIMON_ASSIGN_OR_RAISE(bool matches, predicate(entry.Key()));
            if (matches) {
                PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 bitmap, BitmapGlobalIndexFormat::ReadBitmap(
                                                                   entry.BitmapBlock(), this));
                result |= bitmap;
            }
        }
    }
    return result;
}

Result<std::optional<BitmapGlobalIndexFormat::BlockInfo>> BitmapIndexReader::FindBitmapBlock(
    const Literal& literal) {
    PAIMON_ASSIGN_OR_RAISE(const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>* blocks,
                           GetDictionaryBlocks());
    if (blocks->empty()) {
        return std::optional<BitmapGlobalIndexFormat::BlockInfo>();
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t index, FindLogicalDictionaryBlockIndex(*blocks, literal));
    if (index < 0) {
        return std::optional<BitmapGlobalIndexFormat::BlockInfo>();
    }
    PAIMON_ASSIGN_OR_RAISE(const BitmapGlobalIndexFormat::DictionaryBlock* block,
                           GetDictionaryBlock((*blocks)[index]));
    for (const BitmapGlobalIndexFormat::DictionaryEntry& entry : block->Entries()) {
        PAIMON_ASSIGN_OR_RAISE(
            Literal key, key_serializer_->Deserialize(MemorySlice::Wrap(entry.Key().GetBytes())));
        PAIMON_ASSIGN_OR_RAISE(int32_t comparison, key.CompareTo(literal));
        if (comparison == 0) {
            return std::optional<BitmapGlobalIndexFormat::BlockInfo>(entry.BitmapBlock());
        }
        if (comparison > 0) {
            break;
        }
    }
    return std::optional<BitmapGlobalIndexFormat::BlockInfo>();
}

Result<const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>*>
BitmapIndexReader::GetDictionaryBlocks() {
    if (!dictionary_blocks_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(
            std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta> blocks,
            BitmapGlobalIndexFormat::ReadIndexBlock(footer_.IndexBlock(), this, pool_.get()));
        dictionary_blocks_ = std::move(blocks);
    }
    return &dictionary_blocks_.value();
}

Result<const BitmapGlobalIndexFormat::DictionaryBlock*> BitmapIndexReader::GetDictionaryBlock(
    const BitmapGlobalIndexFormat::DictionaryBlockMeta& block_meta) {
    auto iterator = dictionary_block_cache_.find(block_meta.Offset());
    if (iterator != dictionary_block_cache_.end()) {
        return iterator->second.get();
    }
    PAIMON_ASSIGN_OR_RAISE(
        BitmapGlobalIndexFormat::DictionaryBlock block,
        BitmapGlobalIndexFormat::ReadDictionaryBlock(block_meta, this, pool_.get()));
    auto inserted = dictionary_block_cache_.emplace(
        block_meta.Offset(),
        std::make_shared<BitmapGlobalIndexFormat::DictionaryBlock>(std::move(block)));
    return inserted.first->second.get();
}

Result<const RoaringBitmap64*> BitmapIndexReader::GetNullRows() {
    if (!null_rows_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 bitmap,
                               BitmapGlobalIndexFormat::ReadBitmap(footer_.NullRowsBlock(), this));
        null_rows_ = std::move(bitmap);
    }
    return &null_rows_.value();
}

Result<const RoaringBitmap64*> BitmapIndexReader::GetNonNullRows() {
    if (!non_null_rows_.has_value()) {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap64 bitmap, BitmapGlobalIndexFormat::ReadBitmap(
                                                           footer_.NonNullRowsBlock(), this));
        non_null_rows_ = std::move(bitmap);
    }
    return &non_null_rows_.value();
}

Result<int32_t> BitmapIndexReader::FindLogicalDictionaryBlockIndex(
    const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>& blocks,
    const Literal& literal) {
    int32_t low = 0;
    int32_t high = static_cast<int32_t>(blocks.size()) - 1;
    while (low <= high) {
        int32_t middle = low + ((high - low) / 2);
        PAIMON_ASSIGN_OR_RAISE(
            Literal first_key,
            key_serializer_->Deserialize(MemorySlice::Wrap(blocks[middle].FirstKey().GetBytes())));
        PAIMON_ASSIGN_OR_RAISE(int32_t comparison, first_key.CompareTo(literal));
        if (comparison <= 0) {
            low = middle + 1;
        } else {
            high = middle - 1;
        }
    }
    return high;
}

int32_t BitmapIndexReader::FindSerializedDictionaryBlockIndex(
    const std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>& blocks,
    const BitmapGlobalIndexFormat::SerializedKey& key) {
    int32_t low = 0;
    int32_t high = static_cast<int32_t>(blocks.size()) - 1;
    while (low <= high) {
        int32_t middle = low + ((high - low) / 2);
        int32_t comparison = blocks[middle].FirstKey().CompareTo(key);
        if (comparison <= 0) {
            low = middle + 1;
        } else {
            high = middle - 1;
        }
    }
    return high;
}

bool BitmapIndexReader::StartsWith(const BitmapGlobalIndexFormat::SerializedKey& key,
                                   const BitmapGlobalIndexFormat::SerializedKey& prefix) {
    const std::shared_ptr<Bytes>& key_bytes = key.GetBytes();
    const std::shared_ptr<Bytes>& prefix_bytes = prefix.GetBytes();
    return key_bytes->size() >= prefix_bytes->size() &&
           std::memcmp(key_bytes->data(), prefix_bytes->data(), prefix_bytes->size()) == 0;
}

bool BitmapIndexReader::EndsWith(const BitmapGlobalIndexFormat::SerializedKey& key,
                                 const BitmapGlobalIndexFormat::SerializedKey& suffix) {
    const std::shared_ptr<Bytes>& key_bytes = key.GetBytes();
    const std::shared_ptr<Bytes>& suffix_bytes = suffix.GetBytes();
    return key_bytes->size() >= suffix_bytes->size() &&
           std::memcmp(key_bytes->data() + key_bytes->size() - suffix_bytes->size(),
                       suffix_bytes->data(), suffix_bytes->size()) == 0;
}

bool BitmapIndexReader::Contains(const BitmapGlobalIndexFormat::SerializedKey& key,
                                 const BitmapGlobalIndexFormat::SerializedKey& infix) {
    std::string_view key_bytes(key.GetBytes()->data(), key.GetBytes()->size());
    std::string_view infix_bytes(infix.GetBytes()->data(), infix.GetBytes()->size());
    return key_bytes.find(infix_bytes) != std::string_view::npos;
}

std::optional<BitmapGlobalIndexFormat::SerializedKey> BitmapIndexReader::PrefixUpperBound(
    const BitmapGlobalIndexFormat::SerializedKey& prefix, MemoryPool* pool) {
    const std::shared_ptr<Bytes>& prefix_bytes = prefix.GetBytes();
    for (int64_t i = static_cast<int64_t>(prefix_bytes->size()) - 1; i >= 0; --i) {
        uint8_t value = static_cast<uint8_t>(prefix_bytes->data()[i]);
        if (value != 0xFF) {
            std::shared_ptr<Bytes> upper_bound = Bytes::AllocateBytes(i + 1, pool);
            std::memcpy(upper_bound->data(), prefix_bytes->data(), i + 1);
            upper_bound->data()[i] = static_cast<char>(value + 1);
            return BitmapGlobalIndexFormat::SerializedKey(std::move(upper_bound));
        }
    }
    return std::nullopt;
}

}  // namespace paimon
