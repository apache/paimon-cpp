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

#include "paimon/common/global_index/bitmap/bitmap_global_index_format.h"

#include <algorithm>
#include <limits>

#include "fmt/format.h"
#include "paimon/common/global_index/key_serializer.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/common/sst/block_trailer.h"
#include "paimon/common/sst/sst_file_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/var_length_int_utils.h"
#include "paimon/io/byte_order.h"
#include "paimon/predicate/literal.h"

namespace paimon {
namespace {

Status WriteAll(const char* data, int64_t length, OutputStream* output_stream) {
    PAIMON_ASSIGN_OR_RAISE(int64_t written, output_stream->Write(data, length));
    if (written != length) {
        return Status::IOError(
            fmt::format("Failed to write bitmap global index block: expected {} bytes, wrote {}.",
                        length, written));
    }
    return Status::OK();
}

Result<std::shared_ptr<Bytes>> ReadKey(MemorySliceInput* input, MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t key_length, VarLengthIntUtils::ReadVarLenInt(input));
    if (key_length > input->Available()) {
        return Status::Invalid(
            fmt::format("Bitmap dictionary key length {} exceeds remaining block size {}.",
                        key_length, input->Available()));
    }
    return input->ReadSliceView(key_length).CopyBytes(pool);
}

Status ValidateBlockInfo(const BitmapGlobalIndexFormat::BlockInfo& block) {
    if (block.Offset() < 0) {
        return Status::Invalid("Invalid negative bitmap block offset.");
    }
    if (block.Length() < 0) {
        return Status::Invalid("Invalid negative bitmap block length.");
    }
    return Status::OK();
}

}  // namespace

Result<BitmapGlobalIndexFormat::SerializedKey> BitmapGlobalIndexFormat::SerializedKey::FromLiteral(
    const std::shared_ptr<KeySerializer>& serializer, const Literal& literal) {
    if (serializer == nullptr) {
        return Status::Invalid("Cannot serialize a bitmap dictionary key without KeySerializer.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, serializer->Serialize(literal));
    return SerializedKey(std::move(bytes));
}

int32_t BitmapGlobalIndexFormat::SerializedKey::CompareTo(const SerializedKey& other) const {
    if (bytes_ == nullptr || other.bytes_ == nullptr) {
        if (bytes_ == other.bytes_) {
            return 0;
        }
        return bytes_ == nullptr ? -1 : 1;
    }
    size_t compare_length = std::min(bytes_->size(), other.bytes_->size());
    for (size_t i = 0; i < compare_length; ++i) {
        int32_t left = static_cast<uint8_t>(bytes_->data()[i]);
        int32_t right = static_cast<uint8_t>(other.bytes_->data()[i]);
        if (left != right) {
            return left < right ? -1 : 1;
        }
    }
    if (bytes_->size() == other.bytes_->size()) {
        return 0;
    }
    return bytes_->size() < other.bytes_->size() ? -1 : 1;
}

Result<std::shared_ptr<Bytes>> BitmapGlobalIndexFormat::SeekableReader::Read(
    const BlockInfo& block) {
    PAIMON_RETURN_NOT_OK(ValidateBlockInfo(block));
    return Read(block.Offset(), block.Length());
}

Result<int32_t> BitmapGlobalIndexFormat::DictionaryEntry::EstimatedSize() const {
    if (key_.GetBytes() == nullptr) {
        return Status::Invalid("Bitmap dictionary key is null.");
    }
    PAIMON_RETURN_NOT_OK(
        ValidateValueInRange<int32_t>(key_.GetBytes()->size(), "bitmap dictionary key length"));
    PAIMON_ASSIGN_OR_RAISE(int32_t key_length_size,
                           EstimatedVarLenIntSize(static_cast<int32_t>(key_.GetBytes()->size())));
    PAIMON_ASSIGN_OR_RAISE(int32_t offset_size, EstimatedVarLenLongSize(bitmap_block_.Offset()));
    PAIMON_ASSIGN_OR_RAISE(int32_t length_size, EstimatedVarLenIntSize(bitmap_block_.Length()));
    int64_t size =
        static_cast<int64_t>(key_length_size) + key_.GetBytes()->size() + offset_size + length_size;
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(size, "bitmap dictionary entry size"));
    return static_cast<int32_t>(size);
}

Result<std::unique_ptr<BitmapGlobalIndexFormat::StreamingWriter>>
BitmapGlobalIndexFormat::StreamingWriter::Create(
    const std::shared_ptr<OutputStream>& output_stream, int32_t dictionary_block_size,
    const std::shared_ptr<BlockCompressionFactory>& compression_factory,
    const std::shared_ptr<MemoryPool>& pool) {
    if (output_stream == nullptr) {
        return Status::Invalid("Cannot create bitmap StreamingWriter without an output stream.");
    }
    if (dictionary_block_size <= 0) {
        return Status::Invalid("Bitmap dictionary block size must be greater than 0.");
    }
    if (pool == nullptr) {
        return Status::Invalid("Cannot create bitmap StreamingWriter without a memory pool.");
    }
    return std::unique_ptr<StreamingWriter>(
        new StreamingWriter(output_stream, dictionary_block_size, compression_factory, pool));
}

Status BitmapGlobalIndexFormat::StreamingWriter::Write(SerializedKey key,
                                                       const RoaringBitmap64& bitmap) {
    if (finished_) {
        return Status::Invalid("Cannot write to a finished bitmap StreamingWriter.");
    }
    if (key.GetBytes() == nullptr) {
        return Status::Invalid("Cannot write a null serialized bitmap dictionary key.");
    }
    PAIMON_ASSIGN_OR_RAISE(BlockInfo bitmap_block,
                           WriteBitmapBlock(bitmap, output_stream_.get(), pool_.get()));
    DictionaryEntry entry(std::move(key), std::move(bitmap_block));
    PAIMON_ASSIGN_OR_RAISE(int32_t estimated_size, EstimatedDictionaryBlockSizeAfter(entry));
    if (!current_dictionary_entries_.empty() && estimated_size > dictionary_block_size_) {
        PAIMON_RETURN_NOT_OK(FlushDictionaryBlock());
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t entry_size, entry.EstimatedSize());
    if (value_count_ == std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Bitmap global index value count exceeds INT32_MAX.");
    }
    current_dictionary_entries_size_ += entry_size;
    current_dictionary_entries_.push_back(std::move(entry));
    ++value_count_;
    return Status::OK();
}

Status BitmapGlobalIndexFormat::StreamingWriter::Finish(const RoaringBitmap64& null_rows,
                                                        const RoaringBitmap64& non_null_rows) {
    if (finished_) {
        return Status::Invalid("Bitmap StreamingWriter has already been finished.");
    }
    PAIMON_RETURN_NOT_OK(FlushDictionaryBlock());
    PAIMON_ASSIGN_OR_RAISE(BlockInfo null_rows_block,
                           WriteBitmapBlock(null_rows, output_stream_.get(), pool_.get()));
    PAIMON_ASSIGN_OR_RAISE(BlockInfo non_null_rows_block,
                           WriteBitmapBlock(non_null_rows, output_stream_.get(), pool_.get()));
    PAIMON_ASSIGN_OR_RAISE(BlockInfo index_block,
                           WriteIndexBlock(dictionary_block_metas_, compression_factory_.get(),
                                           output_stream_.get(), pool_.get()));
    PAIMON_RETURN_NOT_OK(WriteFooter(null_rows_block, non_null_rows_block, index_block,
                                     value_count_, output_stream_.get()));
    finished_ = true;
    return Status::OK();
}

Status BitmapGlobalIndexFormat::StreamingWriter::FlushDictionaryBlock() {
    if (current_dictionary_entries_.empty()) {
        return Status::OK();
    }
    PAIMON_ASSIGN_OR_RAISE(
        DictionaryBlockMeta block,
        WriteDictionaryBlock(current_dictionary_entries_, compression_factory_.get(),
                             output_stream_.get(), pool_.get()));
    dictionary_block_metas_.push_back(std::move(block));
    current_dictionary_entries_.clear();
    current_dictionary_entries_size_ = 0;
    return Status::OK();
}

Result<int32_t> BitmapGlobalIndexFormat::StreamingWriter::EstimatedDictionaryBlockSizeAfter(
    const DictionaryEntry& entry) const {
    size_t entry_count = current_dictionary_entries_.size() + 1;
    PAIMON_RETURN_NOT_OK(
        ValidateValueInRange<int32_t>(entry_count, "bitmap dictionary block entry count"));
    PAIMON_ASSIGN_OR_RAISE(int32_t count_size,
                           EstimatedVarLenIntSize(static_cast<int32_t>(entry_count)));
    PAIMON_ASSIGN_OR_RAISE(int32_t entry_size, entry.EstimatedSize());
    int64_t total =
        static_cast<int64_t>(count_size) + current_dictionary_entries_size_ + entry_size;
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(total, "bitmap dictionary block size"));
    return static_cast<int32_t>(total);
}

Status BitmapGlobalIndexFormat::WriteFooter(const BlockInfo& null_rows_block,
                                            const BlockInfo& non_null_rows_block,
                                            const BlockInfo& index_block, int32_t value_count,
                                            OutputStream* output_stream) {
    PAIMON_RETURN_NOT_OK(ValidateBlockInfo(null_rows_block));
    PAIMON_RETURN_NOT_OK(ValidateBlockInfo(non_null_rows_block));
    PAIMON_RETURN_NOT_OK(ValidateBlockInfo(index_block));
    if (value_count < 0) {
        return Status::Invalid("Invalid negative bitmap value count.");
    }
    DataOutputStream output(output_stream);
    output.SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);
    PAIMON_RETURN_NOT_OK(output.WriteValue<int64_t>(null_rows_block.Offset()));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int32_t>(null_rows_block.Length()));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int64_t>(non_null_rows_block.Offset()));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int32_t>(non_null_rows_block.Length()));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int64_t>(index_block.Offset()));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int32_t>(index_block.Length()));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int32_t>(value_count));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int32_t>(kVersion));
    PAIMON_RETURN_NOT_OK(output.WriteValue<int32_t>(kMagic));
    return output_stream->Flush();
}

Result<BitmapGlobalIndexFormat::BlockInfo> BitmapGlobalIndexFormat::WriteBitmapBlock(
    const RoaringBitmap64& bitmap, OutputStream* output_stream, MemoryPool* pool) {
    std::shared_ptr<Bytes> bytes = bitmap.Serialize(pool);
    PAIMON_RETURN_NOT_OK(
        ValidateValueInRange<int32_t>(bytes->size(), "serialized bitmap block size"));
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, output_stream->GetPos());
    PAIMON_RETURN_NOT_OK(WriteAll(bytes->data(), bytes->size(), output_stream));
    return BlockInfo(offset, static_cast<int32_t>(bytes->size()));
}

Result<BitmapGlobalIndexFormat::DictionaryBlockMeta> BitmapGlobalIndexFormat::WriteDictionaryBlock(
    const std::vector<DictionaryEntry>& entries, BlockCompressionFactory* compression_factory,
    OutputStream* output_stream, MemoryPool* pool) {
    if (entries.empty()) {
        return Status::Invalid("Cannot write an empty bitmap dictionary block.");
    }
    PAIMON_RETURN_NOT_OK(
        ValidateValueInRange<int32_t>(entries.size(), "bitmap dictionary block entry count"));
    PAIMON_ASSIGN_OR_RAISE(int32_t entry_count_size,
                           EstimatedVarLenIntSize(static_cast<int32_t>(entries.size())));
    int64_t estimated_size = entry_count_size;
    for (const DictionaryEntry& entry : entries) {
        PAIMON_ASSIGN_OR_RAISE(int32_t entry_size, entry.EstimatedSize());
        estimated_size += entry_size;
    }
    PAIMON_RETURN_NOT_OK(
        ValidateValueInRange<int32_t>(estimated_size, "bitmap dictionary block size"));
    MemorySliceOutput output(static_cast<int32_t>(estimated_size), pool);
    PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(static_cast<int32_t>(entries.size())));
    for (const DictionaryEntry& entry : entries) {
        const std::shared_ptr<Bytes>& key_bytes = entry.Key().GetBytes();
        PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(static_cast<int32_t>(key_bytes->size())));
        output.WriteBytes(key_bytes);
        PAIMON_RETURN_NOT_OK(output.WriteVarLenLong(entry.BitmapBlock().Offset()));
        PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(entry.BitmapBlock().Length()));
    }
    std::shared_ptr<Bytes> bytes = output.ToSlice().CopyBytes(pool);
    PAIMON_ASSIGN_OR_RAISE(BlockInfo block,
                           WriteCompressibleBlock(bytes, compression_factory, output_stream, pool));
    return DictionaryBlockMeta(entries.front().Key(), block.Offset(), block.Length());
}

Result<BitmapGlobalIndexFormat::BlockInfo> BitmapGlobalIndexFormat::WriteIndexBlock(
    const std::vector<DictionaryBlockMeta>& blocks, BlockCompressionFactory* compression_factory,
    OutputStream* output_stream, MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(int32_t estimated_size, EstimatedIndexBlockSize(blocks));
    MemorySliceOutput output(estimated_size, pool);
    PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(static_cast<int32_t>(blocks.size())));
    for (const DictionaryBlockMeta& block : blocks) {
        const std::shared_ptr<Bytes>& key_bytes = block.FirstKey().GetBytes();
        PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(static_cast<int32_t>(key_bytes->size())));
        output.WriteBytes(key_bytes);
        PAIMON_RETURN_NOT_OK(output.WriteVarLenLong(block.Offset()));
        PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(block.Length()));
    }
    std::shared_ptr<Bytes> bytes = output.ToSlice().CopyBytes(pool);
    return WriteCompressibleBlock(bytes, compression_factory, output_stream, pool);
}

Result<BitmapGlobalIndexFormat::BlockInfo> BitmapGlobalIndexFormat::WriteCompressibleBlock(
    const std::shared_ptr<Bytes>& uncompressed, BlockCompressionFactory* compression_factory,
    OutputStream* output_stream, MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(BlockEncoding encoding,
                           EncodeBlock(uncompressed, compression_factory, pool));
    PAIMON_ASSIGN_OR_RAISE(int64_t offset, output_stream->GetPos());
    PAIMON_RETURN_NOT_OK(WriteAll(encoding.bytes->data(), encoding.length, output_stream));

    uint32_t crc = CRC32C::calculate(encoding.bytes->data(), encoding.length);
    char compression_value =
        static_cast<char>(static_cast<int32_t>(encoding.compression_type) & 0xFF);
    crc = CRC32C::calculate(&compression_value, sizeof(compression_value), crc);
    BlockTrailer trailer(static_cast<int8_t>(encoding.compression_type), static_cast<int32_t>(crc));
    MemorySlice trailer_slice = trailer.WriteBlockTrailer(pool);
    PAIMON_RETURN_NOT_OK(WriteAll(trailer_slice.Data(), trailer_slice.Length(), output_stream));
    return BlockInfo(offset, encoding.length);
}

Result<BitmapGlobalIndexFormat::BlockEncoding> BitmapGlobalIndexFormat::EncodeBlock(
    const std::shared_ptr<Bytes>& uncompressed, BlockCompressionFactory* compression_factory,
    MemoryPool* pool) {
    if (uncompressed == nullptr) {
        return Status::Invalid("Uncompressed bitmap index block is null.");
    }
    PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(uncompressed->size(),
                                                       "uncompressed bitmap index block size"));
    auto uncompressed_length = static_cast<int32_t>(uncompressed->size());
    BlockEncoding result{uncompressed, uncompressed_length, BlockCompressionType::NONE};
    if (compression_factory == nullptr ||
        compression_factory->GetCompressionType() == BlockCompressionType::NONE) {
        return result;
    }

    std::shared_ptr<BlockCompressor> compressor = compression_factory->GetCompressor();
    if (compressor == nullptr) {
        return Status::Invalid("Bitmap block compression factory returned a null compressor.");
    }
    int32_t maximum_compressed_size = compressor->GetMaxCompressedSize(uncompressed_length);
    if (maximum_compressed_size < 0 ||
        maximum_compressed_size >
            std::numeric_limits<int32_t>::max() - VarLengthIntUtils::kMaxVarIntSize) {
        return Status::Invalid("Invalid maximum compressed bitmap block size.");
    }
    std::shared_ptr<Bytes> compressed =
        Bytes::AllocateBytes(maximum_compressed_size + VarLengthIntUtils::kMaxVarIntSize, pool);
    PAIMON_ASSIGN_OR_RAISE(int32_t prefix_length,
                           VarLengthIntUtils::EncodeInt(uncompressed_length, compressed->data()));
    PAIMON_ASSIGN_OR_RAISE(
        int32_t compressed_length,
        compressor->Compress(
            uncompressed->data(), uncompressed_length, compressed->data() + prefix_length,
            maximum_compressed_size + VarLengthIntUtils::kMaxVarIntSize - prefix_length));
    if (compressed_length < 0 ||
        compressed_length > std::numeric_limits<int32_t>::max() - prefix_length) {
        return Status::Invalid("Invalid compressed bitmap block size.");
    }
    int32_t encoded_length = prefix_length + compressed_length;
    if (encoded_length < uncompressed_length - (uncompressed_length / 8)) {
        result.bytes = std::move(compressed);
        result.length = encoded_length;
        result.compression_type = compression_factory->GetCompressionType();
    }
    return result;
}

Result<BitmapGlobalIndexFormat::Footer> BitmapGlobalIndexFormat::ReadFooter(
    int64_t file_size, SeekableReader* reader) {
    if (reader == nullptr) {
        return Status::Invalid("Cannot read bitmap footer without a reader.");
    }
    if (file_size < kFooterLength) {
        return Status::Invalid("Invalid bitmap global index file size.");
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes,
                           reader->Read(file_size - kFooterLength, kFooterLength));
    if (bytes == nullptr || bytes->size() != kFooterLength) {
        return Status::Invalid("Truncated bitmap global index footer.");
    }
    MemorySliceInput input(MemorySlice::Wrap(bytes));
    input.SetOrder(ByteOrder::PAIMON_BIG_ENDIAN);
    int64_t null_rows_offset = input.ReadLong();
    int32_t null_rows_length = input.ReadInt();
    BlockInfo null_rows_block(null_rows_offset, null_rows_length);
    int64_t non_null_rows_offset = input.ReadLong();
    int32_t non_null_rows_length = input.ReadInt();
    BlockInfo non_null_rows_block(non_null_rows_offset, non_null_rows_length);
    int64_t index_offset = input.ReadLong();
    int32_t index_length = input.ReadInt();
    BlockInfo index_block(index_offset, index_length);
    int32_t value_count = input.ReadInt();
    int32_t version = input.ReadInt();
    int32_t magic = input.ReadInt();
    if (magic != kMagic) {
        return Status::Invalid("File is not a bitmap global index file (bad footer magic).");
    }
    if (version != kVersion) {
        return Status::Invalid(
            fmt::format("Unsupported bitmap global index file version: {}.", version));
    }
    if (value_count < 0) {
        return Status::Invalid("Invalid bitmap value count.");
    }
    for (const BlockInfo* block : {&null_rows_block, &non_null_rows_block, &index_block}) {
        PAIMON_RETURN_NOT_OK(ValidateBlockInfo(*block));
        if (block->Offset() > file_size - kFooterLength ||
            block->Length() > file_size - kFooterLength - block->Offset()) {
            return Status::Invalid("Bitmap footer references a block outside the file payload.");
        }
    }
    return Footer(std::move(null_rows_block), std::move(non_null_rows_block),
                  std::move(index_block));
}

Result<std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta>>
BitmapGlobalIndexFormat::ReadIndexBlock(const BlockInfo& index_block, SeekableReader* reader,
                                        MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes,
                           ReadCompressibleBlock(index_block, reader, pool));
    MemorySliceInput input(MemorySlice::Wrap(bytes));
    PAIMON_ASSIGN_OR_RAISE(int32_t block_count, VarLengthIntUtils::ReadVarLenInt(&input));
    if (block_count > input.Available()) {
        return Status::Invalid("Bitmap dictionary block count exceeds the encoded block size.");
    }
    std::vector<DictionaryBlockMeta> blocks;
    blocks.reserve(block_count);
    for (int32_t i = 0; i < block_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> key_bytes, ReadKey(&input, pool));
        PAIMON_ASSIGN_OR_RAISE(int64_t offset, VarLengthIntUtils::ReadVarLenLong(&input));
        PAIMON_ASSIGN_OR_RAISE(int32_t length, VarLengthIntUtils::ReadVarLenInt(&input));
        BlockInfo block(offset, length);
        PAIMON_RETURN_NOT_OK(ValidateBlockInfo(block));
        blocks.emplace_back(SerializedKey(std::move(key_bytes)), offset, length);
    }
    if (input.Available() != 0) {
        return Status::Invalid("Bitmap dictionary block index has trailing bytes.");
    }
    return blocks;
}

Result<BitmapGlobalIndexFormat::DictionaryBlock> BitmapGlobalIndexFormat::ReadDictionaryBlock(
    const DictionaryBlockMeta& block, SeekableReader* reader, MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes,
                           ReadCompressibleBlock(block, reader, pool));
    MemorySliceInput input(MemorySlice::Wrap(bytes));
    PAIMON_ASSIGN_OR_RAISE(int32_t entry_count, VarLengthIntUtils::ReadVarLenInt(&input));
    if (entry_count > input.Available()) {
        return Status::Invalid("Bitmap dictionary entry count exceeds the encoded block size.");
    }
    std::vector<DictionaryEntry> entries;
    entries.reserve(entry_count);
    for (int32_t i = 0; i < entry_count; ++i) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> key_bytes, ReadKey(&input, pool));
        PAIMON_ASSIGN_OR_RAISE(int64_t bitmap_offset, VarLengthIntUtils::ReadVarLenLong(&input));
        PAIMON_ASSIGN_OR_RAISE(int32_t bitmap_length, VarLengthIntUtils::ReadVarLenInt(&input));
        BlockInfo bitmap_block(bitmap_offset, bitmap_length);
        PAIMON_RETURN_NOT_OK(ValidateBlockInfo(bitmap_block));
        entries.emplace_back(SerializedKey(std::move(key_bytes)), std::move(bitmap_block));
    }
    if (input.Available() != 0) {
        return Status::Invalid("Bitmap dictionary block has trailing bytes.");
    }
    return DictionaryBlock(std::move(entries));
}

Result<RoaringBitmap64> BitmapGlobalIndexFormat::ReadBitmap(const BlockInfo& block,
                                                            SeekableReader* reader) {
    if (reader == nullptr) {
        return Status::Invalid("Cannot read bitmap block without a reader.");
    }
    PAIMON_RETURN_NOT_OK(ValidateBlockInfo(block));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> bytes, reader->Read(block));
    RoaringBitmap64 bitmap;
    PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bytes->data(), bytes->size()));
    return bitmap;
}

Result<std::shared_ptr<Bytes>> BitmapGlobalIndexFormat::ReadCompressibleBlock(
    const BlockInfo& block, SeekableReader* reader, MemoryPool* pool) {
    if (reader == nullptr || pool == nullptr) {
        return Status::Invalid("Cannot read compressed bitmap block without reader and pool.");
    }
    PAIMON_RETURN_NOT_OK(ValidateBlockInfo(block));
    if (block.Length() > std::numeric_limits<int32_t>::max() - BlockTrailer::ENCODED_LENGTH) {
        return Status::Invalid("Bitmap block is too large.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<Bytes> block_and_trailer,
        reader->Read(block.Offset(), block.Length() + BlockTrailer::ENCODED_LENGTH));
    if (block_and_trailer == nullptr ||
        block_and_trailer->size() !=
            static_cast<size_t>(block.Length() + BlockTrailer::ENCODED_LENGTH)) {
        return Status::Invalid("Truncated compressed bitmap index block.");
    }

    MemorySlice all = MemorySlice::Wrap(block_and_trailer);
    MemorySlice block_slice = all.Slice(0, block.Length());
    MemorySlice trailer_slice = all.Slice(block.Length(), BlockTrailer::ENCODED_LENGTH);
    MemorySliceInput trailer_input(trailer_slice);
    std::unique_ptr<BlockTrailer> trailer = BlockTrailer::ReadBlockTrailer(&trailer_input);
    PAIMON_ASSIGN_OR_RAISE(BlockCompressionType compression_type,
                           SstFileUtils::From(trailer->CompressionType()));
    uint32_t crc = CRC32C::calculate(block_slice.Data(), block_slice.Length());
    auto compression_value = static_cast<char>(static_cast<int32_t>(compression_type) & 0xFF);
    crc = CRC32C::calculate(&compression_value, sizeof(compression_value), crc);
    if (trailer->Crc32c() != static_cast<int32_t>(crc)) {
        return Status::Invalid(
            fmt::format("Expected CRC32C({:#x}) but found CRC32C({:#x}) for bitmap index block.",
                        static_cast<uint32_t>(trailer->Crc32c()), crc));
    }
    if (compression_type == BlockCompressionType::NONE) {
        return block_slice.CopyBytes(pool);
    }

    MemorySliceInput compressed_input(block_slice);
    PAIMON_ASSIGN_OR_RAISE(int32_t uncompressed_length,
                           VarLengthIntUtils::ReadVarLenInt(&compressed_input));
    std::shared_ptr<Bytes> uncompressed = Bytes::AllocateBytes(uncompressed_length, pool);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockCompressionFactory> compression_factory,
                           BlockCompressionFactory::Create(compression_type));
    std::shared_ptr<BlockDecompressor> decompressor = compression_factory->GetDecompressor();
    if (decompressor == nullptr) {
        return Status::Invalid("Bitmap block compression factory returned a null decompressor.");
    }
    PAIMON_ASSIGN_OR_RAISE(
        int32_t actual_length,
        decompressor->Decompress(block_slice.Data() + compressed_input.Position(),
                                 compressed_input.Available(), uncompressed->data(),
                                 uncompressed_length));
    if (actual_length != uncompressed_length) {
        return Status::Invalid(
            fmt::format("Invalid bitmap block: expected uncompressed size {}, actual size {}.",
                        uncompressed_length, actual_length));
    }
    return uncompressed;
}

Result<int32_t> BitmapGlobalIndexFormat::EstimatedVarLenIntSize(int32_t value) {
    if (value < 0) {
        return Status::Invalid(fmt::format("Invalid negative var length int: {}.", value));
    }
    int32_t size = 1;
    while ((value & ~0x7F) != 0) {
        value >>= 7;
        ++size;
    }
    return size;
}

Result<int32_t> BitmapGlobalIndexFormat::EstimatedVarLenLongSize(int64_t value) {
    if (value < 0) {
        return Status::Invalid(fmt::format("Invalid negative var length long: {}.", value));
    }
    int32_t size = 1;
    while ((value & ~0x7FLL) != 0) {
        value >>= 7;
        ++size;
    }
    return size;
}

Result<int32_t> BitmapGlobalIndexFormat::EstimatedIndexBlockSize(
    const std::vector<DictionaryBlockMeta>& blocks) {
    PAIMON_RETURN_NOT_OK(
        ValidateValueInRange<int32_t>(blocks.size(), "bitmap dictionary block count"));
    PAIMON_ASSIGN_OR_RAISE(int32_t count_size,
                           EstimatedVarLenIntSize(static_cast<int32_t>(blocks.size())));
    int64_t size = count_size;
    for (const DictionaryBlockMeta& block : blocks) {
        const std::shared_ptr<Bytes>& key = block.FirstKey().GetBytes();
        if (key == nullptr) {
            return Status::Invalid("Bitmap dictionary index key is null.");
        }
        PAIMON_RETURN_NOT_OK(
            ValidateValueInRange<int32_t>(key->size(), "bitmap dictionary index key length"));
        PAIMON_ASSIGN_OR_RAISE(int32_t key_length_size,
                               EstimatedVarLenIntSize(static_cast<int32_t>(key->size())));
        PAIMON_ASSIGN_OR_RAISE(int32_t offset_size, EstimatedVarLenLongSize(block.Offset()));
        PAIMON_ASSIGN_OR_RAISE(int32_t length_size, EstimatedVarLenIntSize(block.Length()));
        size += key_length_size + key->size() + offset_size + length_size;
        PAIMON_RETURN_NOT_OK(
            ValidateValueInRange<int32_t>(size, "bitmap dictionary index block size"));
    }
    return static_cast<int32_t>(size);
}

}  // namespace paimon
