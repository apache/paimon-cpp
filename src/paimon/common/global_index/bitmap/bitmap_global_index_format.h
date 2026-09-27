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
#include <memory>
#include <utility>
#include <vector>

#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

class KeySerializer;
class Literal;

/// Shared file format helpers for bitmap global index.
///
/// Bitmap blocks for non-null keys are written as keys arrive. Dictionary blocks are flushed
/// periodically, so these two kinds of blocks may be interleaved. The blocks at the end of the
/// file have a fixed order:
///
///    +--------------------------------------------------+
///    | Bitmap Block for key 1 (RoaringBitmap64)         |
///    +--------------------------------------------------+
///    | ... Bitmap / Dictionary Blocks may interleave ...|
///    +--------------------------------------------------+
///    | Dictionary Block N                               |
///    +--------------------------------------------------+
///    | Null Rows Bitmap Block                           |
///    +--------------------------------------------------+
///    | Non-null Rows Bitmap Block                       |
///    +--------------------------------------------------+
///    | Dictionary Block Index                           |
///    +--------------------------------------------------+
///    | Footer (48 bytes)                                |
///    +--------------------------------------------------+
///
/// Each dictionary entry maps one serialized key to its bitmap block:
///
///    Dictionary Block:
///    [entry count]
///    [key length | key bytes | bitmap offset | bitmap length] ...
///
/// The dictionary block index maps the first key of each dictionary block to that block:
///
///    Dictionary Block Index:
///    [block count]
///    [first key length | first key bytes | dictionary offset | dictionary length] ...
///
/// The footer stores the offset and length of the null rows bitmap, non-null rows bitmap and
/// dictionary block index, followed by value count, version and magic. Dictionary blocks and the
/// dictionary block index use the compressible-block encoding and are followed by a block trailer;
/// bitmap blocks contain the serialized RoaringBitmap64 bytes directly.
class BitmapGlobalIndexFormat {
 public:
    BitmapGlobalIndexFormat() = delete;
    ~BitmapGlobalIndexFormat() = delete;

    class BlockInfo;

    /// Serialized bitmap dictionary key.
    class SerializedKey {
     public:
        explicit SerializedKey(std::shared_ptr<Bytes> bytes) : bytes_(std::move(bytes)) {}

        static Result<SerializedKey> FromLiteral(const std::shared_ptr<KeySerializer>& serializer,
                                                 const Literal& literal);

        const std::shared_ptr<Bytes>& GetBytes() const {
            return bytes_;
        }

        int32_t CompareTo(const SerializedKey& other) const;

        bool operator==(const SerializedKey& other) const {
            return CompareTo(other) == 0;
        }

     private:
        std::shared_ptr<Bytes> bytes_;
    };

    /// Minimal random-access reader used by bitmap index format decoders.
    class SeekableReader {
     public:
        virtual ~SeekableReader() = default;

        virtual Result<std::shared_ptr<Bytes>> Read(int64_t offset, int32_t length) = 0;

        Result<std::shared_ptr<Bytes>> Read(const BlockInfo& block);
    };

    /// Encoded block location within a bitmap index file.
    class BlockInfo {
     public:
        BlockInfo(int64_t offset, int32_t length) : offset_(offset), length_(length) {}

        int64_t Offset() const {
            return offset_;
        }

        int32_t Length() const {
            return length_;
        }

     private:
        int64_t offset_;
        int32_t length_;
    };

    /// Dictionary block metadata read from the block index.
    class DictionaryBlockMeta : public BlockInfo {
     public:
        DictionaryBlockMeta(SerializedKey first_key, int64_t offset, int32_t length)
            : BlockInfo(offset, length), first_key_(std::move(first_key)) {}

        const SerializedKey& FirstKey() const {
            return first_key_;
        }

     private:
        SerializedKey first_key_;
    };

    /// One encoded dictionary key and its bitmap block.
    class DictionaryEntry {
     public:
        DictionaryEntry(SerializedKey key, BlockInfo bitmap_block)
            : key_(std::move(key)), bitmap_block_(std::move(bitmap_block)) {}

        const SerializedKey& Key() const {
            return key_;
        }

        const BlockInfo& BitmapBlock() const {
            return bitmap_block_;
        }

        Result<int32_t> EstimatedSize() const;

     private:
        SerializedKey key_;
        BlockInfo bitmap_block_;
    };

    /// Decoded dictionary block.
    class DictionaryBlock {
     public:
        explicit DictionaryBlock(std::vector<DictionaryEntry> entries)
            : entries_(std::move(entries)) {}

        const std::vector<DictionaryEntry>& Entries() const {
            return entries_;
        }

     private:
        std::vector<DictionaryEntry> entries_;
    };

    /// Bitmap index footer block references.
    class Footer {
     public:
        Footer(BlockInfo null_rows_block, BlockInfo non_null_rows_block, BlockInfo index_block)
            : null_rows_block_(std::move(null_rows_block)),
              non_null_rows_block_(std::move(non_null_rows_block)),
              index_block_(std::move(index_block)) {}

        const BlockInfo& NullRowsBlock() const {
            return null_rows_block_;
        }

        const BlockInfo& NonNullRowsBlock() const {
            return non_null_rows_block_;
        }

        const BlockInfo& IndexBlock() const {
            return index_block_;
        }

     private:
        BlockInfo null_rows_block_;
        BlockInfo non_null_rows_block_;
        BlockInfo index_block_;
    };

    /// Streaming writer for encoded bitmap dictionary entries.
    class StreamingWriter {
     public:
        static Result<std::unique_ptr<StreamingWriter>> Create(
            const std::shared_ptr<OutputStream>& output_stream, int32_t dictionary_block_size,
            const std::shared_ptr<BlockCompressionFactory>& compression_factory,
            const std::shared_ptr<MemoryPool>& pool);

        Status Write(SerializedKey key, const RoaringBitmap64& bitmap);

        Status Finish(const RoaringBitmap64& null_rows, const RoaringBitmap64& non_null_rows);

     private:
        StreamingWriter(const std::shared_ptr<OutputStream>& output_stream,
                        int32_t dictionary_block_size,
                        const std::shared_ptr<BlockCompressionFactory>& compression_factory,
                        const std::shared_ptr<MemoryPool>& pool)
            : output_stream_(output_stream),
              dictionary_block_size_(dictionary_block_size),
              compression_factory_(compression_factory),
              pool_(pool) {}

        Status FlushDictionaryBlock();
        Result<int32_t> EstimatedDictionaryBlockSizeAfter(const DictionaryEntry& entry) const;

        std::shared_ptr<OutputStream> output_stream_;
        int32_t dictionary_block_size_;
        std::shared_ptr<BlockCompressionFactory> compression_factory_;
        std::shared_ptr<MemoryPool> pool_;
        std::vector<DictionaryBlockMeta> dictionary_block_metas_;
        std::vector<DictionaryEntry> current_dictionary_entries_;
        int32_t current_dictionary_entries_size_ = 0;
        int32_t value_count_ = 0;
        bool finished_ = false;
    };

    static Result<Footer> ReadFooter(int64_t file_size, SeekableReader* reader);

    static Result<std::vector<DictionaryBlockMeta>> ReadIndexBlock(const BlockInfo& index_block,
                                                                   SeekableReader* reader,
                                                                   MemoryPool* pool);

    static Result<DictionaryBlock> ReadDictionaryBlock(const DictionaryBlockMeta& block,
                                                       SeekableReader* reader, MemoryPool* pool);

    static Result<RoaringBitmap64> ReadBitmap(const BlockInfo& block, SeekableReader* reader);

    static constexpr int32_t kMagic = 0x42474958;
    static constexpr int32_t kVersion = 1;
    static constexpr int32_t kFooterLength = 48;

 private:
    friend class StreamingWriter;

    struct BlockEncoding {
        std::shared_ptr<Bytes> bytes;
        int32_t length;
        BlockCompressionType compression_type;
    };

    static Status WriteFooter(const BlockInfo& null_rows_block,
                              const BlockInfo& non_null_rows_block, const BlockInfo& index_block,
                              int32_t value_count, OutputStream* output_stream);

    static Result<BlockInfo> WriteBitmapBlock(const RoaringBitmap64& bitmap,
                                              OutputStream* output_stream, MemoryPool* pool);

    static Result<DictionaryBlockMeta> WriteDictionaryBlock(
        const std::vector<DictionaryEntry>& entries, BlockCompressionFactory* compression_factory,
        OutputStream* output_stream, MemoryPool* pool);

    static Result<BlockInfo> WriteIndexBlock(const std::vector<DictionaryBlockMeta>& blocks,
                                             BlockCompressionFactory* compression_factory,
                                             OutputStream* output_stream, MemoryPool* pool);

    static Result<BlockInfo> WriteCompressibleBlock(const std::shared_ptr<Bytes>& uncompressed,
                                                    BlockCompressionFactory* compression_factory,
                                                    OutputStream* output_stream, MemoryPool* pool);

    static Result<BlockEncoding> EncodeBlock(const std::shared_ptr<Bytes>& uncompressed,
                                             BlockCompressionFactory* compression_factory,
                                             MemoryPool* pool);

    static Result<std::shared_ptr<Bytes>> ReadCompressibleBlock(const BlockInfo& block,
                                                                SeekableReader* reader,
                                                                MemoryPool* pool);

    static Result<int32_t> EstimatedVarLenIntSize(int32_t value);
    static Result<int32_t> EstimatedVarLenLongSize(int64_t value);
    static Result<int32_t> EstimatedIndexBlockSize(const std::vector<DictionaryBlockMeta>& blocks);
};

}  // namespace paimon
