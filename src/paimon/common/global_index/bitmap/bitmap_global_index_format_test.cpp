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

#include <cstring>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/io/byte_array_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

class BytesSeekableReader : public BitmapGlobalIndexFormat::SeekableReader {
 public:
    BytesSeekableReader(std::shared_ptr<Bytes> bytes, std::shared_ptr<MemoryPool> pool)
        : bytes_(std::move(bytes)), pool_(std::move(pool)) {}

    Result<std::shared_ptr<Bytes>> Read(int64_t offset, int32_t length) override {
        if (offset < 0 || length < 0 || offset > static_cast<int64_t>(bytes_->size()) ||
            length > static_cast<int64_t>(bytes_->size()) - offset) {
            return Status::Invalid("Read exceeds in-memory bitmap index data.");
        }
        std::shared_ptr<Bytes> result = Bytes::AllocateBytes(length, pool_.get());
        std::memcpy(result->data(), bytes_->data() + offset, length);
        return result;
    }

 private:
    std::shared_ptr<Bytes> bytes_;
    std::shared_ptr<MemoryPool> pool_;
};

BitmapGlobalIndexFormat::SerializedKey SerializedString(const std::string& value,
                                                        MemoryPool* pool) {
    return BitmapGlobalIndexFormat::SerializedKey(Bytes::AllocateBytes(value, pool));
}

std::shared_ptr<ByteArrayOutputStream> CreateOutput(const std::shared_ptr<MemoryPool>& pool) {
    std::unique_ptr<MemorySegmentOutputStream> segmented =
        std::make_unique<MemorySegmentOutputStream>(/*segment_size=*/16, pool);
    return std::make_shared<ByteArrayOutputStream>(std::move(segmented));
}

}  // namespace

TEST(BitmapGlobalIndexFormatTest, RoundTripMultipleDictionaryBlocks) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<ByteArrayOutputStream> output = CreateOutput(pool);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlockCompressionFactory> compression_factory,
                         BlockCompressionFactory::Create(BlockCompressionType::NONE));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BitmapGlobalIndexFormat::StreamingWriter> writer,
                         BitmapGlobalIndexFormat::StreamingWriter::Create(
                             output, /*dictionary_block_size=*/1, compression_factory, pool));

    ASSERT_OK(writer->Write(SerializedString("apple", pool.get()), RoaringBitmap64::From({0, 2})));
    ASSERT_OK(writer->Write(SerializedString("banana", pool.get()), RoaringBitmap64::From({3})));
    ASSERT_OK(writer->Finish(RoaringBitmap64::From({1}), RoaringBitmap64::From({0, 2, 3})));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, output->Finish(pool.get()));

    BytesSeekableReader reader(bytes, pool);
    ASSERT_OK_AND_ASSIGN(BitmapGlobalIndexFormat::Footer footer,
                         BitmapGlobalIndexFormat::ReadFooter(bytes->size(), &reader));
    ASSERT_OK_AND_ASSIGN(
        std::vector<BitmapGlobalIndexFormat::DictionaryBlockMeta> block_metas,
        BitmapGlobalIndexFormat::ReadIndexBlock(footer.IndexBlock(), &reader, pool.get()));
    ASSERT_EQ(2, block_metas.size());

    ASSERT_OK_AND_ASSIGN(
        BitmapGlobalIndexFormat::DictionaryBlock first_block,
        BitmapGlobalIndexFormat::ReadDictionaryBlock(block_metas.front(), &reader, pool.get()));
    ASSERT_EQ(1, first_block.Entries().size());
    ASSERT_EQ("apple", std::string(first_block.Entries()[0].Key().GetBytes()->data(),
                                   first_block.Entries()[0].Key().GetBytes()->size()));
    ASSERT_OK_AND_ASSIGN(
        RoaringBitmap64 first_bitmap,
        BitmapGlobalIndexFormat::ReadBitmap(first_block.Entries()[0].BitmapBlock(), &reader));
    ASSERT_EQ(RoaringBitmap64::From({0, 2}), first_bitmap);

    ASSERT_OK_AND_ASSIGN(
        BitmapGlobalIndexFormat::DictionaryBlock second_block,
        BitmapGlobalIndexFormat::ReadDictionaryBlock(block_metas.back(), &reader, pool.get()));
    ASSERT_EQ(1, second_block.Entries().size());
    ASSERT_EQ("banana", std::string(second_block.Entries()[0].Key().GetBytes()->data(),
                                    second_block.Entries()[0].Key().GetBytes()->size()));
    ASSERT_OK_AND_ASSIGN(
        RoaringBitmap64 second_bitmap,
        BitmapGlobalIndexFormat::ReadBitmap(second_block.Entries()[0].BitmapBlock(), &reader));
    ASSERT_EQ(RoaringBitmap64::From({3}), second_bitmap);

    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 null_rows,
                         BitmapGlobalIndexFormat::ReadBitmap(footer.NullRowsBlock(), &reader));
    ASSERT_EQ(RoaringBitmap64::From({1}), null_rows);
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 non_null_rows,
                         BitmapGlobalIndexFormat::ReadBitmap(footer.NonNullRowsBlock(), &reader));
    ASSERT_EQ(RoaringBitmap64::From({0, 2, 3}), non_null_rows);
}

TEST(BitmapGlobalIndexFormatTest, RoundTripEmptyNullBitmap) {
    std::shared_ptr<MemoryPool> pool = GetDefaultPool();
    std::shared_ptr<ByteArrayOutputStream> output = CreateOutput(pool);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<BlockCompressionFactory> compression_factory,
                         BlockCompressionFactory::Create(BlockCompressionType::NONE));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<BitmapGlobalIndexFormat::StreamingWriter> writer,
                         BitmapGlobalIndexFormat::StreamingWriter::Create(
                             output, /*dictionary_block_size=*/4096, compression_factory, pool));

    ASSERT_OK(writer->Write(SerializedString("apple", pool.get()), RoaringBitmap64::From({0})));
    ASSERT_OK(writer->Finish(RoaringBitmap64(), RoaringBitmap64::From({0})));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Bytes> bytes, output->Finish(pool.get()));

    BytesSeekableReader reader(bytes, pool);
    ASSERT_OK_AND_ASSIGN(BitmapGlobalIndexFormat::Footer footer,
                         BitmapGlobalIndexFormat::ReadFooter(bytes->size(), &reader));
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 null_rows,
                         BitmapGlobalIndexFormat::ReadBitmap(footer.NullRowsBlock(), &reader));
    ASSERT_TRUE(null_rows.IsEmpty());
    ASSERT_OK_AND_ASSIGN(RoaringBitmap64 non_null_rows,
                         BitmapGlobalIndexFormat::ReadBitmap(footer.NonNullRowsBlock(), &reader));
    ASSERT_EQ(RoaringBitmap64::From({0}), non_null_rows);
}

}  // namespace paimon::test
