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
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index_format.h"
#include "paimon/global_index/global_index_writer.h"
#include "paimon/global_index/io/global_index_file_writer.h"
#include "paimon/predicate/literal.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

class KeySerializer;

/// Streaming global index writer for the Java-compatible bitmap format.
///
/// Non-null keys must be written in monotonically increasing order so completed bitmaps can be
/// streamed to the output file instead of retained until Finish().
class BitmapGlobalIndexWriter : public GlobalIndexWriter {
 public:
    static Result<std::shared_ptr<BitmapGlobalIndexWriter>> Create(
        const std::string& field_name, const std::shared_ptr<arrow::StructType>& arrow_type,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer, int32_t dictionary_block_size,
        const std::shared_ptr<BlockCompressionFactory>& compression_factory,
        const std::shared_ptr<MemoryPool>& pool);

    ~BitmapGlobalIndexWriter() override = default;

    Status AddBatch(::ArrowArray* arrow_array, std::vector<int64_t>&& relative_row_ids) override;

    Result<std::vector<GlobalIndexIOMeta>> Finish() override;

 private:
    BitmapGlobalIndexWriter(std::string field_name, std::shared_ptr<arrow::DataType> arrow_type,
                            std::shared_ptr<KeySerializer> key_serializer,
                            std::shared_ptr<GlobalIndexFileWriter> file_writer,
                            int32_t dictionary_block_size,
                            std::shared_ptr<BlockCompressionFactory> compression_factory,
                            std::shared_ptr<MemoryPool> pool)
        : field_name_(std::move(field_name)),
          arrow_type_(std::move(arrow_type)),
          key_serializer_(std::move(key_serializer)),
          file_writer_(std::move(file_writer)),
          dictionary_block_size_(dictionary_block_size),
          compression_factory_(std::move(compression_factory)),
          pool_(std::move(pool)) {}

    Status FlushCurrentBitmap();

    Result<BitmapGlobalIndexFormat::StreamingWriter*> GetOrCreateStreamingWriter();

    std::string field_name_;
    std::shared_ptr<arrow::DataType> arrow_type_;
    std::shared_ptr<KeySerializer> key_serializer_;
    std::shared_ptr<GlobalIndexFileWriter> file_writer_;
    int32_t dictionary_block_size_;
    std::shared_ptr<BlockCompressionFactory> compression_factory_;
    std::shared_ptr<MemoryPool> pool_;

    std::string file_name_;
    std::shared_ptr<OutputStream> output_stream_;
    std::unique_ptr<BitmapGlobalIndexFormat::StreamingWriter> streaming_writer_;
    int64_t row_count_ = 0;
    std::optional<Literal> first_key_;
    std::optional<Literal> last_key_;
    RoaringBitmap64 current_bitmap_;
    RoaringBitmap64 null_rows_;
    RoaringBitmap64 non_null_rows_;
    bool finished_ = false;
};

}  // namespace paimon
