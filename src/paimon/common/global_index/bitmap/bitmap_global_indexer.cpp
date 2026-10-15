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

#include "paimon/common/global_index/bitmap/bitmap_global_indexer.h"

#include <utility>

#include "arrow/c/bridge.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index_options.h"
#include "paimon/common/global_index/bitmap/bitmap_global_index_writer.h"
#include "paimon/common/global_index/bitmap/lazy_filtered_bitmap_reader.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/core/options/compress_options.h"
#include "paimon/executor.h"

namespace paimon {

Result<std::unique_ptr<BitmapGlobalIndexer>> BitmapGlobalIndexer::Create(
    const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(std::string fallback_scan_max_size_string,
                           OptionsUtils::GetValueFromMap<std::string>(
                               options, BitmapGlobalIndexOptions::kBitmapIndexFallbackScanMaxSize,
                               BitmapGlobalIndexOptions::kDefaultBitmapIndexFallbackScanMaxSize));
    PAIMON_ASSIGN_OR_RAISE(int64_t fallback_scan_max_size,
                           MemorySize::ParseBytes(fallback_scan_max_size_string));
    return std::unique_ptr<BitmapGlobalIndexer>(
        new BitmapGlobalIndexer(fallback_scan_max_size, options));
}

Result<std::optional<std::vector<std::string>>> BitmapGlobalIndexer::GetExtraFieldNames() const {
    return std::optional<std::vector<std::string>>(std::nullopt);
}

Result<std::shared_ptr<GlobalIndexWriter>> BitmapGlobalIndexer::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    if (arrow_type == nullptr || arrow_type->id() != arrow::Type::STRUCT) {
        return Status::Invalid(
            "arrow schema must be struct type when create BitmapGlobalIndexWriter");
    }
    std::shared_ptr<arrow::StructType> struct_type =
        checked_pointer_cast<arrow::StructType>(arrow_type);

    PAIMON_ASSIGN_OR_RAISE(std::string dictionary_block_size_string,
                           OptionsUtils::GetValueFromMap<std::string>(
                               options_, BitmapGlobalIndexOptions::kBitmapIndexDictionaryBlockSize,
                               BitmapGlobalIndexOptions::kDefaultBitmapIndexDictionaryBlockSize));
    PAIMON_ASSIGN_OR_RAISE(int64_t dictionary_block_size,
                           MemorySize::ParseBytes(dictionary_block_size_string));
    if (dictionary_block_size <= 0) {
        return Status::Invalid("Bitmap dictionary block size must be greater than 0.");
    }
    PAIMON_RETURN_NOT_OK(
        ValidateValueInRange<int32_t>(dictionary_block_size, "bitmap dictionary block size"));

    PAIMON_ASSIGN_OR_RAISE(std::string compression,
                           OptionsUtils::GetValueFromMap<std::string>(
                               options_, BitmapGlobalIndexOptions::kBitmapIndexCompression,
                               BitmapGlobalIndexOptions::kDefaultBitmapIndexCompression));
    PAIMON_ASSIGN_OR_RAISE(int32_t compression_level,
                           OptionsUtils::GetValueFromMap<int32_t>(
                               options_, BitmapGlobalIndexOptions::kBitmapIndexCompressionLevel,
                               BitmapGlobalIndexOptions::kDefaultBitmapIndexCompressionLevel));
    CompressOptions compress_options{compression, compression_level};
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BlockCompressionFactory> compression_factory,
                           BlockCompressionFactory::Create(compress_options));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<BitmapGlobalIndexWriter> writer,
        BitmapGlobalIndexWriter::Create(field_name, struct_type, file_writer,
                                        static_cast<int32_t>(dictionary_block_size),
                                        compression_factory, pool));
    return std::shared_ptr<GlobalIndexWriter>(std::move(writer));
}

Result<std::shared_ptr<GlobalIndexReader>> BitmapGlobalIndexer::CreateReader(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(arrow_schema));
    if (schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BitmapIndexReader, supposed to have single field.");
    }

    std::shared_ptr<Executor> executor;
    if (files.size() > 1) {
        executor = CreateDefaultExecutor();
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<LazyFilteredBitmapReader> reader,
        LazyFilteredBitmapReader::Create(file_reader, files, schema->field(0)->type(),
                                         fallback_scan_max_size_, pool, executor));
    return std::shared_ptr<GlobalIndexReader>(std::move(reader));
}

}  // namespace paimon
