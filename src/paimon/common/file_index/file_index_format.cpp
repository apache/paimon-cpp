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

#include "paimon/file_index/file_index_format.h"

#include <cassert>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/file_index/empty/empty_file_index_reader.h"
#include "paimon/common/file_index/file_index_format_utils.h"
#include "paimon/common/file_index/file_index_format_v1.h"
#include "paimon/common/file_index/file_index_format_v2.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/file_index_options.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
class InputStream;
class MemoryPool;

class FileIndexFormatReaderImpl : public FileIndexFormat::Reader {
 public:
    static Result<std::unique_ptr<FileIndexFormatReaderImpl>> Create(
        const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool,
        const std::map<std::string, std::string>& options) {
        PAIMON_ASSIGN_OR_RAISE(FileIndexOptions file_index_options,
                               FileIndexOptions::FromMap(options));
        DataInputStream data_input_stream(input_stream);
        PAIMON_ASSIGN_OR_RAISE(int64_t magic, data_input_stream.ReadValue<int64_t>());
        if (magic != FileIndexFormat::MAGIC) {
            return Status::Invalid("This file is not file index file.");
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t version, data_input_stream.ReadValue<int32_t>());
        if (version != FileIndexFormat::V_1 && version != FileIndexFormat::V_2) {
            return Status::Invalid(fmt::format("Unsupported file index version {}", version));
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t file_length, input_stream->Length());
        PAIMON_ASSIGN_OR_RAISE(
            FileIndexEntries header,
            version == FileIndexFormat::V_1
                ? FileIndexFormatV1::ReadIndexEntries(input_stream, pool, file_length)
                : FileIndexFormatV2::ReadIndexEntries(input_stream, pool, file_length));
        return std::unique_ptr<FileIndexFormatReaderImpl>(new FileIndexFormatReaderImpl(
            input_stream, std::move(header), pool, std::move(file_index_options)));
    }

    Result<std::vector<std::shared_ptr<FileIndexReader>>> ReadColumnIndex(
        const std::string& column_name, ::ArrowSchema* c_arrow_schema) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                          arrow::ImportSchema(c_arrow_schema));
        auto column_field = arrow_schema->GetFieldByName(column_name);
        if (!column_field) {
            return Status::Invalid(fmt::format("cannot find column {} in schema", column_name));
        }
        std::vector<std::shared_ptr<FileIndexReader>> res;
        auto index_iter = header_.find(column_name);
        if (index_iter != header_.end()) {
            const auto& index_map = index_iter->second;
            for (const auto& [index_type, offset_and_length] : index_map) {
                PAIMON_ASSIGN_OR_RAISE(
                    std::shared_ptr<FileIndexReader> file_index_reader,
                    GetFileIndexReader(column_name, arrow::schema({column_field}), index_type,
                                       offset_and_length));
                if (file_index_reader) {
                    // skip the index not registered
                    res.push_back(std::move(file_index_reader));
                }
            }
        }
        return res;
    }

 private:
    FileIndexFormatReaderImpl(const std::shared_ptr<InputStream>& input_stream,
                              FileIndexEntries&& header, const std::shared_ptr<MemoryPool>& pool,
                              FileIndexOptions&& options)
        : input_stream_(input_stream),
          pool_(pool),
          header_(std::move(header)),
          options_(std::move(options)) {
        assert(input_stream_);
    }

    Result<std::shared_ptr<FileIndexReader>> GetFileIndexReader(
        const std::string& column_name, const std::shared_ptr<arrow::Schema>& arrow_schema,
        const std::string& index_type, const std::pair<int64_t, int64_t>& offset_and_length) const {
        if (offset_and_length.first == FileIndexFormat::EMPTY_INDEX_FLAG) {
            return std::make_shared<EmptyFileIndexReader>();
        }
        const std::map<std::string, std::string>& indexer_options =
            options_.GetIndexerOptions(column_name, index_type);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexer> file_indexer,
                               FileIndexerFactory::Get(index_type, indexer_options));
        // assert(file_indexer);
        if (!file_indexer) {
            return std::shared_ptr<FileIndexReader>();
        }
        ArrowSchema c_arrow_schema;
        ArrowSchemaMarkReleased(&c_arrow_schema);
        ScopeGuard schema_guard([&c_arrow_schema]() { ArrowSchemaRelease(&c_arrow_schema); });
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
        return file_indexer->CreateReader(&c_arrow_schema, offset_and_length.first,
                                          offset_and_length.second, input_stream_, pool_);
    }

 private:
    std::shared_ptr<InputStream> input_stream_;
    std::shared_ptr<MemoryPool> pool_;
    // get header and cache it.
    // [column_name : [index_type : {offset, length}]]
    FileIndexEntries header_;
    FileIndexOptions options_;
};

const int64_t FileIndexFormat::MAGIC = 1493475289347502LL;
const int32_t FileIndexFormat::EMPTY_INDEX_FLAG = -1;
const int32_t FileIndexFormat::V_1 = 1;
const int32_t FileIndexFormat::V_2 = 2;

Result<std::unique_ptr<FileIndexFormat::Reader>> FileIndexFormat::CreateReader(
    const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool,
    const std::map<std::string, std::string>& options) {
    return FileIndexFormatReaderImpl::Create(input_stream, pool, options);
}

Result<std::unique_ptr<FileIndexFormat::Writer>> FileIndexFormat::CreateWriter(
    const std::shared_ptr<OutputStream>& output_stream, const std::shared_ptr<MemoryPool>& pool,
    int32_t version) {
    if (!output_stream) {
        return Status::Invalid("File index output stream cannot be null");
    }
    if (!pool) {
        return Status::Invalid("File index memory pool cannot be null");
    }
    if (version == V_1) {
        return std::make_unique<FileIndexFormatV1::Writer>(output_stream, pool);
    }
    if (version == V_2) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Writer> writer,
                               FileIndexFormatV2::Writer::Create(output_stream));
        return writer;
    }
    return Status::Invalid(fmt::format("Unsupported file index version {}", version));
}
}  // namespace paimon
