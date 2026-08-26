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
#include <map>
#include <unordered_map>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/file_index/empty/empty_file_index_reader.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/status.h"

namespace paimon {
class InputStream;
class MemoryPool;

class FileIndexFormatWriterImpl : public FileIndexFormat::Writer {
 public:
    explicit FileIndexFormatWriterImpl(const std::shared_ptr<OutputStream>& output_stream)
        : output_stream_(output_stream) {
        assert(output_stream_);
    }

    Status WriteColumnIndexes(const FileIndexFormat::ColumnIndexes& indexes) override {
        if (written_) {
            return Status::Invalid("File index column indexes have already been written");
        }

        PAIMON_RETURN_NOT_OK(WriteHead(indexes));
        // Write body.
        DataOutputStream data_output(output_stream_);
        for (const auto& [column_name, column_indexes] : indexes) {
            for (const auto& [index_type, bytes] : column_indexes) {
                if (bytes) {
                    PAIMON_RETURN_NOT_OK(data_output.WriteBytes(bytes));
                }
            }
        }
        written_ = true;
        return Status::OK();
    }

    Status Close() override {
        if (closed_) {
            return Status::OK();
        }
        closed_ = true;
        PAIMON_RETURN_NOT_OK(output_stream_->Flush());
        return output_stream_->Close();
    }

 private:
    static constexpr int32_t kRedundantLength = 0;

    static Result<int32_t> CalculateHeadLength(const FileIndexFormat::ColumnIndexes& indexes) {
        // magic(8), version(4), header length(4), and column count(4).
        int64_t head_length = 8 + 4 + 4 + 4;
        int64_t body_length = 0;
        PAIMON_RETURN_NOT_OK(
            ValidateValueInRange<int32_t>(indexes.size(), "file index column count"));
        for (const auto& [column_name, column_indexes] : indexes) {
            PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint16_t>(column_name.size(),
                                                                "file index column name length"));
            PAIMON_RETURN_NOT_OK(
                ValidateValueInRange<int32_t>(column_indexes.size(), "column index count"));
            // column name(2 + N) + index count(4)
            head_length += 2 + static_cast<int64_t>(column_name.size()) + 4;
            for (const auto& [index_type, bytes] : column_indexes) {
                PAIMON_RETURN_NOT_OK(ValidateValueInRange<uint16_t>(index_type.size(),
                                                                    "file index type name length"));
                // index type(2 + N) + body offset(4) + body length(4)
                head_length += 2 + static_cast<int64_t>(index_type.size()) + 4 + 4;
                if (bytes) {
                    PAIMON_RETURN_NOT_OK(AddChecked(bytes->size(), "index body", &body_length));
                }
            }
        }

        head_length += 4;  // The trailing redundant-length field(4).
        PAIMON_RETURN_NOT_OK(
            ValidateValueInRange<int32_t>(head_length, "file index header length"));
        int64_t container_length = head_length + body_length;
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(container_length, "file index size"));
        return static_cast<int32_t>(head_length);
    }

    Status WriteHead(const FileIndexFormat::ColumnIndexes& indexes) {
        PAIMON_ASSIGN_OR_RAISE(int32_t head_length, CalculateHeadLength(indexes));
        DataOutputStream data_output(output_stream_);
        // Write magic.
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int64_t>(FileIndexFormat::MAGIC));
        // Write version.
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(FileIndexFormat::V_1));
        // Write head length.
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(head_length));
        // Write column count.
        PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(static_cast<int32_t>(indexes.size())));

        int64_t body_offset = head_length;
        for (const auto& [column_name, column_indexes] : indexes) {
            // Write column name.
            PAIMON_RETURN_NOT_OK(data_output.WriteString(column_name));
            // Write index count for the column.
            PAIMON_RETURN_NOT_OK(
                data_output.WriteValue<int32_t>(static_cast<int32_t>(column_indexes.size())));
            for (const auto& [index_type, bytes] : column_indexes) {
                // Write index type.
                PAIMON_RETURN_NOT_OK(data_output.WriteString(index_type));
                // Write body offset and length.
                if (bytes) {
                    PAIMON_RETURN_NOT_OK(
                        data_output.WriteValue<int32_t>(static_cast<int32_t>(body_offset)));
                    PAIMON_RETURN_NOT_OK(
                        data_output.WriteValue<int32_t>(static_cast<int32_t>(bytes->size())));
                    body_offset += static_cast<int64_t>(bytes->size());
                } else {
                    PAIMON_RETURN_NOT_OK(
                        data_output.WriteValue<int32_t>(FileIndexFormat::EMPTY_INDEX_FLAG));
                    PAIMON_RETURN_NOT_OK(data_output.WriteValue<int32_t>(0));
                }
            }
        }
        // Write redundant length for future format extensions.
        return data_output.WriteValue<int32_t>(kRedundantLength);
    }

    template <typename T>
    static Status AddChecked(T value, const char* name, int64_t* total) {
        PAIMON_RETURN_NOT_OK(ValidateValueInRange<int32_t>(value, name));
        *total += static_cast<int64_t>(value);
        return ValidateValueInRange<int32_t>(*total, name);
    }

    std::shared_ptr<OutputStream> output_stream_;
    bool written_ = false;
    bool closed_ = false;
};

class FileIndexFormatReaderImpl : public FileIndexFormat::Reader {
 public:
    using HeaderType =
        std::unordered_map<std::string,
                           std::unordered_map<std::string, std::pair<int32_t, int32_t>>>;

    static Result<std::unique_ptr<FileIndexFormatReaderImpl>> Create(
        const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
        DataInputStream data_input_stream(input_stream);
        PAIMON_ASSIGN_OR_RAISE(int64_t magic, data_input_stream.ReadValue<int64_t>());
        if (magic != FileIndexFormat::MAGIC) {
            return Status::Invalid("This file is not file index file.");
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t version, data_input_stream.ReadValue<int32_t>());
        if (version != FileIndexFormat::V_1) {
            return Status::Invalid(
                fmt::format("This index file is version of {}, not in supported version list [{}]",
                            version, FileIndexFormat::V_1));
        }
        PAIMON_ASSIGN_OR_RAISE(int32_t head_length, data_input_stream.ReadValue<int32_t>());
        auto head_bytes = std::make_unique<Bytes>(head_length - 8 - 4 - 4, pool.get());
        PAIMON_RETURN_NOT_OK(data_input_stream.ReadBytes(head_bytes.get()));

        auto byte_array_input_stream =
            std::make_shared<ByteArrayInputStream>(head_bytes->data(), head_bytes->size());
        DataInputStream inner_data_input_stream(byte_array_input_stream);
        PAIMON_ASSIGN_OR_RAISE(int32_t column_size, inner_data_input_stream.ReadValue<int32_t>());

        HeaderType header;
        for (int32_t i = 0; i < column_size; i++) {
            PAIMON_ASSIGN_OR_RAISE(std::string column_name, inner_data_input_stream.ReadString());
            PAIMON_ASSIGN_OR_RAISE(int32_t index_size,
                                   inner_data_input_stream.ReadValue<int32_t>());
            auto& index_map = header[column_name];
            for (int32_t j = 0; j < index_size; j++) {
                PAIMON_ASSIGN_OR_RAISE(std::string index_type,
                                       inner_data_input_stream.ReadString());
                PAIMON_ASSIGN_OR_RAISE(int32_t offset,
                                       inner_data_input_stream.ReadValue<int32_t>());
                PAIMON_ASSIGN_OR_RAISE(int32_t length,
                                       inner_data_input_stream.ReadValue<int32_t>());
                index_map[index_type] = std::make_pair(offset, length);
            }
        }
        return std::unique_ptr<FileIndexFormatReaderImpl>(
            new FileIndexFormatReaderImpl(input_stream, std::move(header), pool));
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
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexReader> file_index_reader,
                                       GetFileIndexReader(arrow::schema({column_field}), index_type,
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
    FileIndexFormatReaderImpl(const std::shared_ptr<InputStream>& input_stream, HeaderType&& header,
                              const std::shared_ptr<MemoryPool>& pool)
        : input_stream_(input_stream), pool_(pool), header_(std::move(header)) {
        assert(input_stream_);
    }

    Result<std::shared_ptr<FileIndexReader>> GetFileIndexReader(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const std::string& index_type,
        const std::pair<int32_t, int32_t>& offset_and_length) const {
        if (offset_and_length.first == FileIndexFormat::EMPTY_INDEX_FLAG) {
            return std::make_shared<EmptyFileIndexReader>();
        }
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileIndexer> file_indexer,
                               FileIndexerFactory::Get(index_type, /*options=*/{}));
        // assert(file_indexer);
        if (!file_indexer) {
            return std::shared_ptr<FileIndexReader>();
        }
        ArrowSchema c_arrow_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_arrow_schema));
        return file_indexer->CreateReader(&c_arrow_schema, offset_and_length.first,
                                          offset_and_length.second, input_stream_, pool_);
    }

 private:
    std::shared_ptr<InputStream> input_stream_;
    std::shared_ptr<MemoryPool> pool_;
    // get header and cache it.
    // [column_name : [index_type : {offset, length}]]
    HeaderType header_;
};

const int64_t FileIndexFormat::MAGIC = 1493475289347502LL;
const int32_t FileIndexFormat::EMPTY_INDEX_FLAG = -1;
const int32_t FileIndexFormat::V_1 = 1;

Result<std::unique_ptr<FileIndexFormat::Reader>> FileIndexFormat::CreateReader(
    const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool) {
    return FileIndexFormatReaderImpl::Create(input_stream, pool);
}

Result<std::unique_ptr<FileIndexFormat::Writer>> FileIndexFormat::CreateWriter(
    const std::shared_ptr<OutputStream>& output_stream, const std::shared_ptr<MemoryPool>& pool) {
    if (!output_stream) {
        return Status::Invalid("File index output stream cannot be null");
    }
    if (!pool) {
        return Status::Invalid("File index memory pool cannot be null");
    }
    return std::make_unique<FileIndexFormatWriterImpl>(output_stream);
}
}  // namespace paimon
