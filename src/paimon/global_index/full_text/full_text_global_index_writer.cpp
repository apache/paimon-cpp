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

#include "paimon/global_index/full_text/full_text_global_index_writer.h"

#include <optional>
#include <string_view>
#include <utility>

#include "arrow/array.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/full_text/full_text_defs.h"
#include "paimon/memory/bytes.h"

namespace paimon::full_text {

#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

namespace {

/// Context of the native output callbacks.
struct OutputContext {
    OutputStream* stream = nullptr;
    /// First error raised by the stream, reported instead of the generic native error.
    Status status = Status::OK();
};

int WriteToOutputStream(void* ctx, const uint8_t* buf, size_t len) {
    auto* output = static_cast<OutputContext*>(ctx);
    size_t written = 0;
    while (written < len) {
        Result<int64_t> result = output->stream->Write(reinterpret_cast<const char*>(buf + written),
                                                       static_cast<int64_t>(len - written));
        if (!result.ok()) {
            output->status = result.status();
            return -1;
        }
        if (result.value() <= 0) {
            output->status = Status::IOError(fmt::format(
                "short write to full-text index file: wrote {} of {} bytes", written, len));
            return -1;
        }
        written += static_cast<size_t>(result.value());
    }
    return 0;
}

int FlushOutputStream(void* ctx) {
    auto* output = static_cast<OutputContext*>(ctx);
    Status status = output->stream->Flush();
    if (!status.ok()) {
        output->status = std::move(status);
        return -1;
    }
    return 0;
}

}  // namespace

Result<std::shared_ptr<FullTextGlobalIndexWriter>> FullTextGlobalIndexWriter::Create(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    std::vector<const char*> keys;
    std::vector<const char*> values;
    keys.reserve(options.size());
    values.reserve(options.size());
    for (const auto& [key, value] : options) {
        if (key.find('\0') != std::string::npos) {
            return Status::Invalid("full-text index option keys must not contain NUL characters");
        }
        if (value.find('\0') != std::string::npos) {
            return Status::Invalid(
                fmt::format("full-text index option {}{} must not contain NUL characters",
                            kOptionKeyPrefix, key));
        }
        keys.push_back(key.c_str());
        values.push_back(value.c_str());
    }
    FtindexWriterPtr writer(paimon_ftindex_writer_open(keys.data(), values.data(), keys.size()));
    if (!writer) {
        return LastFtindexError(
            fmt::format("open full-text index writer for field {}", field_name));
    }
    return std::shared_ptr<FullTextGlobalIndexWriter>(new FullTextGlobalIndexWriter(
        field_name, arrow_type, std::move(writer), file_writer, options, pool));
}

FullTextGlobalIndexWriter::FullTextGlobalIndexWriter(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    FtindexWriterPtr writer, const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool ? pool : GetDefaultPool()),
      field_name_(field_name),
      arrow_type_(arrow_type),
      writer_(std::move(writer)),
      file_writer_(file_writer),
      options_(options),
      logger_(Logger::GetLogger("FullTextGlobalIndexWriter")) {}

Status FullTextGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array,
                                           std::vector<int64_t>&& relative_row_ids) {
    if (finished_) {
        if (arrow_array != nullptr) {
            ArrowArrayRelease(arrow_array);
        }
        return Status::Invalid("FullTextGlobalIndexWriter is already finished");
    }
    // Documents are keyed by the given relative row ids, as in Java, so only the lengths are
    // checked here.
    PAIMON_RETURN_NOT_OK(
        GlobalIndexUtils::CheckRelativeRowIds(arrow_array, relative_row_ids,
                                              /*expected_next_row_id=*/std::nullopt));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    CHECK_NOT_NULL(struct_array,
                   "invalid input array in FullTextGlobalIndexWriter, must be struct array");
    std::shared_ptr<arrow::Array> field_array = struct_array->GetFieldByName(field_name_);
    CHECK_NOT_NULL(
        field_array,
        fmt::format("invalid input array in FullTextGlobalIndexWriter, field {} not in input array",
                    field_name_));
    auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(field_array);
    CHECK_NOT_NULL(string_array,
                   fmt::format("invalid input array in FullTextGlobalIndexWriter, field array {} "
                               "is not a string array",
                               field_name_));

    std::string text;
    for (int64_t i = 0; i < string_array->length(); ++i) {
        if (!string_array->IsNull(i)) {
            std::string_view value = string_array->GetView(i);
            // The native API takes NUL-terminated strings, so an embedded NUL would silently
            // truncate the document.
            if (value.find('\0') != std::string_view::npos) {
                return Status::Invalid(
                    fmt::format("full-text index value of row {} must not contain NUL characters",
                                relative_row_ids[i]));
            }
            text.assign(value.data(), value.size());
            if (paimon_ftindex_writer_add_document(writer_.get(), relative_row_ids[i],
                                                   text.c_str()) != 0) {
                return LastFtindexError(
                    fmt::format("add row {} to full-text index", relative_row_ids[i]));
            }
        }
        row_count_++;
    }
    return Status::OK();
}

Result<std::vector<GlobalIndexIOMeta>> FullTextGlobalIndexWriter::Finish() {
    if (finished_) {
        return Status::Invalid("FullTextGlobalIndexWriter is already finished");
    }
    finished_ = true;
    // The native writer is finalized by any write attempt, so release it whatever happens.
    FtindexWriterPtr writer = std::move(writer_);
    if (row_count_ == 0) {
        return std::vector<GlobalIndexIOMeta>();
    }
    PAIMON_ASSIGN_OR_RAISE(GlobalIndexIOMeta meta, WriteIndex(writer.get()));
    return std::vector<GlobalIndexIOMeta>({meta});
}

Result<GlobalIndexIOMeta> FullTextGlobalIndexWriter::WriteIndex(
    PaimonFtindexWriterHandle* writer) const {
    PAIMON_ASSIGN_OR_RAISE(std::string file_name, file_writer_->NewFileName(kFileNamePrefix));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out,
                           file_writer_->NewOutputStream(file_name));
    // Closes the stream on every error path, keeping the original error.
    ScopeGuard close_guard([&]() {
        Status status = out->Close();
        if (!status.ok()) {
            PAIMON_LOG_WARN(logger_, "failed to close full-text index file %s: %s",
                            file_name.c_str(), status.ToString().c_str());
        }
    });
    OutputContext ctx;
    ctx.stream = out.get();
    PaimonFtindexOutputFile output{static_cast<void*>(&ctx), &WriteToOutputStream,
                                   &FlushOutputStream};
    if (paimon_ftindex_writer_write_index(writer, output) != 0) {
        return LastFtindexError(fmt::format("write full-text index {}", file_name), ctx.status);
    }
    PAIMON_RETURN_NOT_OK(out->Flush());
    close_guard.Release();
    PAIMON_RETURN_NOT_OK(out->Close());

    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_writer_->GetFileSize(file_name));
    std::string options_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(options_, &options_json));
    auto metadata = std::make_shared<Bytes>(options_json, pool_.get());
    return GlobalIndexIOMeta(file_writer_->ToPath(file_name), file_size, metadata);
}

}  // namespace paimon::full_text
