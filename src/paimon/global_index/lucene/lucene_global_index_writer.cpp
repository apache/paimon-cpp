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

#include "paimon/global_index/lucene/lucene_global_index_writer.h"

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "lucene++/FileUtils.h"
#include "lucene++/NoLockFactory.h"
#include "paimon/common/global_index/global_index_utils.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/uuid.h"
#include "paimon/global_index/lucene/jieba_analyzer.h"
#include "paimon/global_index/lucene/lucene_defs.h"
#include "paimon/global_index/lucene/lucene_utils.h"
namespace paimon::lucene {
#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

LuceneGlobalIndexWriter::LuceneWriteContext::LuceneWriteContext(
    const std::string& output_path, const std::string& tmp_index_path,
    const Lucene::DirectoryPtr& lucene_dir, const Lucene::IndexWriterPtr& index_writer,
    const Lucene::DocumentPtr& doc, const Lucene::FieldPtr& field)
    : output_path(output_path),
      tmp_index_path(tmp_index_path),
      lucene_dir(lucene_dir),
      index_writer(index_writer),
      doc(doc),
      field(field) {}

Result<std::shared_ptr<LuceneGlobalIndexWriter>> LuceneGlobalIndexWriter::Create(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool) {
    try {
        PAIMON_ASSIGN_OR_RAISE(std::string output_path, file_writer->NewPath(kIdentifier));
        std::string tmp_path;
        Lucene::DirectoryPtr lucene_dir;
        if (StringUtils::StartsWith(output_path, "memory://")) {
            lucene_dir = Lucene::newLucene<Lucene::RAMDirectory>();
        } else {
            PAIMON_ASSIGN_OR_RAISE(std::string tmp_dir, OptionsUtils::GetValueFromMap<std::string>(
                                                            options, kLuceneWriteTmpDir));
            std::string uuid;
            if (!UUID::Generate(&uuid)) {
                return Status::Invalid("generate uuid for lucene tmp path failed.");
            }
            tmp_path = PathUtil::JoinPath(tmp_dir, "paimon-lucene-" + uuid);
            // FSDirectory mutates the lock prefix, so each writer needs its own lock factory.
            lucene_dir = Lucene::FSDirectory::open(LuceneUtils::StringToWstring(tmp_path),
                                                   Lucene::newLucene<Lucene::NoLockFactory>());
        }
        // TODO(xinyu.lxy): support other tokenizer
        PAIMON_ASSIGN_OR_RAISE(
            bool omit_term_freq_and_positions,
            OptionsUtils::GetValueFromMap(options, kLuceneWriteOmitTermFreqAndPositions, false));
        PAIMON_ASSIGN_OR_RAISE(
            std::string tokenize_mode,
            OptionsUtils::GetValueFromMap(options, kJiebaTokenizeMode,
                                          std::string(kDefaultJiebaTokenizeMode)));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<const cppjieba::Jieba> jieba,
                               LuceneUtils::GetJieba());
        JiebaTokenizerContext jieba_context(tokenize_mode,
                                            /*with_position=*/!omit_term_freq_and_positions, jieba,
                                            pool);
        auto analyzer = Lucene::newLucene<JiebaAnalyzer>(jieba_context);
        Lucene::IndexWriterPtr writer = Lucene::newLucene<Lucene::IndexWriter>(
            lucene_dir, analyzer,
            /*create=*/true, Lucene::IndexWriter::MaxFieldLengthLIMITED);

        // prepare field and document
        Lucene::DocumentPtr doc = Lucene::newLucene<Lucene::Document>();
        auto field = Lucene::newLucene<Lucene::Field>(LuceneUtils::StringToWstring(field_name),
                                                      kEmptyWstring, Lucene::Field::STORE_NO,
                                                      Lucene::Field::INDEX_ANALYZED_NO_NORMS);
        field->setOmitTermFreqAndPositions(omit_term_freq_and_positions);
        doc->add(field);
        return std::shared_ptr<LuceneGlobalIndexWriter>(new LuceneGlobalIndexWriter(
            field_name, arrow_type,
            LuceneWriteContext(output_path, tmp_path, lucene_dir, writer, doc, field), file_writer,
            options, pool));
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("create lucene global index writer failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "create lucene global index writer failed, with unknown error.");
    }
}

LuceneGlobalIndexWriter::LuceneGlobalIndexWriter(
    const std::string& field_name, const std::shared_ptr<arrow::DataType>& arrow_type,
    LuceneWriteContext&& write_context, const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      field_name_(field_name),
      arrow_type_(arrow_type),
      write_context_(std::move(write_context)),
      file_writer_(file_writer),
      options_(options) {}

LuceneGlobalIndexWriter::~LuceneGlobalIndexWriter() {
    if (write_context_.tmp_index_path.empty()) {
        return;
    }
    try {
        [[maybe_unused]] bool ec = Lucene::FileUtils::removeDirectory(
            LuceneUtils::StringToWstring(write_context_.tmp_index_path));
    } catch (...) {
        // do nothing
    }
}

Status LuceneGlobalIndexWriter::AddBatch(::ArrowArray* arrow_array,
                                         std::vector<int64_t>&& relative_row_ids) {
    PAIMON_RETURN_NOT_OK(
        GlobalIndexUtils::CheckRelativeRowIds(arrow_array, relative_row_ids, row_id_));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> array,
                                      arrow::ImportArray(arrow_array, arrow_type_));
    auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
    CHECK_NOT_NULL(struct_array, "invalid input array in LuceneIndexWriter, must be struct array");
    auto field_array = struct_array->GetFieldByName(field_name_);
    CHECK_NOT_NULL(
        field_array,
        fmt::format("invalid input array in LuceneIndexWriter, field {} not in input array",
                    field_name_));
    auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(field_array);
    CHECK_NOT_NULL(
        string_array,
        fmt::format(
            "invalid input array in LuceneIndexWriter, field array {} is not a string array",
            field_name_));
    try {
        for (int64_t i = 0; i < string_array->length(); i++) {
            if (string_array->IsNull(i)) {
                write_context_.field->setValue(kEmptyWstring);
            } else {
                auto view = string_array->Value(i);
                write_context_.field->setValue(LuceneUtils::StringToWstring(view));
            }
            row_id_++;
            write_context_.index_writer->addDocument(write_context_.doc);
        }
    } catch (const std::exception& e) {
        return Status::Invalid(fmt::format(
            "add batch for lucene global index writer failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "add batch for lucene global index writer failed, with unknown error.");
    }
    return Status::OK();
}

Status LuceneGlobalIndexWriter::FlushIndexToFinal() {
    try {
        if (write_context_.index_writer->numDocs() != row_id_) {
            return Status::Invalid(
                fmt::format("lucene writer row count {} mismatch paimon inner row count {}",
                            write_context_.index_writer->numDocs(), row_id_));
        }
        write_context_.index_writer->optimize();
        write_context_.index_writer->close();

        auto file_names = write_context_.lucene_dir->listAll();
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                               file_writer_->NewOutputStream(write_context_.output_path));
        DataOutputStream data_output_stream(out);
        PAIMON_RETURN_NOT_OK(data_output_stream.WriteValue<int32_t>(kVersion));
        PAIMON_RETURN_NOT_OK(
            data_output_stream.WriteValue<int32_t>(static_cast<int32_t>(file_names.size())));
        auto buffer = std::make_shared<Bytes>(kDefaultReadBufferSize, pool_.get());
        for (const auto& wfile_name : file_names) {
            auto file_name = LuceneUtils::WstringToString(wfile_name);
            PAIMON_RETURN_NOT_OK(
                data_output_stream.WriteValue<int32_t>(static_cast<int32_t>(file_name.size())));
            PAIMON_RETURN_NOT_OK(
                data_output_stream.WriteBytes(std::make_shared<Bytes>(file_name, pool_.get())));
            int64_t file_length = write_context_.lucene_dir->fileLength(wfile_name);
            PAIMON_RETURN_NOT_OK(data_output_stream.WriteValue<int64_t>(file_length));

            Lucene::IndexInputPtr input = write_context_.lucene_dir->openInput(wfile_name);
            int64_t total_write_size = 0;
            while (total_write_size < file_length) {
                int64_t current_write_size = std::min(file_length - total_write_size,
                                                      static_cast<int64_t>(kDefaultReadBufferSize));
                input->readBytes(reinterpret_cast<uint8_t*>(buffer->data()), /*offset=*/0,
                                 static_cast<int32_t>(current_write_size));
                PAIMON_ASSIGN_OR_RAISE(int64_t actual_write_size,
                                       out->Write(buffer->data(), current_write_size));
                if (actual_write_size != current_write_size) {
                    return Status::Invalid(
                        fmt::format("invalid write, try to write {} while actual write {}",
                                    current_write_size, actual_write_size));
                }
                total_write_size += current_write_size;
            }
            input->close();
        }
        PAIMON_RETURN_NOT_OK(out->Flush());
        PAIMON_RETURN_NOT_OK(out->Close());
        write_context_.lucene_dir->close();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::Invalid(
            fmt::format("finish for lucene global index writer failed, with {} error.", e.what()));
    } catch (...) {
        return Status::UnknownError(
            "finish for lucene global index writer failed, with unknown error.");
    }
}

Result<std::vector<GlobalIndexIOMeta>> LuceneGlobalIndexWriter::Finish() {
    PAIMON_RETURN_NOT_OK(FlushIndexToFinal());
    const std::string& index_file_path = write_context_.output_path;
    PAIMON_ASSIGN_OR_RAISE(int64_t file_size, file_writer_->GetFileSize(index_file_path));
    std::string options_json;
    PAIMON_RETURN_NOT_OK(RapidJsonUtil::ToJsonString(options_, &options_json));
    auto meta_bytes = std::make_shared<Bytes>(options_json, pool_.get());
    GlobalIndexIOMeta meta(index_file_path, file_size,
                           /*row_count=*/row_id_,
                           /*metadata=*/meta_bytes);
    return std::vector<GlobalIndexIOMeta>({meta});
}

}  // namespace paimon::lucene
