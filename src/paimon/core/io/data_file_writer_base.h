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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/utils/checked_cast.h"
#include "paimon/core/io/data_file_index_writer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

/// Common lifecycle for data file writers which may finalize schema metadata and publish a
/// file-level index. Concrete writers remain responsible for their record-specific state and
/// DataFileMeta construction.
template <typename Record>
class DataFileWriterBase : public SingleFileWriter<Record, std::shared_ptr<DataFileMeta>> {
 public:
    using Base = SingleFileWriter<Record, std::shared_ptr<DataFileMeta>>;
    using AbortExecutor = typename Base::AbortExecutor;
    /// Callback invoked during BeforeFinish() to finalize file metadata.
    /// Produces an updated schema with per-field metadata (e.g. shredding metadata)
    /// and may perform other finalization work (e.g. reporting stats to cross-file context).
    using MetadataFinalizer = std::function<Result<std::shared_ptr<arrow::Schema>>()>;

    /// Sets the metadata finalizer. Called during BeforeFinish() to produce an updated
    /// schema and perform finalization callbacks. Must be set before Close().
    void SetMetadataFinalizer(MetadataFinalizer finalizer) {
        metadata_finalizer_ = std::move(finalizer);
    }

    void SetFileIndexWriter(std::unique_ptr<DataFileIndexWriter>&& file_index_writer,
                            const std::shared_ptr<arrow::Schema>& logical_schema) {
        file_index_writer_ = std::move(file_index_writer);
        logical_type_ = arrow::struct_(logical_schema->fields());
    }

    void Abort() override {
        if (file_index_writer_) {
            // The external index uses a path different from the data file path deleted by Base.
            file_index_writer_->Abort();
        }
        Base::Abort();
    }

    Result<AbortExecutor> GetAbortExecutor() const override {
        PAIMON_ASSIGN_OR_RAISE(AbortExecutor executor, Base::GetAbortExecutor());
        if (file_index_writer_ && file_index_writer_->ExternalIndexPath()) {
            executor.Add(this->fs_, file_index_writer_->ExternalIndexPath().value());
        }
        return executor;
    }

 protected:
    DataFileWriterBase(const std::string& compression,
                       std::function<Status(Record, ::ArrowArray*)> converter)
        : Base(compression, std::move(converter)) {}

    Status WriteRecord(Record record, ::ArrowArray* logical_batch) {
        PAIMON_RETURN_NOT_OK(AddFileIndexBatch(logical_batch));
        return Base::Write(std::move(record));
    }

    const FileIndexWriteResult& GetFileIndexWriteResult() const {
        return file_index_result_;
    }

    Status BeforeFinish() override {
        if (metadata_finalizer_) {
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> updated_schema,
                                   metadata_finalizer_());
            if (updated_schema) {
                PAIMON_RETURN_NOT_OK(this->UpdateSchema(updated_schema));
            }
        }
        return Status::OK();
    }

    Status BeforeCompletion() override {
        if (file_index_writer_) {
            PAIMON_ASSIGN_OR_RAISE(file_index_result_, file_index_writer_->Finish(this->path_));
        }
        return Status::OK();
    }

 private:
    Status AddFileIndexBatch(::ArrowArray* batch) {
        if (!file_index_writer_) {
            return Status::OK();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> logical_array,
                                          arrow::ImportArray(batch, logical_type_));
        std::shared_ptr<arrow::StructArray> logical_batch =
            checked_pointer_cast<arrow::StructArray>(logical_array);
        PAIMON_RETURN_NOT_OK(file_index_writer_->AddBatch(logical_batch));
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*logical_batch, batch));
        return Status::OK();
    }

    MetadataFinalizer metadata_finalizer_;
    std::unique_ptr<DataFileIndexWriter> file_index_writer_;
    std::shared_ptr<arrow::DataType> logical_type_;
    FileIndexWriteResult file_index_result_;
};

}  // namespace paimon
