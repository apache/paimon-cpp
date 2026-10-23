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

#include "paimon/core/io/vector_data_file_writer_factory.h"

#include <utility>

#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/format/file_format.h"

namespace paimon {

Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>>
VectorDataFileWriterFactory::CreateWriter() const {
    const std::shared_ptr<FileFormat> format = options_.GetVectorFileFormat();
    if (!format) {
        return Status::Invalid("vector.file.format is required for dedicated VECTOR files");
    }
    PAIMON_ASSIGN_OR_RAISE(WriterResources resources,
                           CreateWriterResources(*format, write_schema_,
                                                 /*create_stats_extractor=*/true));
    auto writer = std::make_unique<DataFileWriter>(
        options_.GetFileCompression(), std::function<Status(::ArrowArray*, ::ArrowArray*)>(),
        schema_id_, ResolveSeqNumCounter(), file_source_, resources.stats_extractor,
        path_factory_->IsExternalPath(), write_cols_, pool_);
    PAIMON_RETURN_NOT_OK(writer->Init(options_.GetFileSystem(),
                                      path_factory_->NewVectorPath(format->Identifier()),
                                      resources.writer_builder));
    return std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>(
        std::move(writer));
}

}  // namespace paimon
