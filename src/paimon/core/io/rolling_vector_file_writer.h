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

#include "paimon/core/io/rolling_file_writer.h"

namespace paimon {

/// Writes normal columns and one VECTOR column to files covering identical row ranges.
class RollingVectorFileWriter
    : public RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>> {
 public:
    using Writer = SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>;
    using WriterFactory = SingleFileWriterFactory<::ArrowArray*, std::shared_ptr<DataFileMeta>>;

    RollingVectorFileWriter(int64_t target_file_size, int64_t target_file_row_num,
                            int64_t vector_target_file_size,
                            const std::shared_ptr<WriterFactory>& main_factory,
                            const std::shared_ptr<WriterFactory>& vector_factory,
                            const std::shared_ptr<arrow::Schema>& write_schema,
                            int32_t vector_index);

    Status Write(::ArrowArray* record) override;
    Status Close() override;
    void Abort() override;
    Result<std::vector<std::shared_ptr<DataFileMeta>>> GetResult() override;

 private:
    Status CloseCurrentPair();

    int64_t vector_target_file_size_;
    std::shared_ptr<WriterFactory> vector_factory_;
    std::shared_ptr<arrow::Schema> write_schema_;
    int32_t vector_index_;
    std::unique_ptr<Writer> vector_writer_;
    bool aborted_ = false;
};

}  // namespace paimon
