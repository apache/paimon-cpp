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

#include <memory>
#include <utility>

#include "paimon/file_index/file_index_reader.h"

namespace paimon {

/// Truncated index values can still answer null checks. Other predicates retain all candidates.
class TruncatedValueReader : public FileIndexReader {
 public:
    explicit TruncatedValueReader(std::shared_ptr<FileIndexReader> reader)
        : reader_(std::move(reader)) {}

    Result<std::shared_ptr<FileIndexResult>> VisitIsNull() override {
        return reader_->VisitIsNull();
    }

    Result<std::shared_ptr<FileIndexResult>> VisitIsNotNull() override {
        return reader_->VisitIsNotNull();
    }

 private:
    std::shared_ptr<FileIndexReader> reader_;
};

}  // namespace paimon
