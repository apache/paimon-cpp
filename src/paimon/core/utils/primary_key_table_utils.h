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
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class MergeFunction;
class CoreOptions;
class FieldsComparator;
class DataField;

class PrimaryKeyTableUtils {
 public:
    PrimaryKeyTableUtils() = delete;
    ~PrimaryKeyTableUtils() = delete;

    static Result<std::unique_ptr<MergeFunction>> CreateMergeFunction(
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& primary_keys, const CoreOptions& options);

    static Result<std::unique_ptr<FieldsComparator>> CreateSequenceFieldsComparator(
        const std::vector<DataField>& value_fields, const CoreOptions& options);
};

}  // namespace paimon
