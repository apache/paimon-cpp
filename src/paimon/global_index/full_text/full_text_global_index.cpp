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

#include "paimon/global_index/full_text/full_text_global_index.h"

#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/global_index/full_text/full_text_defs.h"
#include "paimon/global_index/full_text/full_text_global_index_reader.h"
#include "paimon/global_index/full_text/full_text_global_index_writer.h"

namespace paimon::full_text {

#define CHECK_NOT_NULL(pointer, error_msg)     \
    do {                                       \
        if (!(pointer)) {                      \
            return Status::Invalid(error_msg); \
        }                                      \
    } while (0)

FullTextGlobalIndex::FullTextGlobalIndex(const std::map<std::string, std::string>& options)
    : options_(OptionsUtils::FetchOptionsWithPrefix(kOptionKeyPrefix, options)) {}

Result<std::optional<std::vector<std::string>>> FullTextGlobalIndex::GetExtraFieldNames() const {
    return std::optional<std::vector<std::string>>(std::nullopt);
}

Result<std::shared_ptr<GlobalIndexWriter>> FullTextGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    CHECK_NOT_NULL(struct_type,
                   "arrow schema must be struct type when create FullTextGlobalIndexWriter");
    auto index_field = struct_type->GetFieldByName(field_name);
    CHECK_NOT_NULL(
        index_field,
        fmt::format("field {} not exist in arrow schema when create FullTextGlobalIndexWriter",
                    field_name));
    // STRING, CHAR and VARCHAR are all mapped to the arrow utf8 type.
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid(
            fmt::format("full-text index only supports string fields, field {} is {}", field_name,
                        index_field->type()->ToString()));
    }
    if (!file_writer) {
        return Status::Invalid(
            "file writer must not be null when create FullTextGlobalIndexWriter");
    }
    return FullTextGlobalIndexWriter::Create(field_name, arrow_type, file_writer, options_, pool);
}

Result<std::shared_ptr<GlobalIndexReader>> FullTextGlobalIndex::CreateReader(
    ::ArrowSchema* c_arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> arrow_schema,
                                      arrow::ImportSchema(c_arrow_schema));
    if (files.size() != 1) {
        return Status::Invalid(fmt::format(
            "full-text index expects exactly one index file per shard, now num: {}", files.size()));
    }
    if (arrow_schema->num_fields() != 1) {
        return Status::Invalid(fmt::format("full-text index only supports one field, now num: {}",
                                           arrow_schema->num_fields()));
    }
    auto index_field = arrow_schema->field(0);
    if (index_field->type()->id() != arrow::Type::type::STRING) {
        return Status::Invalid(
            fmt::format("full-text index only supports string fields, field {} is {}",
                        index_field->name(), index_field->type()->ToString()));
    }
    // The analyzer configuration is embedded in the index file, so neither the table options nor
    // `GlobalIndexIOMeta::metadata` are needed to read it.
    return FullTextGlobalIndexReader::Create(files[0], file_reader, pool);
}

}  // namespace paimon::full_text
