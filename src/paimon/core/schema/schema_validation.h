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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/status.h"

namespace arrow {
class DataType;
class Field;
}  // namespace arrow

namespace paimon {
class CoreOptions;
class DataField;
class DataSchema;
class FileSystem;
class TableSchema;

/// Validation utils for `TableSchema`.
class SchemaValidation {
 public:
    SchemaValidation() = delete;
    ~SchemaValidation() = delete;

    static Status ValidateTableSchema(const TableSchema& schema);

    /// Validates a schema a table is about to be created from, whichever catalog is creating it.
    ///
    /// It picks the rules by the schema's `type` option: a format table owns no bucket, manifest
    /// or snapshot machinery, so only the structural invariants apply to it. A type this library
    /// cannot load at all is refused rather than persisted, since the table would look created
    /// and fail only when someone tried to open it.
    static Status ValidateNewTableSchema(const TableSchema& schema);

    /// Validates the structural invariants every table shares, whatever its type: partition and
    /// primary key fields exist and are free of duplicates, key fields are primitive, and no field
    /// takes a reserved name.
    static Status ValidateGenericTableSchema(const TableSchema& schema);

    /// The same rules as `ValidateGenericTableSchema()`, for a schema this library did not
    /// create. A `DataSchema` names its field types through its arrow schema rather than through
    /// `DataField`s, which is the only reason this is a second entry point.
    static Status ValidateGenericDataSchema(const DataSchema& schema);

    /// Validates what a format table additionally requires, on top of
    /// `ValidateGenericTableSchema()`. It takes a `DataSchema` so that it can run both at creation
    /// and at `FormatTable::Create()`, where the schema may never have passed through creation
    /// here at all.
    ///
    /// @param effective_options The options the table will actually run with: the schema's own at
    ///        creation, and those with anything given at the call merged on top at
    ///        `FormatTable::Create()`. Passing the schema's alone would let an option given at the
    ///        call reach a table that refuses it in its schema.
    /// @param file_system The file system the caller already resolved, or null when it named one
    ///        through the options. It only spares reading the options from resolving `file-system`
    ///        a second time, which would fail for a caller that handed its own over instead of
    ///        naming one.
    static Status ValidateFormatTableSchema(
        const DataSchema& schema, const std::map<std::string, std::string>& effective_options,
        const std::shared_ptr<FileSystem>& file_system);

    static bool IsPostponeBucketTable(const TableSchema& schema, int32_t bucket);

 private:
    static Status ValidateNoDuplicateField(const std::vector<std::string>& field_names,
                                           const std::string& error_message_intro);
    /// The rules `ValidateGenericTableSchema()` and `ValidateGenericDataSchema()` share, on the
    /// fields both can hand over.
    static Status ValidateGenericSchema(const std::vector<DataField>& fields,
                                        const std::vector<std::string>& bucket_keys,
                                        const std::vector<std::string>& primary_keys,
                                        const std::vector<std::string>& partition_keys);
    static Status ValidateOnlyContainPrimitiveType(const std::vector<DataField>& fields,
                                                   const std::vector<std::string>& field_names,
                                                   const std::string& error_message_intro);
    static Status ValidateNotContainSpecificType(const std::vector<DataField>& fields,
                                                 const std::vector<std::string>& field_names);
    static Status ValidateBucket(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateDefaultValues(const TableSchema& schema) {
        return Status::NotImplemented("validate default values not implemented");
    }
    static Status ValidateStartupMode(const CoreOptions& options) {
        return Status::NotImplemented("validate startup mode not implemented");
    }
    static Status ValidateFieldsPrefix(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateSequenceField(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateSequenceGroup(const TableSchema& schema, const CoreOptions& options);
    static Status ValidateChangelogProducer(const CoreOptions& options);
    static Status ValidateForDeletionVectors(const CoreOptions& options);

    static Status ValidateRowTracking(const TableSchema& table_schema, const CoreOptions& options);

    static Status ValidateBlobFields(const TableSchema& schema, const CoreOptions& options);

    static Status ValidateMapStorageLayout(const TableSchema& schema, const CoreOptions& options);

    static Status ValidateVectorFields(const TableSchema& schema, const CoreOptions& options);

    static bool IsComplexType(const std::shared_ptr<arrow::Field>& field);
};

}  // namespace paimon
