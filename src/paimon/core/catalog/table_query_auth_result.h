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

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "paimon/common/types/data_field.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Array;
class DataType;
class MemoryPool;
class StructArray;
}  // namespace arrow

namespace paimon {
class Predicate;

/// Computes the values replacing a masked column, mirroring the Java `Transform` of
/// `columnMasking`. Only `NULL`, `FIELD_REF`, `CONCAT`, `CONCAT_WS` and `SUBSTRING` are supported.
class MaskTransform {
 public:
    virtual ~MaskTransform() = default;

    virtual const std::string& Name() const = 0;

    virtual std::vector<std::string> InputFields() const = 0;

    virtual Status ValidateTarget(const DataField& target) const = 0;

    /// Computes the masked values for every row of `batch`, which holds every input column. The
    /// inputs and `target_type` are plain, not dictionary-encoded, and a string is not a large one.
    virtual Result<std::shared_ptr<arrow::Array>> Evaluate(
        const arrow::StructArray& batch, const std::shared_ptr<arrow::DataType>& target_type,
        arrow::MemoryPool* pool) const = 0;
};

/// The row filter and column masks of one query of a table with `query-auth.enabled=true`,
/// mirroring Java `TableQueryAuthResult`. The rules are parsed on creation, so a rule this library
/// cannot apply fails the query instead of being dropped.
class TableQueryAuthResult {
 public:
    /// Parses the JSON rules of an `AuthTableQueryResponse`.
    static Result<std::shared_ptr<TableQueryAuthResult>> Create(
        const std::vector<std::string>& filter,
        const std::map<std::string, std::string>& column_masking);

    const std::vector<std::string>& Filter() const {
        return filter_;
    }
    const std::map<std::string, std::string>& ColumnMasking() const {
        return column_masking_;
    }

    /// The conjunction of the filter, or null when it reads no column. Bind it to a schema by
    /// name before evaluating it.
    const std::shared_ptr<Predicate>& RowFilter() const {
        return row_filter_;
    }
    /// Whether the filter folds to FALSE, as Java `PredicateBuilder.alwaysFalse()`: no row is
    /// read, and `RowFilter()` is null.
    bool RejectsAllRows() const {
        return rejects_all_rows_;
    }
    /// The columns `RowFilter()` reads; see `ReferencedFields()` for all the filter names.
    const std::set<std::string>& RowFilterFields() const {
        return row_filter_fields_;
    }
    /// The masks to apply, by the column they replace. A mask of a system column such as
    /// `_ROW_ID`, which a query-auth read never projects, is left out, as Java leaves it inert.
    /// `ValidateAgainstSchema()` refuses a table column whose mask would be skipped this way.
    const std::map<std::string, std::shared_ptr<MaskTransform>>& Masks() const {
        return masks_;
    }
    std::set<std::string> MaskedFields() const;

    bool HasRules() const {
        return row_filter_ != nullptr || rejects_all_rows_ || !masks_.empty();
    }

    /// Whether `other` holds the same row filter conjuncts, however they were serialized.
    bool SameFilter(const TableQueryAuthResult& other) const;

    /// Java `validateAgainstSchema`: every column a rule reads exists with the type the rule
    /// expects, a mask fits its column, and no mask reads another masked column.
    Status ValidateAgainstSchema(const std::vector<DataField>& latest_fields) const;

    /// The columns the rules reference: the row filter operands, those of the branches its
    /// constants folded away included, and the targets and inputs of the masks.
    std::set<std::string> ReferencedFields() const;

    /// The field id of each column the rules reference, as `fields` hold them; a column `fields`
    /// do not hold is left out.
    std::map<std::string, int32_t> BindFieldIds(const std::vector<DataField>& fields) const;

    /// Java `validateReadableWithoutRename`: fails when `field_ids` leave a column the rules
    /// reference unbound, or `fields` expose a bound column under another name, or another column
    /// under its name. Only names and field ids are compared; `ValidateAgainstSchema()` checks the
    /// types the rules declare.
    Status ValidateFieldIds(const std::map<std::string, int32_t>& field_ids,
                            const std::vector<DataField>& fields) const;

    /// The columns the rules read for a read outputting `visible_fields`: the row filter operands
    /// and, transitively, the inputs of every mask whose target is visible.
    std::set<std::string> RequiredFields(const std::vector<std::string>& visible_fields) const;

 private:
    TableQueryAuthResult(const std::vector<std::string>& filter,
                         const std::map<std::string, std::string>& column_masking)
        : filter_(filter), column_masking_(column_masking) {}

    std::vector<std::string> filter_;
    std::map<std::string, std::string> column_masking_;
    std::shared_ptr<Predicate> row_filter_;
    bool rejects_all_rows_ = false;
    std::set<std::string> row_filter_fields_;
    std::map<std::string, std::shared_ptr<MaskTransform>> masks_;
    /// The type every rule was written for, per column it reads, even in a branch a constant
    /// folded away.
    std::map<std::string, std::shared_ptr<arrow::DataType>> referenced_types_;
};

}  // namespace paimon
