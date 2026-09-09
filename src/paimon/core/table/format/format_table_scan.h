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
#include <optional>
#include <string>
#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/binary_row_partition_computer.h"
#include "paimon/result.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {

/// Plans a read of a format table by listing its directories.
///
/// Partitions come from the directory layout and a partition's files are packed into splits of
/// about `source.split.target-size`; `docs/source/user_guide/format_table.rst` describes the
/// layout. A name starting with `_` or `.` is skipped and not descended into, being how an engine
/// marks uncommitted output, with `partition.default-name` the one exception.
///
/// Planning leaves the scan as it was, so one may be shared between threads.
class FormatTableScan : public TableScan {
 public:
    /// A partition's values paired with the directory they were read from.
    using PartitionAndPath = std::pair<std::map<std::string, std::string>, std::string>;

    /// @param partition_filter Partition values to keep, keyed by partition field name. A key
    ///        absent from the map is unconstrained; an empty map keeps every partition. A value is
    ///        matched in its column type rather than as text, so `1` and `01` name the same `INT`
    ///        partition, and one that will not read into that type is refused rather than
    ///        silently matching nothing.
    /// @param limit Upper bound on the rows the caller will read. A format table records no row
    ///        counts, so a positive limit drops no split and the caller still has to stop itself.
    static Result<std::unique_ptr<FormatTableScan>> Create(
        const std::shared_ptr<FormatTable>& table,
        const std::map<std::string, std::string>& partition_filter,
        const std::optional<int32_t>& limit);

    ~FormatTableScan() override;

    /// Plans the read: the splits of every partition that passes the filter, in a stable order.
    ///
    /// A missing table directory is answered differently by the two shapes of table: an
    /// unpartitioned one has only its location to list, so a location that is not there fails -
    /// the only way to tell a wrong location from a table with no rows - while a partitioned one
    /// finds no partitions below it and plans nothing.
    Result<std::shared_ptr<Plan>> CreatePlan() override;

    /// Lists the partitions the table's directory layout holds, whether or not they hold data,
    /// in a stable order. Every partition key gets a value; a directory named
    /// `partition.default-name` reads back as that name, standing for a null partition value.
    Result<std::vector<std::map<std::string, std::string>>> ListPartitions() const override;

 private:
    FormatTableScan(const std::shared_ptr<FormatTable>& table,
                    const std::map<std::string, std::string>& partition_filter,
                    std::map<std::string, BinaryRow> partition_filter_values,
                    const std::optional<int32_t>& limit, int64_t target_split_size,
                    int64_t open_file_cost,
                    std::unique_ptr<BinaryRowPartitionComputer> partition_computer);

    /// Whether the directory value `value` of `partition_key` passes the filter.
    ///
    /// Both sides are compared in the column's type rather than as text, so a blank value and the
    /// null partition stay apart even though a directory name cannot tell them apart. A value that
    /// will not read into that type is skipped.
    bool PartitionValuePassesFilter(const std::string& partition_key,
                                    const std::string& value) const;

    /// Lists partition directories under the table location that pass the partition filter, paired
    /// with the partition values their names spell out.
    Result<std::vector<PartitionAndPath>> FindPartitions() const;

    /// Collects the data files under `directory` and everything below it and packs them into
    /// splits of about the target size. Empty when there is no data file.
    Result<std::vector<std::shared_ptr<Split>>> CreateSplits(
        const std::string& directory, const std::map<std::string, std::string>& partition) const;

    std::shared_ptr<FormatTable> table_;
    /// The filter as the caller gave it, which is what an error or a log line reports.
    std::map<std::string, std::string> partition_filter_;
    /// The same filter as the table's own types hold it, one single-field row per key. Empty when
    /// the table is not partitioned or nothing was filtered on.
    std::map<std::string, BinaryRow> partition_filter_values_;
    std::optional<int32_t> limit_;
    int64_t target_split_size_;
    int64_t open_file_cost_;
    /// Reads a partition value into its column type and renders it back out. Null when the table
    /// is not partitioned, which leaves nothing to filter on.
    std::unique_ptr<BinaryRowPartitionComputer> partition_computer_;
};

}  // namespace paimon
