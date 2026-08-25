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

#include "paimon/result.h"
#include "paimon/table/format/format_table.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {

/// Plans a read of a format table by listing its directories.
///
/// Partitions are discovered from the directory layout: every `key=value` directory that matches
/// the table's partition keys, nested in the order the keys are declared, or, when the table sets
/// `format-table.partition-path-only-value`, every directory at that level, whose bare name is the
/// value. Data files are then collected from a partition directory and everything below it.
///
/// Names starting with `_` or `.` are skipped and not descended into, since that is how an engine
/// marks output that is not committed table data. The one exception is the directory standing for
/// a null partition value in the value-only layout, named by `partition.default-name`.
///
/// A partition's files are packed into splits of about `source.split.target-size`, counting each
/// file as at least `source.split.open-file-cost`. A file larger than that target is still a split
/// of its own: see `FormatDataSplit` for why one file is never shared between readers.
///
/// Planning leaves the scan as it was, so one may be shared between threads; what it plans is
/// whatever the directories held when it looked.
class FormatTableScan : public TableScan {
 public:
    /// A partition's values paired with the directory they were read from.
    using PartitionAndPath = std::pair<std::map<std::string, std::string>, std::string>;

    /// @param table Table to scan.
    /// @param partition_filter Partition values to keep, keyed by partition field name. A key that
    ///        is absent from the map is unconstrained, so a filter may name only some of the
    ///        partition keys. An empty map keeps every partition.
    /// @param limit Upper bound on the rows the caller will read. Splits are dropped once the
    ///        remaining ones cannot be needed, but a format table records no row counts, so a
    ///        positive limit cannot drop anything and the caller still has to stop reading itself.
    static Result<std::unique_ptr<FormatTableScan>> Create(
        const std::shared_ptr<FormatTable>& table,
        const std::map<std::string, std::string>& partition_filter,
        const std::optional<int32_t>& limit);

    ~FormatTableScan() override;

    /// Plans the read: the splits of every partition that passes the filter, in a stable order.
    ///
    /// A missing table directory is answered differently by the two shapes of table. An
    /// unpartitioned table has only its location to list, so a location that is not there fails,
    /// which is the only way to tell a wrong location from a table with no rows. A partitioned
    /// table discovers its partitions by descending, so an absent directory simply has no
    /// partitions below it and the plan comes back empty.
    Result<std::shared_ptr<Plan>> CreatePlan() override;

    /// Lists the partitions the table's directory layout holds, whether or not they hold data,
    /// in a stable order. Every partition key gets a value; a directory named
    /// `partition.default-name` reads back as that name, standing for a null partition value.
    Result<std::vector<std::map<std::string, std::string>>> ListPartitions() const override;

 private:
    FormatTableScan(const std::shared_ptr<FormatTable>& table,
                    const std::map<std::string, std::string>& partition_filter,
                    const std::optional<int32_t>& limit, int64_t target_split_size,
                    int64_t open_file_cost);

    /// Lists partition directories under the table location that pass the partition filter, paired
    /// with the partition values their names spell out.
    Result<std::vector<PartitionAndPath>> FindPartitions() const;

    /// Collects the data files under `directory` and everything below it and packs them into
    /// splits of about the target size. Empty when there is no data file.
    Result<std::vector<std::shared_ptr<Split>>> CreateSplits(
        const std::string& directory, const std::map<std::string, std::string>& partition) const;

    std::shared_ptr<FormatTable> table_;
    std::map<std::string, std::string> partition_filter_;
    std::optional<int32_t> limit_;
    int64_t target_split_size_;
    int64_t open_file_cost_;
};

}  // namespace paimon
