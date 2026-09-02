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

#include "paimon/core/table/system/system_table.h"

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/catalog/identifier.h"
#include "paimon/common/data/generic_row.h"
#include "paimon/core/catalog/file_system_catalog.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/system/audit_log_system_table.h"
#include "paimon/core/table/system/binlog_system_table.h"
#include "paimon/core/table/system/global_system_tables.h"
#include "paimon/core/table/system/read_optimized_system_table.h"
#include "paimon/core/table/system/system_table_scan.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Result<std::shared_ptr<TableSchema>> CreateTableSchemaForTest(
    const std::map<std::string, std::string>& options) {
    std::shared_ptr<arrow::Schema> arrow_schema = arrow::schema({
        arrow::field("pk", arrow::utf8()),
        arrow::field("v", arrow::int32(), true),
    });
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, arrow_schema,
                            /*partition_keys=*/{}, /*primary_keys=*/{"pk"}, options));
    return std::shared_ptr<TableSchema>(std::move(table_schema));
}

}  // namespace

TEST(SystemTableTest, TestChangelogArrowSchemaReturnsInvalidOptions) {
    std::map<std::string, std::string> options = {
        {Options::TABLE_READ_SEQUENCE_NUMBER_ENABLED, "invalid"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         CreateTableSchemaForTest(options));

    AuditLogSystemTable audit_log(/*fs=*/nullptr, "/tmp/table", table_schema, options);
    ASSERT_NOK_WITH_MSG(audit_log.ArrowSchema(),
                        "Invalid Config [table-read.sequence-number.enabled: invalid]");

    BinlogSystemTable binlog(/*fs=*/nullptr, "/tmp/table", table_schema, options);
    ASSERT_NOK_WITH_MSG(binlog.ArrowSchema(),
                        "Invalid Config [table-read.sequence-number.enabled: invalid]");
}

TEST(SystemTableTest, TestBinlogArrowSchemaWithSequenceNumber) {
    std::map<std::string, std::string> options = {
        {Options::TABLE_READ_SEQUENCE_NUMBER_ENABLED, "true"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         CreateTableSchemaForTest(options));

    BinlogSystemTable binlog(/*fs=*/nullptr, "/tmp/table", table_schema, options);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Schema> schema, binlog.ArrowSchema());

    ASSERT_EQ(schema->field_names(),
              (std::vector<std::string>{"rowkind", "_SEQUENCE_NUMBER", "pk", "v"}));
    ASSERT_EQ(schema->field(0)->type()->id(), arrow::Type::STRING);
    ASSERT_FALSE(schema->field(0)->nullable());
    ASSERT_EQ(schema->field(1)->type()->id(), arrow::Type::INT64);
    ASSERT_EQ(schema->field(2)->type()->id(), arrow::Type::LIST);
    ASSERT_EQ(schema->field(3)->type()->id(), arrow::Type::LIST);
}

TEST(SystemTableTest, TestStreamingBinlogPacksUpdateAcrossBatches) {
    std::shared_ptr<arrow::DataType> input_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("pk", arrow::utf8()),
        arrow::field("v", arrow::int32()),
    });
    std::shared_ptr<arrow::Array> input =
        arrow::ipc::internal::json::ArrayFromJSON(
            input_type, R"([[0, 10, "a", 1], [1, 11, "b", 2], [2, 12, "b", 3], [3, 13, "d", 4]])")
            .ValueOrDie();
    std::shared_ptr<arrow::Schema> output_schema = arrow::schema({
        arrow::field("rowkind", arrow::utf8(), /*nullable=*/false),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
        arrow::field("pk", arrow::list(arrow::utf8())),
        arrow::field("v", arrow::list(arrow::int32())),
    });
    std::unique_ptr<BatchReader> reader = CreateChangelogBatchReader(
        std::make_unique<MockFileBatchReader>(input, input_type, /*read_batch_size=*/2),
        output_schema,
        /*include_sequence_number=*/true, CreateBinlogBatchConverter(),
        /*pack_update_before_after=*/true, GetDefaultPool());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         ReadResultCollector::CollectResult(std::move(reader)));
    std::shared_ptr<arrow::Array> expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(actual->type(), R"([
                ["+I", 10, ["a"], [1]],
                ["+U", 11, ["b", "b"], [2, 3]],
                ["-D", 13, ["d"], [4]]
            ])")
            .ValueOrDie();
    auto expected = std::make_shared<arrow::ChunkedArray>(expected_array);
    ASSERT_TRUE(actual->Equals(*expected))
        << "expected: " << expected->ToString() << "\nactual: " << actual->ToString();
}

TEST(SystemTableTest, TestStreamingBinlogEmitsUnmatchedUpdateBefore) {
    std::shared_ptr<arrow::DataType> input_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("pk", arrow::utf8()),
        arrow::field("v", arrow::int32()),
    });
    std::shared_ptr<arrow::Array> input =
        arrow::ipc::internal::json::ArrayFromJSON(input_type, R"([[1, "b", 2]])").ValueOrDie();
    std::shared_ptr<arrow::Schema> output_schema = arrow::schema({
        arrow::field("rowkind", arrow::utf8(), /*nullable=*/false),
        arrow::field("pk", arrow::list(arrow::utf8())),
        arrow::field("v", arrow::list(arrow::int32())),
    });
    std::unique_ptr<BatchReader> reader = CreateChangelogBatchReader(
        std::make_unique<MockFileBatchReader>(input, input_type, /*read_batch_size=*/1),
        output_schema,
        /*include_sequence_number=*/false, CreateBinlogBatchConverter(),
        /*pack_update_before_after=*/true, GetDefaultPool());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::ChunkedArray> actual,
                         ReadResultCollector::CollectResult(std::move(reader)));
    std::shared_ptr<arrow::Array> expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(actual->type(), R"([
                ["-U", ["b"], [2]]
            ])")
            .ValueOrDie();
    auto expected = std::make_shared<arrow::ChunkedArray>(expected_array);
    ASSERT_TRUE(actual->Equals(*expected))
        << "expected: " << expected->ToString() << "\nactual: " << actual->ToString();
}

TEST(SystemTableTest, TestChangelogBatchOutlivesReader) {
    std::unique_ptr<MemoryPool> unique_pool = GetMemoryPool();
    std::shared_ptr<MemoryPool> pool = std::move(unique_pool);
    std::weak_ptr<MemoryPool> weak_pool = pool;
    std::shared_ptr<arrow::DataType> input_type = arrow::struct_({
        arrow::field("_VALUE_KIND", arrow::int8()),
        arrow::field("pk", arrow::utf8()),
    });
    std::shared_ptr<arrow::Array> input =
        arrow::ipc::internal::json::ArrayFromJSON(input_type, R"([[0, "a"]])").ValueOrDie();
    std::shared_ptr<arrow::Schema> output_schema = arrow::schema({
        arrow::field("rowkind", arrow::utf8(), /*nullable=*/false),
        arrow::field("pk", arrow::list(arrow::utf8())),
    });
    std::unique_ptr<BatchReader> reader = CreateChangelogBatchReader(
        std::make_unique<MockFileBatchReader>(input, input_type, /*read_batch_size=*/1),
        output_schema,
        /*include_sequence_number=*/false, CreateBinlogBatchConverter(),
        /*pack_update_before_after=*/true, pool);

    ASSERT_OK_AND_ASSIGN(BatchReader::ReadBatch batch, reader->NextBatch());
    ASSERT_GT(pool->CurrentUsage(), 0);
    reader.reset();
    pool.reset();
    ASSERT_FALSE(weak_pool.expired());

    auto& [c_array, c_schema] = batch;
    arrow::Result<std::shared_ptr<arrow::Array>> imported =
        arrow::ImportArray(c_array.get(), c_schema.get());
    ASSERT_TRUE(imported.ok()) << imported.status().ToString();
    std::shared_ptr<arrow::Array> output = std::move(imported).ValueOrDie();
    ASSERT_EQ(output->length(), 1);
    output.reset();
    ASSERT_TRUE(weak_pool.expired());
}

TEST(SystemTableTest, TestReadOptimizedSystemTableRegistration) {
    ASSERT_TRUE(SystemTableLoader::IsSupported(ReadOptimizedSystemTable::kName));

    std::map<std::string, std::string> options = {{Options::FILE_SYSTEM, "local"},
                                                  {Options::FILE_FORMAT, "orc"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                         CreateTableSchemaForTest(options));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<SystemTable> system_table,
                         SystemTableLoader::Load(ReadOptimizedSystemTable::kName, /*fs=*/nullptr,
                                                 "/tmp/table", table_schema,
                                                 /*dynamic_options=*/{}));
    ASSERT_EQ(system_table->Name(), ReadOptimizedSystemTable::kName);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Schema> arrow_schema, system_table->ArrowSchema());
    ASSERT_EQ(arrow_schema->field_names(), (std::vector<std::string>{"pk", "v"}));
    ASSERT_EQ(arrow_schema->field(0)->type()->id(), arrow::Type::STRING);
    ASSERT_EQ(arrow_schema->field(1)->type()->id(), arrow::Type::INT32);
}

TEST(SystemTableTest, TestReadOptimizedSystemTablePathParsing) {
    ASSERT_OK_AND_ASSIGN(std::optional<SystemTablePath> parsed,
                         SystemTableLoader::TryParsePath("/tmp/db.db/t$branch_audit$ro"));
    ASSERT_TRUE(parsed.has_value());
    ASSERT_EQ(parsed->table_path, "/tmp/db.db/t");
    ASSERT_TRUE(parsed->branch.has_value());
    ASSERT_EQ(parsed->branch.value(), "audit");
    ASSERT_EQ(parsed->system_table_name, ReadOptimizedSystemTable::kName);
}

TEST(SystemTableTest, TestGlobalSystemTableWithoutCatalogReturnsNotImplemented) {
    ASSERT_OK_AND_ASSIGN(auto fs, FileSystemFactory::Get("local", "/tmp", {}));
    std::shared_ptr<FileSystem> shared_fs(std::move(fs));
    ASSERT_NOK_WITH_MSG(SystemTableLoader::LoadFromPath(shared_fs, "/tmp/warehouse/sys/tables", {}),
                        "global system table requires catalog context: tables");
}

namespace {

/// A catalog that serves the files of every table through `GetTableFileSystem`, like one
/// issuing temporary credentials per table does, and records what was asked of it.
class PerTableFileSystemCatalog : public FileSystemCatalog {
 public:
    PerTableFileSystemCatalog(const std::shared_ptr<FileSystem>& fs, const std::string& warehouse,
                              const std::map<std::string, std::string>& options)
        : FileSystemCatalog(fs, warehouse, options) {}

    Result<std::shared_ptr<FileSystem>> GetTableFileSystem(
        const Identifier& identifier) const override {
        requested.push_back(identifier.GetFullName());
        if (failure) {
            return failure.value();
        }
        return GetFileSystem();
    }

    mutable std::vector<std::string> requested;
    std::optional<Status> failure;
};

}  // namespace

TEST(SystemTableTest, TestGlobalPartitionsUsesTheFileSystemOfEachTable) {
    std::map<std::string, std::string> options = {{Options::FILE_SYSTEM, "local"},
                                                  {Options::FILE_FORMAT, "orc"}};
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options, CoreOptions::FromMap(options));
    std::unique_ptr<UniqueTestDirectory> dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    PerTableFileSystemCatalog catalog(core_options.GetFileSystem(), dir->Str(), options);
    ASSERT_OK(catalog.CreateDatabase("db1", options, /*ignore_if_exists=*/true));

    arrow::Schema typed_schema(
        {arrow::field("dt", arrow::utf8()), arrow::field("v", arrow::int32(), /*nullable=*/true)});
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &c_schema).ok());
    Status created = catalog.CreateTable(Identifier("db1", "t1"), &c_schema,
                                         /*partition_keys=*/{"dt"}, /*primary_keys=*/{}, options,
                                         /*ignore_if_exists=*/false);
    ArrowSchemaRelease(&c_schema);
    ASSERT_OK(created);

    GlobalSystemTableContext context;
    context.catalog = &catalog;
    context.fs = core_options.GetFileSystem();
    context.warehouse = dir->Str();
    context.catalog_options = options;
    PartitionsSystemTable partitions(context);

    // The table holds no data, so it contributes no partition, but its credentials are
    // still the ones asked for before its files are listed.
    ASSERT_OK_AND_ASSIGN(std::vector<GenericRow> rows, partitions.BuildRows());
    ASSERT_TRUE(rows.empty());
    ASSERT_EQ((std::vector<std::string>{"db1.t1"}), catalog.requested);

    // A table whose credentials cannot be issued fails the query, so that a partial result
    // is never mistaken for the whole one.
    catalog.failure = Status::Invalid("no credentials for the table");
    ASSERT_NOK_WITH_MSG(partitions.BuildRows(), "no credentials for the table");

    // A table that went away between being listed and being read is skipped instead, like
    // one whose location has already been removed.
    catalog.failure = Status::NotExist("table dropped");
    ASSERT_OK_AND_ASSIGN(std::vector<GenericRow> skipped, partitions.BuildRows());
    ASSERT_TRUE(skipped.empty());
}

TEST(SystemTableTest, TestScanMetricsAreSnapshots) {
    SystemTableScan scan("/tmp/table");
    std::shared_ptr<Metrics> metrics = scan.GetMetrics();
    ASSERT_TRUE(metrics);
    metrics->SetCounter("external", 1);

    std::shared_ptr<Metrics> second_metrics = scan.GetMetrics();
    ASSERT_TRUE(second_metrics);
    ASSERT_NOK_WITH_MSG(second_metrics->GetCounter("external"), "metric 'external' not found");
}

}  // namespace paimon::test
