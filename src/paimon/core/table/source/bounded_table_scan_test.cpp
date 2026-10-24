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

#include <zstd.h>

#include <atomic>
#include <fstream>
#include <iterator>
#include <limits>
#include <thread>

#include "arrow/api.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/memory/planning_budget_internal.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"

#ifdef PAIMON_TEST_PLANNING_ALLOCATIONS
namespace {
thread_local bool watching = false;
thread_local uint64_t requested = 0;
thread_local uint64_t ordinary_allocations = 0;
thread_local uint64_t fail_ordinary_at = UINT64_MAX;
thread_local bool observed_overdraft = false;
thread_local const paimon::PlanningBudget* observed_budget = nullptr;
void Observe(size_t bytes) {
    if (watching) {
        requested += bytes;
        observed_overdraft |= requested > observed_budget->BytesAdmitted();
    }
}
}  // namespace
extern "C" void* __real__Znwm(std::size_t size);
extern "C" void* __wrap__Znwm(std::size_t size) {
    if (watching) {
        if (ordinary_allocations++ == fail_ordinary_at) {
            watching = false;
            throw std::bad_alloc();
        }
        Observe(size);
    }
    return __real__Znwm(size);
}
#endif

namespace paimon::test {
namespace {
constexpr uint64_t kAmpleBudget = 32 * 1024 * 1024;

std::string ReadText(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void WriteText(const std::string& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(text.data(), text.size());
    ASSERT_TRUE(out.good());
}
std::string Fixture(const std::string& format) {
    return GetDataDir() + (format == "orc"
                               ? "/orc/append_types_compatibility.db/java_time_types"
                               : "/parquet/append_types_compatibility.db/rust_time_types");
}
Result<std::unique_ptr<ScanContext>> Context(
    const std::string& path, const std::shared_ptr<MemoryPool>& pool = GetDefaultPool(),
    const std::map<std::string, std::string>& options = {},
    const std::optional<std::string>& schema_override = std::nullopt) {
    ScanContextBuilder builder(path);
    builder.SetTableSchema(schema_override.value_or(ReadText(path + "/schema/schema-0")))
        .WithMemoryPool(pool)
        .AddOption("scan.snapshot-id", "1");
    for (const auto& [key, value] : options) {
        builder.AddOption(key, value);
    }
    return builder.Finish();
}
Result<std::shared_ptr<Plan>> BoundedPlan(const std::string& path,
                                          const std::shared_ptr<PlanningBudget>& budget) {
    PAIMON_ASSIGN_OR_RAISE(auto context, Context(path, PlanningBudgetAccess::Upstream(budget)));
    PAIMON_ASSIGN_OR_RAISE(auto scan,
                           TableScan::CreateWithPlanningBudget(std::move(context), budget));
    return scan->CreatePlan();
}
Result<std::shared_ptr<arrow::Array>> ReadSplits(
    const std::string& path, const std::vector<std::shared_ptr<Split>>& splits) {
    ReadContextBuilder builder(path);
    builder.SetTableSchema(ReadText(path + "/schema/schema-0"));
    if (path.find("/orc/") != std::string::npos) {
        // Existing ORC TIME decoding is unsupported. Exercise its native projected
        // data-reader path without expanding this planning change into type fixes.
        builder.SetReadFieldNames({"id"});
    }
    PAIMON_ASSIGN_OR_RAISE(auto context, builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(auto table, TableRead::Create(std::move(context)));
    PAIMON_ASSIGN_OR_RAISE(auto reader, table->CreateReader(splits));
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(auto batch, reader->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            break;
        }
        auto imported = arrow::ImportArray(batch.first.get(), batch.second.get());
        if (!imported.ok()) {
            reader->Close();
            return Status::Invalid("Cannot import planned data");
        }
        arrays.push_back(std::move(imported).ValueOrDie());
    }
    reader->Close();
    auto combined = arrow::Concatenate(arrays);
    if (!combined.ok()) {
        return Status::Invalid("Cannot concatenate planned data");
    }
    return std::move(combined).ValueOrDie();
}

// Deterministic independent OCF input writer: production never calls this encoder.
void Long(int64_t value, std::string* out) {
    uint64_t bits = (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
    while (bits >= 128) {
        out->push_back(static_cast<char>((bits & 127) | 128));
        bits >>= 7;
    }
    out->push_back(static_cast<char>(bits));
}
void Bytes(std::string_view bytes, std::string* out) {
    Long(bytes.size(), out);
    out->append(bytes);
}
std::string Ocf(std::string_view schema, const std::string& records, int64_t count,
                const std::string& codec = "null", bool unknown_size = false) {
    const std::string sync = "0123456789abcdef";
    std::string out("Obj\1", 4);
    Long(2, &out);
    Bytes("avro.schema", &out);
    Bytes(schema, &out);
    Bytes("avro.codec", &out);
    Bytes(codec, &out);
    Long(0, &out);
    out += sync;
    if (count != 0) {
        std::string block = records;
        if (codec == "zstandard") {
            block.resize(ZSTD_compressBound(records.size()));
            auto* compressor = ZSTD_createCCtx();
            EXPECT_NE(compressor, nullptr);
            EXPECT_FALSE(ZSTD_isError(
                ZSTD_CCtx_setParameter(compressor, ZSTD_c_contentSizeFlag, unknown_size ? 0 : 1)));
            size_t size = ZSTD_compress2(compressor, block.data(), block.size(), records.data(),
                                         records.size());
            ZSTD_freeCCtx(compressor);
            EXPECT_FALSE(ZSTD_isError(size));
            block.resize(size);
        }
        Long(count, &out);
        Bytes(block, &out);
        out += sync;
    }
    return out;
}
constexpr const char* kListSchema =
    R"({"type":"record","name":"List","fields":[{"name":"_VERSION","type":"int"},{"name":"_FILE_NAME","type":"string"}]})";
constexpr const char* kEntrySchema =
    R"({"type":"record","name":"Entry","fields":[{"name":"_VERSION","type":"int"},{"name":"_KIND","type":"int"},{"name":"_PARTITION","type":"bytes"},{"name":"_BUCKET","type":"int"},{"name":"_TOTAL_BUCKETS","type":"int"},{"name":"_FILE","type":{"type":"record","name":"File","fields":[{"name":"_FILE_NAME","type":"string"},{"name":"_FILE_SIZE","type":"long"},{"name":"_ROW_COUNT","type":"long"},{"name":"_SCHEMA_ID","type":"long"},{"name":"_LEVEL","type":"int"},{"name":"_MIN_SEQUENCE_NUMBER","type":"long"},{"name":"_MAX_SEQUENCE_NUMBER","type":"long"}]}}]})";
std::string Entry(int64_t kind = 0, std::string_view filename = "data.parquet") {
    std::string row;
    Long(2, &row);
    Long(kind, &row);
    Bytes(std::string(12, '\0'), &row);
    Long(0, &row);
    Long(-1, &row);
    Bytes(filename, &row);
    for (int64_t field : {10, 3, 0, 0, 0, 2}) {
        Long(field, &row);
    }
    return row;
}
void Synthetic(const std::string& path, const std::string& records, int64_t count,
               const std::string& codec = "null", bool unknown_size = false) {
    WriteText(
        path + "/snapshot/snapshot-1",
        R"({"id":1,"schemaId":0,"baseManifestList":"bounded-base","deltaManifestList":"bounded-delta"})");
    WriteText(path + "/manifest/bounded-base", Ocf(kListSchema, "", 0));
    std::string list;
    Long(2, &list);
    Bytes("bounded-manifest", &list);
    WriteText(path + "/manifest/bounded-delta", Ocf(kListSchema, list, 1));
    WriteText(path + "/manifest/bounded-manifest",
              Ocf(kEntrySchema, records, count, codec, unknown_size));
}

class FaultPool : public MemoryPool {
 public:
    void* Malloc(uint64_t size, uint64_t alignment) override {
        if (calls++ == fail_at) {
            throw std::bad_alloc();
        }
#ifdef PAIMON_TEST_PLANNING_ALLOCATIONS
        Observe(size);
#endif
        void* p = upstream->Malloc(size, alignment);
        live += size;
        peak = std::max(live, peak);
        return p;
    }
    void* Realloc(void*, size_t, size_t, uint64_t) override {
        throw std::bad_alloc();
    }
    void Free(void* p, uint64_t size) override {
        upstream->Free(p, size);
        live -= size;
    }
    uint64_t CurrentUsage() const override {
        return live;
    }
    uint64_t MaxMemoryUsage() const override {
        return peak;
    }
    std::shared_ptr<MemoryPool> upstream = GetDefaultPool();
    uint64_t calls = 0, live = 0, peak = 0;
    uint64_t fail_at = UINT64_MAX;
};
}  // namespace

TEST(PlanningBudgetTest, ExactAdmissionOverflowAndNonReplenishingFree) {
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(8));
    void* allocation = PlanningBudgetAccess::Allocate(budget, 8, 8);
    PlanningBudgetAccess::Free(budget, allocation, 8, 8);
    EXPECT_EQ(budget->BytesAdmitted(), 8);
    EXPECT_THROW((void)PlanningBudgetAccess::Allocate(budget, 1, 8), std::bad_alloc);
    EXPECT_THROW((void)PlanningBudgetAccess::Allocate(budget, 0, 8), std::bad_alloc);
    EXPECT_TRUE(PlanningBudgetAccess::Failure(budget).IsCapacityError());
    ASSERT_OK_AND_ASSIGN(auto huge, PlanningBudget::Create(UINT64_MAX));
    PlanningBudgetAccess::Reserve(huge, UINT64_MAX);
    EXPECT_THROW(PlanningBudgetAccess::Reserve(huge, 1), std::bad_alloc);
    EXPECT_EQ(huge->BytesAdmitted(), UINT64_MAX);
    EXPECT_FALSE(PlanningBudget::Create(1, nullptr).ok());
}
TEST(PlanningBudgetTest, SharedConcurrentAdmissionCannotOversubscribe) {
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(1000));
    std::atomic<uint64_t> accepted{0};
    std::vector<std::thread> workers;
    for (int32_t i = 0; i < 8; ++i) {
        workers.emplace_back([&] {
            for (int32_t j = 0; j < 200; ++j) {
                try {
                    PlanningBudgetAccess::Reserve(budget, 1);
                    ++accepted;
                } catch (const std::bad_alloc&) {
                    break;
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(accepted, 1000);
    EXPECT_EQ(budget->BytesAdmitted(), 1000);
}

#if defined(__linux__) && defined(__GLIBCXX__) && _GLIBCXX_USE_CXX11_ABI && _GLIBCXX_RELEASE >= 9
#ifdef PAIMON_TEST_PLANNING_ALLOCATIONS
TEST(BoundedTableScanTest, CalibrateStringAllocationObservationAndInjection) {
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
    PlanningBudgetAccess::Reserve(budget, 2049);
    requested = 0;
    ordinary_allocations = 0;
    observed_overdraft = false;
    observed_budget = budget.get();
    watching = true;
    std::string value(2048, 'x');
    watching = false;
    EXPECT_EQ(value.size(), 2048);
    EXPECT_EQ(requested, 2049);
    EXPECT_EQ(ordinary_allocations, 1);
    ordinary_allocations = 0;
    fail_ordinary_at = 0;
    watching = true;
    EXPECT_THROW((void)std::string(2048, 'x'), std::bad_alloc);
    watching = false;
    fail_ordinary_at = UINT64_MAX;
}

TEST(BoundedTableScanTest, ObserveRealAllocationRequestsAndFailEveryPublicationAllocation) {
    // Initialize fixed process-owned constants before observing query payload.
    (void)BinaryRow::EmptyRow();
    (void)SimpleStats::EmptyStats();
    auto path = Fixture("parquet");
    auto pool = std::make_shared<FaultPool>();
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget, pool));
    ASSERT_OK_AND_ASSIGN(auto context, Context(path, pool));
    requested = 0;
    ordinary_allocations = 0;
    observed_overdraft = false;
    observed_budget = budget.get();
    watching = true;
    auto scan = TableScan::CreateWithPlanningBudget(std::move(context), budget);
    // Retained input was constructed before observation. Consume all factory
    // admission before planning so that those credits cannot hide a missing
    // reservation in a later native publication constructor.
    if (scan.ok()) {
        requested = budget->BytesAdmitted();
    }
    auto plan =
        scan.ok() ? scan.value()->CreatePlan() : Result<std::shared_ptr<Plan>>(scan.status());
    watching = false;
    ASSERT_OK(plan);
    EXPECT_GT(requested, 0);
    EXPECT_GT(ordinary_allocations, 0);
    EXPECT_LE(requested, budget->BytesAdmitted());
    EXPECT_FALSE(observed_overdraft);
    const uint64_t publications = ordinary_allocations;
    std::move(plan).value().reset();
    std::move(scan).value().reset();
    EXPECT_EQ(pool->live, 0);
    for (uint64_t allocation = 0; allocation < publications; ++allocation) {
        SCOPED_TRACE(allocation);
        auto faulty_pool = std::make_shared<FaultPool>();
        ASSERT_OK_AND_ASSIGN(auto faulty_budget, PlanningBudget::Create(kAmpleBudget, faulty_pool));
        ASSERT_OK_AND_ASSIGN(auto faulty_context, Context(path, faulty_pool));
        requested = 0;
        ordinary_allocations = 0;
        fail_ordinary_at = allocation;
        observed_budget = faulty_budget.get();
        watching = true;
        auto faulty_scan =
            TableScan::CreateWithPlanningBudget(std::move(faulty_context), faulty_budget);
        auto failed = faulty_scan.ok() ? faulty_scan.value()->CreatePlan()
                                       : Result<std::shared_ptr<Plan>>(faulty_scan.status());
        watching = false;
        fail_ordinary_at = UINT64_MAX;
        ASSERT_FALSE(failed.ok());
        EXPECT_TRUE(failed.status().IsOutOfMemory());
        EXPECT_EQ(faulty_pool->live, 0);
    }
}
#endif

TEST(BoundedTableScanTest, RealParquetAndOrcPlanReadParityAndEscapedSplit) {
    for (const std::string format : {"parquet", "orc"}) {
        SCOPED_TRACE(format);
        auto path = Fixture(format);
        ASSERT_OK_AND_ASSIGN(auto context, Context(path));
        ASSERT_OK_AND_ASSIGN(auto ordinary, TableScan::Create(std::move(context)));
        ASSERT_OK_AND_ASSIGN(auto baseline, ordinary->CreatePlan());
        ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
        ASSERT_OK_AND_ASSIGN(auto bounded, BoundedPlan(path, budget));
        ASSERT_EQ(bounded->SnapshotId(), baseline->SnapshotId());
        ASSERT_FALSE(bounded->Splits().empty());
        std::vector<DataSplit::SimpleDataFileMeta> actual, expected;
        for (const auto& split : bounded->Splits()) {
            auto files = std::dynamic_pointer_cast<DataSplit>(split)->GetFileList();
            actual.insert(actual.end(), files.begin(), files.end());
        }
        for (const auto& split : baseline->Splits()) {
            auto files = std::dynamic_pointer_cast<DataSplit>(split)->GetFileList();
            expected.insert(expected.end(), files.begin(), files.end());
        }
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_EQ(actual[i].file_path, expected[i].file_path);
            EXPECT_EQ(actual[i].row_count, expected[i].row_count);
            EXPECT_EQ(actual[i].file_size, expected[i].file_size);
            EXPECT_EQ(actual[i].schema_id, expected[i].schema_id);
        }
        ASSERT_OK_AND_ASSIGN(auto actual_rows, ReadSplits(path, bounded->Splits()));
        ASSERT_OK_AND_ASSIGN(auto expected_rows, ReadSplits(path, baseline->Splits()));
        EXPECT_EQ(actual_rows->length(), format == "orc" ? 3 : 2);
        EXPECT_TRUE(actual_rows->Equals(expected_rows));
        auto ids = std::static_pointer_cast<arrow::Int32Array>(
            std::static_pointer_cast<arrow::StructArray>(actual_rows)->GetFieldByName("id"));
        ASSERT_TRUE(ids);
        for (int64_t row = 0; row < ids->length(); ++row) {
            EXPECT_FALSE(ids->IsNull(row));
            EXPECT_EQ(ids->Value(row), row + 1);
        }
        EXPECT_LE(budget->BytesAdmitted(), budget->ByteLimit());
        auto split = bounded->Splits().front();
        std::weak_ptr<PlanningBudget> weak = budget;
        bounded.reset();
        budget.reset();
        EXPECT_FALSE(weak.expired());
        ASSERT_OK_AND_ASSIGN(auto escaped_rows, ReadSplits(path, {split}));
        EXPECT_TRUE(escaped_rows->Equals(expected_rows));
        split.reset();
        EXPECT_TRUE(weak.expired());
    }
}
TEST(BoundedTableScanTest, ExactSuccessfulLimitAndOneByteShortFailClosed) {
    auto path = Fixture("parquet");
    ASSERT_OK_AND_ASSIGN(auto initial, PlanningBudget::Create(kAmpleBudget));
    ASSERT_OK(BoundedPlan(path, initial));
    auto needed = initial->BytesAdmitted();
    ASSERT_GT(needed, 0);
    ASSERT_OK_AND_ASSIGN(auto exact, PlanningBudget::Create(needed));
    ASSERT_OK(BoundedPlan(path, exact));
    ASSERT_EQ(exact->BytesAdmitted(), needed);
    ASSERT_OK_AND_ASSIGN(auto short_budget, PlanningBudget::Create(needed - 1));
    auto failed = BoundedPlan(path, short_budget);
    ASSERT_FALSE(failed.ok());
    EXPECT_TRUE(failed.status().IsCapacityError());
    EXPECT_LE(short_budget->BytesAdmitted(), needed - 1);
    auto retry = BoundedPlan(path, short_budget);
    EXPECT_FALSE(retry.ok());
    ASSERT_OK_AND_ASSIGN(auto zero, PlanningBudget::Create(0));
    EXPECT_TRUE(BoundedPlan(path, zero).status().IsCapacityError());
}
TEST(BoundedTableScanTest, AllocationFailureAtEveryArenaAllocationReleasesEverything) {
    auto path = Fixture("parquet");
    auto successful_pool = std::make_shared<FaultPool>();
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget, successful_pool));
    {
        ASSERT_OK_AND_ASSIGN(auto plan, BoundedPlan(path, budget));
        EXPECT_FALSE(plan->Splits().empty());
    }
    auto allocation_count = successful_pool->calls;
    EXPECT_EQ(successful_pool->live, 0);
    ASSERT_GT(allocation_count, 10);
    for (uint64_t allocation = 0; allocation < allocation_count; ++allocation) {
        SCOPED_TRACE(allocation);
        auto pool = std::make_shared<FaultPool>();
        pool->fail_at = allocation;
        ASSERT_OK_AND_ASSIGN(auto limited, PlanningBudget::Create(kAmpleBudget, pool));
        auto plan = BoundedPlan(path, limited);
        ASSERT_FALSE(plan.ok());
        EXPECT_TRUE(plan.status().IsOutOfMemory());
        EXPECT_EQ(pool->live, 0);
        EXPECT_LE(limited->BytesAdmitted(), limited->ByteLimit());
    }
}
TEST(BoundedTableScanTest, RefusesUnsupportedProfileAndIdentityRatherThanFallingBack) {
    auto path = Fixture("parquet");
    for (const auto& options :
         std::vector<std::map<std::string, std::string>>{{{"bucket", "1"}},
                                                         {{"branch", "other"}},
                                                         {{"scan.mode", "latest"}},
                                                         {{"scan.snapshot-id", "LATEST"}},
                                                         {{"data-evolution.enabled", "true"}},
                                                         {{"deletion-vectors.enabled", "true"}},
                                                         {{"manifest.format", "orc"}}}) {
        ASSERT_OK_AND_ASSIGN(auto context, Context(path, GetDefaultPool(), options));
        ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
        ASSERT_OK_AND_ASSIGN(auto scan,
                             TableScan::CreateWithPlanningBudget(std::move(context), budget));
        auto first = scan->CreatePlan();
        ASSERT_FALSE(first.ok());
        auto second = scan->CreatePlan();
        ASSERT_FALSE(second.ok());
        EXPECT_EQ(first.status().ToString(), second.status().ToString());
    }
    ASSERT_OK_AND_ASSIGN(auto context, Context(path));
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget, GetMemoryPool()));
    EXPECT_FALSE(TableScan::CreateWithPlanningBudget(std::move(context), budget).ok());
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    ASSERT_TRUE(TestUtil::CopyDirectory(path, directory->Str()));
    WriteText(directory->Str() + "/snapshot/snapshot-1", R"({"id":2,"schemaId":0})");
    ASSERT_OK_AND_ASSIGN(auto identity, PlanningBudget::Create(kAmpleBudget));
    EXPECT_FALSE(BoundedPlan(directory->Str(), identity).ok());
}
TEST(BoundedTableScanTest, OcfNullZstdDuplicateDeleteAndLateCorruption) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    ASSERT_TRUE(TestUtil::CopyDirectory(Fixture("parquet"), directory->Str()));
    auto path = directory->Str();
    for (const auto& [codec, unknown] : std::vector<std::pair<std::string, bool>>{
             {"null", false}, {"zstandard", false}, {"zstandard", true}}) {
        Synthetic(path, Entry(), 1, codec, unknown);
        ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
        ASSERT_OK_AND_ASSIGN(auto plan, BoundedPlan(path, budget));
        ASSERT_EQ(plan->Splits().size(), 1);
    }
    for (const auto& [records, count] : std::vector<std::pair<std::string, int64_t>>{
             {Entry() + Entry(), 2}, {Entry(1), 1}, {Entry() + Entry().substr(0, 3), 2}}) {
        Synthetic(path, records, count);
        ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
        EXPECT_FALSE(BoundedPlan(path, budget).ok());
    }
    Synthetic(path, Entry(), 1);
    auto valid = ReadText(path + "/manifest/bounded-manifest");
    for (size_t length : {size_t(0), size_t(4), valid.size() - 1}) {
        WriteText(path + "/manifest/bounded-manifest", valid.substr(0, length));
        ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
        EXPECT_FALSE(BoundedPlan(path, budget).ok());
    }
    valid.back() ^= 1;
    WriteText(path + "/manifest/bounded-manifest", valid);
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
    EXPECT_FALSE(BoundedPlan(path, budget).ok());
}
TEST(BoundedTableScanTest, RejectMalformedWriterTypesAndDuplicateFields) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    ASSERT_TRUE(TestUtil::CopyDirectory(Fixture("parquet"), directory->Str()));
    auto path = directory->Str();
    for (const std::string field : {"_DELETE_ROW_COUNT", "_EXTERNAL_PATH", "_FILE_SOURCE"}) {
        for (const std::string type : {"float", "double", "boolean"}) {
            SCOPED_TRACE(field + type);
            Synthetic(path, Entry(), 1);
            std::string schema = kEntrySchema;
            auto position = schema.rfind("]}}");
            ASSERT_NE(position, std::string::npos);
            schema.insert(position, ",{\"name\":\"" + field + "\",\"type\":\"" + type + "\"}");
            std::string row = Entry();
            row.append(type == "double" ? 8 : type == "float" ? 4 : 1, '\0');
            WriteText(path + "/manifest/bounded-manifest", Ocf(schema, row, 1));
            ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
            EXPECT_FALSE(BoundedPlan(path, budget).ok());
        }
    }
    Synthetic(path, Entry(), 1);
    std::string schema = kEntrySchema;
    schema.insert(schema.rfind("]}}"),
                  "," + std::string(R"({"name":"_FILE_NAME","type":"string"})"));
    std::string row = Entry();
    Bytes("other.parquet", &row);
    WriteText(path + "/manifest/bounded-manifest", Ocf(schema, row, 1));
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
    EXPECT_FALSE(BoundedPlan(path, budget).ok());
}

TEST(BoundedTableScanTest, RejectEmptyWrongTypesAndJsonAmbiguity) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    ASSERT_TRUE(TestUtil::CopyDirectory(Fixture("parquet"), directory->Str()));
    auto path = directory->Str();
    for (const std::string type :
         {R"({"type":"array","items":"string"})", R"({"type":"map","values":"string"})",
          R"({"type":"fixed","name":"NotAPath","size":0})", R"("bytes")", R"("string")"}) {
        Synthetic(path, Entry(), 1);
        std::string schema = kEntrySchema;
        schema.insert(schema.rfind("]}}"), ",{\"name\":\"_EXTERNAL_PATH\",\"type\":" + type + "}");
        std::string row = Entry();
        if (type.find("fixed") == std::string::npos) {
            Long(0, &row);
        }
        WriteText(path + "/manifest/bounded-manifest", Ocf(schema, row, 1));
        ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
        EXPECT_FALSE(BoundedPlan(path, budget).ok());
    }
    Synthetic(path, Entry(), 1);
    WriteText(
        path + "/snapshot/snapshot-1",
        R"({"id":1,"id":2,"schemaId":0,"baseManifestList":"bounded-base","deltaManifestList":"bounded-delta"})");
    ASSERT_OK_AND_ASSIGN(auto duplicate, PlanningBudget::Create(kAmpleBudget));
    EXPECT_FALSE(BoundedPlan(path, duplicate).ok());
    std::string huge_name(8 * 1024 * 1024, 'a');
    huge_name += ".parquet";
    Synthetic(path, Entry(0, huge_name), 1, "zstandard");
    ASSERT_OK_AND_ASSIGN(auto small, PlanningBudget::Create(4 * 1024 * 1024));
    auto exhausted = BoundedPlan(path, small);
    ASSERT_FALSE(exhausted.ok());
    EXPECT_TRUE(exhausted.status().IsCapacityError());
    EXPECT_LE(small->BytesAdmitted(), small->ByteLimit());
}

TEST(BoundedTableScanTest, RejectNonRecordEmptyWriterAndFixedFileName) {
    auto directory = UniqueTestDirectory::Create();
    ASSERT_TRUE(directory);
    ASSERT_TRUE(TestUtil::CopyDirectory(Fixture("parquet"), directory->Str()));
    const auto path = directory->Str();
    Synthetic(path, Entry(), 1);
    std::string bad_root = kListSchema;
    const auto root_type = bad_root.find("record");
    ASSERT_NE(root_type, std::string::npos);
    bad_root.replace(root_type, 6, "int");
    WriteText(path + "/manifest/bounded-base", Ocf(bad_root, "", 0));
    ASSERT_OK_AND_ASSIGN(auto non_record, PlanningBudget::Create(kAmpleBudget));
    EXPECT_FALSE(BoundedPlan(path, non_record).ok());

    Synthetic(path, Entry(), 1);
    std::string schema = kEntrySchema;
    const std::string original = R"({"name":"_FILE_NAME","type":"string"})";
    const auto field = schema.find(original);
    ASSERT_NE(field, std::string::npos);
    schema.replace(field, original.size(),
                   R"({"name":"_FILE_NAME","type":{"type":"fixed","name":"FixedName","size":12}})");
    std::string row = Entry();
    auto name = row.find("data.parquet");
    ASSERT_NE(name, std::string::npos);
    ASSERT_GT(name, 0);
    row.erase(name - 1, 1);  // fixed has no string-length prefix.
    WriteText(path + "/manifest/bounded-manifest", Ocf(schema, row, 1));
    ASSERT_OK_AND_ASSIGN(auto fixed_name, PlanningBudget::Create(kAmpleBudget));
    EXPECT_FALSE(BoundedPlan(path, fixed_name).ok());
}

#else
TEST(BoundedTableScanTest, UnqualifiedPlatformFailsExplicitly) {
    ASSERT_OK_AND_ASSIGN(auto budget, PlanningBudget::Create(kAmpleBudget));
    auto result = TableScan::CreateWithPlanningBudget(nullptr, budget);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().IsNotImplemented());
}
#endif
}  // namespace paimon::test
