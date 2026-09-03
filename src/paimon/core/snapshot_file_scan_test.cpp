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

#include "paimon/snapshot/snapshot_file_scan.h"

#include <algorithm>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/defs.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

Result<std::set<std::string>> ListFiles(const std::string& table_path,
                                        const std::optional<int64_t>& snapshot_id = std::nullopt,
                                        const std::shared_ptr<ScanFilter>& scan_filter = nullptr,
                                        const std::string& branch = "main") {
    return SnapshotFileScan::ListFiles(table_path, branch, snapshot_id, scan_filter, /*options=*/{},
                                       /*file_system=*/nullptr, /*executor=*/nullptr,
                                       /*memory_pool=*/nullptr);
}

std::shared_ptr<ScanFilter> CreateFilter(
    const std::vector<std::map<std::string, std::string>>& partition_filters,
    const std::optional<int32_t>& bucket_filter = std::nullopt,
    const std::shared_ptr<Predicate>& predicate = nullptr) {
    return std::make_shared<ScanFilter>(predicate, partition_filters, bucket_filter);
}

std::set<std::string> ExpectedFiles(const std::string& table_path,
                                    std::initializer_list<std::string> relative_paths) {
    std::set<std::string> paths;
    for (const std::string& relative_path : relative_paths) {
        paths.insert(table_path + "/" + relative_path);
    }
    return paths;
}

}  // namespace

TEST(SnapshotFileScanTest, TestLatestSnapshotAndBucketFilter) {
    std::string table_path = GetDataDir() + "/orc/append_09.db/append_09";

    ASSERT_OK_AND_ASSIGN(std::set<std::string> all_files, ListFiles(table_path));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"f1=10/bucket-0/data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc",
                                   "f1=10/bucket-1/data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc",
                                   "f1=20/bucket-0/data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc",
                                   "f1=20/bucket-0/data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                                   "manifest/manifest-3a44a0da-1008-463c-914e-28d271375e24-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1",
                                   "manifest/manifest-c5904353-0236-46a2-891f-62a326dd8e5e-0",
                                   "manifest/manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-3",
                                   "schema/schema-0", "snapshot/snapshot-5"}),
        all_files);

    std::shared_ptr<ScanFilter> bucket_zero_filter =
        CreateFilter(/*partition_filters=*/{}, /*bucket_filter=*/0);
    ASSERT_OK_AND_ASSIGN(std::set<std::string> bucket_zero_files,
                         ListFiles(table_path, std::nullopt, bucket_zero_filter));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"f1=10/bucket-0/data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc",
                                   "f1=20/bucket-0/data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc",
                                   "f1=20/bucket-0/data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                                   "manifest/manifest-3a44a0da-1008-463c-914e-28d271375e24-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1",
                                   "manifest/manifest-c5904353-0236-46a2-891f-62a326dd8e5e-0",
                                   "manifest/manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-3",
                                   "schema/schema-0", "snapshot/snapshot-5"}),
        bucket_zero_files);

    std::shared_ptr<ScanFilter> bucket_one_filter =
        CreateFilter(/*partition_filters=*/{}, /*bucket_filter=*/1);
    ASSERT_OK_AND_ASSIGN(std::set<std::string> bucket_one_files,
                         ListFiles(table_path, std::nullopt, bucket_one_filter));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"f1=10/bucket-1/data-b9e7c41f-66e8-4dad-b25a-e6e1963becc4-0.orc",
                                   "manifest/manifest-3a44a0da-1008-463c-914e-28d271375e24-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1",
                                   "manifest/manifest-c5904353-0236-46a2-891f-62a326dd8e5e-0",
                                   "manifest/manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-3",
                                   "schema/schema-0", "snapshot/snapshot-5"}),
        bucket_one_files);
}

TEST(SnapshotFileScanTest, TestExplicitSnapshot) {
    std::string table_path = GetDataDir() + "/orc/append_09.db/append_09";

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files, ListFiles(table_path, /*snapshot_id=*/1));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"f1=10/bucket-0/data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc",
                                   "f1=10/bucket-1/data-4e30d6c0-f109-4300-a010-4ba03047dd9d-0.orc",
                                   "f1=20/bucket-0/data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc",
                                   "manifest/manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0",
                                   "manifest/manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-0",
                                   "manifest/manifest-list-616d1847-a02c-495f-9cca-2c8b7def0fec-1",
                                   "schema/schema-0", "snapshot/snapshot-1"}),
        files);
}

TEST(SnapshotFileScanTest, TestExplicitSnapshotIncludesLatestSchema) {
    std::string table_path =
        GetDataDir() + "/orc/append_table_with_alter_table.db/append_table_with_alter_table";

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files, ListFiles(table_path, /*snapshot_id=*/1));
    ASSERT_EQ(
        ExpectedFiles(table_path,
                      {"key0=0/key1=1/bucket-0/data-2190cec3-ce87-4175-8d19-9268becf4440-0.orc",
                       "key0=1/key1=1/bucket-0/data-492ed5ab-4740-4e93-8a0a-79a6893b1770-0.orc",
                       "manifest/manifest-f2299c3d-c3f1-400f-ad3d-124e3a342389-0",
                       "manifest/manifest-list-83964df9-8a98-4f91-a4e9-05f3e07be3f9-0",
                       "manifest/manifest-list-83964df9-8a98-4f91-a4e9-05f3e07be3f9-1",
                       "schema/schema-0", "schema/schema-1", "snapshot/snapshot-1"}),
        files);
}

TEST(SnapshotFileScanTest, TestExternalFileIndex) {
    std::string table_path =
        GetDataDir() + "/orc/append_with_bloomfilter.db/append_with_bloomfilter";

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files, ListFiles(table_path));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"bucket-0/data-34e8acb2-110b-4c32-9dc6-d6435178d0ad-0.orc",
                                   "bucket-0/data-34e8acb2-110b-4c32-9dc6-d6435178d0ad-0.orc.index",
                                   "manifest/manifest-7a45ff6a-e225-4f4c-8141-dc5c88a2a81f-0",
                                   "manifest/manifest-list-13cc1371-7074-42aa-83c4-94ce8b2819c3-0",
                                   "manifest/manifest-list-13cc1371-7074-42aa-83c4-94ce8b2819c3-1",
                                   "schema/schema-0", "snapshot/snapshot-1"}),
        files);
}

TEST(SnapshotFileScanTest, TestExternalDataPath) {
    std::string table_path = GetDataDir() +
                             "/orc/pk_dv_index_not_in_data_with_external.db/"
                             "pk_dv_index_not_in_data_with_external";
    std::shared_ptr<ScanFilter> scan_filter =
        CreateFilter(/*partition_filters=*/{{{"f1", "20"}}}, /*bucket_filter=*/0);

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files,
                         ListFiles(table_path, /*snapshot_id=*/1, scan_filter));
    const std::string external_file_suffix =
        "/external/f1=20/bucket-0/data-8b1ebe94-3177-4805-b74b-ae5e1bb1086f-0.orc";
    auto external_file =
        std::find_if(files.begin(), files.end(), [&external_file_suffix](const std::string& path) {
            return StringUtils::EndsWith(path, external_file_suffix);
        });
    ASSERT_NE(files.end(), external_file);
    files.erase(external_file);
    ASSERT_EQ(
        ExpectedFiles(table_path, {"manifest/manifest-0dff6454-a796-469a-8452-81a7601d1b34-0",
                                   "manifest/manifest-list-0ef45e5c-7b8b-42f6-b3a1-6d16bc9f522a-0",
                                   "manifest/manifest-list-0ef45e5c-7b8b-42f6-b3a1-6d16bc9f522a-1",
                                   "schema/schema-0", "snapshot/snapshot-1"}),
        files);
}

TEST(SnapshotFileScanTest, TestPartitionAndBucketFilter) {
    std::string table_path = GetDataDir() + "/orc/append_09.db/append_09";
    std::shared_ptr<ScanFilter> scan_filter =
        CreateFilter(/*partition_filters=*/{{{"f1", "10"}}}, /*bucket_filter=*/0);

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files,
                         ListFiles(table_path, std::nullopt, scan_filter));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"f1=10/bucket-0/data-d41fd7d1-b3e4-4905-aad9-b20a780e90a2-0.orc",
                                   "manifest/manifest-3a44a0da-1008-463c-914e-28d271375e24-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-0",
                                   "manifest/manifest-3ea5ee21-d399-4f1c-a749-2fc63dbf0852-1",
                                   "manifest/manifest-c5904353-0236-46a2-891f-62a326dd8e5e-0",
                                   "manifest/manifest-f8b15cfc-437a-4d21-a6a0-e45b639ae7ed-0",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-3",
                                   "schema/schema-0", "snapshot/snapshot-5"}),
        files);
}

TEST(SnapshotFileScanTest, TestPartitionFilterWithoutMatchingEntries) {
    std::string table_path = GetDataDir() + "/orc/append_09.db/append_09";
    std::shared_ptr<ScanFilter> scan_filter = CreateFilter(/*partition_filters=*/{{{"f1", "999"}}});

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files,
                         ListFiles(table_path, std::nullopt, scan_filter));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-2",
                                   "manifest/manifest-list-f2d59cb8-3ec6-4860-b34b-050b1a533416-3",
                                   "schema/schema-0", "snapshot/snapshot-5"}),
        files);
}

TEST(SnapshotFileScanTest, TestDeletionVectorIndexBucketFilter) {
    std::string table_path = GetDataDir() +
                             "/orc/pk_table_with_dv_cardinality.db/"
                             "pk_table_with_dv_cardinality";

    std::shared_ptr<ScanFilter> bucket_zero_filter =
        CreateFilter(/*partition_filters=*/{}, /*bucket_filter=*/0);
    ASSERT_OK_AND_ASSIGN(std::set<std::string> bucket_zero_files,
                         ListFiles(table_path, /*snapshot_id=*/4, bucket_zero_filter));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"f1=10/bucket-0/data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-0.orc",
                                   "f1=10/bucket-0/data-0d0f29cc-63c6-4fab-a594-71bd7d06fcde-1.orc",
                                   "index/index-86356766-3238-46e6-990b-656cd7409eaa-0",
                                   "manifest/index-manifest-59bcb792-830f-4b57-b838-cfcc50d29266-0",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-0",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-1",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-2",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-3",
                                   "manifest/manifest-list-540cf68e-698e-4b66-af30-eb558d8db43d-6",
                                   "manifest/manifest-list-540cf68e-698e-4b66-af30-eb558d8db43d-7",
                                   "schema/schema-0", "snapshot/snapshot-4"}),
        bucket_zero_files);

    std::shared_ptr<ScanFilter> bucket_one_filter =
        CreateFilter(/*partition_filters=*/{}, /*bucket_filter=*/1);
    ASSERT_OK_AND_ASSIGN(std::set<std::string> bucket_one_files,
                         ListFiles(table_path, /*snapshot_id=*/4, bucket_one_filter));
    ASSERT_EQ(
        ExpectedFiles(table_path, {"f1=10/bucket-1/data-2ffe7ae9-2cf7-41e9-944b-2065585cde31-0.orc",
                                   "index/index-86356766-3238-46e6-990b-656cd7409eaa-1",
                                   "manifest/index-manifest-59bcb792-830f-4b57-b838-cfcc50d29266-0",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-0",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-1",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-2",
                                   "manifest/manifest-1d9dbf4f-667b-4a05-ad5a-f7fed8908aa4-3",
                                   "manifest/manifest-list-540cf68e-698e-4b66-af30-eb558d8db43d-6",
                                   "manifest/manifest-list-540cf68e-698e-4b66-af30-eb558d8db43d-7",
                                   "schema/schema-0", "snapshot/snapshot-4"}),
        bucket_one_files);
}

TEST(SnapshotFileScanTest, TestGlobalIndex) {
    std::string table_path =
        GetDataDir() + "/orc/append_with_global_index.db/append_with_global_index";

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files, ListFiles(table_path, /*snapshot_id=*/4));
    ASSERT_EQ(ExpectedFiles(table_path,
                            {"bucket-0/data-2430f01c-b947-48dc-82a8-7c60aaa348e4-0.orc",
                             "index/bitmap-global-index-0c950cb6-e6e9-46cd-a9a9-cfcd55f870d3.index",
                             "index/bitmap-global-index-21ac35d9-200a-489d-b649-ec241f832345.index",
                             "index/bitmap-global-index-8b1bed37-31c5-4288-8c46-3d0d30fbd302.index",
                             "manifest/index-manifest-795a9d8a-60bf-401d-ab02-6f13f8ca1098-0",
                             "manifest/manifest-65b0d403-a1bc-4157-b242-bff73c46596d-0",
                             "manifest/manifest-list-2bccccf8-9f5e-48f2-b706-5b33f8c3bfc0-0",
                             "manifest/manifest-list-2bccccf8-9f5e-48f2-b706-5b33f8c3bfc0-1",
                             "schema/schema-0", "snapshot/snapshot-4"}),
              files);
}

TEST(SnapshotFileScanTest, TestBranch) {
    std::string table_path = GetDataDir() +
                             "/orc/append_table_with_append_pt_branch.db/"
                             "append_table_with_append_pt_branch";

    ASSERT_OK_AND_ASSIGN(std::set<std::string> files,
                         ListFiles(table_path, std::nullopt, /*scan_filter=*/nullptr, "test"));
    ASSERT_EQ(
        ExpectedFiles(table_path,
                      {"branch/branch-test/schema/schema-0", "branch/branch-test/schema/schema-1",
                       "branch/branch-test/snapshot/snapshot-2",
                       "manifest/manifest-4e72f3a9-4ad2-4ce3-a387-febef513ee24-0",
                       "manifest/manifest-529f38e6-ae67-4a61-83fa-ede384f720e4-0",
                       "manifest/manifest-list-10f6b65d-7580-42b7-9c56-c9c46bf2a3bd-0",
                       "manifest/manifest-list-10f6b65d-7580-42b7-9c56-c9c46bf2a3bd-1",
                       "pt=2/bucket-0/data-150a7c45-2972-4d8c-983e-a1ab2e382e6a-0.orc",
                       "pt=2/bucket-0/data-ea107991-e78c-445a-b225-7af07bcdf8c3-0.orc"}),
        files);
}

TEST(SnapshotFileScanTest, TestInvalidArguments) {
    ASSERT_NOK_WITH_MSG(ListFiles(""), "table path is empty");
    ASSERT_NOK_WITH_MSG(ListFiles("unused", /*snapshot_id=*/0),
                        "snapshot id must be greater than or equal to 1");
    std::shared_ptr<Predicate> predicate =
        PredicateBuilder::IsNull(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING);
    std::shared_ptr<ScanFilter> scan_filter =
        CreateFilter(/*partition_filters=*/{}, /*bucket_filter=*/std::nullopt, predicate);
    ASSERT_NOK_WITH_MSG(ListFiles("unused", std::nullopt, scan_filter),
                        "snapshot file scan does not support predicate filter");
}

}  // namespace paimon::test
