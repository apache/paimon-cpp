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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "paimon/core/operation/commit/realtime_commit_properties.h"

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/fs/file_system.h"
#include "paimon/macros.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
namespace {

using Properties = std::map<std::string, std::string>;

const char kTargetJson[] = R"({
    "version": 1,
    "offsets": [
        {
            "partition": {},
            "bucket": 0,
            "offset": 7
        },
        {
            "partition": {
                "dt": "2"
            },
            "bucket": 2,
            "offset": 9
        },
        {
            "partition": {
                "dt": "a/b",
                "region": "cn"
            },
            "bucket": 10,
            "offset": 12
        }
    ]
})";

class RealtimeCommitPropertiesTest : public testing::Test {
 protected:
    void SetUp() override {
        directory_ = UniqueTestDirectory::Create();
        ASSERT_NE(nullptr, directory_);
        file_system_ = directory_->GetFileSystem();
        ASSERT_NE(nullptr, file_system_);
    }

    Snapshot MakeSnapshot(
        const std::optional<std::map<std::string, std::string>>& properties) const {
        return Snapshot(
            /*id=*/1,
            /*schema_id=*/1,
            /*base_manifest_list=*/"base-manifest-list",
            /*base_manifest_list_size=*/std::nullopt,
            /*delta_manifest_list=*/"delta-manifest-list",
            /*delta_manifest_list_size=*/std::nullopt,
            /*changelog_manifest_list=*/std::nullopt,
            /*changelog_manifest_list_size=*/std::nullopt,
            /*index_manifest=*/std::nullopt,
            /*commit_user=*/"test-user",
            /*commit_identifier=*/1, Snapshot::CommitKind::Append(),
            /*time_millis=*/0,
            /*total_record_count=*/0,
            /*delta_record_count=*/0,
            /*changelog_record_count=*/std::nullopt,
            /*watermark=*/std::nullopt,
            /*statistics=*/std::nullopt, properties,
            /*next_row_id=*/std::nullopt);
    }

    Result<std::string> WriteJson(const std::string& json) {
        std::string path =
            directory_->Str() + "/input-" + std::to_string(next_file_id_++) + ".offsets";
        PAIMON_RETURN_NOT_OK(file_system_->WriteFile(path, json, /*overwrite=*/false));
        return path;
    }

    Result<RealtimeOffsetMap> ReadJson(const std::string& json) {
        PAIMON_ASSIGN_OR_RAISE(std::string path, WriteJson(json));
        std::map<std::string, std::string> properties = {
            {RealtimeCommitProperties::kOffsetsKey, path}};
        return RealtimeCommitProperties::ReadOffsets(
            std::optional<Snapshot>(MakeSnapshot(properties)), file_system_);
    }

    Result<Snapshot> MakeSnapshotWithOffsets(const RealtimeOffsetMap& offsets) {
        PAIMON_ASSIGN_OR_RAISE(std::string json,
                               RealtimeCommitProperties::SerializeOffsets(offsets));
        PAIMON_ASSIGN_OR_RAISE(std::string path, WriteJson(json));
        return MakeSnapshot(Properties{{RealtimeCommitProperties::kOffsetsKey, path}});
    }

    std::unique_ptr<UniqueTestDirectory> directory_;
    std::shared_ptr<FileSystem> file_system_;
    int32_t next_file_id_ = 0;
};

RealtimeOffsetMap TargetOffsets() {
    return {{RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0), 7},
            {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/2), 9},
            {RealtimePartitionBucket({{"dt", "a/b"}, {"region", "cn"}}, /*bucket=*/10), 12}};
}

}  // namespace

TEST_F(RealtimeCommitPropertiesTest, SerializeAndDeserializeTargetJson) {
    RealtimeOffsetMap expected = TargetOffsets();
    ASSERT_OK_AND_ASSIGN(std::string actual_json,
                         RealtimeCommitProperties::SerializeOffsets(expected));
    ASSERT_EQ(kTargetJson, actual_json);

    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap actual, ReadJson(kTargetJson));
    ASSERT_EQ(expected, actual);
    ASSERT_OK_AND_ASSIGN(std::string round_trip_json,
                         RealtimeCommitProperties::SerializeOffsets(actual));
    ASSERT_EQ(kTargetJson, round_trip_json);
}

TEST_F(RealtimeCommitPropertiesTest, SerializeEmptyOffsets) {
    ASSERT_OK_AND_ASSIGN(std::string actual_json,
                         RealtimeCommitProperties::SerializeOffsets(/*offsets=*/{}));
    ASSERT_EQ(R"({
    "version": 1,
    "offsets": []
})",
              actual_json);
}

TEST_F(RealtimeCommitPropertiesTest, SerializeRejectsInvalidOffsets) {
    RealtimeOffsetMap negative_bucket = {{RealtimePartitionBucket({{"dt", "2"}}, -1), 0}};
    ASSERT_NOK_WITH_MSG(RealtimeCommitProperties::SerializeOffsets(negative_bucket),
                        "invalid bucket -1");

    RealtimeOffsetMap negative_offset = {
        {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/0), -1}};
    ASSERT_NOK_WITH_MSG(RealtimeCommitProperties::SerializeOffsets(negative_offset),
                        "invalid offset -1");
}

TEST_F(RealtimeCommitPropertiesTest, PartitionBucketAndOffsetsDirectory) {
    RealtimePartitionBucket expected_partition_bucket({{"dt", "a/b"}, {"region", "cn"}}, 3);
    ASSERT_EQ(expected_partition_bucket,
              RealtimePartitionBucket({{"dt", "a/b"}, {"region", "cn"}}, /*bucket=*/3));
    ASSERT_NE(expected_partition_bucket,
              RealtimePartitionBucket({{"dt", "a/b"}, {"region", "cn"}}, /*bucket=*/4));
    ASSERT_EQ("/table/metadata", RealtimeCommitProperties::OffsetsDirectory("/table", "main"));
    ASSERT_EQ("/table/branch/branch-dev/metadata",
              RealtimeCommitProperties::OffsetsDirectory("/table", "dev"));
}

TEST_F(RealtimeCommitPropertiesTest, ReadOffsetsWithoutProgress) {
    ASSERT_OK_AND_ASSIGN(
        RealtimeOffsetMap no_snapshot,
        RealtimeCommitProperties::ReadOffsets(/*snapshot=*/std::nullopt, file_system_));
    ASSERT_TRUE(no_snapshot.empty());

    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap no_properties,
                         RealtimeCommitProperties::ReadOffsets(
                             std::optional<Snapshot>(MakeSnapshot(std::nullopt)), file_system_));
    ASSERT_TRUE(no_properties.empty());

    std::map<std::string, std::string> unrelated_properties = {{"other", "value"}};
    ASSERT_OK_AND_ASSIGN(
        RealtimeOffsetMap no_offsets_property,
        RealtimeCommitProperties::ReadOffsets(
            std::optional<Snapshot>(MakeSnapshot(unrelated_properties)), file_system_));
    ASSERT_TRUE(no_offsets_property.empty());
}

TEST_F(RealtimeCommitPropertiesTest, ReadOffsetsRequiresFileSystem) {
    std::map<std::string, std::string> properties = {
        {RealtimeCommitProperties::kOffsetsKey, "metadata/test.offsets"}};
    ASSERT_NOK_WITH_MSG(
        RealtimeCommitProperties::ReadOffsets(std::optional<Snapshot>(MakeSnapshot(properties)),
                                              /*file_system=*/nullptr),
        "file system is null");
}

TEST_F(RealtimeCommitPropertiesTest, ReadOffsetsPropagatesFileError) {
    std::map<std::string, std::string> properties = {
        {RealtimeCommitProperties::kOffsetsKey, directory_->Str() + "/missing.offsets"}};
    ASSERT_NOK(RealtimeCommitProperties::ReadOffsets(
        std::optional<Snapshot>(MakeSnapshot(properties)), file_system_));
}

TEST_F(RealtimeCommitPropertiesTest, DeserializeRejectsInvalidJson) {
    const std::vector<std::pair<std::string, std::string>> invalid_json_cases = {
        {"{", "deserialize failed"},
        {R"([])", "value must be an object"},
        {R"({"offsets":[]})", "key must exist"},
        {R"({"version":2,"offsets":[]})", "unsupported offsets version 2"},
        {R"({"version":1,"offsets":{}})", "value must be an array"},
        {R"({"version":1,"offsets":[{"bucket":0,"offset":1}]})", "key must exist"},
        {R"({"version":1,"offsets":[{"partition":{},"bucket":0}]})", "key must exist"},
        {R"({"version":1,"offsets":[{"partition":"dt=2","bucket":0,"offset":1}]})",
         "value must be an object"},
        {R"({"version":1,"offsets":[{"partition":{"dt":2},"bucket":0,"offset":1}]})",
         "value must be string"},
        {R"({"version":1,"offsets":[{"partition":{},"bucket":"0","offset":1}]})",
         "value must be int"},
        {R"({"version":1,"offsets":[{"partition":{},"bucket":0,"offset":"1"}]})",
         "value must be int64"},
        {R"({"version":1,"offsets":[{"partition":{},"bucket":-1,"offset":1}]})",
         "invalid bucket -1"},
        {R"({"version":1,"offsets":[{"partition":{},"bucket":0,"offset":-1}]})",
         "invalid offset -1"},
        {R"({"version":1,"offsets":[{"partition":{"dt":"a/b"},"bucket":0,"offset":1},{"partition":{"dt":"a/b"},"bucket":0,"offset":2}]})",
         "duplicate partition-bucket 0"}};

    for (const auto& [json, expected_error] : invalid_json_cases) {
        SCOPED_TRACE(json);
        ASSERT_NOK_WITH_MSG(ReadJson(json), expected_error);
    }
}

TEST_F(RealtimeCommitPropertiesTest, SortProgress) {
    std::vector<RealtimeCommitProgress> commits = {
        {/*commit_message=*/nullptr, RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/0),
         OffsetRange(2, 4)},
        {/*commit_message=*/nullptr, RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/1),
         OffsetRange(5, 7)},
        {/*commit_message=*/nullptr, RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/0),
         OffsetRange(0, 2)}};

    RealtimeCommitProperties::Sort(&commits);
    ASSERT_EQ(OffsetRange(0, 2), commits[0].offset_range);
    ASSERT_EQ(OffsetRange(2, 4), commits[1].offset_range);
    ASSERT_EQ(OffsetRange(5, 7), commits[2].offset_range);
}

TEST_F(RealtimeCommitPropertiesTest, BuildRejectsInvalidProgress) {
    RealtimePartitionBucket bucket0({{"dt", "2"}}, /*bucket=*/0);
    RealtimeOffsetMap committed_offsets = {{bucket0, 1}};
    ASSERT_OK_AND_ASSIGN(Snapshot latest_snapshot, MakeSnapshotWithOffsets(committed_offsets));

    std::map<RealtimePartitionBucket, OffsetRange> invalid_bucket = {
        {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/-1), OffsetRange(0, 1)}};
    ASSERT_NOK_WITH_MSG(
        RealtimeCommitProperties::Build(/*properties=*/{}, /*latest_snapshot=*/std::nullopt,
                                        invalid_bucket, file_system_, directory_->Str(), "main"),
        "bucket -1 is invalid");

    std::map<RealtimePartitionBucket, OffsetRange> gap = {
        {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/0), OffsetRange(3, 5)}};
    ASSERT_NOK_WITH_MSG(RealtimeCommitProperties::Build(/*properties=*/{}, latest_snapshot, gap,
                                                        file_system_, directory_->Str(), "main"),
                        "are not contiguous");

    std::map<RealtimePartitionBucket, OffsetRange> overlap = {
        {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/0), OffsetRange(0, 2)}};
    ASSERT_NOK_WITH_MSG(RealtimeCommitProperties::Build(/*properties=*/{}, latest_snapshot, overlap,
                                                        file_system_, directory_->Str(), "main"),
                        "are not contiguous");

    RealtimeOffsetMap exhausted_offsets = {{bucket0, std::numeric_limits<int64_t>::max()}};
    ASSERT_OK_AND_ASSIGN(Snapshot exhausted_snapshot, MakeSnapshotWithOffsets(exhausted_offsets));
    std::map<RealtimePartitionBucket, OffsetRange> after_max = {
        {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/0),
         OffsetRange(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max())}};
    ASSERT_NOK_WITH_MSG(
        RealtimeCommitProperties::Build(/*properties=*/{}, exhausted_snapshot, after_max,
                                        file_system_, directory_->Str(), "main"),
        "offset range is invalid");
}

TEST_F(RealtimeCommitPropertiesTest, BuildWithoutProgress) {
    std::string latest_offsets_path = directory_->Str() + "/latest.offsets";
    std::map<std::string, std::string> latest_properties = {
        {RealtimeCommitProperties::kOffsetsKey, latest_offsets_path}};
    std::map<std::string, std::string> properties = {{"custom", "value"}};

    ASSERT_OK_AND_ASSIGN(Properties inherited,
                         RealtimeCommitProperties::Build(
                             properties, std::optional<Snapshot>(MakeSnapshot(latest_properties)),
                             /*realtime_ranges=*/{}, /*file_system=*/nullptr,
                             /*table_root=*/"", /*branch=*/"main"));
    ASSERT_EQ("value", inherited.at("custom"));
    ASSERT_EQ(latest_offsets_path, inherited.at(RealtimeCommitProperties::kOffsetsKey));

    ASSERT_OK_AND_ASSIGN(Properties unchanged, RealtimeCommitProperties::Build(
                                                   properties, /*latest_snapshot=*/std::nullopt,
                                                   /*realtime_ranges=*/{}, /*file_system=*/nullptr,
                                                   /*table_root=*/"", /*branch=*/"main"));
    ASSERT_EQ(properties, unchanged);
}

TEST_F(RealtimeCommitPropertiesTest, BuildWritesMergedProgress) {
    ASSERT_OK_AND_ASSIGN(std::string latest_offsets_path, WriteJson(kTargetJson));
    std::map<std::string, std::string> latest_properties = {
        {RealtimeCommitProperties::kOffsetsKey, latest_offsets_path}};
    std::map<RealtimePartitionBucket, OffsetRange> ranges = {
        {RealtimePartitionBucket({{"dt", "3"}}, /*bucket=*/0), OffsetRange(0, 5)},
        {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/2), OffsetRange(9, 12)},
        {RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0), OffsetRange(7, 9)}};
    std::map<std::string, std::string> properties = {{"custom", "value"}};

    ASSERT_OK_AND_ASSIGN(Properties merged,
                         RealtimeCommitProperties::Build(
                             properties, std::optional<Snapshot>(MakeSnapshot(latest_properties)),
                             ranges, file_system_, directory_->Str(), "main"));
    ASSERT_EQ("value", merged.at("custom"));
    ASSERT_NE(latest_offsets_path, merged.at(RealtimeCommitProperties::kOffsetsKey));

    ASSERT_OK_AND_ASSIGN(RealtimeOffsetMap actual,
                         RealtimeCommitProperties::ReadOffsets(
                             std::optional<Snapshot>(MakeSnapshot(merged)), file_system_));
    RealtimeOffsetMap expected = TargetOffsets();
    expected[RealtimePartitionBucket(/*partition=*/{}, /*bucket=*/0)] = 9;
    expected[RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/2)] = 12;
    expected[RealtimePartitionBucket({{"dt", "3"}}, /*bucket=*/0)] = 5;
    ASSERT_EQ(expected, actual);
}

TEST_F(RealtimeCommitPropertiesTest, BuildRequiresFileSystem) {
    std::map<RealtimePartitionBucket, OffsetRange> ranges = {
        {RealtimePartitionBucket({{"dt", "2"}}, /*bucket=*/0), OffsetRange(0, 2)}};
    ASSERT_NOK_WITH_MSG(RealtimeCommitProperties::Build(
                            /*properties=*/{}, /*latest_snapshot=*/std::nullopt, ranges,
                            /*file_system=*/nullptr, directory_->Str(), "main"),
                        "file system is null");
}

}  // namespace paimon::test
