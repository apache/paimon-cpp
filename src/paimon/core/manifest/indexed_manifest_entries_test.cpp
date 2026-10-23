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

#include "paimon/core/manifest/indexed_manifest_entries.h"

#include "gtest/gtest.h"
#include "paimon/core/manifest/manifest_entry_serializer.h"
#include "paimon/core/manifest/snapshot_live_manifest_entries.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class IndexedManifestEntriesTest : public ::testing::Test {
 protected:
    Result<ManifestEntry> Entry(int32_t partition, int32_t bucket, int32_t total_buckets,
                                const std::string& name, const FileKind& kind = FileKind::Add()) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<DataFileMeta> meta,
            DataFileMeta::ForAppend(name, 100, 10, SimpleStats::EmptyStats(), 0, 0, 0, std::nullopt,
                                    std::nullopt, std::nullopt, std::nullopt, std::nullopt));
        return ManifestEntry(kind, BinaryRowGenerator::GenerateRow({partition}, pool_.get()),
                             bucket, total_buckets, meta);
    }
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(IndexedManifestEntriesTest, GroupsPruneBeforeDecodingAndPreserveOrder) {
    ASSERT_OK_AND_ASSIGN(auto a, Entry(20, 0, 2, "a"));
    ASSERT_OK_AND_ASSIGN(auto b, Entry(10, 1, 2, "b"));
    ASSERT_OK_AND_ASSIGN(auto c, Entry(20, 1, 2, "c"));
    ASSERT_OK_AND_ASSIGN(auto deleted, Entry(20, 0, 2, "a", FileKind::Delete()));
    // Invalid kinds in an excluded group must not be decoded as ManifestEntry objects.
    ASSERT_OK_AND_ASSIGN(auto invalid, Entry(30, 0, 2, "bad", FileKind(2)));
    std::vector<ManifestEntry> source = {a, b, invalid, c, deleted};
    ASSERT_OK_AND_ASSIGN(auto bytes, IndexedManifestEntries::Serialize(source, pool_));
    IndexedManifestEntries::Filter filter;
    int32_t partition_calls = 0;
    filter.partition = [&](const BinaryRow& partition) -> Result<bool> {
        ++partition_calls;
        return partition.GetInt(0) == 20;
    };
    ManifestEntrySerializer serializer(pool_);
    std::vector<ManifestEntry> actual;
    auto consume = [&](const InternalRow& row) -> Status {
        PAIMON_ASSIGN_OR_RAISE(ManifestEntry entry, serializer.FromRow(row));
        actual.push_back(std::move(entry));
        return Status::OK();
    };
    ASSERT_OK_AND_ASSIGN(auto candidates,
                         IndexedManifestEntries::Read(MemorySegment::Wrap(bytes), filter, consume));
    ASSERT_EQ(source.size(), candidates);
    ASSERT_EQ(3, partition_calls);
    ASSERT_EQ(std::vector<ManifestEntry>({a, c, deleted}), actual);
    filter.bucket = 1;
    partition_calls = 0;
    actual.clear();
    ASSERT_OK(IndexedManifestEntries::Read(MemorySegment::Wrap(bytes), filter, consume));
    ASSERT_EQ(2, partition_calls);
    ASSERT_EQ(std::vector<ManifestEntry>({c}), actual);
    ASSERT_NOK_WITH_MSG(IndexedManifestEntries::Read(MemorySegment::Wrap(bytes), {}, consume),
                        "Unsupported byte value 2");
}

TEST_F(IndexedManifestEntriesTest, InferredBucketsRetainHistoricalAndUnknownLayouts) {
    ASSERT_OK_AND_ASSIGN(auto a, Entry(10, 0, 2, "current"));
    ASSERT_OK_AND_ASSIGN(auto b, Entry(10, 1, 2, "other"));
    ASSERT_OK_AND_ASSIGN(auto c, Entry(10, 1, 4, "historical"));
    ManifestEntrySerializer serializer(pool_);
    IndexedManifestEntries index(pool_);
    for (const auto& entry : {a, b, c}) {
        ASSERT_OK_AND_ASSIGN(auto row, serializer.ToRow(entry));
        ASSERT_OK(index.Add(row));
    }
    ASSERT_OK_AND_ASSIGN(auto unknown_total, serializer.ToRow(b));
    unknown_total.SetNullAt(4);
    ASSERT_OK(index.Add(unknown_total));
    ASSERT_OK_AND_ASSIGN(auto unknown_bucket, serializer.ToRow(b));
    unknown_bucket.SetNullAt(3);
    ASSERT_OK(index.Add(unknown_bucket));
    ASSERT_OK_AND_ASSIGN(auto bytes, index.Serialize(pool_));
    IndexedManifestEntries::Filter filter;
    filter.bucket = 0;
    filter.total_buckets = 2;
    int32_t rows = 0;
    ASSERT_OK_AND_ASSIGN(auto candidates,
                         IndexedManifestEntries::Read(MemorySegment::Wrap(bytes), filter,
                                                      [&](const InternalRow&) -> Status {
                                                          ++rows;
                                                          return Status::OK();
                                                      }));
    ASSERT_EQ(5, candidates);
    ASSERT_EQ(4, rows);
}

TEST_F(IndexedManifestEntriesTest, SnapshotWrapperUsesPartitionGroups) {
    ASSERT_OK_AND_ASSIGN(auto a, Entry(20, 0, 2, "a"));
    ASSERT_OK_AND_ASSIGN(auto b, Entry(10, 0, 2, "b"));
    ASSERT_OK_AND_ASSIGN(auto c, Entry(20, 0, 2, "c"));
    SnapshotLiveManifestEntries snapshots(2);
    snapshots.Put(1, {a});
    snapshots.Put(2, {a, b, c});
    ASSERT_OK_AND_ASSIGN(auto bytes, snapshots.Serialize(pool_));
    int32_t partition_calls = 0;
    ASSERT_OK_AND_ASSIGN(auto selection, SnapshotLiveManifestEntries::ReadSnapshot(
                                             MemorySegment::Wrap(bytes), 2, 2, nullptr, pool_,
                                             [&](const BinaryRow& partition) -> Result<bool> {
                                                 ++partition_calls;
                                                 return partition.GetInt(0) == 20;
                                             }));
    ASSERT_TRUE(selection);
    ASSERT_EQ(3, selection->candidate_entries);
    ASSERT_EQ(2, partition_calls);
    ASSERT_EQ(std::vector<ManifestEntry>({a, c}), selection->entries);
    ASSERT_OK_AND_ASSIGN(auto restored, SnapshotLiveManifestEntries::Deserialize(
                                            MemorySegment::Wrap(bytes), 2, pool_));
    ASSERT_EQ(2, restored.Size());
}

TEST_F(IndexedManifestEntriesTest, RejectsMalformedEnvelopesAndDoesNotRetryConsumers) {
    ASSERT_OK_AND_ASSIGN(auto a, Entry(10, 0, 1, "a"));
    ASSERT_OK_AND_ASSIGN(auto bytes, IndexedManifestEntries::Serialize({a}, pool_));
    int32_t calls = 0;
    auto consume = [&](const InternalRow&) -> Status {
        ++calls;
        return Status::OK();
    };
    for (size_t length = 0; length < bytes->size(); ++length) {
        ASSERT_NOK(IndexedManifestEntries::Read(MemorySegment::WrapView(bytes->data(), length), {},
                                                consume));
    }
    ASSERT_EQ(0, calls);
    bool semantic_error = false;
    IndexedManifestEntries::Filter filter;
    filter.partition = [](const BinaryRow&) -> Result<bool> {
        return Status::IOError("predicate failed");
    };
    ASSERT_NOK_WITH_MSG(
        IndexedManifestEntries::Read(MemorySegment::Wrap(bytes), filter, consume, &semantic_error),
        "predicate failed");
    ASSERT_TRUE(semantic_error);
    ASSERT_EQ(0, calls);
    ASSERT_NOK(IndexedManifestEntries::Read(MemorySegment::WrapView(bytes->data(), 7), {}, consume,
                                            &semantic_error));
    ASSERT_FALSE(semantic_error);
}
}  // namespace paimon::test
