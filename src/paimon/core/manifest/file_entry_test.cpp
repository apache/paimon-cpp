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
#include "paimon/core/manifest/file_entry.h"

#include <cassert>
#include <list>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/manifest_entry.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class MockFileEntry : public FileEntry {
 public:
    MockFileEntry(FileKind kind, BinaryRow partition, int32_t bucket, int32_t level,
                  const std::string& file_name,
                  const std::optional<std::string>& external_path = std::nullopt)
        : kind_(kind),
          partition_(partition),
          bucket_(bucket),
          level_(level),
          file_name_(file_name),
          external_path_(external_path),
          min_key_(BinaryRow::EmptyRow()),
          max_key_(BinaryRow::EmptyRow()) {}

    const FileKind& Kind() const override {
        return kind_;
    }
    const BinaryRow& Partition() const override {
        return partition_;
    }
    int32_t Bucket() const override {
        return bucket_;
    }
    int32_t Level() const override {
        return level_;
    }
    const std::string& FileName() const override {
        return file_name_;
    }
    const std::optional<std::string>& ExternalPath() const override {
        return external_path_;
    }
    Identifier CreateIdentifier() const override {
        return Identifier(partition_, bucket_, level_, file_name_, {}, nullptr, external_path_);
    }
    const BinaryRow& MinKey() const override {
        assert(false);
        return min_key_;
    }
    const BinaryRow& MaxKey() const override {
        assert(false);
        return max_key_;
    }

 private:
    FileKind kind_;
    BinaryRow partition_;
    int32_t bucket_;
    int32_t level_;
    std::string file_name_;
    std::optional<std::string> external_path_;
    BinaryRow min_key_;
    BinaryRow max_key_;
};

class FileEntryTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    BinaryRow GetPartition(const std::string& part_str) {
        BinaryRow part(/*arity=*/1);
        BinaryRowWriter writer(&part, /*initial_size=*/20, pool_.get());
        writer.WriteString(0, BinaryString::FromString(part_str, pool_.get()));
        return part;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(FileEntryTest, TestMergeEntriesSimple) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file2");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file3");
    entries.emplace_back(FileKind::Delete(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");

    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file2", merged_entries[0].FileName());
    ASSERT_EQ("file3", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFile) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_NOK_WITH_MSG(FileEntry::MergeEntries(entries, &merged_entries),
                        "which is already added.");
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffPart) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("1"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffBucket) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/1, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffLevel) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/1, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddSameFileWithDiffExternalPath) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1",
                         "/tmp/external_path1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1",
                         "/tmp/external_path2");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(2u, merged_entries.size());
    ASSERT_EQ("file1", merged_entries[0].FileName());
    ASSERT_EQ("file1", merged_entries[1].FileName());
}

TEST_F(FileEntryTest, TestMergeEntriesAddAndDelete) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Delete(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_OK(FileEntry::MergeEntries(entries, &merged_entries));
    ASSERT_EQ(0u, merged_entries.size());
}

TEST_F(FileEntryTest, TestMergeEntriesDeleteAndAdd) {
    std::vector<MockFileEntry> entries;
    entries.emplace_back(FileKind::Delete(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    entries.emplace_back(FileKind::Add(), GetPartition("0"), /*bucket=*/0, /*level=*/0, "file1");
    std::vector<MockFileEntry> merged_entries;
    ASSERT_NOK(FileEntry::MergeEntries(entries, &merged_entries));
}

TEST_F(FileEntryTest, TestIndexMetadataIdentifiers) {
    auto pool = GetDefaultPool();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> file,
                         DataFileMeta::ForAppend("file1", 1024, 10, SimpleStats::EmptyStats(), 1, 1,
                                                 0, FileSource::Append(), std::nullopt,
                                                 std::nullopt, std::nullopt, std::nullopt));
    const BinaryRow partition = GetPartition("0");
    auto identifier = [&](const std::shared_ptr<DataFileMeta>& meta) {
        return ManifestEntry(FileKind::Add(), partition, 0, 1, meta).CreateIdentifier();
    };
    file->extra_files = {"shared.aux", "old.index"};
    file->embedded_index = std::make_shared<Bytes>(std::string("\0\xff", 2), pool.get());
    auto equal_file = file->CopyWithExtraFiles(file->extra_files);
    equal_file->embedded_index = std::make_shared<Bytes>(std::string("\0\xff", 2), pool.get());
    ASSERT_NE(file->embedded_index, equal_file->embedded_index);
    ASSERT_EQ(identifier(file), identifier(equal_file));
    ASSERT_EQ(identifier(file).HashCode(), identifier(equal_file).HashCode());

    std::unordered_set<FileEntry::Identifier> identifiers = {identifier(file)};
    ASSERT_FALSE(identifiers.insert(identifier(equal_file)).second);
    ASSERT_TRUE(
        identifiers.insert(identifier(file->CopyWithExtraFiles({"shared.aux", "new.index"})))
            .second);
    ASSERT_TRUE(
        identifiers.insert(identifier(file->CopyWithExtraFiles({"old.index", "shared.aux"})))
            .second);
    ASSERT_TRUE(identifiers.insert(identifier(file->CopyWithExtraFiles({std::nullopt}))).second);
    ASSERT_TRUE(identifiers.insert(identifier(file->CopyWithExtraFiles({""}))).second);

    auto changed_file = file->CopyWithExtraFiles(file->extra_files);
    changed_file->embedded_index = std::make_shared<Bytes>(std::string("\0\xfe", 2), pool.get());
    ASSERT_TRUE(identifiers.insert(identifier(changed_file)).second);
    changed_file->embedded_index = Bytes::EmptyBytes();
    ASSERT_TRUE(identifiers.insert(identifier(changed_file)).second);
    changed_file->embedded_index.reset();
    ASSERT_TRUE(identifiers.insert(identifier(changed_file)).second);
}

TEST_F(FileEntryTest, TestMergeIndexMetadataRewrite) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<DataFileMeta> before,
                         DataFileMeta::ForAppend("file1", 1024, 10, SimpleStats::EmptyStats(), 1, 1,
                                                 0, FileSource::Append(), std::nullopt,
                                                 std::nullopt, std::nullopt, std::nullopt));
    const BinaryRow partition = GetPartition("0");
    for (bool embedded : {false, true}) {
        before->extra_files = embedded ? std::vector<std::optional<std::string>>{}
                                       : std::vector<std::optional<std::string>>{"old.index"};
        before->embedded_index =
            embedded ? std::make_shared<Bytes>("old", GetDefaultPool().get()) : nullptr;
        auto after = before->CopyWithExtraFiles({"new.index"});
        after->embedded_index.reset();
        std::vector<ManifestEntry> delta = {{FileKind::Delete(), partition, 0, 1, before},
                                            {FileKind::Add(), partition, 0, 1, after}};
        std::vector<ManifestEntry> merged;
        ASSERT_OK(FileEntry::MergeEntries(delta, &merged));
        ASSERT_EQ(2u, merged.size());

        std::vector<ManifestEntry> history = {{FileKind::Add(), partition, 0, 1, before}};
        history.insert(history.end(), delta.begin(), delta.end());
        merged.clear();
        ASSERT_OK(FileEntry::MergeEntries(history, &merged));
        ASSERT_EQ(1u, merged.size());
        ASSERT_EQ(FileKind::Add(), merged[0].Kind());
        ASSERT_EQ(*after, *merged[0].File());
    }
}

}  // namespace paimon::test
