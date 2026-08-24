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

#include "paimon/core/operation/commit/uncommitted_file_cleaner.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/io/managed_blob_reference_file.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

/// A data file meta carrying `sidecar` in its extra files, the way a managed blob write records
/// its `.blobref` companion.
std::shared_ptr<DataFileMeta> CreateDataFile(const std::string& file_name,
                                             const std::optional<std::string>& sidecar) {
    std::vector<std::optional<std::string>> extra_files;
    if (sidecar) {
        extra_files.emplace_back(sidecar.value());
    }
    return DataFileMeta::ForAppend(file_name, /*file_size=*/1, /*row_count=*/1,
                                   /*row_stats=*/SimpleStats::EmptyStats(),
                                   /*min_sequence_number=*/0, /*max_sequence_number=*/1,
                                   /*schema_id=*/0, extra_files, /*embedded_index=*/nullptr,
                                   FileSource::Append(), /*value_stats_cols=*/std::nullopt,
                                   /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
                                   /*write_cols=*/std::nullopt)
        .value();
}

}  // namespace

class UncommittedFileCleanerTest : public ::testing::Test {
 public:
    void SetUp() override {
        dir_ = UniqueTestDirectory::Create("local");
        auto arrow_schema = arrow::schema({arrow::field("f0", arrow::int32())});
        ASSERT_OK_AND_ASSIGN(
            path_factory_,
            FileStorePathFactory::Create(dir_->Str(), arrow_schema, /*partition_keys=*/{},
                                         /*default_part_value=*/"__DEFAULT_PARTITION__",
                                         /*identifier=*/"parquet",
                                         /*data_file_prefix=*/"data-",
                                         /*legacy_partition_name_enabled=*/true,
                                         /*external_paths=*/{},
                                         /*global_index_external_path=*/std::nullopt,
                                         /*index_file_in_data_file_dir=*/false, GetDefaultPool()));
        // The bucket directory is created by the writers in production; here the files are
        // placed directly, so it has to exist first.
        ASSERT_OK(dir_->GetFileSystem()->Mkdirs(BucketPath()));
        logger_ = Logger::GetLogger("UncommittedFileCleanerTest");
    }

    void TearDown() override {
        dir_.reset();
    }

    /// Creates an empty file under the single bucket directory and returns its path.
    std::string Touch(const std::string& file_name) {
        std::string path = PathUtil::JoinPath(BucketPath(), file_name);
        Result<std::unique_ptr<OutputStream>> out =
            dir_->GetFileSystem()->Create(path, /*overwrite=*/true);
        EXPECT_TRUE(out.ok());
        if (out.ok()) {
            EXPECT_TRUE(out.value()->Close().ok());
        }
        return path;
    }

    std::string BucketPath() const {
        return PathUtil::JoinPath(dir_->Str(), "bucket-0");
    }

    bool Exists(const std::string& path) const {
        return dir_->GetFileSystem()->GetFileStatus(path).ok();
    }

 protected:
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
    std::shared_ptr<Logger> logger_;
};

TEST_F(UncommittedFileCleanerTest, TestRemovesDataFilesSidecarsAndOwnedPacks) {
    // What a managed blob write leaves behind before its commit lands: a data file, the
    // `.blobref` sidecar in its extra files, and the pack the writer created for it.
    std::string data_path = Touch("data-1.parquet");
    std::string sidecar_path = Touch("data-1.parquet.blobref");
    std::string owned_pack = Touch("data-owned.managed.blob");
    // A pack the message only references — what a compaction output's sidecar would list.
    // It belongs to a live snapshot and must survive.
    std::string referenced_pack = Touch("data-referenced.managed.blob");

    DataIncrement data_increment({CreateDataFile("data-1.parquet", "data-1.parquet.blobref")},
                                 /*deleted_files=*/{}, /*changelog_files=*/{});
    auto message = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/std::nullopt, data_increment,
        CompactIncrement({}, {}, {}));
    message->SetOwnedManagedBlobPacks({owned_pack});

    ASSERT_OK(UncommittedFileCleaner::Delete(path_factory_, dir_->GetFileSystem(), {message},
                                             logger_.get()));

    EXPECT_FALSE(Exists(data_path));
    EXPECT_FALSE(Exists(sidecar_path));
    EXPECT_FALSE(Exists(owned_pack));
    EXPECT_TRUE(Exists(referenced_pack));
}

TEST_F(UncommittedFileCleanerTest, TestRemovesCompactOutputsButNoInheritedPack) {
    // A compaction output owns no pack: it rewrites blob descriptors verbatim, so the packs its
    // sidecar lists belong to the files it merged and are still read by the base snapshot.
    std::string compacted_path = Touch("data-compacted.parquet");
    std::string compacted_sidecar = Touch("data-compacted.parquet.blobref");
    std::string inherited_pack = Touch("data-inherited.managed.blob");

    CompactIncrement compact_increment(
        /*compact_before=*/{},
        {CreateDataFile("data-compacted.parquet", "data-compacted.parquet.blobref")},
        /*changelog_files=*/{});
    auto message = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/std::nullopt,
        DataIncrement({}, {}, {}), compact_increment);

    ASSERT_OK(UncommittedFileCleaner::Delete(path_factory_, dir_->GetFileSystem(), {message},
                                             logger_.get()));

    EXPECT_FALSE(Exists(compacted_path));
    EXPECT_FALSE(Exists(compacted_sidecar));
    EXPECT_TRUE(Exists(inherited_pack));
}

TEST_F(UncommittedFileCleanerTest, TestToleratesMissingFilesAndRejectsForeignMessages) {
    // Nothing on disk: a rollback runs after a failure that may have removed some files
    // already, so a missing file is normal.
    DataIncrement data_increment({CreateDataFile("data-gone.parquet", std::nullopt)},
                                 /*deleted_files=*/{}, /*changelog_files=*/{});
    auto message = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/std::nullopt, data_increment,
        CompactIncrement({}, {}, {}));
    message->SetOwnedManagedBlobPacks({PathUtil::JoinPath(BucketPath(), "data-gone.managed.blob")});
    ASSERT_OK(UncommittedFileCleaner::Delete(path_factory_, dir_->GetFileSystem(), {message},
                                             logger_.get()));

    // A message this cleaner cannot interpret would silently leave files behind, so it fails
    // instead of reporting success.
    ASSERT_NOK_WITH_MSG(UncommittedFileCleaner::Delete(path_factory_, dir_->GetFileSystem(),
                                                       {nullptr}, logger_.get()),
                        "fail to cast commit message to impl");
}

TEST_F(UncommittedFileCleanerTest, TestAnUnusableMessageDoesNotStrandTheOnesBehindIt) {
    // Giving up on the whole list at the first unusable message would leave every file the
    // remaining ones describe behind - exactly the leak this cleaner exists to prevent. The
    // failure is still reported, just after the rest has been cleaned.
    std::string data_path = Touch("data-2.parquet");
    std::string owned_pack = Touch("data-second.managed.blob");

    DataIncrement data_increment({CreateDataFile("data-2.parquet", std::nullopt)},
                                 /*deleted_files=*/{}, /*changelog_files=*/{});
    auto usable = std::make_shared<CommitMessageImpl>(BinaryRow::EmptyRow(), /*bucket=*/0,
                                                      /*total_buckets=*/std::nullopt,
                                                      data_increment, CompactIncrement({}, {}, {}));
    usable->SetOwnedManagedBlobPacks({owned_pack});

    // The unusable message comes first, so a cleaner that stopped there would clean nothing.
    ASSERT_NOK_WITH_MSG(UncommittedFileCleaner::Delete(path_factory_, dir_->GetFileSystem(),
                                                       {nullptr, usable}, logger_.get()),
                        "fail to cast commit message to impl");
    EXPECT_FALSE(Exists(data_path));
    EXPECT_FALSE(Exists(owned_pack));
}

}  // namespace paimon::test
