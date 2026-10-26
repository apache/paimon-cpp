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

#include "paimon/core/global_index/global_index_file_manager.h"

#include <limits>
#include <map>
#include <set>
#include <vector>

#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/global_index_write_task.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/range.h"

namespace paimon::test {

class GlobalIndexFileManagerTest : public ::testing::Test {
 public:
    class TrackingFileSystem : public LocalFileSystem {
     public:
        Status ListDir(const std::string& directory,
                       std::vector<BasicFileStatus>* status_list) const override {
            ++list_count_;
            if (fail_list_) {
                return Status::IOError("checkpoint list failed");
            }
            return LocalFileSystem::ListDir(directory, status_list);
        }
        Status Delete(const std::string& path, bool recursive = true) const override {
            delete_paths_.push_back(path);
            if (path == fail_delete_path_) {
                return Status::IOError("checkpoint delete failed");
            }
            return LocalFileSystem::Delete(path, recursive);
        }
        Result<bool> Exists(const std::string& path) const override {
            if (path == fail_exists_path_) {
                return Status::IOError("checkpoint exists failed");
            }
            return LocalFileSystem::Exists(path);
        }
        bool fail_list_ = false;
        std::string fail_delete_path_;
        std::string fail_exists_path_;
        mutable int32_t list_count_ = 0;
        mutable std::vector<std::string> delete_paths_;
    };

    void SetUp() override {
        dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir_);
        ASSERT_OK_AND_ASSIGN(
            path_factory_,
            FileStorePathFactory::Create(
                dir_->Str(), arrow::schema({}), /*partition_keys=*/{}, /*default_part_value=*/"",
                /*identifier=*/"mock", /*data_file_prefix=*/"data-",
                /*legacy_partition_name_enabled=*/true, /*external_paths=*/{},
                /*global_index_external_path=*/std::nullopt,
                /*index_file_in_data_file_dir=*/false, GetDefaultPool()));
    }

    Result<std::shared_ptr<GlobalIndexFileManager>> CreateManager() const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory,
                               path_factory_->CreateGlobalIndexCheckpointPathFactory(
                                   "lumina", "vector", Range(10, 20), "task-1"));
        return std::make_shared<GlobalIndexFileManager>(
            fs_, path_factory_->CreateGlobalIndexFileFactory(), std::move(checkpoint_path_factory));
    }

    Result<std::string> CreateCheckpointFile(
        const std::shared_ptr<GlobalIndexFileManager>& manager) const {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> output,
                               manager->CreateCheckpointOutputStream());
        PAIMON_ASSIGN_OR_RAISE(std::string uri, output->GetUri());
        PAIMON_RETURN_NOT_OK(output->Close());
        return PathUtil::GetName(uri);
    }

    Status CreateSchema(const std::map<std::string, std::string>& options = {}) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<TableSchema> schema,
            TableSchema::Create(/*schema_id=*/0,
                                arrow::schema({arrow::field("vector", arrow::int32())}),
                                /*partition_keys=*/{}, /*primary_keys=*/{}, options));
        PAIMON_ASSIGN_OR_RAISE(std::string content, schema->ToJsonString());
        return fs_->WriteFile(PathUtil::JoinPath(dir_->Str(), "schema/schema-0"), content,
                              /*overwrite=*/false);
    }

    std::shared_ptr<TrackingFileSystem> fs_ = std::make_shared<TrackingFileSystem>();
    std::unique_ptr<UniqueTestDirectory> dir_;
    std::shared_ptr<FileStorePathFactory> path_factory_;
};

TEST_F(GlobalIndexFileManagerTest, TestIndexIOWithoutCheckpointAccess) {
    fs_->fail_list_ = true;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    std::shared_ptr<GlobalIndexFileWriter> writer = manager;
    std::shared_ptr<GlobalIndexFileReader> reader = manager;
    ASSERT_TRUE(std::dynamic_pointer_cast<GlobalIndexCheckpointFileManager>(writer));
    ASSERT_TRUE(std::dynamic_pointer_cast<GlobalIndexCheckpointFileManager>(reader));
    ASSERT_TRUE(manager->SupportsCheckpoint());
    auto plain_manager = std::make_shared<GlobalIndexFileManager>(
        fs_, path_factory_->CreateGlobalIndexFileFactory(), /*checkpoint_path_factory=*/nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<GlobalIndexCheckpointFileManager>(plain_manager));
    ASSERT_FALSE(plain_manager->SupportsCheckpoint());

    ASSERT_OK_AND_ASSIGN(std::string name, writer->NewFileName("lumina"));
    ASSERT_TRUE(StringUtils::EndsWith(name, ".index"));
    ASSERT_EQ(writer->ToPath(name), PathUtil::JoinPath(dir_->Str(), "index/" + name));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> output, writer->NewOutputStream(name));
    ASSERT_OK_AND_ASSIGN(int64_t written, output->Write("index", 5));
    ASSERT_EQ(written, 5);
    ASSERT_OK(output->Close());
    ASSERT_OK_AND_ASSIGN(int64_t size, writer->GetFileSize(name));
    ASSERT_EQ(size, 5);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input,
                         reader->GetInputStream(writer->ToPath(name)));
    char buffer[5];
    ASSERT_OK_AND_ASSIGN(int64_t read, input->Read(buffer, sizeof(buffer)));
    ASSERT_EQ(read, 5);
    ASSERT_EQ(std::string(buffer, sizeof(buffer)), "index");
    ASSERT_OK(input->Close());
    ASSERT_EQ(fs_->list_count_, 0);
    ASSERT_OK_AND_ASSIGN(bool exists,
                         fs_->Exists(PathUtil::JoinPath(dir_->Str(), "index/checkpoint")));
    ASSERT_FALSE(exists);
}

TEST_F(GlobalIndexFileManagerTest, TestCheckpointNotConfigured) {
    fs_->fail_list_ = true;
    GlobalIndexFileManager manager(fs_, path_factory_->CreateGlobalIndexFileFactory(),
                                   /*checkpoint_path_factory=*/nullptr);
    ASSERT_FALSE(manager.SupportsCheckpoint());
    ASSERT_NOK_WITH_MSG(manager.CreateCheckpointOutputStream(),
                        "checkpoint storage is not configured");
    ASSERT_NOK_WITH_MSG(manager.OpenCheckpointInputStream(),
                        "checkpoint storage is not configured");
    ASSERT_NOK_WITH_MSG(manager.CheckpointExists(), "checkpoint storage is not configured");
    ASSERT_NOK_WITH_MSG(manager.DeleteCheckpoint(), "checkpoint storage is not configured");
    ASSERT_EQ(fs_->list_count_, 0);
    ASSERT_OK_AND_ASSIGN(bool exists,
                         fs_->Exists(PathUtil::JoinPath(dir_->Str(), "index/checkpoint")));
    ASSERT_FALSE(exists);
}

TEST_F(GlobalIndexFileManagerTest, TestCheckpointIOAndRestart) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    ASSERT_EQ(fs_->list_count_, 0);
    std::string directory = PathUtil::JoinPath(dir_->Str(), "index/checkpoint/task-1");
    std::string prefix = "lumina-global-index-vector-10-20-";
    std::string previous_path = PathUtil::JoinPath(directory, prefix + "9.index.ckpt");
    // The latest sequence must be discovered on first file name allocation, not at construction.
    ASSERT_OK(fs_->WriteFile(previous_path, "old", /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::string name, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::StartsWith(name, prefix));
    ASSERT_TRUE(StringUtils::EndsWith(name, "-10.index.ckpt"));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> output,
                         manager->CreateCheckpointOutputStream());
    ASSERT_OK_AND_ASSIGN(int64_t written, output->Write("latest", 6));
    ASSERT_EQ(written, 6);
    ASSERT_OK(output->Close());

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> restarted, CreateManager());
    ASSERT_OK_AND_ASSIGN(bool exists, restarted->CheckpointExists());
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input,
                         restarted->OpenCheckpointInputStream());
    char buffer[6];
    ASSERT_OK_AND_ASSIGN(int64_t read, input->Read(buffer, sizeof(buffer)));
    ASSERT_EQ(read, 6);
    ASSERT_EQ(std::string(buffer, sizeof(buffer)), "latest");
    ASSERT_OK(input->Close());
    ASSERT_OK_AND_ASSIGN(name, CreateCheckpointFile(restarted));
    ASSERT_TRUE(StringUtils::EndsWith(name, "-12.index.ckpt"));

    std::string other_task_path = PathUtil::JoinPath(
        dir_->Str(), "index/checkpoint/task-2/lumina-global-index-vector-10-20-99.index.ckpt");
    std::string index_path = manager->ToPath("retained.index");
    ASSERT_OK(fs_->WriteFile(other_task_path, "other", /*overwrite=*/false));
    ASSERT_OK(fs_->WriteFile(index_path, "index", /*overwrite=*/false));
    ASSERT_OK(restarted->DeleteCheckpoint());
    ASSERT_OK_AND_ASSIGN(exists, restarted->CheckpointExists());
    ASSERT_FALSE(exists);
    ASSERT_NOK_WITH_MSG(restarted->OpenCheckpointInputStream(), "checkpoint file does not exist");
    ASSERT_OK(restarted->DeleteCheckpoint());
    ASSERT_OK_AND_ASSIGN(exists, fs_->Exists(previous_path));
    ASSERT_FALSE(exists);
    ASSERT_OK_AND_ASSIGN(exists, fs_->Exists(other_task_path));
    ASSERT_TRUE(exists);
    ASSERT_OK_AND_ASSIGN(exists, fs_->Exists(index_path));
    ASSERT_TRUE(exists);
}

TEST_F(GlobalIndexFileManagerTest, TestInitializationFailureCanRetry) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    fs_->fail_list_ = true;
    ASSERT_TRUE(manager->SupportsCheckpoint());
    ASSERT_EQ(fs_->list_count_, 0);
    ASSERT_NOK_WITH_MSG(CreateCheckpointFile(manager), "checkpoint list failed");
    ASSERT_NOK_WITH_MSG(manager->CreateCheckpointOutputStream(), "checkpoint list failed");
    ASSERT_NOK_WITH_MSG(manager->OpenCheckpointInputStream(), "checkpoint list failed");
    ASSERT_NOK_WITH_MSG(manager->CheckpointExists(), "checkpoint list failed");
    ASSERT_NOK_WITH_MSG(manager->DeleteCheckpoint(), "checkpoint list failed");
    fs_->fail_list_ = false;
    ASSERT_OK_AND_ASSIGN(std::string first, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::EndsWith(first, "-0.index.ckpt"));
    ASSERT_OK_AND_ASSIGN(std::string second, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::EndsWith(second, "-1.index.ckpt"));
    ASSERT_EQ(fs_->list_count_, 6);
}

TEST_F(GlobalIndexFileManagerTest, TestLatestCheckpointUsesNumericId) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    std::string directory = PathUtil::JoinPath(dir_->Str(), "index/checkpoint/task-1");
    std::string prefix = "lumina-global-index-vector-10-20-";
    ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(directory, prefix + "9.index.ckpt"), "older",
                             /*overwrite=*/false));
    ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(directory, prefix + "10.index.ckpt"), "newer",
                             /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input, manager->OpenCheckpointInputStream());
    char buffer[5];
    ASSERT_OK_AND_ASSIGN(int64_t read, input->Read(buffer, sizeof(buffer)));
    ASSERT_EQ(read, 5);
    ASSERT_EQ(std::string(buffer, sizeof(buffer)), "newer");
    ASSERT_OK(input->Close());
}

TEST_F(GlobalIndexFileManagerTest, TestReadsDoNotInitializeFileId) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    ASSERT_OK_AND_ASSIGN(bool exists, manager->CheckpointExists());
    ASSERT_FALSE(exists);
    std::string directory = PathUtil::JoinPath(dir_->Str(), "index/checkpoint/task-1");
    std::string prefix = "lumina-global-index-vector-10-20-";
    ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(directory, prefix + "9.index.ckpt"), "old",
                             /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input, manager->OpenCheckpointInputStream());
    ASSERT_OK(input->Close());
    ASSERT_EQ(fs_->list_count_, 2);

    ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(directory, prefix + "19.index.ckpt"), "latest",
                             /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::string first, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::EndsWith(first, "-20.index.ckpt"));
    ASSERT_EQ(fs_->list_count_, 3);
    ASSERT_OK_AND_ASSIGN(std::string second, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::EndsWith(second, "-21.index.ckpt"));
    ASSERT_EQ(fs_->list_count_, 3);
}

TEST_F(GlobalIndexFileManagerTest, TestFileIdOverflow) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    std::string directory = PathUtil::JoinPath(dir_->Str(), "index/checkpoint/task-1");
    std::string prefix = "lumina-global-index-vector-10-20-";
    int64_t max_id = std::numeric_limits<int64_t>::max();
    ASSERT_OK(fs_->WriteFile(
        PathUtil::JoinPath(directory, fmt::format("{}{}.index.ckpt", prefix, max_id - 1)), "old",
        /*overwrite=*/false));
    ASSERT_OK_AND_ASSIGN(std::string name, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::EndsWith(name, fmt::format("-{}.index.ckpt", max_id)));
    ASSERT_NOK_WITH_MSG(CreateCheckpointFile(manager), "checkpoint file id exceeds int64 max");
    ASSERT_NOK_WITH_MSG(manager->CreateCheckpointOutputStream(),
                        "checkpoint file id exceeds int64 max");
    ASSERT_EQ(fs_->list_count_, 1);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> restarted, CreateManager());
    ASSERT_EQ(fs_->list_count_, 1);
    ASSERT_NOK_WITH_MSG(CreateCheckpointFile(restarted), "checkpoint file id exceeds int64 max");
    ASSERT_NOK_WITH_MSG(CreateCheckpointFile(restarted), "checkpoint file id exceeds int64 max");
    ASSERT_EQ(fs_->list_count_, 2);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> input,
                         restarted->OpenCheckpointInputStream());
    ASSERT_OK(input->Close());
    ASSERT_OK(restarted->DeleteCheckpoint());
}

TEST_F(GlobalIndexFileManagerTest, TestSerialAllocations) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    std::vector<std::string> names(8);
    for (size_t i = 0; i < names.size(); ++i) {
        ASSERT_OK_AND_ASSIGN(names[i], CreateCheckpointFile(manager));
        ASSERT_TRUE(StringUtils::EndsWith(names[i], fmt::format("-{}.index.ckpt", i)));
    }
    ASSERT_EQ(fs_->list_count_, 1);
    ASSERT_EQ(std::set<std::string>(names.begin(), names.end()).size(), names.size());
    for (const std::string& name : names) {
        ASSERT_FALSE(name.empty());
    }
}

TEST_F(GlobalIndexFileManagerTest, TestDeleteDoesNotResetFileId) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    ASSERT_OK_AND_ASSIGN(std::string first, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::EndsWith(first, "-0.index.ckpt"));
    ASSERT_OK(manager->DeleteCheckpoint());
    ASSERT_OK_AND_ASSIGN(std::string second, CreateCheckpointFile(manager));
    ASSERT_TRUE(StringUtils::EndsWith(second, "-1.index.ckpt"));
    ASSERT_EQ(fs_->list_count_, 2);
}

TEST_F(GlobalIndexFileManagerTest, TestDeleteTaskCheckpoint) {
    ASSERT_OK(CreateSchema());
    std::string directory = PathUtil::JoinPath(dir_->Str(), "index/checkpoint/task-1");
    std::vector<std::string> matching_names = {"lumina-global-index-vector-10-20-9.index.ckpt",
                                               "lumina-global-index-vector-10-20-10.index.ckpt",
                                               "lumina-global-index-other-10-20-0.index.ckpt",
                                               "other-global-index-vector-10-20-0.index.ckpt",
                                               "lumina-global-index-vector-10-21-0.index.ckpt",
                                               "lumina-global-index-vector-11-20-0.index.ckpt",
                                               "lumina-global-index-vector-10-20-0.index.ckpt.bak",
                                               "incomplete-file",
                                               "nested/unfinished-file"};
    for (const std::string& name : matching_names) {
        ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(directory, name), "incomplete checkpoint",
                                 /*overwrite=*/false));
    }
    std::vector<std::string> retained_tasks = {"task-2", "task-1-extended", "task-1-9", "task-1-"};
    std::vector<std::string> retained_paths;
    for (const std::string& task_id : retained_tasks) {
        std::string path = PathUtil::JoinPath(
            dir_->Str(), "index/checkpoint/" + task_id + "/" + matching_names.front());
        ASSERT_OK(fs_->WriteFile(path, "retained", /*overwrite=*/false));
        retained_paths.push_back(path);
    }
    ASSERT_OK(GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}, fs_));
    ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(directory));
    ASSERT_FALSE(exists);
    ASSERT_EQ(fs_->delete_paths_, std::vector<std::string>({directory}));
    for (const std::string& path : retained_paths) {
        ASSERT_OK_AND_ASSIGN(exists, fs_->Exists(path));
        ASSERT_TRUE(exists) << path;
    }
    ASSERT_OK(GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}, fs_));
}

TEST_F(GlobalIndexFileManagerTest, TestDeleteTaskCheckpointWithoutCustomFileSystem) {
    ASSERT_OK(CreateSchema());
    ASSERT_NOK_WITH_MSG(GlobalIndexWriteTask::DeleteCheckpoint(
                            dir_->Str(), "task-1", {{Options::FILE_SYSTEM, "unregistered-fs"}}),
                        "Create factory failed with identifier 'unregistered-fs'");
    ASSERT_OK(GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}));
    std::string directory = PathUtil::JoinPath(dir_->Str(), "index/checkpoint/task-1");
    ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(directory));
    ASSERT_FALSE(exists);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    ASSERT_OK(CreateCheckpointFile(manager));
    ASSERT_OK(GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(exists, manager->CheckpointExists());
    ASSERT_FALSE(exists);
    ASSERT_OK(GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}));
}

TEST_F(GlobalIndexFileManagerTest, TestDeleteTaskCheckpointRejectsInvalidTaskId) {
    for (const std::string& invalid : std::vector<std::string>{"", ".", "..", " ", "a/b", "a\\b",
                                                               "a\nb", std::string("a\0b", 3)}) {
        ASSERT_NOK(
            GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), invalid, /*options=*/{}, fs_));
    }
    ASSERT_EQ(fs_->list_count_, 0);
    ASSERT_TRUE(fs_->delete_paths_.empty());
}

TEST_F(GlobalIndexFileManagerTest, TestDeleteTaskCheckpointPropagatesFileSystemErrors) {
    ASSERT_OK(CreateSchema());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexFileManager> manager, CreateManager());
    ASSERT_OK_AND_ASSIGN(std::string first, CreateCheckpointFile(manager));
    ASSERT_OK_AND_ASSIGN(std::string second, CreateCheckpointFile(manager));
    fs_->fail_list_ = true;
    ASSERT_NOK_WITH_MSG(
        GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}, fs_),
        "checkpoint list failed");
    ASSERT_TRUE(fs_->delete_paths_.empty());

    fs_->fail_list_ = false;
    std::string directory = PathUtil::JoinPath(dir_->Str(), "index/checkpoint/task-1");
    fs_->fail_exists_path_ = directory;
    ASSERT_NOK_WITH_MSG(
        GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}, fs_),
        "checkpoint exists failed");
    ASSERT_TRUE(fs_->delete_paths_.empty());
    fs_->fail_exists_path_.clear();
    fs_->fail_delete_path_ = directory;
    ASSERT_NOK_WITH_MSG(
        GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}, fs_),
        "checkpoint delete failed");
    ASSERT_EQ(fs_->delete_paths_, std::vector<std::string>({directory}));
    for (const std::string& name : {first, second}) {
        ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(PathUtil::JoinPath(directory, name)));
        ASSERT_TRUE(exists);
    }

    fs_->fail_delete_path_.clear();
    ASSERT_OK(GlobalIndexWriteTask::DeleteCheckpoint(dir_->Str(), "task-1", /*options=*/{}, fs_));
    ASSERT_OK_AND_ASSIGN(bool exists, fs_->Exists(directory));
    ASSERT_FALSE(exists);
}

}  // namespace paimon::test
