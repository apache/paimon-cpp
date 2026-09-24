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
#include "paimon/indexer/lumina/lumina_global_index.h"

#include <random>
#include <thread>

#include "arrow/c/bridge.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "lumina/api/Dataset.h"
#include "lumina/api/LuminaBuilder.h"
#include "lumina/core/Constants.h"
#include "lumina/extensions/experimental/BuildCombinedExtensionV0.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/global_index/global_index_file_manager.h"
#include "paimon/core/index/index_checkpoint_path_factory.h"
#include "paimon/core/index/index_path_factory.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/global_index/bitmap_scored_global_index_result.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/indexer/lumina/lumina_checkpoint_manager.h"
#include "paimon/indexer/lumina/lumina_memory_pool.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lumina::test {
class LuminaGlobalIndexTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class FakeIndexPathFactory : public IndexPathFactory {
     public:
        explicit FakeIndexPathFactory(const std::string& index_path) : index_path_(index_path) {}
        std::string NewPath() const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::shared_ptr<IndexFileMeta>& file) const override {
            assert(false);
            return "";
        }
        std::string ToPath(const std::string& file_name) const override {
            return PathUtil::JoinPath(index_path_, file_name);
        }
        bool IsExternalPath() const override {
            return false;
        }

     private:
        std::string index_path_;
    };

    class CountingCheckpointFileManager : public GlobalIndexFileManager {
     public:
        using GlobalIndexFileManager::GlobalIndexFileManager;

        Result<bool> CheckpointExists() const override {
            check_checkpoint_count_++;
            return GlobalIndexFileManager::CheckpointExists();
        }

        Result<std::unique_ptr<OutputStream>> CreateCheckpointOutputStream() const override {
            create_checkpoint_count_++;
            return GlobalIndexFileManager::CreateCheckpointOutputStream();
        }

        Result<std::unique_ptr<InputStream>> OpenCheckpointInputStream() const override {
            open_checkpoint_count_++;
            return GlobalIndexFileManager::OpenCheckpointInputStream();
        }

        mutable int32_t check_checkpoint_count_ = 0;
        mutable int32_t create_checkpoint_count_ = 0;
        mutable int32_t open_checkpoint_count_ = 0;
    };

    class TestLuminaDataset : public ::lumina::api::Dataset {
     public:
        TestLuminaDataset(uint32_t dimension, const std::vector<float>& vectors,
                          const std::vector<::lumina::core::vector_id_t>& ids)
            : dimension_(dimension), vectors_(vectors), ids_(ids) {}

        uint32_t Dim() const noexcept override {
            return dimension_;
        }

        uint64_t TotalSize() const noexcept override {
            return ids_.size();
        }

        ::lumina::core::Result<uint64_t> GetNextBatch(
            std::vector<float>& vector_buffer,
            std::vector<::lumina::core::vector_id_t>& id_buffer) noexcept override {
            if (consumed_) {
                return ::lumina::core::Result<uint64_t>::Ok(0);
            }
            vector_buffer = vectors_;
            id_buffer = ids_;
            consumed_ = true;
            return ::lumina::core::Result<uint64_t>::Ok(ids_.size());
        }

     private:
        uint32_t dimension_;
        std::vector<float> vectors_;
        std::vector<::lumina::core::vector_id_t> ids_;
        bool consumed_ = false;
    };

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportType(*data_type, c_schema.get()).ok());
        return c_schema;
    }

    Result<std::shared_ptr<FileStorePathFactory>> CreateFileStorePathFactory(
        const std::string& table_path) const {
        return FileStorePathFactory::Create(
            table_path, arrow::schema({}), /*partition_keys=*/{}, /*default_part_value=*/"",
            /*identifier=*/"mock", /*data_file_prefix=*/"data-",
            /*legacy_partition_name_enabled=*/true, /*external_paths=*/{},
            /*global_index_external_path=*/std::nullopt,
            /*index_file_in_data_file_dir=*/false, pool_);
    }

    Result<std::unique_ptr<IndexCheckpointPathFactory>> CreateCheckpointPathFactory(
        const std::string& table_path, const std::string& index_type, const std::string& field_name,
        const Range& range) const {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> file_store_path_factory,
                               CreateFileStorePathFactory(table_path));
        return file_store_path_factory->CreateGlobalIndexCheckpointPathFactory(
            index_type, field_name, range, "task-1");
    }

    Result<std::shared_ptr<CountingCheckpointFileManager>> CreateCountingCheckpointFileManager(
        const std::shared_ptr<FileStorePathFactory>& path_factory, const Range& range) const {
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory,
            path_factory->CreateGlobalIndexCheckpointPathFactory("lumina", "f0", range, "task-1"));
        return std::make_shared<CountingCheckpointFileManager>(
            fs_, path_factory->CreateGlobalIndexFileFactory(), std::move(checkpoint_path_factory));
    }

    Result<GlobalIndexIOMeta> WriteGlobalIndex(const std::string& index_root,
                                               const std::shared_ptr<arrow::DataType>& data_type,
                                               const std::map<std::string, std::string>& options,
                                               const std::shared_ptr<arrow::Array>& array,
                                               const Range& expected_range) const {
        auto global_index = std::make_shared<LuminaGlobalIndex>(options);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileStorePathFactory> path_factory,
                               CreateFileStorePathFactory(index_root));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory,
                               path_factory->CreateGlobalIndexCheckpointPathFactory(
                                   "lumina", "f0", expected_range, "task-1"));
        auto file_writer = std::make_shared<GlobalIndexFileManager>(
            fs_, path_factory->CreateGlobalIndexFileFactory(), std::move(checkpoint_path_factory));

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<GlobalIndexWriter> global_writer,
                               global_index->CreateWriter("f0", CreateArrowSchema(data_type).get(),
                                                          file_writer, pool_));

        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        std::vector<int64_t> row_ids(array->length(), 0);
        std::iota(row_ids.begin(), row_ids.end(), 0);
        PAIMON_RETURN_NOT_OK(global_writer->AddBatch(&c_array, std::move(row_ids)));
        PAIMON_ASSIGN_OR_RAISE(auto result_metas, global_writer->Finish());
        // check meta
        EXPECT_EQ(result_metas.size(), 1);
        auto file_name = PathUtil::GetName(result_metas[0].file_path);
        EXPECT_TRUE(StringUtils::StartsWith(file_name, "lumina-global-index-"));
        EXPECT_TRUE(StringUtils::EndsWith(file_name, ".index"));
        EXPECT_TRUE(result_metas[0].metadata);
        return result_metas[0];
    }

    void CheckResult(const std::shared_ptr<ScoredGlobalIndexResult>& result,
                     const std::vector<int64_t>& expected_ids,
                     const std::vector<float>& expected_scores) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap64::From(expected_ids))
            << "result=" << (typed_result->GetBitmap().value())->ToString()
            << ", expected=" << RoaringBitmap64::From(expected_ids).ToString();
        ASSERT_EQ(typed_result->scores_.size(), expected_scores.size());

        std::map<int64_t, float> id_to_score;
        for (size_t i = 0; i < expected_ids.size(); i++) {
            id_to_score[expected_ids[i]] = expected_scores[i];
        }
        std::vector<float> expected_scores_ordered_by_id;
        for (const auto& [id, score] : id_to_score) {
            expected_scores_ordered_by_id.push_back(score);
        }
        for (size_t i = 0; i < expected_scores.size(); i++) {
            ASSERT_NEAR(typed_result->scores_[i], expected_scores_ordered_by_id[i], 0.01);
        }
    }

    Result<std::shared_ptr<GlobalIndexReader>> CreateGlobalIndexReader(
        const std::string& index_root, const std::shared_ptr<arrow::DataType>& data_type,
        const std::map<std::string, std::string>& options, const GlobalIndexIOMeta& meta) const {
        auto global_index = std::make_shared<LuminaGlobalIndex>(options);
        auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
        auto file_reader = std::make_shared<GlobalIndexFileManager>(
            fs_, path_factory, /*checkpoint_path_factory=*/nullptr);
        return global_index->CreateReader(CreateArrowSchema(data_type).get(), file_reader, {meta},
                                          pool_);
    }

    std::shared_ptr<arrow::Array> CreateRandomVector(int32_t element_size,
                                                     int32_t dimension) const {
        int64_t total_values = element_size * dimension;
        auto float_builder = std::make_shared<arrow::FloatBuilder>();
        arrow::ListBuilder list_builder(arrow::default_memory_pool(), float_builder);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<float> dis(0.0f, 2.0f);

        EXPECT_TRUE(float_builder->Reserve(total_values).ok());
        EXPECT_TRUE(list_builder.Reserve(element_size).ok());

        for (int64_t i = 0; i < element_size; ++i) {
            EXPECT_TRUE(list_builder.Append().ok());
            for (int64_t j = 0; j < dimension; ++j) {
                float val = dis(gen);
                EXPECT_TRUE(float_builder->Append(val).ok());
            }
        }

        std::shared_ptr<arrow::Array> list_array;
        EXPECT_TRUE(list_builder.Finish(&list_array).ok());

        auto struct_array = arrow::StructArray::Make({list_array}, {"f0"}).ValueOrDie();
        return struct_array;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
    std::shared_ptr<FileSystem> fs_ = std::make_shared<LocalFileSystem>();
    std::map<std::string, std::string> options_ = {{"lumina.index.dimension", "4"},
                                                   {"lumina.index.type", "bruteforce"},
                                                   {"lumina.distance.metric", "l2"},
                                                   {"lumina.encoding.type", "rawf32"},
                                                   {"lumina.search.parallel_number", "10"}};
    std::shared_ptr<arrow::DataType> data_type_ =
        arrow::struct_({arrow::field("f0", arrow::list(arrow::float32()))});
    std::shared_ptr<arrow::Array> array_ = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                     R"([
        [[0.0, 0.0, 0.0, 0.0]],
        [[0.0, 1.0, 0.0, 1.0]],
        [[1.0, 0.0, 1.0, 0.0]],
        [[1.0, 1.0, 1.0, 1.0]]
    ])")
                                               .ValueOrDie();
    std::vector<float> query_ = {1.0f, 1.0f, 1.0f, 1.1f};
};

TEST_F(LuminaGlobalIndexTest, TestSimple) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    ASSERT_OK_AND_ASSIGN(auto meta,
                         WriteGlobalIndex(test_root, data_type_, options_, array_, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));
    {
        // recall all data
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(scored_result, {3l, 1l, 2l, 0l}, {0.01f, 2.01f, 2.21f, 4.21f});
    }
    {
        // small limit
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/3, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(scored_result, {3l, 1l, 2l}, {0.01f, 2.01f, 2.21f});
    }
    {
        // visit equal will return all rows
        ASSERT_OK_AND_ASSIGN(auto is_null_result, reader->VisitIsNull());
        ASSERT_FALSE(is_null_result);
    }
}

TEST_F(LuminaGlobalIndexTest, TestWithFilter) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    ASSERT_OK_AND_ASSIGN(auto meta,
                         WriteGlobalIndex(test_root, data_type_, options_, array_, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));
    {
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/2, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(scored_result, {3l, 1l}, {0.01f, 2.01f});
    }
    {
        auto filter = [](int64_t id) -> bool { return id < 3; };
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/2, query_, filter,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(scored_result, {1l, 2l}, {2.01f, 2.21f});
    }
    {
        auto filter = [](int64_t id) -> bool { return id < 3; };
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, filter,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(scored_result, {1l, 2l, 0l}, {2.01f, 2.21f, 4.21f});
    }
}

TEST_F(LuminaGlobalIndexTest, TestCheckpointCapabilityOnlyRequiredWhenEnabled) {
    ASSERT_TRUE(LuminaGlobalIndex(options_).SupportsCheckpoint());
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> path_factory,
                         CreateFileStorePathFactory(dir->Str()));
    auto plain_manager =
        std::make_shared<GlobalIndexFileManager>(fs_, path_factory->CreateGlobalIndexFileFactory(),
                                                 /*checkpoint_path_factory=*/nullptr);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CountingCheckpointFileManager> checkpoint_manager,
                         CreateCountingCheckpointFileManager(path_factory, Range(0, 3)));
    std::map<std::string, std::string> options = options_;
    options["other.extension.build.ckpt.count"] = "1";
    ASSERT_OK(LuminaGlobalIndex(options).CreateWriter("f0", CreateArrowSchema(data_type_).get(),
                                                      plain_manager, pool_));
    ASSERT_OK(LuminaGlobalIndex(options).CreateWriter("f0", CreateArrowSchema(data_type_).get(),
                                                      checkpoint_manager, pool_));

    for (const std::string& checkpoint_key :
         std::vector<std::string>{"extension.build.ckpt.count", "extension.build.ckpt.threshold"}) {
        auto enabled = options;
        enabled["lumina." + checkpoint_key] = "1";
        ASSERT_NOK_WITH_MSG(LuminaGlobalIndex(enabled).CreateWriter(
                                "f0", CreateArrowSchema(data_type_).get(), plain_manager, pool_),
                            "Lumina checkpoint requires a checkpoint-capable file writer");
        ASSERT_NOK_WITH_MSG(LuminaGlobalIndex(enabled).CreateWriter(
                                "f0", CreateArrowSchema(data_type_).get(), nullptr, pool_),
                            "Lumina checkpoint requires a checkpoint-capable file writer");
        ASSERT_OK(LuminaGlobalIndex(enabled).CreateWriter("f0", CreateArrowSchema(data_type_).get(),
                                                          checkpoint_manager, pool_));
    }
    ASSERT_EQ(checkpoint_manager->check_checkpoint_count_, 0);
    ASSERT_EQ(checkpoint_manager->create_checkpoint_count_, 0);
}

TEST_F(LuminaGlobalIndexTest, TestBuildWithCheckpoint) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> checkpoint_options = options_;
    checkpoint_options["lumina.extension.build.ckpt.threshold"] = "1";
    checkpoint_options["lumina.extension.build.ckpt.count"] = "1";

    auto global_index = std::make_shared<LuminaGlobalIndex>(checkpoint_options);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> path_factory,
                         CreateFileStorePathFactory(test_root));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CountingCheckpointFileManager> file_manager,
                         CreateCountingCheckpointFileManager(path_factory, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexWriter> global_writer,
        global_index->CreateWriter("f0", CreateArrowSchema(data_type_).get(), file_manager, pool_));

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array_, &c_array).ok());
    ASSERT_OK(global_writer->AddBatch(&c_array, {0, 1, 2, 3}));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> result_metas, global_writer->Finish());
    ASSERT_EQ(result_metas.size(), 1);
    ASSERT_GT(file_manager->create_checkpoint_count_, 0);
    ASSERT_OK_AND_ASSIGN(bool checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_FALSE(checkpoint_exists);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexReader> reader,
        CreateGlobalIndexReader(test_root, data_type_, checkpoint_options, result_metas[0]));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> scored_result,
                         reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                             /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr,
                             /*predicate=*/nullptr, /*distance_type=*/std::nullopt,
                             /*options=*/checkpoint_options)));
    CheckResult(scored_result, {3l, 1l, 2l, 0l}, {0.01f, 2.01f, 2.21f, 4.21f});
}

TEST_F(LuminaGlobalIndexTest, TestCheckpointFileManagement) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    std::string prefix = "lumina-global-index-f0-10-20-task-1-";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory,
                         CreateCheckpointPathFactory(test_root, "lumina", "f0", Range(10, 20)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> path_factory,
                         CreateFileStorePathFactory(test_root));
    auto file_manager = std::make_shared<GlobalIndexFileManager>(
        fs_, path_factory->CreateGlobalIndexFileFactory(), std::move(checkpoint_path_factory));

    ASSERT_OK_AND_ASSIGN(bool checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_FALSE(checkpoint_exists);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> first_output,
                         file_manager->CreateCheckpointOutputStream());
    ASSERT_OK_AND_ASSIGN(std::string first_path, first_output->GetUri());
    ASSERT_TRUE(StringUtils::StartsWith(PathUtil::GetName(first_path), prefix));
    ASSERT_TRUE(StringUtils::EndsWith(first_path, "-0.index.ckpt"));
    ASSERT_OK_AND_ASSIGN(int64_t first_written, first_output->Write("old", 3));
    ASSERT_EQ(first_written, 3);
    ASSERT_OK(first_output->Close());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> second_output,
                         file_manager->CreateCheckpointOutputStream());
    ASSERT_OK_AND_ASSIGN(std::string second_path, second_output->GetUri());
    ASSERT_TRUE(StringUtils::StartsWith(PathUtil::GetName(second_path), prefix));
    ASSERT_TRUE(StringUtils::EndsWith(second_path, "-1.index.ckpt"));
    ASSERT_OK_AND_ASSIGN(int64_t second_written, second_output->Write("latest", 6));
    ASSERT_EQ(second_written, 6);
    ASSERT_OK(second_output->Close());

    std::string checkpoint_dir = PathUtil::JoinPath(test_root, "index/checkpoint");
    ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(checkpoint_dir, prefix + "9.index.ckpt"), "id-nine",
                             /*overwrite=*/false));
    ASSERT_OK(fs_->WriteFile(PathUtil::JoinPath(checkpoint_dir, prefix + "10.index.ckpt"), "id-ten",
                             /*overwrite=*/false));
    std::string unrelated_file =
        PathUtil::JoinPath(checkpoint_dir, "lumina-global-index-other-10-20-task-2-99.index.ckpt");
    ASSERT_OK(fs_->WriteFile(unrelated_file, "unrelated", /*overwrite=*/false));
    std::string malformed_file =
        PathUtil::JoinPath(checkpoint_dir, prefix + "invalid-99.index.ckpt");
    ASSERT_OK(fs_->WriteFile(malformed_file, "malformed", /*overwrite=*/false));

    ASSERT_OK_AND_ASSIGN(checkpoint_path_factory,
                         CreateCheckpointPathFactory(test_root, "lumina", "f0", Range(10, 20)));
    file_manager = std::make_shared<GlobalIndexFileManager>(
        fs_, path_factory->CreateGlobalIndexFileFactory(), std::move(checkpoint_path_factory));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<InputStream> latest_input,
                         file_manager->OpenCheckpointInputStream());
    char buffer[6];
    ASSERT_OK_AND_ASSIGN(int64_t read_bytes, latest_input->Read(buffer, sizeof(buffer)));
    ASSERT_EQ(read_bytes, 6);
    ASSERT_EQ(std::string(buffer, sizeof(buffer)), "id-ten");
    ASSERT_OK(latest_input->Close());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> next_output,
                         file_manager->CreateCheckpointOutputStream());
    ASSERT_OK_AND_ASSIGN(std::string next_path, next_output->GetUri());
    ASSERT_TRUE(StringUtils::EndsWith(next_path, "-11.index.ckpt"));
    ASSERT_OK(next_output->Close());

    ASSERT_OK(file_manager->DeleteCheckpoint());
    ASSERT_OK_AND_ASSIGN(checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_FALSE(checkpoint_exists);
    ASSERT_OK_AND_ASSIGN(bool unrelated_exists, fs_->Exists(unrelated_file));
    ASSERT_TRUE(unrelated_exists);
    ASSERT_OK_AND_ASSIGN(bool malformed_exists, fs_->Exists(malformed_file));
    ASSERT_TRUE(malformed_exists);
}

TEST_F(LuminaGlobalIndexTest, TestResumeFromCheckpoint) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> path_factory,
                         CreateFileStorePathFactory(test_root));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CountingCheckpointFileManager> file_manager,
                         CreateCountingCheckpointFileManager(path_factory, Range(0, 3)));

    std::map<std::string, std::string> checkpoint_options = options_;
    checkpoint_options["lumina.extension.build.ckpt.threshold"] = "1";
    checkpoint_options["lumina.extension.build.ckpt.count"] = "1";
    std::vector<float> vectors = {
        0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 1.0f, 0.0f, 1.0f,
        1.0f, 0.0f, 1.0f, 0.0f, 1.0f, 1.0f, 1.0f, 1.0f,
    };
    std::vector<::lumina::core::vector_id_t> ids = {0, 1, 2, 3};

    {
        ::lumina::api::BuilderOptions builder_options;
        builder_options.Set(::lumina::core::kIndexType, ::lumina::core::kIndexTypeBruteforce)
            .Set(::lumina::core::kDimension, static_cast<int64_t>(4))
            .Set(::lumina::core::kDistanceMetric, ::lumina::core::kDistanceL2)
            .Set(::lumina::core::kEncodingType, ::lumina::core::kEncodingRawf32)
            .Set(::lumina::core::kExtensionCkptThreshold, static_cast<int64_t>(1))
            .Set(::lumina::core::kExtensionCkptCount, static_cast<int64_t>(1));
        LuminaMemoryPool lumina_pool(pool_);
        ::lumina::core::MemoryResourceConfig memory_resource(&lumina_pool);
        auto builder_result =
            ::lumina::api::LuminaBuilder::Create(builder_options, memory_resource);
        ASSERT_TRUE(builder_result.IsOk()) << builder_result.GetStatus().Message();
        ::lumina::api::LuminaBuilder builder = std::move(builder_result).TakeValue();
        ::lumina::extensions::experimental::BuildWithCheckpointExtension checkpoint_extension;
        ASSERT_TRUE(builder.Attach(checkpoint_extension).IsOk());
        ASSERT_TRUE(checkpoint_extension
                        .LoadCkptManager(std::make_unique<LuminaCheckpointManager>(file_manager))
                        .IsOk());
        TestLuminaDataset pretrain_dataset(/*dimension=*/4, vectors, ids);
        ASSERT_TRUE(builder.PretrainFrom(pretrain_dataset).IsOk());
        TestLuminaDataset insert_dataset(/*dimension=*/4, vectors, ids);
        ASSERT_TRUE(builder.InsertFrom(insert_dataset).IsOk());
    }
    ASSERT_GT(file_manager->create_checkpoint_count_, 0);
    ASSERT_OK_AND_ASSIGN(bool checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_TRUE(checkpoint_exists);

    auto global_index = std::make_shared<LuminaGlobalIndex>(checkpoint_options);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexWriter> global_writer,
        global_index->CreateWriter("f0", CreateArrowSchema(data_type_).get(), file_manager, pool_));
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array_, &c_array).ok());
    ASSERT_OK(global_writer->AddBatch(&c_array, {0, 1, 2, 3}));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> result_metas, global_writer->Finish());
    ASSERT_EQ(result_metas.size(), 1);
    ASSERT_GT(file_manager->open_checkpoint_count_, 0);
    ASSERT_OK_AND_ASSIGN(checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_FALSE(checkpoint_exists);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexReader> reader,
        CreateGlobalIndexReader(test_root, data_type_, checkpoint_options, result_metas[0]));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> scored_result,
                         reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                             /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr,
                             /*predicate=*/nullptr, /*distance_type=*/std::nullopt,
                             /*options=*/checkpoint_options)));
    CheckResult(scored_result, {3l, 1l, 2l, 0l}, {0.01f, 2.01f, 2.21f, 4.21f});
}

TEST_F(LuminaGlobalIndexTest, TestDiscardInvalidCheckpointAndRebuild) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> path_factory,
                         CreateFileStorePathFactory(test_root));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CountingCheckpointFileManager> file_manager,
                         CreateCountingCheckpointFileManager(path_factory, Range(0, 3)));

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<OutputStream> invalid_checkpoint,
                         file_manager->CreateCheckpointOutputStream());
    ASSERT_OK_AND_ASSIGN(int64_t written, invalid_checkpoint->Write("invalid checkpoint", 18));
    ASSERT_EQ(written, 18);
    ASSERT_OK(invalid_checkpoint->Close());

    std::map<std::string, std::string> checkpoint_options = options_;
    checkpoint_options["lumina.extension.build.ckpt.threshold"] = "1";
    checkpoint_options["lumina.extension.build.ckpt.count"] = "1";
    auto global_index = std::make_shared<LuminaGlobalIndex>(checkpoint_options);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexWriter> global_writer,
        global_index->CreateWriter("f0", CreateArrowSchema(data_type_).get(), file_manager, pool_));
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array_, &c_array).ok());
    ASSERT_OK(global_writer->AddBatch(&c_array, {0, 1, 2, 3}));
    ASSERT_OK_AND_ASSIGN(std::vector<GlobalIndexIOMeta> result_metas, global_writer->Finish());
    ASSERT_EQ(result_metas.size(), 1);
    ASSERT_GT(file_manager->open_checkpoint_count_, 0);
    ASSERT_OK_AND_ASSIGN(bool checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_FALSE(checkpoint_exists);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexReader> reader,
        CreateGlobalIndexReader(test_root, data_type_, checkpoint_options, result_metas[0]));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> scored_result,
                         reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                             /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr,
                             /*predicate=*/nullptr, /*distance_type=*/std::nullopt,
                             /*options=*/checkpoint_options)));
    CheckResult(scored_result, {3l, 1l, 2l, 0l}, {0.01f, 2.01f, 2.21f, 4.21f});
}

TEST_F(LuminaGlobalIndexTest, TestBuildFailureDiscardsCheckpointAndRetries) {
    class DeletionCountingCheckpointFileManager : public CountingCheckpointFileManager {
     public:
        using CountingCheckpointFileManager::CountingCheckpointFileManager;

        Status DeleteCheckpoint() const override {
            delete_count_++;
            return CountingCheckpointFileManager::DeleteCheckpoint();
        }

        mutable int32_t delete_count_ = 0;
    };

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileStorePathFactory> path_factory,
                         CreateFileStorePathFactory(dir->Str()));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<IndexCheckpointPathFactory> checkpoint_path_factory,
                         path_factory->CreateGlobalIndexCheckpointPathFactory(
                             "lumina", "f0", Range(0, 3), "task-1"));
    auto file_manager = std::make_shared<DeletionCountingCheckpointFileManager>(
        fs_, path_factory->CreateGlobalIndexFileFactory(), std::move(checkpoint_path_factory));

    ::lumina::api::BuilderOptions builder_options;
    builder_options.Set(::lumina::core::kIndexType, ::lumina::core::kIndexTypeBruteforce)
        .Set(::lumina::core::kDimension, static_cast<int64_t>(4))
        .Set(::lumina::core::kDistanceMetric, ::lumina::core::kDistanceL2)
        .Set(::lumina::core::kEncodingType, ::lumina::core::kEncodingRawf32)
        .Set(::lumina::core::kExtensionCkptThreshold, static_cast<int64_t>(1))
        .Set(::lumina::core::kExtensionCkptCount, static_cast<int64_t>(1));
    LuminaMemoryPool lumina_pool(pool_);
    ::lumina::core::MemoryResourceConfig memory_resource(&lumina_pool);
    auto builder_result = ::lumina::api::LuminaBuilder::Create(builder_options, memory_resource);
    ASSERT_TRUE(builder_result.IsOk()) << builder_result.GetStatus().Message();
    {
        ::lumina::api::LuminaBuilder builder = std::move(builder_result).TakeValue();
        ::lumina::extensions::experimental::BuildWithCheckpointExtension checkpoint_extension;
        ASSERT_TRUE(builder.Attach(checkpoint_extension).IsOk());
        ASSERT_TRUE(checkpoint_extension
                        .LoadCkptManager(std::make_unique<LuminaCheckpointManager>(file_manager))
                        .IsOk());
        std::vector<float> vectors = {
            0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 1.0f, 0.0f, 1.0f,
            1.0f, 0.0f, 1.0f, 0.0f, 1.0f, 1.0f, 1.0f, 1.0f,
        };
        std::vector<::lumina::core::vector_id_t> ids = {0, 1, 2, 3};
        TestLuminaDataset pretrain_dataset(/*dimension=*/4, vectors, ids);
        ASSERT_TRUE(builder.PretrainFrom(pretrain_dataset).IsOk());
        TestLuminaDataset insert_dataset(/*dimension=*/4, vectors, ids);
        ASSERT_TRUE(builder.InsertFrom(insert_dataset).IsOk());
    }
    ASSERT_OK_AND_ASSIGN(bool checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_TRUE(checkpoint_exists);

    std::map<std::string, std::string> checkpoint_options = options_;
    checkpoint_options["lumina.extension.build.ckpt.threshold"] = "1";
    checkpoint_options["lumina.extension.build.ckpt.count"] = "1";
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexWriter> writer,
        LuminaGlobalIndex(checkpoint_options)
            .CreateWriter("f0", CreateArrowSchema(data_type_).get(), file_manager, pool_));
    // The two-row build fails after loading the checkpoint, and must be retried without it.
    file_manager->check_checkpoint_count_ = 0;
    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*array_->Slice(0, 2), &c_array).ok());
    ASSERT_OK(writer->AddBatch(&c_array, {0, 1}));
    ASSERT_NOK(writer->Finish());
    ASSERT_GT(file_manager->open_checkpoint_count_, 0);
    ASSERT_EQ(file_manager->delete_count_, 1);
    ASSERT_GE(file_manager->check_checkpoint_count_, 3);
    ASSERT_OK_AND_ASSIGN(checkpoint_exists, file_manager->CheckpointExists());
    ASSERT_FALSE(checkpoint_exists);
}

TEST_F(LuminaGlobalIndexTest, TestWriteAndReadWithTagFilter) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> tag_options = options_;
    tag_options["lumina.extension.build.ckpt.threshold"] = "1";
    tag_options["lumina.extension.build.ckpt.count"] = "1";
    tag_options["lumina.extension.build.tag.tag_schema"] =
        R"({"key_name":"color","type":"enum","value_type":"string"})";

    std::shared_ptr<arrow::DataType> tag_data_type = arrow::struct_(
        {arrow::field("f0", arrow::list(arrow::float32())), arrow::field("color", arrow::utf8())});
    std::shared_ptr<arrow::Array> tag_array =
        arrow::ipc::internal::json::ArrayFromJSON(tag_data_type,
                                                  R"([
        [[0.0, 0.0, 0.0, 0.0], "cold"],
        [[0.0, 1.0, 0.0, 1.0], "warm"],
        [[1.0, 0.0, 1.0, 0.0], "cold"],
        [[1.0, 1.0, 1.0, 1.0], "warm"]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        GlobalIndexIOMeta meta,
        WriteGlobalIndex(test_root, tag_data_type, tag_options, tag_array, Range(0, 3)));
    std::vector<BasicFileStatus> checkpoint_files;
    ASSERT_OK(fs_->ListDir(PathUtil::JoinPath(test_root, "index/checkpoint"), &checkpoint_files));
    ASSERT_TRUE(checkpoint_files.empty());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader,
                         CreateGlobalIndexReader(test_root, data_type_, tag_options, meta));

    std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
        Literal(FieldType::STRING, "warm", 4));
    {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<ScoredGlobalIndexResult> scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr, predicate,
                /*distance_type=*/std::nullopt, /*options=*/tag_options)));
        CheckResult(scored_result, {3l, 1l}, {0.01f, 2.01f});
    }
    {
        auto pre_filter = [](int64_t id) -> bool { return id < 3; };
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> filtered_scored_result,
                             reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                 /*field_name=*/"f0", /*limit=*/4, query_, pre_filter, predicate,
                                 /*distance_type=*/std::nullopt, /*options=*/tag_options)));
        CheckResult(filtered_scored_result, {1l}, {2.01f});
    }
}

TEST_F(LuminaGlobalIndexTest, TestWriteAndReadWithMixedTagPredicates) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> tag_options = options_;
    tag_options["lumina.extension.build.tag.tag_schema"] =
        R"([{"key_name":"color","type":"enum","value_type":"string"},)"
        R"({"key_name":"price","type":"range","value_type":"float"}])";

    std::shared_ptr<arrow::DataType> tag_data_type = arrow::struct_(
        {arrow::field("f0", arrow::list(arrow::float32())), arrow::field("color", arrow::utf8()),
         arrow::field("price", arrow::float32())});
    std::shared_ptr<arrow::Array> tag_array =
        arrow::ipc::internal::json::ArrayFromJSON(tag_data_type,
                                                  R"([
        [[0.0, 0.0, 0.0, 0.0], "cold", 5.0],
        [[0.0, 1.0, 0.0, 1.0], "warm", 10.0],
        [[1.0, 0.0, 1.0, 0.0], "warm", 20.0],
        [[1.0, 1.0, 1.0, 1.0], "warm", 30.0]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        GlobalIndexIOMeta meta,
        WriteGlobalIndex(test_root, tag_data_type, tag_options, tag_array, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader,
                         CreateGlobalIndexReader(test_root, data_type_, tag_options, meta));

    std::shared_ptr<Predicate> color_predicate = PredicateBuilder::Equal(
        /*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
        Literal(FieldType::STRING, "warm", 4));
    std::shared_ptr<Predicate> low_price_predicate = PredicateBuilder::LessOrEqual(
        /*field_index=*/2, /*field_name=*/"price", FieldType::FLOAT, Literal(10.0f));
    std::shared_ptr<Predicate> high_price_predicate = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/2, /*field_name=*/"price", FieldType::FLOAT, Literal(30.0f));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> price_predicate,
                         PredicateBuilder::Or({low_price_predicate, high_price_predicate}));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> predicate,
                         PredicateBuilder::And({color_predicate, price_predicate}));

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ScoredGlobalIndexResult> scored_result,
        reader->VisitVectorSearch(std::make_shared<VectorSearch>(
            /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr, predicate,
            /*distance_type=*/std::nullopt, /*options=*/tag_options)));
    CheckResult(scored_result, {3l, 1l}, {0.01f, 2.01f});
}

TEST_F(LuminaGlobalIndexTest, TestWriteAndReadWithIntegerListTagFilter) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> tag_options = options_;
    tag_options["lumina.extension.build.tag.tag_schema"] =
        R"([{"key_name":"category_ids","type":"enum","value_type":"int32"}])";

    std::shared_ptr<arrow::DataType> tag_data_type =
        arrow::struct_({arrow::field("f0", arrow::list(arrow::float32())),
                        arrow::field("category_ids", arrow::list(arrow::int32()))});
    std::shared_ptr<arrow::Array> tag_array =
        arrow::ipc::internal::json::ArrayFromJSON(tag_data_type,
                                                  R"([
        [[0.0, 0.0, 0.0, 0.0], [1, 2]],
        [[0.0, 1.0, 0.0, 1.0], [3, 8]],
        [[1.0, 0.0, 1.0, 0.0], [4, 5]],
        [[1.0, 1.0, 1.0, 1.0], [6, 9]]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        GlobalIndexIOMeta meta,
        WriteGlobalIndex(test_root, tag_data_type, tag_options, tag_array, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader,
                         CreateGlobalIndexReader(test_root, data_type_, tag_options, meta));

    std::shared_ptr<Predicate> predicate = PredicateBuilder::In(
        /*field_index=*/1, /*field_name=*/"category_ids", FieldType::INT, {Literal(8), Literal(9)});
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<ScoredGlobalIndexResult> scored_result,
        reader->VisitVectorSearch(std::make_shared<VectorSearch>(
            /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr, predicate,
            /*distance_type=*/std::nullopt, /*options=*/tag_options)));
    CheckResult(scored_result, {3l, 1l}, {0.01f, 2.01f});
}

TEST_F(LuminaGlobalIndexTest, TestWriteAndReadWithCompatibleTagArrowTypes) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> tag_options = options_;
    tag_options["lumina.extension.build.tag.tag_schema"] =
        R"([{"key_name":"tag_i8","type":"enum","value_type":"int32"},)"
        R"({"key_name":"tag_i16","type":"enum","value_type":"int32"},)"
        R"({"key_name":"tag_i32","type":"range","value_type":"int32"},)"
        R"({"key_name":"tag_i64","type":"enum","value_type":"int64"},)"
        R"({"key_name":"tag_f32","type":"range","value_type":"float"},)"
        R"({"key_name":"tag_f64","type":"enum","value_type":"double"}])";

    std::shared_ptr<arrow::DataType> tag_data_type = arrow::struct_(
        {arrow::field("f0", arrow::list(arrow::float32())), arrow::field("tag_i8", arrow::int8()),
         arrow::field("tag_i16", arrow::int16()), arrow::field("tag_i32", arrow::int32()),
         arrow::field("tag_i64", arrow::int64()), arrow::field("tag_f32", arrow::float32()),
         arrow::field("tag_f64", arrow::float64())});
    std::shared_ptr<arrow::Array> tag_array =
        arrow::ipc::internal::json::ArrayFromJSON(tag_data_type,
                                                  R"([
        [[0.0, 0.0, 0.0, 0.0], 1, 10, 100, 10000000001, 1.5, 1.25],
        [[0.0, 1.0, 0.0, 1.0], 2, 20, 200, 10000000002, 2.5, 2.25],
        [[1.0, 0.0, 1.0, 0.0], 3, 30, 300, 10000000003, 3.5, 3.25],
        [[1.0, 1.0, 1.0, 1.0], 4, 40, 400, 10000000004, 4.5, 4.25]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        GlobalIndexIOMeta meta,
        WriteGlobalIndex(test_root, tag_data_type, tag_options, tag_array, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader,
                         CreateGlobalIndexReader(test_root, data_type_, tag_options, meta));

    auto search_and_check = [&](const std::shared_ptr<Predicate>& predicate,
                                const std::vector<int64_t>& expected_ids,
                                const std::vector<float>& expected_scores) {
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<ScoredGlobalIndexResult> scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr, predicate,
                /*distance_type=*/std::nullopt, /*options=*/tag_options)));
        CheckResult(scored_result, expected_ids, expected_scores);
    };

    search_and_check(PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"tag_i8",
                                             FieldType::TINYINT, Literal(static_cast<int8_t>(2))),
                     {1l}, {2.01f});
    search_and_check(
        PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"tag_i16", FieldType::SMALLINT,
                                Literal(static_cast<int16_t>(30))),
        {2l}, {2.21f});
    search_and_check(PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"tag_i32",
                                                   FieldType::INT, Literal(250)),
                     {3l, 2l}, {0.01f, 2.21f});
    search_and_check(PredicateBuilder::Equal(
                         /*field_index=*/4, /*field_name=*/"tag_i64", FieldType::BIGINT,
                         Literal(static_cast<int64_t>(10000000003))),
                     {2l}, {2.21f});
    search_and_check(PredicateBuilder::In(
                         /*field_index=*/4, /*field_name=*/"tag_i64", FieldType::BIGINT,
                         {Literal(static_cast<int64_t>(10000000001)),
                          Literal(static_cast<int64_t>(10000000004))}),
                     {3l, 0l}, {0.01f, 4.21f});
    search_and_check(PredicateBuilder::GreaterThan(/*field_index=*/5, /*field_name=*/"tag_f32",
                                                   FieldType::FLOAT, Literal(4.0f)),
                     {3l}, {0.01f});
    search_and_check(PredicateBuilder::Equal(/*field_index=*/6, /*field_name=*/"tag_f64",
                                             FieldType::DOUBLE, Literal(3.25)),
                     {2l}, {2.21f});
    search_and_check(PredicateBuilder::In(/*field_index=*/6, /*field_name=*/"tag_f64",
                                          FieldType::DOUBLE, {Literal(2.25), Literal(4.25)}),
                     {3l, 1l}, {0.01f, 2.01f});
}

TEST_F(LuminaGlobalIndexTest, TestWriteAndReadWithTagNullAndEmptyValues) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    std::map<std::string, std::string> tag_options = options_;
    tag_options["lumina.extension.build.tag.tag_schema"] =
        R"([{"key_name":"color","type":"enum","value_type":"string"},)"
        R"({"key_name":"labels","type":"enum","value_type":"string"},)"
        R"({"key_name":"price","type":"range","value_type":"float"},)"
        R"({"key_name":"scores","type":"enum","value_type":"float"},)"
        R"({"key_name":"category","type":"enum","value_type":"int32"},)"
        R"({"key_name":"category_ids","type":"enum","value_type":"int32"}])";

    std::shared_ptr<arrow::DataType> tag_data_type = arrow::struct_(
        {arrow::field("f0", arrow::list(arrow::float32())), arrow::field("color", arrow::utf8()),
         arrow::field("labels", arrow::list(arrow::utf8())),
         arrow::field("price", arrow::float32()),
         arrow::field("scores", arrow::list(arrow::float32())),
         arrow::field("category", arrow::int32()),
         arrow::field("category_ids", arrow::list(arrow::int32()))});
    std::shared_ptr<arrow::Array> tag_array =
        arrow::ipc::internal::json::ArrayFromJSON(tag_data_type,
                                                  R"([
        [[0.0, 0.0, 0.0, 0.0], "red", ["hot"], 5.0, [0.25], 7, [1, 2]],
        [null, "red", ["vip"], 8.0, [0.8], 7, [9]],
        [[0.0, 1.0, 0.0, 1.0], null, null, null, null, null, null],
        [[1.0, 0.0, 1.0, 0.0], " ", [], 20.0, [], 0, []],
        [[1.0, 1.0, 1.0, 1.0], "blue", ["vip", null], 30.0, [null, 0.75], 3, [null, 9]]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        GlobalIndexIOMeta meta,
        WriteGlobalIndex(test_root, tag_data_type, tag_options, tag_array, Range(0, 4)));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<GlobalIndexReader> reader,
                         CreateGlobalIndexReader(test_root, data_type_, tag_options, meta));
    auto search_and_check_with_filter =
        [&](VectorSearch::PreFilter pre_filter, const std::shared_ptr<Predicate>& predicate,
            const std::vector<int64_t>& expected_ids, const std::vector<float>& expected_scores) {
            std::shared_ptr<VectorSearch> vector_search = std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/5, query_, pre_filter, predicate,
                /*distance_type=*/std::nullopt, /*options=*/tag_options);
            ASSERT_OK_AND_ASSIGN(std::shared_ptr<ScoredGlobalIndexResult> scored_result,
                                 reader->VisitVectorSearch(vector_search));
            CheckResult(scored_result, expected_ids, expected_scores);
        };
    auto search_and_check = [&](const std::shared_ptr<Predicate>& predicate,
                                const std::vector<int64_t>& expected_ids,
                                const std::vector<float>& expected_scores) {
        search_and_check_with_filter(/*pre_filter=*/nullptr, predicate, expected_ids,
                                     expected_scores);
    };
    auto search_and_check_error = [&](const std::shared_ptr<Predicate>& predicate,
                                      const std::string& expected_message) {
        ASSERT_NOK_WITH_MSG(
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/5, query_, /*filter=*/nullptr, predicate,
                /*distance_type=*/std::nullopt, /*options=*/tag_options)),
            expected_message);
    };

    search_and_check(/*predicate=*/nullptr, {4l, 2l, 3l, 0l}, {0.01f, 2.01f, 2.21f, 4.21f});
    search_and_check(
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
                                Literal(FieldType::STRING, "red", 3)),
        {0l}, {4.21f});
    search_and_check(PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"color",
                                             FieldType::STRING, Literal(FieldType::STRING, " ", 1)),
                     {3l}, {2.21f});
    search_and_check(
        PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"labels", FieldType::STRING,
                             {Literal(FieldType::STRING, "vip", 3)}),
        {4l}, {0.01f});
    search_and_check(PredicateBuilder::LessOrEqual(/*field_index=*/3, /*field_name=*/"price",
                                                   FieldType::FLOAT, Literal(10.0f)),
                     {0l}, {4.21f});
    search_and_check(PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"price",
                                                FieldType::FLOAT, Literal(10.0f)),
                     {0l}, {4.21f});
    search_and_check(PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"price",
                                                   FieldType::FLOAT, Literal(10.0f)),
                     {4l, 3l}, {0.01f, 2.21f});
    search_and_check(PredicateBuilder::In(/*field_index=*/4, /*field_name=*/"scores",
                                          FieldType::FLOAT, {Literal(0.25f), Literal(0.75f)}),
                     {4l, 0l}, {0.01f, 4.21f});
    search_and_check(PredicateBuilder::Equal(/*field_index=*/5, /*field_name=*/"category",
                                             FieldType::INT, Literal(7)),
                     {0l}, {4.21f});
    search_and_check(PredicateBuilder::In(/*field_index=*/6, /*field_name=*/"category_ids",
                                          FieldType::INT, {Literal(9)}),
                     {4l}, {0.01f});
    search_and_check(
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
                                Literal(FieldType::STRING, "green", 5)),
        {}, {});
    search_and_check_error(
        PredicateBuilder::NotIn(/*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
                                {Literal(FieldType::STRING, "red", 3)}),
        "lumina tag predicate does not support leaf function NotIn");
    search_and_check_error(
        PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
                                   Literal(FieldType::STRING, "red", 3)),
        "lumina tag predicate does not support leaf function NotEqual");
    search_and_check_error(
        PredicateBuilder::Equal(/*field_index=*/7, /*field_name=*/"unknown", FieldType::STRING,
                                Literal(FieldType::STRING, "red", 3)),
        "unknown tag key 'unknown' in label filter");
    search_and_check_error(PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"color",
                                                   FieldType::INT, Literal(1)),
                           "tag value type mismatch for key 'color'");
    search_and_check_error(
        PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"price", FieldType::STRING,
                                Literal(FieldType::STRING, "x", 1)),
        "tag value type mismatch for key 'price'");
    {
        std::shared_ptr<Predicate> predicate = PredicateBuilder::Equal(
            /*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
            Literal(FieldType::STRING, "red", 3));
        search_and_check_with_filter([](int64_t id) { return id == 0 || id == 4; }, predicate, {0l},
                                     {4.21f});
        search_and_check_with_filter([](int64_t id) { return id == 4; }, predicate, {}, {});
    }
    {
        std::shared_ptr<Predicate> red_predicate = PredicateBuilder::Equal(
            /*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
            Literal(FieldType::STRING, "red", 3));
        std::shared_ptr<Predicate> blue_predicate = PredicateBuilder::Equal(
            /*field_index=*/1, /*field_name=*/"color", FieldType::STRING,
            Literal(FieldType::STRING, "blue", 4));
        std::shared_ptr<Predicate> high_price_predicate = PredicateBuilder::GreaterOrEqual(
            /*field_index=*/3, /*field_name=*/"price", FieldType::FLOAT, Literal(30.0f));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> blue_high_price_predicate,
                             PredicateBuilder::And({blue_predicate, high_price_predicate}));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Predicate> compound_predicate,
                             PredicateBuilder::Or({red_predicate, blue_high_price_predicate}));
        search_and_check(compound_predicate, {0l, 4l}, {4.21f, 0.01f});
        search_and_check_with_filter([](int64_t id) { return id == 4; }, compound_predicate, {4l},
                                     {0.01f});
    }
}

TEST_F(LuminaGlobalIndexTest, TestTagSchemaValidation) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string index_root = test_root_dir->Str();

    std::shared_ptr<arrow::DataType> tag_data_type = arrow::struct_(
        {arrow::field("f0", arrow::list(arrow::float32())), arrow::field("color", arrow::utf8())});

    {
        std::map<std::string, std::string> tag_options = options_;
        tag_options["lumina.extension.build.tag.tag_schema"] =
            R"([{"key_name":"color","type":"range","value_type":"string"}])";
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, tag_data_type, tag_options, array_, Range(0, 3)),
            "Option extension.build.tag.tag_schema tag[0] range type does not support value_type "
            "'string'");
    }
    {
        std::map<std::string, std::string> tag_options = options_;
        tag_options["lumina.extension.build.tag.tag_schema"] =
            R"([{"key_name":"color","type":"enum","value_type":"int32"}])";
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, tag_data_type, tag_options, array_, Range(0, 3)),
            "lumina tag field color type string is not compatible with tag_schema value_type");
    }
    {
        std::shared_ptr<arrow::DataType> int64_tag_data_type =
            arrow::struct_({arrow::field("f0", arrow::list(arrow::float32())),
                            arrow::field("category", arrow::int64())});
        std::map<std::string, std::string> tag_options = options_;
        tag_options["lumina.extension.build.tag.tag_schema"] =
            R"([{"key_name":"category","type":"enum","value_type":"int32"}])";
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, int64_tag_data_type, tag_options, array_, Range(0, 3)),
            "lumina tag field category type int64 is not compatible with tag_schema value_type");
    }
    {
        std::shared_ptr<arrow::DataType> double_tag_data_type =
            arrow::struct_({arrow::field("f0", arrow::list(arrow::float32())),
                            arrow::field("price", arrow::float64())});
        std::map<std::string, std::string> tag_options = options_;
        tag_options["lumina.extension.build.tag.tag_schema"] =
            R"([{"key_name":"price","type":"range","value_type":"double"}])";
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, double_tag_data_type, tag_options, array_, Range(0, 3)),
            "Option extension.build.tag.tag_schema tag[0] range type does not support value_type "
            "'double'");
    }
    {
        std::shared_ptr<arrow::DataType> int64_tag_data_type =
            arrow::struct_({arrow::field("f0", arrow::list(arrow::float32())),
                            arrow::field("price", arrow::int64())});
        std::map<std::string, std::string> tag_options = options_;
        tag_options["lumina.extension.build.tag.tag_schema"] =
            R"([{"key_name":"price","type":"range","value_type":"int64"}])";
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, int64_tag_data_type, tag_options, array_, Range(0, 3)),
            "Option extension.build.tag.tag_schema tag[0] range type does not support value_type "
            "'int64'");
    }
}

TEST_F(LuminaGlobalIndexTest, TestGetExtraFieldNames) {
    {
        LuminaGlobalIndex global_index(options_);
        ASSERT_OK_AND_ASSIGN(std::optional<std::vector<std::string>> field_names,
                             global_index.GetExtraFieldNames());
        ASSERT_FALSE(field_names);
    }
    {
        std::map<std::string, std::string> tag_options = options_;
        tag_options["lumina.extension.build.tag.tag_schema"] =
            R"([{"key_name":"color","type":"enum","value_type":"string"},)"
            R"({"key_name":"price","type":"range","value_type":"float"},)"
            R"({"key_name":"category_ids","type":"enum","value_type":"int32"}])";
        LuminaGlobalIndex global_index(tag_options);
        ASSERT_OK_AND_ASSIGN(std::optional<std::vector<std::string>> field_names,
                             global_index.GetExtraFieldNames());
        ASSERT_TRUE(field_names);
        ASSERT_EQ(field_names.value(),
                  std::vector<std::string>({"color", "price", "category_ids"}));
    }
}

TEST_F(LuminaGlobalIndexTest, TestInvalidInputs) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string index_root = test_root_dir->Str();
    // invalid inputs in write
    {
        auto data_type = arrow::int32();
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "arrow schema must be struct type when create LuminaIndexWriter");
    }
    {
        auto data_type = arrow::struct_({arrow::field("f1", arrow::list(arrow::float32()))});
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "field f0 not exist in arrow schema when create LuminaIndexWriter");
    }
    {
        auto data_type = arrow::struct_({arrow::field("f0", arrow::float32())});
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "field type must be list[float] when create LuminaIndexWriter");
    }
    {
        auto data_type = arrow::struct_({arrow::field("f0", arrow::list(arrow::float64()))});
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type, options_, array_, Range(0, 3)),
                            "field type must be list[float] when create LuminaIndexWriter");
    }
    {
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                        R"([
               [[0.0, 0.0, 0.0, 0.0]],
               null
            ])")
                                                  .ValueOrDie();
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type_, options_, array, Range(0, 2)),
                            "arrow_array in LuminaIndexWriter is invalid, must not null");
    }
    {
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                        R"([
               [[0.0, 0.0, 0.0, 0.0]],
               [[0.0, 1.0, 0.0, null]]
            ])")
                                                  .ValueOrDie();
        ASSERT_NOK_WITH_MSG(WriteGlobalIndex(index_root, data_type_, options_, array, Range(0, 2)),
                            "field value array in LuminaIndexWriter is invalid, must not null");
    }
    {
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                        R"([
               [[0.0, 0.0, 0.0, 0.0]],
               [[0.0, 1.0, 0.0]]
            ])")
                                                  .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, data_type_, options_, array, Range(0, 2)),
            "invalid input array in LuminaIndexWriter, vector at row [1] has length [3], "
            "expected dimension [4]");
    }
    {
        std::shared_ptr<arrow::Array> array = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                        R"([
               [[0.0, 0.0, 0.0]],
               [[0.0, 1.0, 0.0, 1.0, 0.0]]
            ])")
                                                  .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            WriteGlobalIndex(index_root, data_type_, options_, array, Range(0, 2)),
            "invalid input array in LuminaIndexWriter, vector at row [0] has length [3], "
            "expected dimension [4]");
    }

    {
        // invalid inputs in read
        auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(test_root_dir);
        std::string index_root = test_root_dir->Str();
        ASSERT_OK_AND_ASSIGN(
            auto meta, WriteGlobalIndex(index_root, data_type_, options_, array_, Range(0, 3)));
        // read
        {
            auto fake_meta = meta;
            fake_meta.metadata = nullptr;
            ASSERT_NOK_WITH_MSG(
                CreateGlobalIndexReader(index_root, data_type_, options_, /*meta=*/fake_meta),
                "Lumina global index must have meta data");
        }
        {
            auto fake_meta = meta;
            auto fake_index_meta_json = StringUtils::Replace(
                std::string(fake_meta.metadata->data(), fake_meta.metadata->size()),
                /*search_string=*/"l2", /*replacement=*/"unknown");
            fake_meta.metadata = std::make_shared<Bytes>(fake_index_meta_json, pool_.get());
            ASSERT_NOK_WITH_MSG(
                CreateGlobalIndexReader(index_root, data_type_, options_, fake_meta),
                "invalid distance type unknown for lumina");
        }
        {
            auto global_index = std::make_shared<LuminaGlobalIndex>(options_);
            auto path_factory = std::make_shared<FakeIndexPathFactory>(index_root);
            auto file_reader = std::make_shared<GlobalIndexFileManager>(
                fs_, path_factory, /*checkpoint_path_factory=*/nullptr);

            ASSERT_NOK_WITH_MSG(global_index->CreateReader(CreateArrowSchema(data_type_).get(),
                                                           file_reader, {meta, meta}, pool_),
                                "lumina index only has one index file per shard");
        }
        {
            auto data_type = arrow::struct_({arrow::field("f0", arrow::list(arrow::float32())),
                                             arrow::field("f1", arrow::list(arrow::float32()))});
            ASSERT_NOK_WITH_MSG(CreateGlobalIndexReader(index_root, data_type, options_, meta),
                                "LuminaGlobalIndex now only support one field");
        }
        {
            auto data_type = arrow::struct_({arrow::field("f0", arrow::float32())});
            ASSERT_NOK_WITH_MSG(CreateGlobalIndexReader(index_root, data_type, options_, meta),
                                "field type must be list[float] when create LuminaIndexReader");
        }
        {
            auto data_type = arrow::struct_({arrow::field("f0", arrow::list(arrow::float64()))});
            ASSERT_NOK_WITH_MSG(CreateGlobalIndexReader(index_root, data_type, options_, meta),
                                "field type must be list[float] when create LuminaIndexReader");
        }
        {
            auto fake_meta = meta;
            fake_meta.file_path = "non-exist-file";
            ASSERT_NOK_WITH_MSG(
                CreateGlobalIndexReader(index_root, data_type_, options_, fake_meta),
                "non-exist-file\' not exists");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query_, /*filter=*/nullptr,
                                    PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f0",
                                                            FieldType::INT, Literal(5)),
                                    /*distance_type=*/std::nullopt,
                                    /*options=*/std::map<std::string, std::string>())),
                                "lumina index was not built with tag");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query_, /*filter=*/nullptr,
                                    /*predicate=*/nullptr,
                                    /*distance_type=*/VectorSearch::DistanceType::COSINE,
                                    /*options=*/std::map<std::string, std::string>())),
                                "distance type for index and search not match");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            auto query = query_;
            query.push_back(1.0f);
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query, /*filter=*/nullptr,
                                    /*predicate=*/nullptr,
                                    /*distance_type=*/std::nullopt,
                                    /*options=*/std::map<std::string, std::string>())),
                                "dimension for index and search not match");
        }
        {
            ASSERT_OK_AND_ASSIGN(auto reader,
                                 CreateGlobalIndexReader(index_root, data_type_, options_, meta));
            auto fake_options = options_;
            fake_options["lumina.index.type"] = "diskann";
            ASSERT_NOK_WITH_MSG(reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                                    "f1",
                                    /*limit=*/2, query_, /*filter=*/nullptr,
                                    /*predicate=*/nullptr,
                                    /*distance_type=*/std::nullopt,
                                    /*options=*/fake_options)),
                                "index type for index and search not match");
        }
    }
}

TEST_F(LuminaGlobalIndexTest, TestHighCardinalityAndMultiThreadSearch) {
    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    auto array = CreateRandomVector(/*element_size*/ 10000, /*dimension=*/4);
    ASSERT_OK_AND_ASSIGN(
        auto meta, WriteGlobalIndex(test_root, data_type_, options_, array, Range(0, 10000 - 1)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));

    auto search_with_filter = [&]() {
        int32_t limit = paimon::test::RandomNumber(1, 100);
        auto filter = [](int64_t id) -> bool { return id % 2; };
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                "f0", limit, query_, filter,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        auto typed_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(scored_result);
        ASSERT_TRUE(typed_result);
        ASSERT_EQ(typed_result->bitmap_.Cardinality(), limit);
    };

    auto search = [&]() {
        int32_t limit = paimon::test::RandomNumber(1, 100);
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                "f0", limit, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        auto typed_result = std::dynamic_pointer_cast<BitmapScoredGlobalIndexResult>(scored_result);
        ASSERT_TRUE(typed_result);
        ASSERT_EQ(typed_result->bitmap_.Cardinality(), limit);
    };

    std::vector<std::thread> threads;
    for (int32_t i = 0; i < 5; ++i) {
        threads.emplace_back(search);
    }
    for (int32_t i = 0; i < 5; ++i) {
        threads.emplace_back(search_with_filter);
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

TEST_F(LuminaGlobalIndexTest, TestWriteWithNullRows) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    // Array with null at row 1 (middle): rows 0,2,3 are valid, row 1 is null
    // This should split into two segments: [0,0] and [2,3]
    std::shared_ptr<arrow::Array> array_with_null =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                  R"([
        [[0.0, 0.0, 0.0, 0.0]],
        [null],
        [[1.0, 0.0, 1.0, 0.0]],
        [[1.0, 1.0, 1.0, 1.0]]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        auto meta, WriteGlobalIndex(test_root, data_type_, options_, array_with_null, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));
    {
        // Search should return ids 0, 2, 3 (skipping null row 1)
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        // Only 3 vectors indexed (row 1 is null), so limit=4 returns 3
        CheckResult(scored_result, {3l, 2l, 0l}, {0.01f, 2.21f, 4.21f});
    }
}

TEST_F(LuminaGlobalIndexTest, TestWriteWithMultipleNullSegments) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    // Nulls at rows 0, 2, 5: valid rows are 1, 3, 4
    // Splits into segments: [1,1], [3,4]
    std::shared_ptr<arrow::Array> array_with_nulls =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                  R"([
        [null],
        [[0.0, 1.0, 0.0, 1.0]],
        [null],
        [[1.0, 0.0, 1.0, 0.0]],
        [[1.0, 1.0, 1.0, 1.0]],
        [null]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto meta, WriteGlobalIndex(test_root, data_type_, options_,
                                                     array_with_nulls, Range(0, 5)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));
    {
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        // Only 3 vectors indexed at ids 1, 3, 4
        CheckResult(scored_result, {4l, 1l, 3l}, {0.01f, 2.01f, 2.21f});
    }
}

TEST_F(LuminaGlobalIndexTest, TestWriteWithAllNullRows) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    // All rows are null — no vectors to index
    std::shared_ptr<arrow::Array> all_null_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                  R"([
        [null],
        [null],
        [null]
    ])")
            .ValueOrDie();

    auto global_index = std::make_shared<LuminaGlobalIndex>(options_);
    auto path_factory = std::make_shared<FakeIndexPathFactory>(test_root);
    auto file_writer = std::make_shared<GlobalIndexFileManager>(
        fs_, path_factory, /*checkpoint_path_factory=*/nullptr);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexWriter> global_writer,
        global_index->CreateWriter("f0", CreateArrowSchema(data_type_).get(), file_writer, pool_));

    ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*all_null_array, &c_array).ok());
    std::vector<int64_t> row_ids = {0, 1, 2};
    ASSERT_OK(global_writer->AddBatch(&c_array, std::move(row_ids)));
    // Finish with zero indexed vectors — returns empty metas
    ASSERT_OK_AND_ASSIGN(auto result_metas, global_writer->Finish());
    ASSERT_TRUE(result_metas.empty());
}

TEST_F(LuminaGlobalIndexTest, TestWriteWithNullAndFilter) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    // Null at row 2: valid rows are 0, 1, 3
    std::shared_ptr<arrow::Array> array_with_null =
        arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                  R"([
        [[0.0, 0.0, 0.0, 0.0]],
        [[0.0, 1.0, 0.0, 1.0]],
        [null],
        [[1.0, 1.0, 1.0, 1.0]]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(
        auto meta, WriteGlobalIndex(test_root, data_type_, options_, array_with_null, Range(0, 3)));
    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, meta));
    {
        // Filter: only allow ids < 3 (filters out id=3), so only ids 0, 1 remain
        auto filter = [](int64_t id) -> bool { return id < 3; };
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/4, query_, filter,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        CheckResult(scored_result, {1l, 0l}, {2.01f, 4.21f});
    }
}

TEST_F(LuminaGlobalIndexTest, TestWriteWithNullAcrossMultipleBatches) {
    auto test_root_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_root_dir);
    std::string test_root = test_root_dir->Str();

    // Batch 1: rows 0-2, null at row 1 → indexed ids: {0, 2}
    std::shared_ptr<arrow::Array> batch1 = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                     R"([
        [[0.0, 0.0, 0.0, 0.0]],
        [null],
        [[1.0, 0.0, 1.0, 0.0]]
    ])")
                                               .ValueOrDie();

    // Batch 2: rows 3-5, null at row 3 → indexed ids: {4, 5}
    std::shared_ptr<arrow::Array> batch2 = arrow::ipc::internal::json::ArrayFromJSON(data_type_,
                                                                                     R"([
        [null],
        [[1.0, 1.0, 1.0, 1.0]],
        [[0.0, 1.0, 0.0, 1.0]]
    ])")
                                               .ValueOrDie();

    auto global_index = std::make_shared<LuminaGlobalIndex>(options_);
    auto path_factory = std::make_shared<FakeIndexPathFactory>(test_root);
    auto file_writer = std::make_shared<GlobalIndexFileManager>(
        fs_, path_factory, /*checkpoint_path_factory=*/nullptr);

    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<GlobalIndexWriter> global_writer,
        global_index->CreateWriter("f0", CreateArrowSchema(data_type_).get(), file_writer, pool_));

    // AddBatch 1: row_ids {0, 1, 2}
    {
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*batch1, &c_array).ok());
        std::vector<int64_t> row_ids = {0, 1, 2};
        ASSERT_OK(global_writer->AddBatch(&c_array, std::move(row_ids)));
    }
    // AddBatch 2: row_ids {3, 4, 5}
    {
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportArray(*batch2, &c_array).ok());
        std::vector<int64_t> row_ids = {3, 4, 5};
        ASSERT_OK(global_writer->AddBatch(&c_array, std::move(row_ids)));
    }

    ASSERT_OK_AND_ASSIGN(auto result_metas, global_writer->Finish());
    ASSERT_EQ(result_metas.size(), 1);

    ASSERT_OK_AND_ASSIGN(auto reader,
                         CreateGlobalIndexReader(test_root, data_type_, options_, result_metas[0]));
    {
        // Search all: should return ids {0, 2, 4, 5}, never {1, 3}
        ASSERT_OK_AND_ASSIGN(
            auto scored_result,
            reader->VisitVectorSearch(std::make_shared<VectorSearch>(
                /*field_name=*/"f0", /*limit=*/10, query_, /*filter=*/nullptr,
                /*predicate=*/nullptr, /*distance_type=*/std::nullopt, /*options=*/options_)));
        // id 0: [0,0,0,0] → L2 dist to [1,1,1,1.1] = 4.21
        // id 2: [1,0,1,0] → L2 dist = 2.21
        // id 4: [1,1,1,1] → L2 dist = 0.01
        // id 5: [0,1,0,1] → L2 dist = 2.01
        CheckResult(scored_result, {4l, 5l, 2l, 0l}, {0.01f, 2.01f, 2.21f, 4.21f});
    }
}

}  // namespace paimon::lumina::test
