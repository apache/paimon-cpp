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
#include "paimon/global_index/lucene/lucene_utils.h"

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "cppjieba/Jieba.hpp"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/fs/file_system.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lucene::test {
class LuceneUtilsTest : public ::testing::Test {
 public:
    void SetUp() override {
        const char* dict_dir_env = std::getenv(kJiebaDictDirEnv);
        if (dict_dir_env != nullptr) {
            original_dict_dir_env_ = dict_dir_env;
        }
        ASSERT_OK_AND_ASSIGN(working_dir_, PathUtil::GetWorkingDirectory());
        ASSERT_OK_AND_ASSIGN(std::string dict_dir, LuceneUtils::GetJiebaDictionaryDir());
        default_dict_dir_ = std::filesystem::canonical(dict_dir);
        test_dir_ = paimon::test::UniqueTestDirectory::Create();
        ASSERT_TRUE(test_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::current_path(working_dir_, ec);
        if (original_dict_dir_env_) {
            setenv(kJiebaDictDirEnv, original_dict_dir_env_->c_str(), /*overwrite=*/1);
        } else {
            unsetenv(kJiebaDictDirEnv);
        }
    }

    std::string CopyDictionary(const std::string& relative_dir) const {
        std::string dir = PathUtil::JoinPath(test_dir_->Str(), relative_dir);
        EXPECT_TRUE(paimon::test::TestUtil::CopyDirectory(default_dict_dir_, dir));
        return dir;
    }

 protected:
    std::optional<std::string> original_dict_dir_env_;
    std::string working_dir_;
    std::filesystem::path default_dict_dir_;
    std::unique_ptr<paimon::test::UniqueTestDirectory> test_dir_;
};

TEST_F(LuceneUtilsTest, TestGetJiebaSharesOneSegmenter) {
    std::string dict_dir = CopyDictionary("dict");
    setenv(kJiebaDictDirEnv, dict_dir.c_str(), /*overwrite=*/1);

    constexpr size_t kThreadNum = 4;
    std::vector<std::shared_ptr<const cppjieba::Jieba>> jiebas(kThreadNum);
    std::vector<std::thread> threads;
    threads.reserve(kThreadNum);
    for (size_t i = 0; i < kThreadNum; ++i) {
        threads.emplace_back([&jiebas, i]() {
            ASSERT_OK_AND_ASSIGN(jiebas[i], LuceneUtils::GetJieba());
            std::vector<std::string> words;
            jiebas[i]->CutForSearch("我爱机器学习", words);
            std::vector<std::string> expected = {"我", "爱", "机器", "学习"};
            ASSERT_EQ(words, expected);
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_TRUE(jiebas[0]);
    for (const auto& jieba : jiebas) {
        ASSERT_EQ(jieba, jiebas[0]);
    }

    std::weak_ptr<const cppjieba::Jieba> cached_jieba = jiebas[0];
    jiebas.clear();
    ASSERT_FALSE(cached_jieba.expired());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const cppjieba::Jieba> jieba, LuceneUtils::GetJieba());
    ASSERT_EQ(jieba, cached_jieba.lock());
}

TEST_F(LuceneUtilsTest, TestGetJiebaResolvesRelativeDictionaryDir) {
    std::string relative_dir = default_dict_dir_.filename().string();
    setenv(kJiebaDictDirEnv, relative_dir.c_str(), /*overwrite=*/1);
    std::filesystem::current_path(test_dir_->Str());
    ASSERT_NOK_WITH_MSG(LuceneUtils::GetJieba(),
                        fmt::format("cannot resolve dictionary dir {} for jieba", relative_dir));

    std::string user_dict = PathUtil::JoinPath(CopyDictionary(relative_dir), "user.dict.utf8");
    ASSERT_OK(test_dir_->GetFileSystem()->WriteFile(user_dict, "机器学习\n", /*overwrite=*/true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const cppjieba::Jieba> jieba_with_user_word,
                         LuceneUtils::GetJieba());
    std::filesystem::current_path(default_dict_dir_.parent_path());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const cppjieba::Jieba> default_jieba,
                         LuceneUtils::GetJieba());
    ASSERT_NE(jieba_with_user_word, default_jieba);

    std::vector<std::string> words;
    jieba_with_user_word->Cut("机器学习", words);
    ASSERT_EQ(words, std::vector<std::string>({"机器学习"}));
    default_jieba->Cut("机器学习", words);
    ASSERT_EQ(words, std::vector<std::string>({"机器", "学习"}));

    setenv(kJiebaDictDirEnv, default_dict_dir_.c_str(), /*overwrite=*/1);
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<const cppjieba::Jieba> jieba, LuceneUtils::GetJieba());
    ASSERT_EQ(jieba, default_jieba);
}

}  // namespace paimon::lucene::test
