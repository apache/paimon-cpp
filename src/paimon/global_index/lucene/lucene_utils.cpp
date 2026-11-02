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

#include <filesystem>
#include <map>
#include <mutex>
#include <system_error>

#include "cppjieba/Jieba.hpp"
namespace paimon::lucene {

Result<std::string> LuceneUtils::GetJiebaDictionaryDir() {
    const char* env_dir = std::getenv(kJiebaDictDirEnv);
    if (env_dir && *env_dir != '\0') {
        return std::string(env_dir);
    }
#ifdef JIEBA_TEST_DICT_DIR
    return std::string(JIEBA_TEST_DICT_DIR);
#endif
    return Status::Invalid(
        fmt::format("cannot get dictionary dir for jieba, must set env {}", kJiebaDictDirEnv));
}

Result<std::shared_ptr<const cppjieba::Jieba>> LuceneUtils::GetJieba() {
    PAIMON_ASSIGN_OR_RAISE(std::string configured_dir, GetJiebaDictionaryDir());
    std::error_code ec;
    std::string dictionary_dir = std::filesystem::canonical(configured_dir, ec).string();
    if (ec) {
        return Status::IOError(fmt::format("cannot resolve dictionary dir {} for jieba: {}",
                                           configured_dir, ec.message()));
    }
    static std::mutex mutex;
    static std::map<std::string, std::shared_ptr<const cppjieba::Jieba>> dir_to_jieba;
    // The lock is held while loading, so concurrent callers wait for one load of a directory
    // instead of each loading it.
    std::lock_guard<std::mutex> lock(mutex);
    std::shared_ptr<const cppjieba::Jieba>& jieba = dir_to_jieba[dictionary_dir];
    if (!jieba) {
        jieba = std::make_shared<cppjieba::Jieba>(
            dictionary_dir + "/jieba.dict.utf8", dictionary_dir + "/hmm_model.utf8",
            dictionary_dir + "/user.dict.utf8", dictionary_dir + "/idf.utf8",
            dictionary_dir + "/stop_words.utf8");
    }
    return jieba;
}

}  // namespace paimon::lucene
