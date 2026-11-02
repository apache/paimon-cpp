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
#include "paimon/logging.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "glog/log_severity.h"
#include "glog/logging.h"
#include "paimon/common/executor/future.h"
#include "paimon/executor.h"
#include "paimon/fs/file_system.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
namespace {

std::atomic<int> g_creator_calls{0};

std::string CaptureInfoLogs(const std::function<void()>& log) {
    const int32_t prev_stderrthreshold = FLAGS_stderrthreshold;
    FLAGS_stderrthreshold = google::GLOG_INFO;
    ::testing::internal::CaptureStderr();
    log();
    std::string output = ::testing::internal::GetCapturedStderr();
    FLAGS_stderrthreshold = prev_stderrthreshold;
    return output;
}

void RegisterCustomLoggerCreatorAndExit() {
    auto default_logger = Logger::GetLogger("default_path");
    const Logger* expected_logger = default_logger.get();
    g_creator_calls.store(0);
    Logger::RegisterLogger([&default_logger](const std::string&) -> std::unique_ptr<Logger> {
        g_creator_calls.fetch_add(1);
        return std::move(default_logger);
    });

    auto logger = Logger::GetLogger("custom_path");
    const bool used_creator = logger && logger.get() == expected_logger &&
                              g_creator_calls.load() == 1 &&
                              logger->IsLevelEnabled(PAIMON_LOG_LEVEL_INFO);
    std::_Exit(used_creator ? 0 : 1);
}

void RegisterStatefulLoggerCreatorAndExit() {
    g_creator_calls.store(0);
    Logger::RegisterLogger([calls = 0](const std::string&) mutable -> std::unique_ptr<Logger> {
        g_creator_calls.store(++calls);
        return nullptr;
    });
    Logger::GetLogger("first");
    Logger::GetLogger("second");
    std::_Exit(g_creator_calls.load() == 2 ? 0 : 1);
}

void RegisterLoggerCreatorWhileGettingLoggerAndExit() {
    g_creator_calls.store(0);
    std::thread getter([]() {
        while (g_creator_calls.load() == 0) {
            Logger::GetLogger("getter");
        }
    });
    Logger::RegisterLogger([](const std::string&) -> std::unique_ptr<Logger> {
        g_creator_calls.fetch_add(1);
        return nullptr;
    });
    getter.join();
    // std::exit runs ThreadSanitizer's exit check, which fails the process on a race.
    std::exit(0);
}

void RegisterLoggerFromLoggerCreatorAndExit() {
    static std::weak_ptr<int> creator_state;
    static bool kept_alive = false;
    auto state = std::make_shared<int>(0);
    creator_state = state;
    Logger::RegisterLogger([state](const std::string&) -> std::unique_ptr<Logger> {
        Logger::RegisterLogger(
            [](const std::string&) -> std::unique_ptr<Logger> { return nullptr; });
        kept_alive = !creator_state.expired();
        return nullptr;
    });
    state.reset();
    std::future<void> getter =
        std::async(std::launch::async, []() { Logger::GetLogger("getter"); });
    if (getter.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
        std::cerr << "GetLogger() did not finish within 30 seconds" << std::endl;
        std::_Exit(1);
    }
    getter.get();
    std::_Exit(kept_alive && creator_state.expired() ? 0 : 1);
}

}  // namespace
TEST(LoggerTest, TestMultiThreadGetLogger) {
    ASSERT_OK_AND_ASSIGN(auto executor, CreateDefaultExecutor(/*thread_count=*/4));
    auto get_logger = []() {
        auto logger = Logger::GetLogger("my_log");
        ASSERT_TRUE(logger);
    };

    std::vector<std::future<void>> futures;
    for (int32_t i = 0; i < 1000; ++i) {
        futures.push_back(Via(executor.get(), get_logger));
    }
    Wait(futures);
}

TEST(LoggerTest, TestLogAllSeverities) {
    // The default logger routes every PaimonLogLevel through the severity mapping.
    auto logger = Logger::GetLogger("severity_test");
    ASSERT_TRUE(logger);

    logger->LogV(PAIMON_LOG_LEVEL_DEBUG, __FILE__, __LINE__, __FUNCTION__, "debug severity");
    logger->LogV(PAIMON_LOG_LEVEL_INFO, __FILE__, __LINE__, __FUNCTION__, "info severity");
    logger->LogV(PAIMON_LOG_LEVEL_WARN, __FILE__, __LINE__, __FUNCTION__, "warn severity");
    logger->LogV(PAIMON_LOG_LEVEL_ERROR, __FILE__, __LINE__, __FUNCTION__, "error severity");
    // NONE and MAX fall into the default branch of the mapping.
    logger->LogV(PAIMON_LOG_LEVEL_NONE, __FILE__, __LINE__, __FUNCTION__, "none severity");
    logger->LogV(PAIMON_LOG_LEVEL_MAX, __FILE__, __LINE__, __FUNCTION__, "max severity");
}

TEST(LoggerTest, TestLogFormatsArguments) {
    auto logger = Logger::GetLogger("format_test");
    ASSERT_TRUE(logger);

    const std::string output = CaptureInfoLogs(
        [&]() { PAIMON_LOG_INFO(logger, "name=%s count=%d raw=%s", "paimon", 42, "100%d"); });
    ASSERT_NE(output.find("name=paimon count=42 raw=100%d"), std::string::npos) << output;
}

TEST(LoggerTest, TestLogTruncatesLongMessage) {
    auto logger = Logger::GetLogger("truncate_test");
    ASSERT_TRUE(logger);
    constexpr size_t kMaxMessageLength = 2047;
    const std::string fitting_message = "[" + std::string(kMaxMessageLength - 2, 'a') + "]";
    const std::string long_message = "[" + std::string(4096, 'b');

    const std::string output = CaptureInfoLogs([&]() {
        PAIMON_LOG_INFO(logger, "%s", fitting_message.c_str());
        PAIMON_LOG_INFO(logger, "%s", long_message.c_str());
    });
    ASSERT_NE(output.find(fitting_message + "\n"), std::string::npos);
    ASSERT_NE(output.find(long_message.substr(0, kMaxMessageLength) + "\n"), std::string::npos);
}

// Demonstrates that glog's normal LOG() path actually writes a log file to disk.
// Note: Paimon's Logger/GlogAdaptor uses google::RawLog__, which only goes to
// stderr and never touches disk, so this test drives glog's file sink directly.
TEST(LoggerTest, TestGlogWritesLogFileToDisk) {
    // Save the global glog flags we are about to change so other tests are unaffected.
    const bool prev_logtostderr = FLAGS_logtostderr;
    const bool prev_timestamp_in_name = FLAGS_timestamp_in_logfile_name;
    const int32_t prev_minloglevel = FLAGS_minloglevel;

    // A unique, empty directory so the only file inside is the one glog creates for us.
    // The directory (and everything glog wrote into it) is removed on destruction.
    auto tmp_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(tmp_dir);
    std::shared_ptr<FileSystem> fs = tmp_dir->GetFileSystem();
    const std::string base = tmp_dir->Str() + "/paimon_demo";

    FLAGS_logtostderr = false;                // must be false, otherwise glog skips the file sink
    FLAGS_timestamp_in_logfile_name = false;  // deterministic file name (no time/pid suffix)
    FLAGS_minloglevel = google::GLOG_INFO;    // do not filter out INFO

    if (!google::IsGoogleLoggingInitialized()) {
        google::InitGoogleLogging("paimon-log-disk-test");
    }
    // Force INFO logs to a file under our unique directory. Disable the
    // "<program>.INFO" symlink glog normally creates next to the log file: it would
    // dangle during recursive deletion and break UniqueTestDirectory cleanup.
    google::SetLogDestination(google::GLOG_INFO, base.c_str());
    google::SetLogSymlink(google::GLOG_INFO, "");

    const std::string token = "PAIMON_DISK_LOG_DEMO_" + std::to_string(RandomNumber(0, 1'000'000));
    LOG(INFO) << "hello from disk logging, token=" << token;
    google::FlushLogFiles(google::GLOG_INFO);

    // Collect the content of whatever file glog created in our directory.
    std::string on_disk_path;
    std::string content;
    std::vector<BasicFileStatus> entries;
    ASSERT_OK(fs->ListDir(tmp_dir->Str(), &entries));
    for (const auto& entry : entries) {
        if (entry.IsDir()) {
            continue;
        }
        std::string file_content;
        ASSERT_OK(fs->ReadFile(entry.GetPath(), &file_content));
        if (file_content.find(token) != std::string::npos) {
            on_disk_path = entry.GetPath();
            content = std::move(file_content);
            break;
        }
    }

    // Show the real on-disk file and its raw content so it can be eyeballed in test output.
    std::cout << "\n===== on-disk glog file: " << on_disk_path << " =====\n"
              << content << "===== end of file =====\n";

    ASSERT_FALSE(on_disk_path.empty())
        << "no glog file containing the token was written to " << tmp_dir->Str();
    ASSERT_NE(content.find(token), std::string::npos);

    // Stop writing INFO logs to the directory that is deleted when tmp_dir goes out of
    // scope, then restore flags.
    google::SetLogDestination(google::GLOG_INFO, "");
    FLAGS_logtostderr = prev_logtostderr;
    FLAGS_timestamp_in_logfile_name = prev_timestamp_in_name;
    FLAGS_minloglevel = prev_minloglevel;
}

// A registered logger creator cannot be unset, so register it in a separate process
// to keep the default logger for other tests regardless of test order.
TEST(LoggerTest, TestRegisterCustomLoggerCreator) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(RegisterCustomLoggerCreatorAndExit(), ::testing::ExitedWithCode(0), "");
}

TEST(LoggerTest, TestRegisterStatefulLoggerCreator) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(RegisterStatefulLoggerCreatorAndExit(), ::testing::ExitedWithCode(0), "");
}

TEST(LoggerTest, TestRegisterLoggerCreatorWhileGettingLogger) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(RegisterLoggerCreatorWhileGettingLoggerAndExit(), ::testing::ExitedWithCode(0), "");
}

TEST(LoggerTest, TestRegisterLoggerFromLoggerCreator) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(RegisterLoggerFromLoggerCreatorAndExit(), ::testing::ExitedWithCode(0), "");
}
}  // namespace paimon::test
