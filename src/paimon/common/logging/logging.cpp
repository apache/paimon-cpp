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

#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "glog/log_severity.h"
#include "glog/logging.h"
#include "glog/raw_logging.h"

namespace paimon {

static const char* GetProgramName() {
#if defined(__APPLE__)
    return getprogname();
#elif defined(__GLIBC__)
    return program_invocation_name;
#else
    return "paimon-cpp";
#endif
}

static std::shared_ptr<Logger::LoggerCreator>& getLoggerCreator() {
    static std::shared_ptr<Logger::LoggerCreator> _loggerCreator;
    return _loggerCreator;
}

static std::shared_mutex& getRegistryLock() {
    static std::shared_mutex registryMutex;
    return registryMutex;
}

void Logger::RegisterLogger(LoggerCreator creator) {
    std::unique_lock<std::shared_mutex> lock(getRegistryLock());
    getLoggerCreator() = std::make_shared<LoggerCreator>(std::move(creator));
}

static google::LogSeverity ToGlogLevel(PaimonLogLevel level) {
    switch (level) {
        case PAIMON_LOG_LEVEL_DEBUG:
            return google::GLOG_INFO;
        case PAIMON_LOG_LEVEL_INFO:
            return google::GLOG_INFO;
        case PAIMON_LOG_LEVEL_WARN:
            return google::GLOG_WARNING;
        case PAIMON_LOG_LEVEL_ERROR:
            return google::GLOG_ERROR;
        case PAIMON_LOG_LEVEL_NONE:
        case PAIMON_LOG_LEVEL_MAX:
        default:
            return google::GLOG_INFO;
    }
}

class GlogAdaptor : public Logger {
 public:
    void LogV(PaimonLogLevel level, const char* fname, int lineno, const char* function,
              const char* fmt, ...) override {
        // RawLog__ is variadic and cannot take a va_list, so format the message first.
        char message[kMessageBufferSize] = {};
        va_list args;
        va_start(args, fmt);
        std::vsnprintf(message, sizeof(message), fmt, args);
        va_end(args);
        google::RawLog__(ToGlogLevel(level), fname, lineno, "%s", message);
    }

    bool IsLevelEnabled(PaimonLogLevel level) const override {
        return true;
    }

 private:
    // Stays below glog's 3000-byte raw log buffer, so long messages are truncated
    // instead of being replaced by glog's "Message was too long" error.
    static constexpr size_t kMessageBufferSize = 2048;
};

std::unique_ptr<Logger> Logger::GetLogger(const std::string& path) {
    // Share the registered creator under the lock and call it after unlocking, so it keeps
    // its state, stays alive if replaced, and may call RegisterLogger() without deadlocking.
    std::shared_ptr<LoggerCreator> creator;
    {
        std::shared_lock<std::shared_mutex> lock(getRegistryLock());
        creator = getLoggerCreator();
    }
    if (creator) {
        return (*creator)(path);
    }
    static std::once_flag glog_init_flag;
    std::call_once(glog_init_flag, []() {
        if (!google::IsGoogleLoggingInitialized()) {
            google::InitGoogleLogging(GetProgramName());
        }
    });
    return std::make_unique<GlogAdaptor>();
}

}  // namespace paimon
