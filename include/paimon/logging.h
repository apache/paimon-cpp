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

#pragma once

#include <functional>
#include <memory>
#include <string>

#include "paimon/visibility.h"

namespace paimon {

#define PAIMON_LOGGER_IMPL "logger_impl"

enum PaimonLogLevel {
    PAIMON_LOG_LEVEL_DEBUG = 0,
    PAIMON_LOG_LEVEL_INFO = 1,
    PAIMON_LOG_LEVEL_WARN = 2,
    PAIMON_LOG_LEVEL_ERROR = 3,
    PAIMON_LOG_LEVEL_NONE = 4,
    PAIMON_LOG_LEVEL_MAX = 5
};

#define PAIMON_LOG_V(level, logger, format, ...)                                             \
    do {                                                                                     \
        if (logger->IsLevelEnabled(PAIMON_LOG_LEVEL_##level)) {                              \
            logger->LogV(PAIMON_LOG_LEVEL_##level, __FILE__, __LINE__, __FUNCTION__, format, \
                         __VA_ARGS__);                                                       \
        }                                                                                    \
    } while (0)

#define PAIMON_LOG_INFO(logger, fmt, ...) PAIMON_LOG_V(INFO, logger, fmt, __VA_ARGS__)
#define PAIMON_LOG_ERROR(logger, fmt, ...) PAIMON_LOG_V(ERROR, logger, fmt, __VA_ARGS__)
#define PAIMON_LOG_WARN(logger, fmt, ...) PAIMON_LOG_V(WARN, logger, fmt, __VA_ARGS__)
#define PAIMON_LOG_DEBUG(logger, fmt, ...) PAIMON_LOG_V(DEBUG, logger, fmt, __VA_ARGS__)

class PAIMON_EXPORT Logger {
 public:
    using LoggerCreator = std::function<std::unique_ptr<Logger>(const std::string&)>;

    /// Register a creator that `GetLogger()` uses instead of the default glog logger.
    ///
    /// @param creator Creates a logger from the `path` passed to `GetLogger()`. All `GetLogger()`
    /// calls share it, so it must be safe to call from multiple threads simultaneously, and its
    /// state persists across calls. It may call `RegisterLogger()`. It replaces any previously
    /// registered creator, which stays alive until running `GetLogger()` calls finish with it.
    ///
    /// @note This method is thread-safe and can be called concurrently with `GetLogger()`.
    static void RegisterLogger(LoggerCreator creator);

    /// Get a logger from the registered creator, or a glog logger if no creator is registered.
    ///
    /// @param path Name of the logger, such as the class that logs. It is passed to the
    /// registered creator, and the glog logger ignores it.
    ///
    /// @return A unique pointer to the logger.
    ///
    /// @note This method is thread-safe.
    static std::unique_ptr<Logger> GetLogger(const std::string& path);

    virtual void LogV(PaimonLogLevel level, const char* fname, int lineno, const char* function,
                      const char* fmt, ...) = 0;

    virtual bool IsLevelEnabled(PaimonLogLevel level) const = 0;

    virtual ~Logger() = default;

 protected:
    Logger() = default;
};

}  // namespace paimon
