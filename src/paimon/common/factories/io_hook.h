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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "paimon/factories/singleton.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// IOHook is for IO event capture before real IO operation happens,
/// and to simulate an IO error.
class PAIMON_EXPORT IOHook : public Singleton<IOHook> {
 public:
    IOHook();
    ~IOHook();
    IOHook(const IOHook&) = delete;
    IOHook& operator=(const IOHook&) = delete;

    // Enum class to define modes of operation for error handling.
    enum class Mode {
        RETURN_ERROR,     // Mode to return an error code on IO operation.
        THROW_EXCEPTION,  // Mode to throw an exception on IO operation.
        SILENT,           // Mode to suppress errors silently without action.
    };

    /// Reset the IO exception position and behavior mode to handle the exception.
    /// IOCount will be reset to 0.
    ///
    /// @params pos The position where the IO exception occurs.
    /// @params mode The mode of behavior for handling the exception.
    void Reset(int64_t pos, IOHook::Mode mode);
    /// Try to trigger the IO exception based on the current settings.
    ///
    /// @return Status indicating the result of the operation (success or failure).
    Status Try(const std::string& path);

    /// Get the count of IO operations that have already occurred.
    ///
    /// @return The number of IO operations executed.
    int64_t IOCount() const;

    /// Clear the state of the IOHook, including resetting IO count and
    /// any stored exception state.
    void Clear();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
