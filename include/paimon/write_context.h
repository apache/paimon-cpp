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
#include <optional>
#include <string>
#include <vector>

#include "paimon/result.h"
#include "paimon/type_fwd.h"
#include "paimon/visibility.h"

namespace paimon {
class Executor;
class MemoryPool;

/// `WriteContext` is some configuration for write operations.
///
/// Please do not use this class directly, use `WriteContextBuilder` to build a `WriteContext` which
/// has input validation.
/// @see WriteContextBuilder
class PAIMON_EXPORT WriteContext {
 public:
    WriteContext(const std::string& commit_user, bool is_streaming_mode,
                 bool ignore_num_bucket_check, bool ignore_previous_files,
                 bool enable_multi_thread_spill, const std::optional<int32_t>& write_id,
                 const std::vector<std::string>& write_schema,
                 const std::shared_ptr<MemoryPool>& memory_pool,
                 const std::shared_ptr<Executor>& executor, const std::string& temp_directory,
                 const std::shared_ptr<RealtimeContext>& realtime_context);

    ~WriteContext();

    const std::string& GetCommitUser() const {
        return commit_user_;
    }

    bool IsStreamingMode() const {
        return is_streaming_mode_;
    }

    bool IgnoreNumBucketCheck() const {
        return ignore_num_bucket_check_;
    }

    bool IgnorePreviousFiles() const {
        return ignore_previous_files_;
    }

    const std::optional<int32_t> GetWriteId() const {
        return write_id_;
    }

    const std::vector<std::string>& GetWriteSchema() const {
        return write_schema_;
    }

    std::shared_ptr<MemoryPool> GetMemoryPool() const {
        return memory_pool_;
    }

    std::shared_ptr<Executor> GetExecutor() const {
        return executor_;
    }

    const std::string& GetTempDirectory() const {
        return temp_directory_;
    }

    bool EnableMultiThreadSpill() const {
        return enable_multi_thread_spill_;
    }

    /// Returns the configured real-time context, or `nullptr` when real-time writing is disabled.
    std::shared_ptr<RealtimeContext> GetRealtimeContext() const {
        return realtime_context_;
    }

 private:
    std::string commit_user_;
    bool is_streaming_mode_;
    bool ignore_num_bucket_check_;
    bool ignore_previous_files_;
    bool enable_multi_thread_spill_;
    std::optional<int32_t> write_id_;
    std::vector<std::string> write_schema_;
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<Executor> executor_;
    std::string temp_directory_;
    std::shared_ptr<RealtimeContext> realtime_context_;
};

/// `WriteContextBuilder` used to build a `WriteContext`, has input validation.
class PAIMON_EXPORT WriteContextBuilder {
 public:
    explicit WriteContextBuilder(const std::string& commit_user);

    ~WriteContextBuilder();

    /// Set whether to enable streaming mode (default is false)
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithStreamingMode(bool is_streaming_mode);

    /// Set whether to skip num-bucket consistency check (default is false)
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithIgnoreNumBucketCheck(bool ignore_num_bucket_check);

    /// Set whether the write operation should ignore previously stored files. (default is false)
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithIgnorePreviousFiles(bool ignore_previous_files);

    /// Set custom memory pool for memory management.
    /// @param memory_pool The memory pool to use.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithMemoryPool(const std::shared_ptr<MemoryPool>& memory_pool);

    /// Set custom executor for task execution.
    /// @param executor The executor to use.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithExecutor(const std::shared_ptr<Executor>& executor);

    /// Set the temporary directory path for IO operations (lookup, external disk spill, and
    /// real-time spill).
    /// @param temp_dir The temporary directory path.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithTempDirectory(const std::string& temp_dir);

    /// For postpone bucket mode in pk table, `WithWriteId()` supposed to be used.
    ///
    /// Each worker must have its own unique `write_id` within a task, which is used as the prefix
    /// for its data files. This ensures that files from the same worker share the same prefix and
    /// can be consumed by the same compaction reader to preserve input order.
    ///
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithWriteId(int32_t write_id);

    /// For data evolution, user can write partial specific fields from table schema.
    /// If not set, write all fields in table.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithWriteSchema(const std::vector<std::string>& write_schema);

    /// Enables the real-time write path with the provided shared context.
    /// @param realtime_context Non-null context that owns the real-time stores.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& WithRealtimeContext(
        const std::shared_ptr<RealtimeContext>& realtime_context);

    /// Set the thread number for write buffer spill operations. (default is 0)
    /// If <= 0, threading is disabled for spill IPC read/write.
    /// If > 0, sets arrow CPU thread pool capacity for spill operations.
    /// @param thread_number The thread number to use for spill operations.
    /// @return Reference to this builder for method chaining.
    WriteContextBuilder& SetWriteBufferSpillThreadNumber(int32_t thread_number);

    /// Build and return a `WriteContext` instance with input validation.
    /// @return Result containing the constructed `WriteContext` or an error status.
    Result<std::unique_ptr<WriteContext>> Finish();

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
