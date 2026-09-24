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

#include <memory>

#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {
class InputStream;
class OutputStream;

/// Abstract interface for managing checkpoints belonging to one global index build identity.
class PAIMON_EXPORT GlobalIndexCheckpointFileManager {
 public:
    virtual ~GlobalIndexCheckpointFileManager() = default;

    /// Returns whether checkpoint storage is configured, without accessing storage.
    virtual bool SupportsCheckpoint() const = 0;

    /// Creates a new checkpoint file and opens it for writing.
    virtual Result<std::unique_ptr<OutputStream>> CreateCheckpointOutputStream() const = 0;

    /// Opens the matching checkpoint with the largest numeric id for reading.
    virtual Result<std::unique_ptr<InputStream>> OpenCheckpointInputStream() const = 0;

    /// Returns whether the checkpoint file exists.
    virtual Result<bool> CheckpointExists() const = 0;

    /// Deletes all matching checkpoint files. Deleting missing checkpoints succeeds.
    virtual Status DeleteCheckpoint() const = 0;
};

}  // namespace paimon
