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

#include <cstdint>

namespace paimon {

class ExpireConfig {
 public:
    ExpireConfig() = default;
    ExpireConfig(int32_t snapshot_retain_max, int32_t snapshot_retain_min,
                 int64_t snapshot_time_retain_ms, int32_t snapshot_max_deletes,
                 bool snapshot_clean_empty_directories)
        : snapshot_retain_max_(snapshot_retain_max),
          snapshot_retain_min_(snapshot_retain_min),
          snapshot_time_retain_ms_(snapshot_time_retain_ms),
          snapshot_max_deletes_(snapshot_max_deletes),
          snapshot_clean_empty_directories_(snapshot_clean_empty_directories) {}

    int32_t GetSnapshotRetainMin() const {
        return snapshot_retain_min_;
    }
    int32_t GetSnapshotRetainMax() const {
        return snapshot_retain_max_;
    }
    int64_t GetSnapshotTimeRetainMs() const {
        return snapshot_time_retain_ms_;
    }
    int32_t GetSnapshotMaxDeletes() const {
        return snapshot_max_deletes_;
    }
    bool CleanEmptyDirectories() const {
        return snapshot_clean_empty_directories_;
    }

 private:
    int32_t snapshot_retain_max_;
    int32_t snapshot_retain_min_;
    int64_t snapshot_time_retain_ms_;
    int32_t snapshot_max_deletes_;
    bool snapshot_clean_empty_directories_;
};

}  // namespace paimon
