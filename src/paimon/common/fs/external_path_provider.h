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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "paimon/common/utils/path_util.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// Provider for external file paths.
class ExternalPathProvider {
 public:
    static Result<std::unique_ptr<ExternalPathProvider>> Create(
        const std::vector<std::string>& external_table_paths,
        const std::string& relative_bucket_path, const std::vector<int32_t>& weights) {
        if (external_table_paths.empty()) {
            return Status::Invalid("external table paths cannot be empty");
        }
        std::vector<uint64_t> cumulative_weights;
        if (external_table_paths.size() > 1 && !weights.empty()) {
            if (weights.size() != external_table_paths.size()) {
                return Status::Invalid("external paths and weights must have the same size");
            }
            cumulative_weights.reserve(weights.size());
            uint64_t total = 0;
            for (int32_t weight : weights) {
                total += weight;
                cumulative_weights.push_back(total);
            }
        }
        return std::unique_ptr<ExternalPathProvider>(new ExternalPathProvider(
            external_table_paths, relative_bucket_path, cumulative_weights));
    }

    /// Get the next external file path.
    ///
    /// @return the next external file path
    std::string GetNextExternalDataPath(const std::string& file_name) {
        size_t position = (++position_) % external_table_paths_.size();
        if (!cumulative_weights_.empty()) {
            thread_local std::mt19937_64 generator(
                std::random_device{}());  // NOLINT(whitespace/braces)
            std::uniform_int_distribution<uint64_t> distribution(0, cumulative_weights_.back() - 1);
            position = std::upper_bound(cumulative_weights_.begin(), cumulative_weights_.end(),
                                        distribution(generator)) -
                       cumulative_weights_.begin();
        }
        return PathUtil::JoinPath(
            PathUtil::JoinPath(external_table_paths_[position], relative_bucket_path_), file_name);
    }

 private:
    ExternalPathProvider(const std::vector<std::string>& external_table_paths,
                         const std::string& relative_bucket_path,
                         const std::vector<uint64_t>& cumulative_weights)
        : external_table_paths_(external_table_paths),
          relative_bucket_path_(relative_bucket_path),
          cumulative_weights_(cumulative_weights) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(0, external_table_paths_.size() - 1);
        position_ = dist(gen);
    }

 private:
    std::vector<std::string> external_table_paths_;
    std::string relative_bucket_path_;
    std::atomic<size_t> position_;
    std::vector<uint64_t> cumulative_weights_;
};
}  // namespace paimon
